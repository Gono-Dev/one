use std::{
    collections::HashSet,
    convert::Infallible,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    body::{to_bytes, Body},
    extract::OriginalUri,
    http::{
        header::{HeaderName, CONTENT_LENGTH},
        HeaderValue, Method, Request, Response, StatusCode, Uri,
    },
    response::IntoResponse,
};
use dav_server::{davpath::DavPath, DavConfig, DavHandler};
use futures_util::future::BoxFuture;
use tower::Service;
use tracing::{error, warn};
use xmltree::{Element, Namespace, XMLNode};

use crate::{
    auth::Principal,
    dav_handler::{chunked_upload, report, search},
    db,
    state::AppState,
    storage,
};

const OC_ETAG: HeaderName = HeaderName::from_static("oc-etag");
const OC_FILEID: HeaderName = HeaderName::from_static("oc-fileid");
const X_NC_OWNER_ID: HeaderName = HeaderName::from_static("x-nc-ownerid");
const X_NC_PERMISSIONS: HeaderName = HeaderName::from_static("x-nc-permissions");
const DAV_NS: &str = "DAV:";
const OC_NS: &str = "http://owncloud.org/ns";
const NC_NS: &str = "http://nextcloud.org/ns";
const PROPFIND_BODY_LIMIT: usize = 1024 * 1024;
const PROPFIND_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct NcDavService {
    handler: DavHandler,
    state: Arc<AppState>,
}

impl NcDavService {
    pub fn new(handler: DavHandler, state: Arc<AppState>) -> Self {
        Self { handler, state }
    }

    async fn dispatch(self, request: Request<Body>) -> Response<Body> {
        let original_uri = original_request_uri(&request);
        if request_target_has_fragment(&original_uri) {
            warn!(uri = %original_uri, "rejecting WebDAV request URI with fragment");
            return (
                StatusCode::BAD_REQUEST,
                "Request URI fragments are not allowed",
            )
                .into_response();
        }
        if let Err(err) = reject_parent_segments(original_uri.path()) {
            warn!(
                ?err,
                path = original_uri.path(),
                "invalid WebDAV request path"
            );
            return (StatusCode::FORBIDDEN, "Invalid WebDAV path").into_response();
        }

        match request.method().as_str() {
            "REPORT" => report::handle(self.state.clone(), request).await,
            "SEARCH" => search::handle(self.state.clone(), request).await,
            "MKCOL" | "PUT" | "MOVE" | "DELETE" if is_chunking_path(request.uri().path()) => {
                chunked_upload::handle(self.state.clone(), request).await
            }
            _ => self.forward_to_dav_server(request).await,
        }
    }

    async fn forward_to_dav_server(self, mut request: Request<Body>) -> Response<Body> {
        let method = request.method().clone();
        let original_uri = original_request_uri(&request);
        let request_path = original_uri.path().to_owned();
        let mount_prefix = mount_prefix_for_path(&request_path, &self.state.owner);
        let destination = request
            .headers()
            .get("destination")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let source_record = if matches!(method.as_str(), "DELETE" | "MOVE") {
            self.record_for_request_path(&request_path).await.ok()
        } else {
            None
        };
        if matches!(method.as_str(), "COPY" | "MOVE") {
            if let Some(destination) = destination.as_deref() {
                if let Err(err) = parse_rel_path_for_owner(destination, &self.state.owner) {
                    error!(?err, "failed to validate Destination header");
                    return (StatusCode::BAD_REQUEST, "Invalid Destination header").into_response();
                }
            }
        }
        let requested_props = if method.as_str() == "PROPFIND" {
            let (buffered_request, requested_props) = match buffer_propfind_request(request).await {
                Ok(result) => result,
                Err(response) => return response,
            };
            request = buffered_request;
            requested_props
        } else {
            None
        };
        let principal = request
            .extensions()
            .get::<Principal>()
            .map(|principal| principal.username.clone())
            .unwrap_or_else(|| "gono".to_owned());
        *request.uri_mut() = original_uri;

        let response = self
            .handler
            .handle_with(
                DavConfig::new()
                    .strip_prefix(mount_prefix.as_str())
                    .principal(principal),
                request,
            )
            .await
            .map(Body::new);

        let response = match requested_props.as_deref() {
            Some(requested_props) => {
                complete_propfind_not_found_propstats(response, requested_props).await
            }
            None => response,
        };

        self.finalize_webdav_response(response, &method, &request_path, destination, source_record)
            .await
    }

    async fn finalize_webdav_response(
        &self,
        mut response: Response<Body>,
        method: &Method,
        request_path: &str,
        destination: Option<String>,
        source_record: Option<db::FileRecord>,
    ) -> Response<Body> {
        if !response.status().is_success() {
            return response;
        }
        let status = response.status();

        if method == Method::DELETE {
            let source_rel = match parse_rel_path_for_owner(request_path, &self.state.owner) {
                Ok(path) => path,
                Err(err) => {
                    error!(?err, "failed to resolve DELETE source path");
                    return metadata_error_response();
                }
            };
            let Some(source_record) = source_record else {
                error!("missing DELETE source metadata after successful delete");
                return metadata_error_response();
            };
            return match self
                .record_change(source_record.id, &source_rel, "delete")
                .await
            {
                Ok(_) => response,
                Err(response) => response,
            };
        }

        let target_rel = match write_target_rel_path(
            method,
            request_path,
            destination.as_deref(),
            &self.state.owner,
        ) {
            Ok(Some(path)) => path,
            Ok(None) => {
                if method.as_str() == "PROPPATCH" {
                    return self.record_proppatch(response, request_path).await;
                }
                return response;
            }
            Err(err) => {
                error!(?err, "failed to resolve WebDAV write target");
                return metadata_error_response();
            }
        };

        let abs_path = match storage::safe_existing_path(&self.state.files_root, &target_rel) {
            Ok(path) => path,
            Err(err) => {
                error!(?err, "write target escaped storage root");
                return metadata_error_response();
            }
        };

        let record = match db::ensure_file_record(
            &self.state.db,
            db::FileRecordInput {
                owner: &self.state.owner,
                rel_path: &target_rel,
                abs_path: &abs_path,
                instance_id: &self.state.instance_id,
                xattr_ns: &self.state.xattr_ns,
            },
        )
        .await
        {
            Ok(record) => record,
            Err(err) => {
                error!(?err, "failed to persist WebDAV write metadata");
                return metadata_error_response();
            }
        };

        if method.as_str() == "MOVE" {
            let source_rel = match parse_rel_path_for_owner(request_path, &self.state.owner) {
                Ok(path) => path,
                Err(err) => {
                    error!(?err, "failed to resolve MOVE source path");
                    return metadata_error_response();
                }
            };
            let Some(source_record) = source_record else {
                error!("missing MOVE source metadata after successful move");
                return metadata_error_response();
            };
            if let Err(response) = self
                .record_change(source_record.id, &source_rel, "delete")
                .await
            {
                return response;
            }
        }

        let operation = match method.as_str() {
            "PUT" if status == StatusCode::CREATED => "create",
            "PUT" => "modify",
            "MKCOL" | "COPY" | "MOVE" => "create",
            _ => "modify",
        };
        if let Err(response) = self.record_change(record.id, &target_rel, operation).await {
            return response;
        }

        let headers = response.headers_mut();
        insert_header(headers, OC_ETAG, &record.etag);
        insert_header(headers, OC_FILEID, &record.oc_file_id);
        insert_header(headers, X_NC_OWNER_ID, &self.state.owner);
        insert_header(headers, X_NC_PERMISSIONS, "RGDNVW");
        response
    }

    async fn record_proppatch(
        &self,
        response: Response<Body>,
        request_path: &str,
    ) -> Response<Body> {
        let record = match self.record_for_request_path(request_path).await {
            Ok(record) => record,
            Err(err) => {
                error!(?err, "failed to resolve PROPPATCH target metadata");
                return metadata_error_response();
            }
        };
        let rel_path = match parse_rel_path_for_owner(request_path, &self.state.owner) {
            Ok(path) => path,
            Err(err) => {
                error!(?err, "failed to resolve PROPPATCH target path");
                return metadata_error_response();
            }
        };
        match self.record_change(record.id, &rel_path, "modify").await {
            Ok(_) => response,
            Err(response) => response,
        }
    }

    async fn record_for_request_path(&self, request_path: &str) -> anyhow::Result<db::FileRecord> {
        let rel_path = parse_rel_path_for_owner(request_path, &self.state.owner)?;
        let abs_path = storage::safe_existing_path(&self.state.files_root, &rel_path)?;
        db::ensure_file_record(
            &self.state.db,
            db::FileRecordInput {
                owner: &self.state.owner,
                rel_path: &rel_path,
                abs_path: &abs_path,
                instance_id: &self.state.instance_id,
                xattr_ns: &self.state.xattr_ns,
            },
        )
        .await
    }

    async fn record_change(
        &self,
        file_id: i64,
        rel_path: &Path,
        operation: &str,
    ) -> Result<i64, Response<Body>> {
        let sync_token = db::record_change(
            &self.state.db,
            &self.state.owner,
            file_id,
            rel_path,
            operation,
        )
        .await
        .map_err(|err| {
            error!(?err, "failed to record WebDAV change");
            metadata_error_response()
        })?;
        self.state.notify_file_changed(Some(file_id));
        self.state.compact_change_log().await;
        Ok(sync_token)
    }
}

impl Service<Request<Body>> for NcDavService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { Ok(this.dispatch(request).await) })
    }
}

fn is_chunking_path(path: &str) -> bool {
    path.starts_with("/uploads/")
        || path.starts_with("/remote.php/dav/uploads/")
        || path.starts_with("/remote.php/webdav/uploads/")
}

fn request_target_has_fragment(uri: &Uri) -> bool {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().contains('#'))
        .unwrap_or_else(|| uri.to_string().contains('#'))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RequestedProp {
    namespace: Option<String>,
    name: String,
}

async fn buffer_propfind_request(
    request: Request<Body>,
) -> Result<(Request<Body>, Option<Vec<RequestedProp>>), Response<Body>> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, PROPFIND_BODY_LIMIT).await.map_err(|err| {
        warn!(?err, "failed to buffer PROPFIND request body");
        (StatusCode::BAD_REQUEST, "Invalid PROPFIND request body").into_response()
    })?;
    let requested_props = requested_propfind_props(&bytes);
    Ok((
        Request::from_parts(parts, Body::from(bytes)),
        requested_props,
    ))
}

fn requested_propfind_props(body: &[u8]) -> Option<Vec<RequestedProp>> {
    if body.is_empty() {
        return None;
    }

    let root = Element::parse(Cursor::new(body)).ok()?;
    if root.name != "propfind" || root.namespace.as_deref() != Some(DAV_NS) {
        return None;
    }

    let prop = root
        .children
        .iter()
        .filter_map(XMLNode::as_element)
        .find(|element| element.name == "prop")?;
    let mut seen = HashSet::new();
    let mut props = Vec::new();
    for element in prop.children.iter().filter_map(XMLNode::as_element) {
        let requested = RequestedProp {
            namespace: element.namespace.clone(),
            name: element.name.clone(),
        };
        if seen.insert(requested.clone()) {
            props.push(requested);
        }
    }

    let needs_compat = props.iter().any(needs_missing_propstat_compat);
    (!props.is_empty() && needs_compat).then_some(props)
}

fn needs_missing_propstat_compat(prop: &RequestedProp) -> bool {
    !matches!(
        prop.namespace.as_deref(),
        Some(DAV_NS) | Some(OC_NS) | Some(NC_NS)
    )
}

async fn complete_propfind_not_found_propstats(
    response: Response<Body>,
    requested_props: &[RequestedProp],
) -> Response<Body> {
    if response.status() != StatusCode::MULTI_STATUS || requested_props.is_empty() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, PROPFIND_RESPONSE_LIMIT).await {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(?err, "failed to buffer PROPFIND response body");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid PROPFIND response body",
            )
                .into_response();
        }
    };
    let patched =
        append_missing_propstats(&bytes, requested_props).unwrap_or_else(|| bytes.to_vec());
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(patched))
}

fn append_missing_propstats(body: &[u8], requested_props: &[RequestedProp]) -> Option<Vec<u8>> {
    let mut root = Element::parse(Cursor::new(body)).ok()?;
    ensure_namespace(&mut root, "D", DAV_NS);
    let mut changed = false;

    for response in root
        .children
        .iter_mut()
        .filter_map(XMLNode::as_mut_element)
        .filter(|element| is_dav_element(element, "response"))
    {
        let present = response_present_props(response);
        let missing = requested_props
            .iter()
            .filter(|requested| !present.contains(*requested))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            continue;
        }

        response
            .children
            .push(XMLNode::Element(not_found_propstat(&missing)));
        changed = true;
    }

    if !changed {
        return None;
    }

    let mut output = Vec::new();
    root.write(&mut output).ok()?;
    Some(output)
}

fn response_present_props(response: &Element) -> HashSet<RequestedProp> {
    let mut props = HashSet::new();
    for propstat in response
        .children
        .iter()
        .filter_map(XMLNode::as_element)
        .filter(|element| is_dav_element(element, "propstat"))
    {
        for prop in propstat
            .children
            .iter()
            .filter_map(XMLNode::as_element)
            .filter(|element| is_dav_element(element, "prop"))
        {
            for element in prop.children.iter().filter_map(XMLNode::as_element) {
                props.insert(RequestedProp {
                    namespace: element.namespace.clone(),
                    name: element.name.clone(),
                });
            }
        }
    }
    props
}

fn not_found_propstat(missing: &[RequestedProp]) -> Element {
    let mut propstat = dav_element("propstat");
    let mut prop = dav_element("prop");
    for (index, requested) in missing.iter().enumerate() {
        prop.children
            .push(XMLNode::Element(requested_prop_element(requested, index)));
    }
    propstat.children.push(XMLNode::Element(prop));

    let mut status = dav_element("status");
    status
        .children
        .push(XMLNode::Text("HTTP/1.1 404 Not Found".to_owned()));
    propstat.children.push(XMLNode::Element(status));
    propstat
}

fn dav_element(name: &str) -> Element {
    let mut element = Element::new(name);
    element.prefix = Some("D".to_owned());
    element.namespace = Some(DAV_NS.to_owned());
    element
}

fn requested_prop_element(requested: &RequestedProp, index: usize) -> Element {
    let mut element = Element::new(&requested.name);
    let Some(namespace) = requested.namespace.as_deref() else {
        return element;
    };

    let prefix = match namespace {
        DAV_NS => "D".to_owned(),
        OC_NS => "oc".to_owned(),
        NC_NS => "nc".to_owned(),
        _ => format!("gono{index}"),
    };
    element.prefix = Some(prefix.clone());
    element.namespace = Some(namespace.to_owned());
    ensure_namespace(&mut element, &prefix, namespace);
    element
}

fn is_dav_element(element: &Element, name: &str) -> bool {
    element.name == name && element.namespace.as_deref() == Some(DAV_NS)
}

fn ensure_namespace(element: &mut Element, prefix: &str, namespace: &str) {
    let mut namespaces = element.namespaces.take().unwrap_or_else(Namespace::empty);
    namespaces.put(prefix, namespace);
    element.namespaces = Some(namespaces);
}

fn write_target_rel_path(
    method: &Method,
    request_path: &str,
    destination: Option<&str>,
    owner: &str,
) -> anyhow::Result<Option<PathBuf>> {
    let path = match method.as_str() {
        "PUT" | "MKCOL" => Some(request_path),
        "COPY" | "MOVE" => destination,
        "PROPPATCH" => Some(request_path),
        _ => None,
    };

    path.map(|path| parse_rel_path_for_owner(path, owner))
        .transpose()
}

pub(crate) fn original_request_uri(request: &Request<Body>) -> Uri {
    request
        .extensions()
        .get::<OriginalUri>()
        .map(|uri| uri.0.clone())
        .unwrap_or_else(|| request.uri().clone())
}

pub(crate) fn mount_prefix_for_path(path: &str, owner: &str) -> String {
    let remote_dav_files = format!("/remote.php/dav/files/{owner}");
    if has_path_prefix(path, &remote_dav_files) {
        return remote_dav_files;
    }
    if has_path_prefix(path, "/remote.php/webdav") {
        return "/remote.php/webdav".to_owned();
    }
    if has_path_prefix(path, "/remote.php/dav") {
        return "/remote.php/dav".to_owned();
    }
    String::new()
}

pub(crate) fn parse_rel_path(path_or_uri: &str) -> anyhow::Result<PathBuf> {
    parse_rel_path_for_owner(path_or_uri, "gono")
}

pub(crate) fn parse_rel_path_for_owner(path_or_uri: &str, owner: &str) -> anyhow::Result<PathBuf> {
    let path = if path_or_uri.starts_with("http://") || path_or_uri.starts_with("https://") {
        path_or_uri.parse::<Uri>()?.path().to_owned()
    } else {
        path_or_uri.to_owned()
    };
    reject_parent_segments(&path)?;
    let (path, supports_owner_files_mount) =
        if let Some(path) = path.strip_prefix("/remote.php/dav") {
            (path, true)
        } else if let Some(path) = path.strip_prefix("/remote.php/webdav") {
            (path, false)
        } else {
            (path.as_str(), false)
        };
    let owner_prefix = format!("/files/{owner}");
    let path = if supports_owner_files_mount && path == owner_prefix {
        "/"
    } else if supports_owner_files_mount {
        path.strip_prefix(&(owner_prefix + "/")).unwrap_or(path)
    } else {
        path
    };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let dav_path = DavPath::new(&path)?;
    Ok(Path::new(dav_path.as_rel_ospath()).to_path_buf())
}

fn has_path_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn reject_parent_segments(path: &str) -> anyhow::Result<()> {
    for segment in path.split('/') {
        let decoded = percent_decode_segment(segment)?;
        if decoded == b".." {
            anyhow::bail!("path contains parent segment");
        }
        if decoded.iter().any(|byte| *byte == 0 || *byte == b'/') {
            anyhow::bail!("path contains forbidden decoded byte");
        }
    }
    Ok(())
}

fn percent_decode_segment(segment: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    anyhow::bail!("invalid percent escape");
                }
                let high = hex_value(bytes[index + 1])
                    .ok_or_else(|| anyhow::anyhow!("invalid percent escape"))?;
                let low = hex_value(bytes[index + 2])
                    .ok_or_else(|| anyhow::anyhow!("invalid percent escape"))?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    Ok(decoded)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn insert_header(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn metadata_error_response() -> Response<Body> {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Failed to persist file metadata",
    )
        .into_response()
}
