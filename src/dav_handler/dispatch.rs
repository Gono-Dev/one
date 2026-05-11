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
    http::{
        header::{HeaderName, CONTENT_LENGTH},
        HeaderMap, HeaderValue, Method, Request, Response, StatusCode,
    },
    response::IntoResponse,
};
use dav_server::{DavConfig, DavHandler};
use filetime::FileTime;
use futures_util::future::BoxFuture;
use tower::Service;
use tracing::{error, warn};
use xmltree::{Element, Namespace, XMLNode};

use crate::{
    auth::Principal,
    dav_handler::{
        chunked_upload,
        fs::{permissions_string, NcLocalFs},
        pathmap::{
            mount_prefix_for_path, original_request_uri, parse_rel_path_for_owner,
            path_for_rel_path, path_for_request_style, path_part, reject_parent_segments,
            request_target_has_fragment, uri_with_replaced_path,
        },
        report, search,
    },
    db,
    locks::SqliteLs,
    permissions::{self, PermissionLevel, ScopeMatch},
    state::AppState,
    storage,
};

const OC_ETAG: HeaderName = HeaderName::from_static("oc-etag");
const OC_FILEID: HeaderName = HeaderName::from_static("oc-fileid");
const ETAG: HeaderName = HeaderName::from_static("etag");
const X_OC_MTIME: HeaderName = HeaderName::from_static("x-oc-mtime");
const X_NC_WEBDAV_AUTOMKCOL: HeaderName = HeaderName::from_static("x-nc-webdav-automkcol");
const X_NC_OWNER_ID: HeaderName = HeaderName::from_static("x-nc-ownerid");
const X_NC_PERMISSIONS: HeaderName = HeaderName::from_static("x-nc-permissions");
const DAV_NS: &str = "DAV:";
const OC_NS: &str = "http://owncloud.org/ns";
const NC_NS: &str = "http://nextcloud.org/ns";
const PROPFIND_BODY_LIMIT: usize = 1024 * 1024;
const PROPFIND_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct NcDavService {
    state: Arc<AppState>,
}

impl NcDavService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
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

        let principal = match authenticated_principal(&request) {
            Ok(principal) => principal,
            Err(response) => return response,
        };
        let owner = principal.username.clone();
        if let Err(response) = ensure_request_owner_path_matches(original_uri.path(), &owner) {
            return response;
        }
        let files_root = match self.state.ensure_files_root_for_owner(&owner).await {
            Ok(files_root) => files_root,
            Err(err) => {
                error!(?err, owner, "failed to prepare owner files root");
                return metadata_error_response();
            }
        };

        if request.method() == Method::HEAD {
            return self.handle_head(&principal, &files_root, request).await;
        }

        match request.method().as_str() {
            "REPORT" => report::handle(self.state.clone(), principal, files_root, request).await,
            "SEARCH" => search::handle(self.state.clone(), principal, files_root, request).await,
            "MKCOL" | "PUT" | "MOVE" | "DELETE" if is_chunking_path(request.uri().path()) => {
                chunked_upload::handle(self.state.clone(), principal, files_root, request).await
            }
            _ => {
                self.forward_to_dav_server(principal, files_root, request)
                    .await
            }
        }
    }

    async fn forward_to_dav_server(
        self,
        principal: Principal,
        files_root: PathBuf,
        mut request: Request<Body>,
    ) -> Response<Body> {
        let owner = principal.username.clone();
        let method = request.method().clone();
        let original_uri = original_request_uri(&request);
        let client_request_path = original_uri.path().to_owned();
        let client_rel_path = match parse_rel_path_for_owner(&client_request_path, &owner) {
            Ok(path) => path,
            Err(err) => {
                warn!(?err, path = %client_request_path, "failed to resolve WebDAV request path");
                return (StatusCode::FORBIDDEN, "Invalid WebDAV path").into_response();
            }
        };
        let scope_match = match permissions::resolve_scope_for_client_path(
            &principal,
            &client_rel_path,
        ) {
            Ok(scope_match) => scope_match,
            Err(_err)
                if method.as_str() == "PROPFIND"
                    && permissions::is_virtual_collection_path(&principal, &client_rel_path) =>
            {
                return virtual_propfind_response(
                    &principal,
                    &client_request_path,
                    request.headers(),
                    &client_rel_path,
                );
            }
            Err(err) => {
                warn!(?err, path = %client_rel_path.display(), "WebDAV path is outside app password scopes");
                return (StatusCode::FORBIDDEN, "Path is outside app password scope")
                    .into_response();
            }
        };
        if !permissions::is_method_allowed(&scope_match, &method) {
            return (StatusCode::FORBIDDEN, "App password scope is view-only").into_response();
        }
        let request_path = match path_for_request_style(
            &client_request_path,
            &owner,
            &scope_match.storage_rel_path,
        ) {
            Ok(path) => path,
            Err(err) => {
                error!(?err, "failed to rewrite WebDAV request path");
                return metadata_error_response();
            }
        };
        let request_uri = match uri_with_replaced_path(&original_uri, &request_path) {
            Ok(uri) => uri,
            Err(err) => {
                error!(?err, "failed to build rewritten WebDAV URI");
                return metadata_error_response();
            }
        };
        let mount_prefix = mount_prefix_for_path(&request_path, &owner);
        let destination = request
            .headers()
            .get("destination")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut storage_destination = None::<String>;
        let upload_mtime = if method == Method::PUT {
            match parse_x_oc_mtime(request.headers()) {
                Ok(mtime) => mtime,
                Err(response) => return response,
            }
        } else {
            None
        };
        let source_record = if matches!(method.as_str(), "DELETE" | "MOVE") {
            self.record_for_request_path(&owner, &files_root, &request_path)
                .await
                .ok()
        } else {
            None
        };
        if matches!(method.as_str(), "COPY" | "MOVE") {
            if let Some(destination) = destination.as_deref() {
                let destination_rel = match parse_rel_path_for_owner(destination, &owner) {
                    Ok(path) => path,
                    Err(err) => {
                        error!(?err, "failed to validate Destination header");
                        return (StatusCode::BAD_REQUEST, "Invalid Destination header")
                            .into_response();
                    }
                };
                let destination_scope = match permissions::resolve_scope_for_client_path(
                    &principal,
                    &destination_rel,
                ) {
                    Ok(scope_match) => scope_match,
                    Err(err) => {
                        warn!(?err, path = %destination_rel.display(), "Destination is outside app password scopes");
                        return (
                            StatusCode::FORBIDDEN,
                            "Destination is outside app password scope",
                        )
                            .into_response();
                    }
                };
                if destination_scope.scope.permission != PermissionLevel::Full
                    || scope_match.scope.permission != PermissionLevel::Full
                {
                    return (
                        StatusCode::FORBIDDEN,
                        "MOVE and COPY require full scope access",
                    )
                        .into_response();
                }
                let destination_style_path = match path_part(destination) {
                    Ok(path) => path,
                    Err(err) => {
                        error!(?err, "failed to parse Destination path");
                        return (StatusCode::BAD_REQUEST, "Invalid Destination header")
                            .into_response();
                    }
                };
                let rewritten_destination = match path_for_request_style(
                    &destination_style_path,
                    &owner,
                    &destination_scope.storage_rel_path,
                ) {
                    Ok(path) => path,
                    Err(err) => {
                        error!(?err, "failed to rewrite Destination header");
                        return metadata_error_response();
                    }
                };
                let header_value = match HeaderValue::from_str(&rewritten_destination) {
                    Ok(value) => value,
                    Err(err) => {
                        error!(?err, "rewritten Destination header is invalid");
                        return metadata_error_response();
                    }
                };
                request.headers_mut().insert("destination", header_value);
                storage_destination = Some(rewritten_destination);
            } else {
                return (StatusCode::BAD_REQUEST, "Destination header is required").into_response();
            }
        }
        if method == Method::PUT && auto_mkcol_enabled(request.headers()) {
            if let Err(response) = self
                .ensure_auto_mkcol_parents(&owner, &files_root, &request_path)
                .await
            {
                return response;
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
        *request.uri_mut() = request_uri;
        let handler = DavHandler::builder()
            .filesystem(Box::new(NcLocalFs::new(
                &files_root,
                self.state.db.clone(),
                owner.clone(),
                self.state.instance_id.clone(),
                self.state.xattr_ns.clone(),
            )))
            .locksystem(SqliteLs::for_principal(
                self.state.db.clone(),
                owner.clone(),
            ))
            .build_handler();

        let response = handler
            .handle_with(
                DavConfig::new()
                    .strip_prefix(mount_prefix.as_str())
                    .principal(owner.clone()),
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
        let response = rewrite_multistatus_hrefs(response, &principal).await;

        self.finalize_webdav_response(
            &owner,
            &files_root,
            response,
            &method,
            &request_path,
            storage_destination.or(destination),
            source_record,
            upload_mtime,
        )
        .await
    }

    async fn finalize_webdav_response(
        &self,
        owner: &str,
        files_root: &Path,
        mut response: Response<Body>,
        method: &Method,
        request_path: &str,
        destination: Option<String>,
        source_record: Option<db::FileRecord>,
        upload_mtime: Option<FileTime>,
    ) -> Response<Body> {
        if !response.status().is_success() {
            return response;
        }
        let status = response.status();

        if method == Method::DELETE {
            let source_rel = match parse_rel_path_for_owner(request_path, owner) {
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
                .record_change(owner, source_record.id, &source_rel, "delete")
                .await
            {
                Ok(_) => response,
                Err(response) => response,
            };
        }

        let target_rel =
            match write_target_rel_path(method, request_path, destination.as_deref(), owner) {
                Ok(Some(path)) => path,
                Ok(None) => {
                    if method.as_str() == "PROPPATCH" {
                        return self
                            .record_proppatch(owner, files_root, response, request_path)
                            .await;
                    }
                    return response;
                }
                Err(err) => {
                    error!(?err, "failed to resolve WebDAV write target");
                    return metadata_error_response();
                }
            };

        let abs_path = match storage::safe_existing_path(files_root, &target_rel) {
            Ok(path) => path,
            Err(err) => {
                error!(?err, "write target escaped storage root");
                return metadata_error_response();
            }
        };

        let accepted_upload_mtime = upload_mtime.is_some();
        if method == Method::PUT {
            if let Some(mtime) = upload_mtime {
                if let Err(err) = filetime::set_file_mtime(&abs_path, mtime) {
                    error!(?err, path = %abs_path.display(), "failed to set uploaded file mtime");
                    return metadata_error_response();
                }
            }
        }

        let record = match db::ensure_file_record(
            &self.state.db,
            db::FileRecordInput {
                owner,
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
            let source_rel = match parse_rel_path_for_owner(request_path, owner) {
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
                .record_change(owner, source_record.id, &source_rel, "delete")
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
        if let Err(response) = self
            .record_change(owner, record.id, &target_rel, operation)
            .await
        {
            return response;
        }

        let headers = response.headers_mut();
        let quoted_etag = format!("\"{}\"", record.etag);
        insert_header(headers, ETAG, &quoted_etag);
        insert_header(headers, OC_ETAG, &record.etag);
        insert_header(headers, OC_FILEID, &record.oc_file_id);
        insert_header(headers, X_NC_OWNER_ID, owner);
        insert_header(
            headers,
            X_NC_PERMISSIONS,
            permissions_string(record.permissions, abs_path.is_dir()),
        );
        if method == Method::PUT && accepted_upload_mtime {
            insert_header(headers, X_OC_MTIME, "accepted");
        }
        response
    }

    async fn ensure_auto_mkcol_parents(
        &self,
        owner: &str,
        files_root: &Path,
        request_path: &str,
    ) -> Result<(), Response<Body>> {
        let target_rel = parse_rel_path_for_owner(request_path, owner).map_err(|err| {
            warn!(?err, "failed to resolve auto-mkcol upload target");
            (StatusCode::BAD_REQUEST, "Invalid upload path").into_response()
        })?;
        let Some(parent) = target_rel.parent() else {
            return Ok(());
        };
        if parent.as_os_str().is_empty() {
            return Ok(());
        }

        let normalized_parent = storage::normalize_rel_path(parent).map_err(|err| {
            warn!(?err, "invalid auto-mkcol parent path");
            (StatusCode::FORBIDDEN, "Invalid upload path").into_response()
        })?;
        let mut current = PathBuf::new();
        for component in normalized_parent.components() {
            current.push(component.as_os_str());
            match storage::safe_existing_path(files_root, &current) {
                Ok(path) => {
                    let metadata = std::fs::metadata(&path).map_err(|err| {
                        error!(?err, path = %path.display(), "failed to inspect auto-mkcol parent");
                        metadata_error_response()
                    })?;
                    if !metadata.is_dir() {
                        return Err((StatusCode::CONFLICT, "Upload parent is not a directory")
                            .into_response());
                    }
                }
                Err(_) => {
                    let path = storage::safe_create_path(files_root, &current).map_err(|err| {
                        warn!(?err, rel_path = %current.display(), "invalid auto-mkcol parent");
                        (StatusCode::CONFLICT, "Invalid upload parent").into_response()
                    })?;
                    std::fs::create_dir(&path).map_err(|err| {
                        error!(?err, path = %path.display(), "failed to create auto-mkcol parent");
                        metadata_error_response()
                    })?;
                    let record = db::ensure_file_record(
                        &self.state.db,
                        db::FileRecordInput {
                            owner,
                            rel_path: &current,
                            abs_path: &path,
                            instance_id: &self.state.instance_id,
                            xattr_ns: &self.state.xattr_ns,
                        },
                    )
                    .await
                    .map_err(|err| {
                        error!(?err, rel_path = %current.display(), "failed to persist auto-mkcol parent metadata");
                        metadata_error_response()
                    })?;
                    self.record_change(owner, record.id, &current, "create")
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_head(
        &self,
        principal: &Principal,
        files_root: &Path,
        request: Request<Body>,
    ) -> Response<Body> {
        let owner = &principal.username;
        let original_uri = original_request_uri(&request);
        let client_rel_path = match parse_rel_path_for_owner(original_uri.path(), owner) {
            Ok(path) => path,
            Err(err) => {
                warn!(
                    ?err,
                    path = original_uri.path(),
                    "failed to resolve HEAD target"
                );
                return empty_response(StatusCode::FORBIDDEN);
            }
        };
        let scope_match = match permissions::resolve_scope_for_client_path(
            principal,
            &client_rel_path,
        ) {
            Ok(scope_match) => scope_match,
            Err(_err) if permissions::is_virtual_collection_path(principal, &client_rel_path) => {
                let mut response = empty_response(StatusCode::OK);
                insert_header(response.headers_mut(), CONTENT_LENGTH, "0");
                return response;
            }
            Err(err) => {
                warn!(?err, path = %client_rel_path.display(), "HEAD target is outside app password scopes");
                return empty_response(StatusCode::FORBIDDEN);
            }
        };
        let rel_path = scope_match.storage_rel_path.clone();
        let abs_path = match storage::safe_existing_path(files_root, &rel_path) {
            Ok(path) => path,
            Err(_) => return empty_response(StatusCode::NOT_FOUND),
        };
        let metadata = match std::fs::metadata(&abs_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return empty_response(StatusCode::NOT_FOUND);
            }
            Err(err) => {
                error!(?err, path = %abs_path.display(), "failed to inspect HEAD target");
                return metadata_error_response();
            }
        };
        let record = match db::ensure_file_record(
            &self.state.db,
            db::FileRecordInput {
                owner,
                rel_path: &rel_path,
                abs_path: &abs_path,
                instance_id: &self.state.instance_id,
                xattr_ns: &self.state.xattr_ns,
            },
        )
        .await
        {
            Ok(record) => record,
            Err(err) => {
                error!(?err, "failed to persist HEAD target metadata");
                return metadata_error_response();
            }
        };

        let mut response = empty_response(StatusCode::OK);
        let headers = response.headers_mut();
        let quoted_etag = format!("\"{}\"", record.etag);
        insert_header(headers, ETAG, &quoted_etag);
        insert_header(headers, OC_ETAG, &record.etag);
        insert_header(headers, OC_FILEID, &record.oc_file_id);
        insert_header(headers, X_NC_OWNER_ID, owner);
        insert_header(
            headers,
            X_NC_PERMISSIONS,
            scoped_permissions_string(&scope_match, record.permissions, metadata.is_dir()),
        );
        let content_length = if metadata.is_file() {
            metadata.len().to_string()
        } else {
            "0".to_owned()
        };
        insert_header(headers, CONTENT_LENGTH, &content_length);
        response
    }

    async fn record_proppatch(
        &self,
        owner: &str,
        files_root: &Path,
        response: Response<Body>,
        request_path: &str,
    ) -> Response<Body> {
        let record = match self
            .record_for_request_path(owner, files_root, request_path)
            .await
        {
            Ok(record) => record,
            Err(err) => {
                error!(?err, "failed to resolve PROPPATCH target metadata");
                return metadata_error_response();
            }
        };
        let rel_path = match parse_rel_path_for_owner(request_path, owner) {
            Ok(path) => path,
            Err(err) => {
                error!(?err, "failed to resolve PROPPATCH target path");
                return metadata_error_response();
            }
        };
        match self
            .record_change(owner, record.id, &rel_path, "modify")
            .await
        {
            Ok(_) => response,
            Err(response) => response,
        }
    }

    async fn record_for_request_path(
        &self,
        owner: &str,
        files_root: &Path,
        request_path: &str,
    ) -> anyhow::Result<db::FileRecord> {
        let rel_path = parse_rel_path_for_owner(request_path, owner)?;
        let abs_path = storage::safe_existing_path(files_root, &rel_path)?;
        db::ensure_file_record(
            &self.state.db,
            db::FileRecordInput {
                owner,
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
        owner: &str,
        file_id: i64,
        rel_path: &Path,
        operation: &str,
    ) -> Result<i64, Response<Body>> {
        let sync_token = db::record_change(&self.state.db, owner, file_id, rel_path, operation)
            .await
            .map_err(|err| {
                error!(?err, "failed to record WebDAV change");
                metadata_error_response()
            })?;
        self.state
            .notify_file_changed_for_owner(owner, Some(file_id));
        self.state.compact_change_log_for_owner(owner).await;
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

fn authenticated_principal(request: &Request<Body>) -> Result<Principal, Response<Body>> {
    let Some(principal) = request.extensions().get::<Principal>() else {
        return Err((StatusCode::UNAUTHORIZED, "Authentication required").into_response());
    };
    if principal.username.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "Authentication required").into_response());
    }
    Ok(principal.clone())
}

fn ensure_request_owner_path_matches(path: &str, owner: &str) -> Result<(), Response<Body>> {
    let Some(rest) = path.strip_prefix("/remote.php/dav/files") else {
        return Ok(());
    };
    if !(rest.is_empty() || rest.starts_with('/')) {
        return Ok(());
    }
    let requested_owner = rest
        .trim_start_matches('/')
        .split('/')
        .next()
        .filter(|segment| !segment.is_empty());
    if requested_owner == Some(owner) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "WebDAV owner does not match authenticated user",
        )
            .into_response())
    }
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

async fn rewrite_multistatus_hrefs(
    response: Response<Body>,
    principal: &Principal,
) -> Response<Body> {
    if response.status() != StatusCode::MULTI_STATUS {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, PROPFIND_RESPONSE_LIMIT).await {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(?err, "failed to buffer multistatus response body");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid WebDAV response body",
            )
                .into_response();
        }
    };
    let patched =
        rewrite_multistatus_hrefs_body(&bytes, principal).unwrap_or_else(|| bytes.to_vec());
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(patched))
}

fn rewrite_multistatus_hrefs_body(body: &[u8], principal: &Principal) -> Option<Vec<u8>> {
    let mut root = Element::parse(Cursor::new(body)).ok()?;
    let mut changed = false;

    root.children.retain_mut(|node| {
        let XMLNode::Element(element) = node else {
            return true;
        };
        if !is_dav_element(element, "response") {
            return true;
        }
        match rewrite_response_href(element, principal) {
            Ok(Some(())) => {
                changed = true;
                true
            }
            Ok(None) => {
                changed = true;
                false
            }
            Err(err) => {
                warn!(?err, "failed to rewrite WebDAV response href");
                true
            }
        }
    });

    if !changed {
        return None;
    }

    let mut output = Vec::new();
    root.write(&mut output).ok()?;
    Some(output)
}

fn rewrite_response_href(
    response: &mut Element,
    principal: &Principal,
) -> anyhow::Result<Option<()>> {
    let Some(href) = response_href(response) else {
        return Ok(Some(()));
    };
    let href_path = path_part(&href)?;
    let storage_rel = parse_rel_path_for_owner(&href_path, &principal.username)?;
    let Some(scope_match) = permissions::resolve_scope_for_storage_path(principal, &storage_rel)?
    else {
        return Ok(None);
    };

    let href_prefix = mount_prefix_for_path(&href_path, &principal.username);
    let client_href = path_for_rel_path(&href_prefix, &scope_match.client_rel_path)?;
    if set_response_href(response, &client_href) {
        patch_response_permissions(response, scope_match.scope.permission);
    }
    Ok(Some(()))
}

fn response_href(response: &Element) -> Option<String> {
    response
        .children
        .iter()
        .filter_map(XMLNode::as_element)
        .find(|element| is_dav_element(element, "href"))
        .and_then(element_text)
}

fn set_response_href(response: &mut Element, value: &str) -> bool {
    let Some(href) = response
        .children
        .iter_mut()
        .filter_map(XMLNode::as_mut_element)
        .find(|element| is_dav_element(element, "href"))
    else {
        return false;
    };
    set_element_text(href, value);
    true
}

fn patch_response_permissions(response: &mut Element, permission: PermissionLevel) {
    if permission == PermissionLevel::Full {
        return;
    }

    for propstat in response
        .children
        .iter_mut()
        .filter_map(XMLNode::as_mut_element)
        .filter(|element| is_dav_element(element, "propstat"))
    {
        for prop in propstat
            .children
            .iter_mut()
            .filter_map(XMLNode::as_mut_element)
            .filter(|element| is_dav_element(element, "prop"))
        {
            for child in prop.children.iter_mut().filter_map(XMLNode::as_mut_element) {
                if child.name == "permissions" && child.namespace.as_deref() == Some(OC_NS) {
                    set_element_text(child, readonly_permissions_string());
                }
            }
        }
    }
}

fn element_text(element: &Element) -> Option<String> {
    element.children.iter().find_map(|child| match child {
        XMLNode::Text(text) => Some(text.clone()),
        _ => None,
    })
}

fn set_element_text(element: &mut Element, value: &str) {
    element.children.clear();
    element.children.push(XMLNode::Text(value.to_owned()));
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

fn virtual_propfind_response(
    principal: &Principal,
    request_path: &str,
    headers: &HeaderMap,
    client_rel_path: &Path,
) -> Response<Body> {
    let href_prefix = mount_prefix_for_path(request_path, &principal.username);
    let depth_zero = headers
        .get("depth")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim() == "0")
        .unwrap_or(false);
    let mut xml = multistatus_start();
    append_virtual_collection_response(
        &mut xml,
        &href_prefix,
        client_rel_path,
        PermissionLevel::View,
    );

    if !depth_zero {
        match permissions::virtual_collection_children(principal, client_rel_path) {
            Ok(children) => {
                for child in children {
                    append_virtual_collection_response(
                        &mut xml,
                        &href_prefix,
                        &child,
                        permission_for_virtual_child(principal, &child),
                    );
                }
            }
            Err(err) => warn!(?err, "failed to list virtual app password scope children"),
        }
    }

    xml.push_str("</d:multistatus>");
    Response::builder()
        .status(StatusCode::MULTI_STATUS)
        .header("content-type", "application/xml; charset=utf-8")
        .body(Body::from(xml))
        .expect("valid virtual PROPFIND response")
}

fn permission_for_virtual_child(principal: &Principal, child: &Path) -> PermissionLevel {
    principal
        .scopes
        .iter()
        .find(|scope| scope.mount_path == child)
        .map(|scope| scope.permission)
        .unwrap_or(PermissionLevel::View)
}

fn append_virtual_collection_response(
    xml: &mut String,
    href_prefix: &str,
    rel_path: &Path,
    permission: PermissionLevel,
) {
    let Ok(href) = path_for_rel_path(href_prefix, rel_path) else {
        return;
    };
    xml.push_str("<d:response><d:href>");
    xml.push_str(&xml_escape(&href));
    xml.push_str("</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype><oc:permissions>");
    xml.push_str(match permission {
        PermissionLevel::Full => permissions_string(0, true),
        PermissionLevel::View => readonly_permissions_string(),
    });
    xml.push_str("</oc:permissions><nc:has-preview>false</nc:has-preview></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");
}

fn multistatus_start() -> String {
    String::from(
        r#"<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">"#,
    )
}

fn scoped_permissions_string(
    scope_match: &ScopeMatch,
    permissions: i64,
    is_collection: bool,
) -> &'static str {
    match scope_match.scope.permission {
        PermissionLevel::Full => permissions_string(permissions, is_collection),
        PermissionLevel::View => readonly_permissions_string(),
    }
}

fn readonly_permissions_string() -> &'static str {
    "RGDNV"
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn parse_x_oc_mtime(headers: &HeaderMap) -> Result<Option<FileTime>, Response<Body>> {
    let Some(value) = headers.get(X_OC_MTIME) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|err| {
        warn!(?err, "invalid X-OC-MTime header encoding");
        (StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header").into_response()
    })?;
    let seconds = value.trim().parse::<i64>().map_err(|err| {
        warn!(?err, value, "invalid X-OC-MTime header value");
        (StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header").into_response()
    })?;
    if seconds < 0 {
        warn!(seconds, "negative X-OC-MTime header value");
        return Err((StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header").into_response());
    }
    Ok(Some(FileTime::from_unix_time(seconds, 0)))
}

fn auto_mkcol_enabled(headers: &HeaderMap) -> bool {
    headers
        .get(X_NC_WEBDAV_AUTOMKCOL)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
}

fn insert_header(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("empty response builder")
}

fn metadata_error_response() -> Response<Body> {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Failed to persist file metadata",
    )
        .into_response()
}
