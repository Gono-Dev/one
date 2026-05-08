use std::{
    convert::Infallible,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    body::Body,
    http::{header::HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri},
    response::IntoResponse,
};
use dav_server::{davpath::DavPath, DavConfig, DavHandler};
use futures_util::future::BoxFuture;
use tower::Service;
use tracing::{error, warn};

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
        if let Err(err) = reject_parent_segments(request.uri().path()) {
            warn!(
                ?err,
                path = request.uri().path(),
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
        let request_path = request.uri().path().to_owned();
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
                match normalize_destination_header(destination) {
                    Ok(normalized) => {
                        request.headers_mut().insert("destination", normalized);
                    }
                    Err(err) => {
                        error!(?err, "failed to normalize Destination header");
                        return (StatusCode::BAD_REQUEST, "Invalid Destination header")
                            .into_response();
                    }
                }
            }
        }
        let principal = request
            .extensions()
            .get::<Principal>()
            .map(|principal| principal.username.clone())
            .unwrap_or_else(|| "gono".to_owned());

        let response = self
            .handler
            .handle_with(DavConfig::new().principal(principal), request)
            .await
            .map(Body::new);

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
            let source_rel = match parse_rel_path(request_path) {
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

        let target_rel = match write_target_rel_path(method, request_path, destination.as_deref()) {
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
            let source_rel = match parse_rel_path(request_path) {
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
        let rel_path = match parse_rel_path(request_path) {
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
        let rel_path = parse_rel_path(request_path)?;
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
        db::record_change(
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
        })
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

fn write_target_rel_path(
    method: &Method,
    request_path: &str,
    destination: Option<&str>,
) -> anyhow::Result<Option<PathBuf>> {
    let path = match method.as_str() {
        "PUT" | "MKCOL" => Some(request_path),
        "COPY" | "MOVE" => destination,
        "PROPPATCH" => Some(request_path),
        _ => None,
    };

    path.map(parse_rel_path).transpose()
}

pub(crate) fn parse_rel_path(path_or_uri: &str) -> anyhow::Result<PathBuf> {
    let path = if path_or_uri.starts_with("http://") || path_or_uri.starts_with("https://") {
        path_or_uri.parse::<Uri>()?.path().to_owned()
    } else {
        path_or_uri.to_owned()
    };
    reject_parent_segments(&path)?;
    let path = path
        .strip_prefix("/remote.php/dav")
        .or_else(|| path.strip_prefix("/remote.php/webdav"))
        .unwrap_or(&path);
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let dav_path = DavPath::new(&path)?;
    Ok(Path::new(dav_path.as_rel_ospath()).to_path_buf())
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

fn normalize_destination_header(destination: &str) -> anyhow::Result<HeaderValue> {
    let rel_path = parse_rel_path(destination)?;
    let normalized = if rel_path.as_os_str().is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", rel_path.to_string_lossy().replace('\\', "/"))
    };
    HeaderValue::from_str(&normalized).map_err(Into::into)
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
