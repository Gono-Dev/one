use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
    response::IntoResponse,
};
use dav_server::{DavConfig, DavHandler};
use futures_util::future::BoxFuture;
use tower::Service;

use crate::auth::Principal;

#[derive(Clone)]
pub struct NcDavService {
    handler: DavHandler,
}

impl NcDavService {
    pub fn new(handler: DavHandler) -> Self {
        Self { handler }
    }

    async fn dispatch(self, request: Request<Body>) -> Response<Body> {
        match request.method().as_str() {
            "REPORT" | "SEARCH" => extension_placeholder(request.method().as_str()),
            "MKCOL" | "PUT" | "MOVE" if is_chunking_path(request.uri().path()) => {
                extension_placeholder("chunking-v2")
            }
            _ => self.forward_to_dav_server(request).await,
        }
    }

    async fn forward_to_dav_server(self, request: Request<Body>) -> Response<Body> {
        let principal = request
            .extensions()
            .get::<Principal>()
            .map(|principal| principal.username.clone())
            .unwrap_or_else(|| "gono".to_owned());

        self.handler
            .handle_with(DavConfig::new().principal(principal), request)
            .await
            .map(Body::new)
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

fn extension_placeholder(name: &str) -> Response<Body> {
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("{name} dispatch is reserved for a later phase"),
    )
        .into_response()
}

fn is_chunking_path(path: &str) -> bool {
    path.starts_with("/uploads/")
        || path.starts_with("/remote.php/dav/uploads/")
        || path.starts_with("/remote.php/webdav/uploads/")
}
