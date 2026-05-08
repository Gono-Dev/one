use std::sync::Arc;

use axum::{
    body::Body,
    http::{header::HeaderName, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use dav_server::{memls::MemLs, DavHandler};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::{
    auth::require_basic_auth,
    dav_handler::{NcDavService, NcLocalFs},
    nc_proto,
    state::AppState,
};

const DEPTH: HeaderName = HeaderName::from_static("depth");

pub fn build_router(state: Arc<AppState>) -> Router {
    let dav_handler = DavHandler::builder()
        .filesystem(Box::new(NcLocalFs::new(
            &state.files_root,
            state.db.clone(),
            state.owner.clone(),
            state.instance_id.clone(),
            state.xattr_ns.clone(),
        )))
        .locksystem(MemLs::new())
        .build_handler();

    let dav_service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(depth_guard))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_basic_auth,
        ))
        .service(NcDavService::new(dav_handler, state.clone()));

    Router::new()
        .route("/status.php", get(nc_proto::status::handler))
        .route(
            "/ocs/v2.php/cloud/capabilities",
            get(nc_proto::capabilities::handler),
        )
        .route(
            "/metrics",
            get(nc_proto::metrics::handler).route_layer(middleware::from_fn_with_state(
                state.clone(),
                require_basic_auth,
            )),
        )
        .route(
            "/.well-known/caldav",
            get(|| async { Redirect::permanent("/remote.php/dav") }),
        )
        .route(
            "/.well-known/carddav",
            get(|| async { Redirect::permanent("/remote.php/dav") }),
        )
        .nest_service("/remote.php/dav", dav_service.clone())
        .nest_service("/remote.php/webdav", dav_service)
        .with_state(state)
}

async fn depth_guard(request: Request<Body>, next: Next) -> Response {
    let depth_is_infinity = request
        .headers()
        .get(DEPTH)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("infinity"))
        .unwrap_or(false);

    if request.method().as_str() == "PROPFIND" && depth_is_infinity {
        return (StatusCode::FORBIDDEN, "Depth infinity is not allowed").into_response();
    }

    next.run(request).await
}
