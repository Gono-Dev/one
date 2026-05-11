use std::sync::Arc;

use axum::{
    body::Body,
    http::{header::HeaderName, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::{
    admin, auth::require_basic_auth, dav_handler::NcDavService, nextcloud_proto, notify_push,
    state::AppState,
};

const DEPTH: HeaderName = HeaderName::from_static("depth");

pub fn build_router(state: Arc<AppState>) -> Router {
    let dav_service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(depth_guard))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_basic_auth,
        ))
        .service(NcDavService::new(state.clone()));

    let capabilities = get(nextcloud_proto::capabilities::handler);
    let mut app = Router::new()
        .route("/status.php", get(nextcloud_proto::status::handler))
        .route("/204", get(no_content))
        .route("/index.php/204", get(no_content))
        .route("/ocs/v1.php/cloud/capabilities", capabilities.clone())
        .route(
            "/index.php/ocs/v1.php/cloud/capabilities",
            capabilities.clone(),
        )
        .route("/ocs/v2.php/cloud/capabilities", capabilities.clone())
        .route("/index.php/ocs/v2.php/cloud/capabilities", capabilities)
        .merge(nextcloud_proto::ocs::router(state.clone()))
        .route(
            "/metrics",
            get(nextcloud_proto::metrics::handler).route_layer(middleware::from_fn_with_state(
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
        );

    if state.notify_push.is_some() {
        app = app.merge(notify_push::routes::router(&state.notify_push_config.path));
    }
    if state.admin_config.enabled {
        app = app.merge(admin::routes::router(state.clone()));
    } else {
        app = app.merge(admin::routes::disabled_router());
    }

    app.nest_service("/remote.php/dav", dav_service.clone())
        .nest_service("/remote.php/webdav", dav_service.clone())
        .fallback_service(dav_service)
        .with_state(state)
}

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
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
