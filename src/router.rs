use std::{net::SocketAddr, sync::Arc, time::Instant};

use axum::{
    body::Body,
    extract::connect_info::ConnectInfo,
    http::{header::HeaderName, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::info;

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
        .layer(middleware::from_fn(access_log))
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

async fn access_log(request: Request<Body>, next: Next) -> Response {
    let started_at = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let peer_addr = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let user_agent = request
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(truncate_user_agent)
        .unwrap_or("-")
        .to_owned();
    let depth = request
        .headers()
        .get(DEPTH)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    let content_length = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    let kind = classify_access_request(method.as_str(), &path);

    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = started_at.elapsed().as_millis();

    info!(
        target: "gono_cloud::access",
        %peer_addr,
        method = %method,
        %path,
        status = status.as_u16(),
        elapsed_ms,
        %kind,
        depth,
        content_length,
        user_agent,
        "http access"
    );

    response
}

fn classify_access_request(method: &str, path: &str) -> &'static str {
    if path.ends_with("/push/ws") {
        return "notify_push_ws";
    }
    if path.ends_with("/apps/notify_push/pre_auth")
        || path.ends_with("/index.php/apps/notify_push/pre_auth")
    {
        return "notify_push_pre_auth";
    }
    if method == "REPORT" {
        return "webdav_report";
    }
    if method == "PROPFIND" {
        return "webdav_propfind";
    }
    if method == "PUT" {
        return "webdav_put";
    }
    if method == "MOVE" {
        return "webdav_move";
    }
    if method == "GET" && path.contains("/ocs/") && path.ends_with("/cloud/capabilities") {
        return "ocs_capabilities";
    }
    "http"
}

fn truncate_user_agent(value: &str) -> &str {
    const MAX_USER_AGENT_LEN: usize = 160;
    value.get(..MAX_USER_AGENT_LEN).unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::classify_access_request;

    #[test]
    fn classifies_nextcloud_sync_requests() {
        assert_eq!(classify_access_request("GET", "/push/ws"), "notify_push_ws");
        assert_eq!(
            classify_access_request("REPORT", "/remote.php/dav/files/gono/"),
            "webdav_report"
        );
        assert_eq!(
            classify_access_request("PROPFIND", "/remote.php/dav/files/gono/"),
            "webdav_propfind"
        );
    }
}
