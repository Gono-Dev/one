use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{any, delete, get, post},
    Json, Router,
};
use serde_json::{json, Value};

use crate::{auth::require_basic_auth, state::AppState};

use super::user;

pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let inner = v2_routes();

    Router::new()
        .nest("/ocs/v2.php", inner.clone())
        .nest("/index.php/ocs/v2.php", inner)
        .route_layer(middleware::from_fn_with_state(state, require_basic_auth))
}

fn v2_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/cloud/user", get(super::user::handler))
        .route("/cloud/users", get(list_users))
        .route("/cloud/users/{user_id}", get(user_metadata))
        .route("/apps/dav/api/v1/direct", post(not_implemented))
        .route("/core/autocomplete/get", get(autocomplete))
        .route(
            "/apps/files_sharing/api/v1/shares",
            get(empty_list).post(not_implemented),
        )
        .route(
            "/apps/files_sharing/api/v1/shares/inherited",
            get(empty_list),
        )
        .route("/apps/files_sharing/api/v1/shares/pending", get(empty_list))
        .route(
            "/apps/files_sharing/api/v1/shares/{id}",
            get(not_found).put(not_implemented).delete(not_implemented),
        )
        .route(
            "/apps/files_sharing/api/v1/shares/{id}/send-email",
            post(not_implemented),
        )
        .route("/apps/files_sharing/api/v1/sharees", get(sharees))
        .route(
            "/apps/files_sharing/api/v1/sharees_recommended",
            get(sharees),
        )
        .route("/apps/files_sharing/api/v1/remote_shares", get(empty_list))
        .route(
            "/apps/files_sharing/api/v1/remote_shares/pending",
            get(empty_list),
        )
        .route(
            "/apps/files_sharing/api/v1/remote_shares/pending/{id}",
            post(not_implemented).delete(not_implemented),
        )
        .route(
            "/apps/files_sharing/api/v1/remote_shares/{id}",
            get(not_found).delete(not_implemented),
        )
        .route(
            "/apps/notifications/api/v2/notifications",
            get(empty_list).delete(ok_empty),
        )
        .route(
            "/apps/notifications/api/v2/notifications/exists",
            post(notification_exists),
        )
        .route(
            "/apps/notifications/api/v2/notifications/{id}",
            get(not_found).delete(not_implemented),
        )
        .route("/apps/user_status/api/v1/user_status", get(own_status))
        .route(
            "/apps/user_status/api/v1/user_status/status",
            axum::routing::put(not_implemented),
        )
        .route(
            "/apps/user_status/api/v1/user_status/message/predefined",
            axum::routing::put(not_implemented),
        )
        .route(
            "/apps/user_status/api/v1/user_status/message/custom",
            axum::routing::put(not_implemented),
        )
        .route(
            "/apps/user_status/api/v1/user_status/message",
            delete(not_implemented),
        )
        .route(
            "/apps/user_status/api/v1/predefined_statuses",
            get(empty_list),
        )
        .route("/apps/user_status/api/v1/statuses", get(empty_list))
        .route(
            "/apps/user_status/api/v1/statuses/{user_id}",
            get(user_status_by_id),
        )
        .route(
            "/apps/user_status/api/v1/statuses/revert/{message_id}",
            delete(not_implemented),
        )
        .route(
            "/apps/recommendations/api/v1/recommendations",
            get(recommendations),
        )
        .route(
            "/apps/recommendations/api/v1/recommendations/always",
            get(recommendations),
        )
        .route(
            "/apps/provisioning_api/api/v1/config/users/{app_id}",
            post(not_implemented).delete(not_implemented),
        )
        .route(
            "/apps/provisioning_api/api/v1/config/users/{app_id}/{config_key}",
            post(not_implemented).delete(not_implemented),
        )
        .route("/translation/languages", get(translation_languages))
        .route("/translation/translate", post(no_translation_provider))
        .route("/textprocessing/tasktypes", get(text_processing_tasktypes))
        .route("/textprocessing/schedule", post(no_processing_provider))
        .route("/textprocessing/task/{id}", post(not_found))
        .route("/text2image/is_available", get(text2image_availability))
        .route("/text2image/schedule", post(no_processing_provider))
        .route(
            "/text2image/task/{id}",
            post(not_found).delete(not_implemented),
        )
        .route("/text2image/task/{id}/image/{index}", post(not_found))
        .route("/text2image/tasks/app/{app_id}", delete(not_found))
        .route("/taskprocessing", any(not_implemented))
        .route("/taskprocessing/{*path}", any(not_implemented))
        .route("/apps/dav/api/v1/outOfOffice/{user_id}/now", get(not_found))
        .route(
            "/apps/dav/api/v1/outOfOffice/{user_id}",
            get(not_found).post(not_implemented).delete(not_implemented),
        )
        .route(
            "/apps/fulltextsearch/collection/{collection}/index",
            get(not_implemented).delete(not_implemented),
        )
        .route(
            "/apps/fulltextsearch/collection/{collection}/document/{provider}/{document}",
            get(not_implemented),
        )
        .route(
            "/apps/fulltextsearch/collection/{collection}/document/{provider}/{document}/done",
            post(not_implemented),
        )
}

async fn list_users(State(state): State<Arc<AppState>>) -> Response {
    ocs_ok(json!({
        "users": [
            state.owner
        ]
    }))
}

async fn user_metadata(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Response {
    if user_id == state.owner {
        ocs_ok(user::user_data(&state, &user_id))
    } else {
        ocs_error(StatusCode::NOT_FOUND, 404, "User not found")
    }
}

async fn autocomplete() -> Response {
    ocs_ok(sharee_payload())
}

async fn empty_list() -> Response {
    ocs_ok(json!([]))
}

async fn ok_empty() -> Response {
    ocs_ok(json!({}))
}

async fn sharees() -> Response {
    ocs_ok(sharee_payload())
}

async fn notification_exists() -> Response {
    ocs_ok(json!({
        "ids": []
    }))
}

async fn own_status(State(state): State<Arc<AppState>>) -> Response {
    ocs_ok(user_status_payload(&state.owner))
}

async fn user_status_by_id(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Response {
    let user_id = user_id.strip_prefix('_').unwrap_or(&user_id);
    if user_id == state.owner {
        ocs_ok(user_status_payload(user_id))
    } else {
        ocs_error(StatusCode::NOT_FOUND, 404, "User status not found")
    }
}

async fn recommendations() -> Response {
    ocs_ok(json!({
        "enabled": false,
        "recommendations": []
    }))
}

async fn translation_languages() -> Response {
    ocs_ok(json!({
        "languageDetection": false,
        "languages": []
    }))
}

async fn no_translation_provider() -> Response {
    ocs_error(
        StatusCode::PRECONDITION_FAILED,
        412,
        "No translation provider installed",
    )
}

async fn text_processing_tasktypes() -> Response {
    ocs_ok(json!({
        "types": []
    }))
}

async fn text2image_availability() -> Response {
    ocs_ok(json!({
        "isAvailable": false
    }))
}

async fn no_processing_provider() -> Response {
    ocs_error(
        StatusCode::PRECONDITION_FAILED,
        412,
        "No processing provider installed",
    )
}

async fn not_found() -> Response {
    ocs_error(StatusCode::NOT_FOUND, 404, "Not found")
}

async fn not_implemented() -> Response {
    ocs_error(
        StatusCode::NOT_IMPLEMENTED,
        501,
        "This OCS v2 endpoint is not implemented yet",
    )
}

fn sharee_payload() -> Value {
    json!({
        "exact": {
            "users": [],
            "groups": [],
            "remotes": [],
            "emails": [],
            "circles": [],
            "rooms": []
        },
        "users": [],
        "groups": [],
        "remotes": [],
        "emails": [],
        "circles": [],
        "rooms": [],
        "lookup": []
    })
}

fn user_status_payload(user_id: &str) -> Value {
    json!({
        "userId": user_id,
        "message": null,
        "icon": null,
        "clearAt": null,
        "status": "online",
        "statusIsUserDefined": false
    })
}

fn ocs_ok(data: Value) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "ocs": {
                "meta": {
                    "status": "ok",
                    "statuscode": 100,
                    "message": "OK"
                },
                "data": data
            }
        })),
    )
        .into_response()
}

fn ocs_error(http_status: StatusCode, ocs_status: u16, message: &str) -> Response {
    (
        http_status,
        Json(json!({
            "ocs": {
                "meta": {
                    "status": "failure",
                    "statuscode": ocs_status,
                    "message": message
                },
                "data": {}
            }
        })),
    )
        .into_response()
}
