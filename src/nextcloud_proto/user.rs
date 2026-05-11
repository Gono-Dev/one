use std::sync::Arc;

use axum::{extract::State, Extension, Json};
use serde_json::{json, Value};

use crate::auth::Principal;

pub async fn handler(
    State(_state): State<Arc<crate::state::AppState>>,
    Extension(principal): Extension<Principal>,
) -> Json<Value> {
    let user_id = principal.username;

    Json(json!({
        "ocs": {
            "meta": {
                "status": "ok",
                "statuscode": 100,
                "message": "OK"
            },
            "data": user_data(&user_id)
        }
    }))
}

pub(crate) fn user_data(user_id: &str) -> Value {
    json!({
        "id": user_id,
        "enabled": true,
        "storageLocation": format!("/remote.php/dav/files/{user_id}"),
        "lastLogin": 0,
        "backend": "Database",
        "subadmin": [],
        "quota": {
            "free": -3,
            "used": 0,
            "total": -3,
            "relative": 0.0,
            "quota": -3
        },
        "email": "",
        "displayname": user_id,
        "display-name": user_id,
        "phone": "",
        "address": "",
        "website": "",
        "twitter": "",
        "fediverse": "",
        "organisation": "",
        "role": "",
        "headline": "",
        "biography": "",
        "profile_enabled": false,
        "groups": [],
        "language": "en",
        "locale": "en",
        "notify_email": "",
        "backendCapabilities": {
            "setDisplayName": false,
            "setPassword": false
        }
    })
}
