use std::sync::Arc;

use axum::{extract::State, Extension, Json};
use serde_json::{json, Value};

use crate::{auth::Principal, state::AppState};

pub async fn handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Json<Value> {
    let user_id = if principal.username.is_empty() {
        state.owner.clone()
    } else {
        principal.username
    };

    Json(json!({
        "ocs": {
            "meta": {
                "status": "ok",
                "statuscode": 100,
                "message": "OK"
            },
            "data": user_data(&state, &user_id)
        }
    }))
}

pub(crate) fn user_data(state: &AppState, user_id: &str) -> Value {
    let storage_location = state.files_root.to_string_lossy().into_owned();

    json!({
        "id": user_id,
        "enabled": true,
        "storageLocation": storage_location,
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
