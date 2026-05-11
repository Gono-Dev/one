use std::sync::Arc;

use axum::{extract::State, Json};
use serde_json::{json, Value};

use crate::{notify_push::routes, state::AppState};

pub async fn handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    let mut response = json!({
        "ocs": {
            "meta": {
                "status": "ok",
                "statuscode": 100,
                "message": "OK"
            },
            "data": {
                "version": {
                    "major": 27,
                    "minor": 0,
                    "micro": 0,
                    "string": "27.0.0",
                    "edition": "",
                    "extendedSupport": false
                },
                "capabilities": {
                    "core": {
                        "pollinterval": 60,
                        "webdav-root": "remote.php/webdav"
                    },
                    "dav": {
                        "chunking": "1.0"
                    },
                    "files": {
                        "bigfilechunking": true,
                        "blacklisted_files": []
                    }
                }
            }
        }
    });

    if let Some(runtime) = &state.notify_push {
        response["ocs"]["data"]["capabilities"]["notify_push"] = json!({
            "type": runtime.config().advertised_types.clone(),
            "endpoints": {
                "websocket": routes::websocket_endpoint(&state.base_url, &runtime.config().path),
                "pre_auth": routes::pre_auth_endpoint(&state.base_url),
            }
        });
        response["ocs"]["data"]["capabilities"]["gono_cloud"] = json!({
            "notify_push_client_info": {
                "version": 1
            }
        });
    }

    Json(response)
}
