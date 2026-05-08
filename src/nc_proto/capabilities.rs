use axum::Json;
use serde_json::{json, Value};

pub async fn handler() -> Json<Value> {
    Json(json!({
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
    }))
}
