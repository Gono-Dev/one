use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    installed: bool,
    maintenance: bool,
    #[serde(rename = "needsDbUpgrade")]
    needs_db_upgrade: bool,
    version: &'static str,
    versionstring: &'static str,
    edition: &'static str,
    productname: &'static str,
}

pub async fn handler() -> Json<StatusResponse> {
    Json(StatusResponse {
        installed: true,
        maintenance: false,
        needs_db_upgrade: false,
        version: "27.0.0.0",
        versionstring: "27.0.0",
        edition: "",
        productname: "Nextcloud",
    })
}
