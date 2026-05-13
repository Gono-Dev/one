use std::path::Path;

use axum::{
    body::Body,
    http::{HeaderMap, Response, StatusCode, header},
    response::IntoResponse,
};
use tracing::{error, warn};

use crate::config::StorageConfig;

pub fn ensure_upload_space(
    root: &Path,
    required_bytes: Option<u64>,
    config: &StorageConfig,
) -> Result<(), Response<Body>> {
    let available = fs2::available_space(root).map_err(|err| {
        error!(?err, path = %root.display(), "failed to check available upload storage");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to check available storage",
        )
            .into_response()
    })?;
    let total = if config.upload_min_free_percent > 0 {
        Some(fs2::total_space(root).map_err(|err| {
            error!(?err, path = %root.display(), "failed to check total upload storage");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to check available storage",
            )
                .into_response()
        })?)
    } else {
        None
    };
    let reserved = reserved_space_threshold(total.unwrap_or(0), config);
    let needed = required_bytes.unwrap_or(0).saturating_add(reserved);
    if available < needed {
        warn!(
            path = %root.display(),
            available_bytes = available,
            required_bytes = required_bytes.unwrap_or(0),
            reserved_bytes = reserved,
            needed_bytes = needed,
            "insufficient storage for upload"
        );
        Err(insufficient_storage_response())
    } else {
        Ok(())
    }
}

pub fn content_length(headers: &HeaderMap) -> Result<Option<u64>, Response<Body>> {
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid Content-Length header").into_response())?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid Content-Length header").into_response())
}

pub fn reserved_space_threshold(total_bytes: u64, config: &StorageConfig) -> u64 {
    let percent_reserved = if config.upload_min_free_percent == 0 {
        0
    } else {
        total_bytes.saturating_mul(config.upload_min_free_percent.min(100) as u64) / 100
    };
    config.upload_min_free_bytes.max(percent_reserved)
}

pub fn insufficient_storage_response() -> Response<Body> {
    let xml = concat!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
        "<d:error xmlns:d=\"DAV:\" xmlns:g=\"https://gono.cloud/ns\">",
        "<d:quota-not-exceeded/>",
        "<g:message>Insufficient storage for upload</g:message>",
        "</d:error>"
    );
    (
        StatusCode::INSUFFICIENT_STORAGE,
        [("content-type", "application/xml; charset=utf-8")],
        xml,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_space_uses_stricter_percent_or_bytes() {
        let mut config = StorageConfig {
            data_dir: "data".to_owned(),
            xattr_ns: "user.nc".to_owned(),
            upload_min_free_bytes: 1024,
            upload_min_free_percent: 10,
        };
        assert_eq!(reserved_space_threshold(50_000, &config), 5_000);

        config.upload_min_free_percent = 1;
        assert_eq!(reserved_space_threshold(50_000, &config), 1024);
    }
}
