use std::net::{IpAddr, SocketAddr};

use axum::http::{header::HOST, HeaderMap, HeaderName};

const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub fn runtime_base_url(headers: &HeaderMap, bind: &str) -> String {
    request_origin(headers).unwrap_or_else(|| local_origin(bind))
}

pub fn request_origin(headers: &HeaderMap) -> Option<String> {
    let scheme = request_scheme(headers);
    let host = headers.get(HOST)?.to_str().ok()?.trim();
    if !safe_authority(host) {
        return None;
    }
    Some(format!("{scheme}://{host}"))
}

pub fn request_scheme(headers: &HeaderMap) -> &'static str {
    forwarded_proto(headers).unwrap_or_else(default_runtime_scheme)
}

pub fn forwarded_proto(headers: &HeaderMap) -> Option<&'static str> {
    if let Some(value) = headers.get(FORWARDED).and_then(|value| value.to_str().ok()) {
        for entry in value.split(',') {
            for part in entry.split(';') {
                let Some((name, value)) = part.trim().split_once('=') else {
                    continue;
                };
                if name.eq_ignore_ascii_case("proto") {
                    return normalized_scheme(value.trim_matches('"'));
                }
            }
        }
    }
    let value = headers
        .get(X_FORWARDED_PROTO)
        .and_then(|value| value.to_str().ok())?
        .split(',')
        .next()?
        .trim();
    normalized_scheme(value)
}

pub fn default_runtime_scheme() -> &'static str {
    if std::env::var("GONE_CLOUD_INSECURE_HTTP").as_deref() == Ok("1") {
        "http"
    } else {
        "https"
    }
}

fn normalized_scheme(value: &str) -> Option<&'static str> {
    if value.eq_ignore_ascii_case("https") {
        Some("https")
    } else if value.eq_ignore_ascii_case("http") {
        Some("http")
    } else {
        None
    }
}

fn local_origin(bind: &str) -> String {
    let scheme = default_runtime_scheme();
    let Ok(addr) = bind.parse::<SocketAddr>() else {
        return format!("{scheme}://localhost");
    };
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "localhost".to_owned(),
        IpAddr::V6(ip) if ip.is_unspecified() => "localhost".to_owned(),
        IpAddr::V6(ip) => format!("[{ip}]"),
        IpAddr::V4(ip) => ip.to_string(),
    };
    format!("{scheme}://{host}:{}", addr.port())
}

fn safe_authority(value: &str) -> bool {
    if value.is_empty() || value.contains('@') || value.contains('/') || value.contains('\\') {
        return false;
    }
    value.parse::<http::uri::Authority>().is_ok()
}

#[cfg(test)]
mod tests {
    use axum::http::{header::HOST, HeaderMap, HeaderValue};

    use super::{request_origin, runtime_base_url, X_FORWARDED_PROTO};

    #[test]
    fn request_origin_uses_safe_host_with_default_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("files.example.test"));

        assert_eq!(
            request_origin(&headers).as_deref(),
            Some("https://files.example.test")
        );
    }

    #[test]
    fn request_origin_uses_forwarded_proto() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("files.example.test"));
        headers.insert(X_FORWARDED_PROTO, HeaderValue::from_static("http"));

        assert_eq!(
            request_origin(&headers).as_deref(),
            Some("http://files.example.test")
        );
    }

    #[test]
    fn request_origin_rejects_unsafe_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HOST,
            HeaderValue::from_static("files.example.test/@attacker.example"),
        );

        assert!(request_origin(&headers).is_none());
    }

    #[test]
    fn runtime_base_url_uses_localhost_for_wildcard_bind() {
        let headers = HeaderMap::new();
        assert_eq!(
            runtime_base_url(&headers, "0.0.0.0:16102"),
            "https://localhost:16102"
        );
    }
}
