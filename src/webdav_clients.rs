use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use axum::http::{
    header::{HeaderMap, USER_AGENT},
    Method,
};
use dashmap::DashMap;

const CLIENT_RETENTION: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Default)]
pub struct WebDavClientRegistry {
    clients: DashMap<String, WebDavClientRecord>,
}

#[derive(Debug, Clone)]
struct WebDavClientRecord {
    user: String,
    peer_addr: String,
    first_seen: Instant,
    last_seen: Instant,
    request_count: u64,
    last_method: String,
    protocol: Option<String>,
    client_info: WebDavClientInfo,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebDavClientInfo {
    pub device_name: Option<String>,
    pub hostname: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub platform: Option<String>,
    pub os: Option<String>,
    pub device_id: Option<String>,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebDavClientSnapshot {
    pub user: String,
    pub peer_addr: String,
    pub first_seen_secs: u64,
    pub last_seen_secs: u64,
    pub request_count: u64,
    pub last_method: String,
    pub protocol: Option<String>,
    pub client_info: WebDavClientInfo,
}

impl WebDavClientRegistry {
    pub fn observe_request(
        &self,
        user: &str,
        peer_addr: Option<SocketAddr>,
        method: &Method,
        headers: &HeaderMap,
        default_protocol: Option<&str>,
    ) {
        let now = Instant::now();
        self.prune(now);

        let client_info = WebDavClientInfo::from_headers(headers);
        let peer_addr = peer_addr_string(headers, peer_addr);
        let key = client_key(user, &peer_addr, &client_info);
        let method = method.as_str().to_owned();
        let protocol =
            protocol_from_headers(headers).or_else(|| sanitize_optional(default_protocol, 16));

        self.clients
            .entry(key)
            .and_modify(|record| {
                record.peer_addr = peer_addr.clone();
                record.last_seen = now;
                record.request_count = record.request_count.saturating_add(1);
                record.last_method = method.clone();
                record.protocol = protocol.clone();
                record.client_info = client_info.clone();
            })
            .or_insert_with(|| WebDavClientRecord {
                user: user.to_owned(),
                peer_addr,
                first_seen: now,
                last_seen: now,
                request_count: 1,
                last_method: method,
                protocol,
                client_info,
            });
    }

    pub fn recent_clients(&self) -> Vec<WebDavClientSnapshot> {
        let now = Instant::now();
        self.prune(now);

        let mut clients = self
            .clients
            .iter()
            .map(|entry| {
                let record = entry.value();
                WebDavClientSnapshot {
                    user: record.user.clone(),
                    peer_addr: record.peer_addr.clone(),
                    first_seen_secs: now.duration_since(record.first_seen).as_secs(),
                    last_seen_secs: now.duration_since(record.last_seen).as_secs(),
                    request_count: record.request_count,
                    last_method: record.last_method.clone(),
                    protocol: record.protocol.clone(),
                    client_info: record.client_info.clone(),
                }
            })
            .collect::<Vec<_>>();
        clients.sort_by(|left, right| {
            left.last_seen_secs
                .cmp(&right.last_seen_secs)
                .then_with(|| display_name_for_client(left).cmp(&display_name_for_client(right)))
                .then_with(|| left.peer_addr.cmp(&right.peer_addr))
        });
        clients
    }

    fn prune(&self, now: Instant) {
        self.clients
            .retain(|_, record| now.duration_since(record.last_seen) <= CLIENT_RETENTION);
    }
}

impl WebDavClientInfo {
    fn from_headers(headers: &HeaderMap) -> Self {
        let user_agent = header_value(headers, USER_AGENT.as_str(), 180);
        let mut info = Self {
            device_name: header_value(headers, "x-gono-device-name", 80),
            hostname: header_value(headers, "x-gono-hostname", 253),
            client_name: header_value(headers, "x-gono-client-name", 80),
            client_version: header_value(headers, "x-gono-client-version", 40),
            platform: header_value(headers, "x-gono-platform", 20),
            os: header_value(headers, "x-gono-os", 80),
            device_id: header_value(headers, "x-gono-device-id", 80),
            user_agent,
        };

        if let Some(user_agent) = &info.user_agent {
            if info.client_name.is_none() {
                let (name, version) = client_from_user_agent(user_agent);
                info.client_name = Some(name);
                if info.client_version.is_none() {
                    info.client_version = version;
                }
            }
            if info.platform.is_none() {
                info.platform = platform_from_user_agent(user_agent);
            }
            if info.os.is_none() {
                info.os = os_from_user_agent(user_agent);
            }
        }

        info
    }
}

fn client_key(user: &str, peer_addr: &str, info: &WebDavClientInfo) -> String {
    if let Some(device_id) = &info.device_id {
        return format!("{user}\0device\0{device_id}");
    }

    format!(
        "{}\0peer\0{}\0ua\0{}",
        user,
        peer_addr,
        info.user_agent.as_deref().unwrap_or("")
    )
}

fn peer_addr_string(headers: &HeaderMap, peer_addr: Option<SocketAddr>) -> String {
    header_value(headers, "x-forwarded-for", 120)
        .and_then(|value| value.split(',').next().map(str::trim).map(str::to_owned))
        .filter(|value| !value.is_empty())
        .or_else(|| header_value(headers, "x-real-ip", 120))
        .or_else(|| peer_addr.map(|addr| addr.to_string()))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn protocol_from_headers(headers: &HeaderMap) -> Option<String> {
    header_value(headers, "x-forwarded-proto", 16).and_then(|value| {
        let value = value.to_ascii_lowercase();
        matches!(value.as_str(), "http" | "https").then_some(value)
    })
}

fn client_from_user_agent(user_agent: &str) -> (String, Option<String>) {
    if let Some(version) = product_version(user_agent, "GonoCloudDesktop")
        .or_else(|| product_version(user_agent, "Gono-Cloud-Desktop"))
    {
        return ("Gono Cloud Desktop".to_owned(), Some(version));
    }
    if let Some(version) = product_version(user_agent, "Nextcloud-desktop") {
        return ("Nextcloud Desktop".to_owned(), Some(version));
    }
    if let Some(version) = product_version(user_agent, "mirall") {
        if user_agent.contains("Nextcloud") {
            return ("Nextcloud Desktop".to_owned(), Some(version));
        }
        if user_agent.contains("ownCloud") {
            return ("ownCloud Desktop".to_owned(), Some(version));
        }
        return ("WebDAV desktop client".to_owned(), Some(version));
    }
    if user_agent.contains("Nextcloud") {
        return ("Nextcloud Desktop".to_owned(), None);
    }
    if user_agent.contains("ownCloud") {
        return ("ownCloud Desktop".to_owned(), None);
    }
    if let Some(version) = product_version(user_agent, "Cyberduck") {
        return ("Cyberduck".to_owned(), Some(version));
    }

    ("WebDAV client".to_owned(), None)
}

fn product_version(user_agent: &str, product: &str) -> Option<String> {
    let start = user_agent.find(&format!("{product}/"))? + product.len() + 1;
    let version = user_agent[start..]
        .split(|c: char| c.is_whitespace() || matches!(c, ';' | ')' | '(' | ','))
        .next()
        .unwrap_or("");
    sanitize_value(version, 40)
}

fn platform_from_user_agent(user_agent: &str) -> Option<String> {
    let lower = user_agent.to_ascii_lowercase();
    if lower.contains("windows") {
        Some("windows".to_owned())
    } else if lower.contains("macintosh") || lower.contains("mac os") || lower.contains("macos") {
        Some("macos".to_owned())
    } else if lower.contains("android") {
        Some("android".to_owned())
    } else if lower.contains("iphone") || lower.contains("ipad") || lower.contains("ios") {
        Some("ios".to_owned())
    } else if lower.contains("linux") {
        Some("linux".to_owned())
    } else {
        None
    }
}

fn os_from_user_agent(user_agent: &str) -> Option<String> {
    let mut rest = user_agent;
    while let Some(start) = rest.find('(') {
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find(')') else {
            break;
        };
        let group = &after_start[..end];
        for segment in group.split([',', ';']) {
            let segment = segment.trim();
            let lower = segment.to_ascii_lowercase();
            if lower.contains("windows")
                || lower.contains("macos")
                || lower.contains("mac os")
                || lower.contains("macintosh")
                || lower.contains("linux")
                || lower.contains("android")
                || lower.contains("iphone")
                || lower.contains("ipad")
            {
                if let Some(value) = sanitize_value(segment, 80) {
                    return Some(value);
                }
            }
        }
        rest = &after_start[end + 1..];
    }
    None
}

fn header_value(headers: &HeaderMap, name: &str, max_len: usize) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| sanitize_value(value, max_len))
}

fn sanitize_optional(value: Option<&str>, max_len: usize) -> Option<String> {
    value.and_then(|value| sanitize_value(value, max_len))
}

fn sanitize_value(value: &str, max_len: usize) -> Option<String> {
    let cleaned = value
        .trim()
        .chars()
        .filter(|ch| !ch.is_control())
        .take(max_len)
        .collect::<String>();
    let cleaned = cleaned.trim().to_owned();
    (!cleaned.is_empty()).then_some(cleaned)
}

fn display_name_for_client(client: &WebDavClientSnapshot) -> String {
    client
        .client_info
        .device_name
        .as_ref()
        .or(client.client_info.hostname.as_ref())
        .or(client.client_info.client_name.as_ref())
        .unwrap_or(&client.peer_addr)
        .clone()
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::{client_from_user_agent, sanitize_value, WebDavClientInfo, WebDavClientRegistry};

    #[test]
    fn parses_nextcloud_desktop_user_agent() {
        let (name, version) = client_from_user_agent(
            "Mozilla/5.0 (Windows) mirall/3.16.0 (Nextcloud, windows-10.0.22631)",
        );

        assert_eq!(name, "Nextcloud Desktop");
        assert_eq!(version.as_deref(), Some("3.16.0"));
    }

    #[test]
    fn webdav_client_headers_are_sanitized() {
        let mut headers = HeaderMap::new();
        headers.insert("x-gono-device-name", " Test Mac ".parse().unwrap());
        headers.insert(
            "user-agent",
            "GonoCloudDesktop/0.1.0 (macOS 15.5)".parse().unwrap(),
        );

        let info = WebDavClientInfo::from_headers(&headers);

        assert_eq!(
            sanitize_value(" Test\nMac ", 80).as_deref(),
            Some("TestMac")
        );
        assert_eq!(info.device_name.as_deref(), Some("Test Mac"));
        assert_eq!(info.client_name.as_deref(), Some("Gono Cloud Desktop"));
        assert_eq!(info.client_version.as_deref(), Some("0.1.0"));
        assert_eq!(info.platform.as_deref(), Some("macos"));
    }

    #[test]
    fn registry_groups_by_device_id() {
        let registry = WebDavClientRegistry::default();
        let mut headers = HeaderMap::new();
        headers.insert("x-gono-device-id", "device-1".parse().unwrap());
        headers.insert("x-gono-device-name", "Office Mac".parse().unwrap());

        registry.observe_request(
            "gono",
            None,
            &axum::http::Method::from_bytes(b"PROPFIND").unwrap(),
            &headers,
            Some("http"),
        );
        registry.observe_request(
            "gono",
            None,
            &axum::http::Method::PUT,
            &headers,
            Some("http"),
        );

        let clients = registry.recent_clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].request_count, 2);
        assert_eq!(clients[0].last_method, "PUT");
        assert_eq!(
            clients[0].client_info.device_name.as_deref(),
            Some("Office Mac")
        );
    }
}
