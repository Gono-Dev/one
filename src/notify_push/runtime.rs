use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use dashmap::DashMap;
use rand::RngCore;
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::info;

use crate::{auth::Principal, config::NotifyPushConfig};

use super::{MessageType, PushMessage};

#[derive(Debug)]
pub struct NotifyRuntime {
    config: NotifyPushConfig,
    channels: DashMap<String, broadcast::Sender<PushMessage>>,
    pre_auth: DashMap<String, PreAuthEntry>,
    connections: DashMap<u64, NotifyConnectionRecord>,
    test_values: DashMap<String, String>,
    test_cookie: AtomicU32,
    next_connection_id: AtomicU64,
    metrics: NotifyMetrics,
}

#[derive(Debug, Clone)]
struct PreAuthEntry {
    principal: Principal,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct NotifyConnectionRecord {
    user: String,
    peer_addr: String,
    connected_at: Instant,
    listen_file_id: bool,
    client_info: NotifyClientInfo,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotifyClientInfo {
    pub device_name: Option<String>,
    pub hostname: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub platform: Option<String>,
    pub os: Option<String>,
    pub device_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotifyClientInfoWire {
    v: u8,
    device_name: Option<String>,
    hostname: Option<String>,
    client_name: Option<String>,
    client_version: Option<String>,
    platform: Option<String>,
    os: Option<String>,
    device_id: Option<String>,
}

#[derive(Debug, Default)]
struct NotifyMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicUsize,
    events_received: AtomicUsize,
    auth_failures: AtomicUsize,
    messages_sent: AtomicUsize,
    messages_sent_file: AtomicUsize,
    messages_sent_activity: AtomicUsize,
    messages_sent_notification: AtomicUsize,
    messages_sent_custom: AtomicUsize,
    test_endpoint_hits: AtomicUsize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NotifyMetricsSnapshot {
    pub active_connections: usize,
    pub active_users: usize,
    pub total_connections: usize,
    pub events_received: usize,
    pub auth_failures: usize,
    pub messages_sent: usize,
    pub messages_sent_file: usize,
    pub messages_sent_activity: usize,
    pub messages_sent_notification: usize,
    pub messages_sent_custom: usize,
    pub test_endpoint_hits: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyConnectionSnapshot {
    pub user: String,
    pub peer_addr: String,
    pub connected_secs: u64,
    pub listen_file_id: bool,
    pub client_info: NotifyClientInfo,
}

impl NotifyRuntime {
    pub fn new(config: NotifyPushConfig) -> Arc<Self> {
        let runtime = Arc::new(Self {
            config,
            channels: DashMap::new(),
            pre_auth: DashMap::new(),
            connections: DashMap::new(),
            test_values: DashMap::new(),
            test_cookie: AtomicU32::new(rand::random()),
            next_connection_id: AtomicU64::new(1),
            metrics: NotifyMetrics::default(),
        });
        runtime.set_test_token(random_token());
        runtime
    }

    pub fn config(&self) -> &NotifyPushConfig {
        &self.config
    }

    pub fn pre_auth_ttl(&self) -> Duration {
        Duration::from_secs(self.config.pre_auth_ttl_secs)
    }

    pub fn auth_timeout(&self) -> Duration {
        Duration::from_secs(self.config.auth_timeout_secs)
    }

    pub fn ping_interval(&self) -> Duration {
        Duration::from_secs(self.config.ping_interval_secs)
    }

    pub fn max_debounce(&self) -> Duration {
        Duration::from_secs(self.config.max_debounce_secs)
    }

    pub fn max_connection_time(&self) -> Option<Duration> {
        match self.config.max_connection_secs {
            0 => None,
            seconds => Some(Duration::from_secs(seconds)),
        }
    }

    pub fn issue_pre_auth(&self, principal: Principal) -> String {
        self.cleanup_pre_auth();
        let token = random_token();
        self.pre_auth.insert(
            token.clone(),
            PreAuthEntry {
                principal,
                created_at: Instant::now(),
            },
        );
        token
    }

    pub fn take_pre_auth(&self, token: &str) -> Option<Principal> {
        self.cleanup_pre_auth();
        let (_, entry) = self.pre_auth.remove(token)?;
        (entry.created_at.elapsed() <= self.pre_auth_ttl()).then_some(entry.principal)
    }

    pub fn subscribe(
        &self,
        user: &str,
    ) -> Result<broadcast::Receiver<PushMessage>, SubscribeError> {
        if let Some(sender) = self.channels.get(user) {
            if sender.receiver_count() >= self.config.user_connection_limit {
                return Err(SubscribeError::LimitExceeded);
            }
            self.metrics.add_connection();
            return Ok(sender.subscribe());
        }

        let (sender, receiver) = broadcast::channel(16);
        self.channels.insert(user.to_owned(), sender);
        self.metrics.add_connection();
        Ok(receiver)
    }

    pub fn register_connection(&self, user: &str, peer_addr: impl ToString) -> u64 {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        self.connections.insert(
            id,
            NotifyConnectionRecord {
                user: user.to_owned(),
                peer_addr: peer_addr.to_string(),
                connected_at: Instant::now(),
                listen_file_id: false,
                client_info: NotifyClientInfo::default(),
            },
        );
        id
    }

    pub fn set_connection_listen_file_id(&self, id: u64, listen_file_id: bool) {
        if let Some(mut record) = self.connections.get_mut(&id) {
            record.listen_file_id = listen_file_id;
        }
    }

    pub fn update_connection_client_info(&self, id: u64, client_info: NotifyClientInfo) {
        if let Some(mut record) = self.connections.get_mut(&id) {
            record.client_info = client_info;
        }
    }

    pub fn disconnect(&self, user: &str, connection_id: Option<u64>) {
        if let Some(connection_id) = connection_id {
            self.connections.remove(&connection_id);
        }
        self.metrics.remove_connection();
        if let Some(sender) = self.channels.get(user) {
            if sender.receiver_count() <= 1 {
                drop(sender);
                self.channels.remove(user);
            }
        }
    }

    pub fn notify_file(&self, user: &str, file_id: Option<i64>) {
        self.send_to_user(user, PushMessage::file(file_id));
    }

    pub fn notify_activity(&self, user: &str) {
        self.send_to_user(user, PushMessage::Activity);
    }

    pub fn notify_notification(&self, user: &str) {
        self.send_to_user(user, PushMessage::Notification);
    }

    pub fn notify_custom(&self, user: &str, message: impl Into<String>, body: Option<String>) {
        self.send_to_user(
            user,
            PushMessage::Custom {
                message: message.into(),
                body,
            },
        );
    }

    pub fn message_sent(&self, message_type: MessageType) {
        self.metrics.add_message(message_type);
    }

    pub fn auth_failed(&self) {
        self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn test_endpoint_hit(&self) {
        self.metrics
            .test_endpoint_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_test_token(&self, token: impl Into<String>) {
        self.test_values
            .insert("test-token".to_owned(), token.into());
    }

    pub fn validate_test_token(&self, token: Option<&str>) -> bool {
        self.test_values
            .get("test-token")
            .and_then(|expected| token.map(|token| token == expected.as_str()))
            .unwrap_or(false)
    }

    pub fn set_test_cookie(&self, cookie: u32) {
        self.test_cookie.store(cookie, Ordering::Relaxed);
    }

    pub fn test_cookie(&self) -> u32 {
        self.test_cookie.load(Ordering::Relaxed)
    }

    pub fn set_version(&self, version: impl Into<String>) {
        self.test_values
            .insert("notify_push_version".to_owned(), version.into());
    }

    pub fn metrics(&self) -> NotifyMetricsSnapshot {
        NotifyMetricsSnapshot {
            active_connections: self.metrics.active_connections.load(Ordering::Relaxed),
            active_users: self.channels.len(),
            total_connections: self.metrics.total_connections.load(Ordering::Relaxed),
            events_received: self.metrics.events_received.load(Ordering::Relaxed),
            auth_failures: self.metrics.auth_failures.load(Ordering::Relaxed),
            messages_sent: self.metrics.messages_sent.load(Ordering::Relaxed),
            messages_sent_file: self.metrics.messages_sent_file.load(Ordering::Relaxed),
            messages_sent_activity: self.metrics.messages_sent_activity.load(Ordering::Relaxed),
            messages_sent_notification: self
                .metrics
                .messages_sent_notification
                .load(Ordering::Relaxed),
            messages_sent_custom: self.metrics.messages_sent_custom.load(Ordering::Relaxed),
            test_endpoint_hits: self.metrics.test_endpoint_hits.load(Ordering::Relaxed),
        }
    }

    pub fn active_connections_by_user(&self) -> Vec<NotifyConnectionSnapshot> {
        let mut connections = self.active_connections();
        connections.sort_by(|left, right| {
            left.user
                .cmp(&right.user)
                .then_with(|| left.peer_addr.cmp(&right.peer_addr))
        });
        connections
    }

    pub fn active_connections(&self) -> Vec<NotifyConnectionSnapshot> {
        let now = Instant::now();
        let mut connections = self
            .connections
            .iter()
            .map(|entry| {
                let record = entry.value();
                NotifyConnectionSnapshot {
                    user: record.user.clone(),
                    peer_addr: record.peer_addr.clone(),
                    connected_secs: now.duration_since(record.connected_at).as_secs(),
                    listen_file_id: record.listen_file_id,
                    client_info: record.client_info.clone(),
                }
            })
            .collect::<Vec<_>>();
        connections.sort_by(|left, right| {
            display_name_for_connection(left)
                .cmp(&display_name_for_connection(right))
                .then_with(|| left.peer_addr.cmp(&right.peer_addr))
        });
        connections
    }

    fn send_to_user(&self, user: &str, message: PushMessage) {
        self.metrics.events_received.fetch_add(1, Ordering::Relaxed);
        if let Some(sender) = self.channels.get(user) {
            let receiver_count = sender.receiver_count();
            info!(
                user,
                receiver_count,
                message = ?message,
                "notify_push file event queued"
            );
            let _ = sender.send(message);
        } else {
            info!(
                user,
                message = ?message,
                "notify_push file event dropped because user has no active subscribers"
            );
        }
    }

    fn cleanup_pre_auth(&self) {
        let ttl = self.pre_auth_ttl();
        self.pre_auth
            .retain(|_, entry| entry.created_at.elapsed() <= ttl);
    }
}

impl NotifyClientInfo {
    pub fn from_json(payload: &str) -> Result<Self, serde_json::Error> {
        let wire = serde_json::from_str::<NotifyClientInfoWire>(payload)?;
        if wire.v != 1 {
            return Ok(Self::default());
        }

        Ok(Self {
            device_name: sanitize_optional(wire.device_name, 80),
            hostname: sanitize_optional(wire.hostname, 253),
            client_name: sanitize_optional(wire.client_name, 80),
            client_version: sanitize_optional(wire.client_version, 40),
            platform: sanitize_optional(wire.platform, 20),
            os: sanitize_optional(wire.os, 80),
            device_id: sanitize_optional(wire.device_id, 80),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.device_name.is_none()
            && self.hostname.is_none()
            && self.client_name.is_none()
            && self.client_version.is_none()
            && self.platform.is_none()
            && self.os.is_none()
            && self.device_id.is_none()
    }
}

fn sanitize_optional(value: Option<String>, max_chars: usize) -> Option<String> {
    let value = value?;
    let cleaned = value
        .chars()
        .filter(|ch| !ch.is_control())
        .take(max_chars)
        .collect::<String>();
    let trimmed = cleaned.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn display_name_for_connection(connection: &NotifyConnectionSnapshot) -> String {
    connection
        .client_info
        .device_name
        .as_ref()
        .or(connection.client_info.hostname.as_ref())
        .unwrap_or(&connection.peer_addr)
        .to_owned()
}

impl NotifyMetrics {
    fn add_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn remove_connection(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn add_message(&self, message_type: MessageType) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        match message_type {
            MessageType::File => self.messages_sent_file.fetch_add(1, Ordering::Relaxed),
            MessageType::Activity => self.messages_sent_activity.fetch_add(1, Ordering::Relaxed),
            MessageType::Notification => self
                .messages_sent_notification
                .fetch_add(1, Ordering::Relaxed),
            MessageType::Custom => self.messages_sent_custom.fetch_add(1, Ordering::Relaxed),
        };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    LimitExceeded,
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::{auth::Principal, config::NotifyPushConfig, permissions};

    use super::NotifyRuntime;
    use super::{sanitize_optional, NotifyClientInfo};

    fn principal(username: &str) -> Principal {
        Principal {
            username: username.to_owned(),
            app_password_id: 1,
            app_password_label: "test".to_owned(),
            expires_at: None,
            scopes: vec![permissions::default_scope()],
        }
    }

    #[test]
    fn pre_auth_token_is_one_time() {
        let runtime = NotifyRuntime::new(NotifyPushConfig::default());
        let token = runtime.issue_pre_auth(principal("gono"));
        assert_eq!(runtime.take_pre_auth(&token).unwrap().username, "gono");
        assert!(runtime.take_pre_auth(&token).is_none());
    }

    #[test]
    fn pre_auth_token_expires() {
        let mut config = NotifyPushConfig::default();
        config.pre_auth_ttl_secs = 0;
        let runtime = NotifyRuntime::new(config);
        let token = runtime.issue_pre_auth(principal("gono"));
        thread::sleep(Duration::from_millis(1));
        assert!(runtime.take_pre_auth(&token).is_none());
    }

    #[test]
    fn client_info_is_sanitized() {
        let payload = r#"{
            "v": 1,
            "device_name": "  Test\nMac  ",
            "hostname": "host.local",
            "client_name": "Gono Cloud Desktop",
            "client_version": "0.1.0",
            "platform": "macos",
            "os": "macOS 15.5",
            "device_id": "00000000-0000-4000-8000-000000000001"
        }"#;
        let info = NotifyClientInfo::from_json(payload).expect("parse client info");
        assert_eq!(info.device_name.as_deref(), Some("TestMac"));
        assert_eq!(info.hostname.as_deref(), Some("host.local"));
        assert_eq!(
            sanitize_optional(Some("x".repeat(90)), 80).unwrap().len(),
            80
        );
    }
}
