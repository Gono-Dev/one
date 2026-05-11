use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const DEFAULT_FAILURE_DELAY_SECS: u64 = 5;
const ENTRY_TTL_SECS: u64 = 60 * 60;

#[derive(Debug, Clone)]
pub struct AuthRateLimiter {
    inner: Arc<Mutex<AuthRateLimiterInner>>,
    failure_delay: Duration,
    entry_ttl: Duration,
}

#[derive(Debug, Default)]
struct AuthRateLimiterInner {
    entries: HashMap<AuthRateKey, AuthRateEntry>,
    total_failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AuthRateKey {
    ip: String,
    username: String,
}

#[derive(Debug)]
struct AuthRateEntry {
    failures: u32,
    last_failure: Instant,
    current_delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthRateLimitStats {
    pub active_keys: usize,
    pub total_failures: u64,
    pub max_delay_secs: u64,
}

impl AuthRateLimiter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AuthRateLimiterInner::default())),
            failure_delay: Duration::from_secs(DEFAULT_FAILURE_DELAY_SECS),
            entry_ttl: Duration::from_secs(ENTRY_TTL_SECS),
        }
    }

    pub fn register_failure(&self, ip: &str, username: &str) -> Duration {
        let mut inner = self.inner.lock().expect("auth rate limiter mutex");
        self.prune_locked(&mut inner);
        inner.total_failures = inner.total_failures.saturating_add(1);

        let entry = inner
            .entries
            .entry(AuthRateKey {
                ip: ip.to_owned(),
                username: username.to_owned(),
            })
            .or_insert_with(|| AuthRateEntry {
                failures: 0,
                last_failure: Instant::now(),
                current_delay: Duration::ZERO,
            });
        entry.failures = entry.failures.saturating_add(1);
        entry.last_failure = Instant::now();
        entry.current_delay = self.failure_delay.saturating_mul(entry.failures);
        entry.current_delay
    }

    pub fn clear(&self, ip: &str, username: &str) {
        let mut inner = self.inner.lock().expect("auth rate limiter mutex");
        inner.entries.remove(&AuthRateKey {
            ip: ip.to_owned(),
            username: username.to_owned(),
        });
    }

    pub fn stats(&self) -> AuthRateLimitStats {
        let mut inner = self.inner.lock().expect("auth rate limiter mutex");
        self.prune_locked(&mut inner);
        AuthRateLimitStats {
            active_keys: inner.entries.len(),
            total_failures: inner.total_failures,
            max_delay_secs: inner
                .entries
                .values()
                .map(|entry| entry.current_delay.as_secs())
                .max()
                .unwrap_or(0),
        }
    }

    fn prune_locked(&self, inner: &mut AuthRateLimiterInner) {
        let now = Instant::now();
        inner
            .entries
            .retain(|_, entry| now.duration_since(entry.last_failure) < self.entry_ttl);
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::AuthRateLimiter;

    #[test]
    fn rate_limiter_tracks_failures_by_ip_and_username() {
        let limiter = AuthRateLimiter::new();

        assert_eq!(limiter.register_failure("192.0.2.10", "gono").as_secs(), 5);
        assert_eq!(limiter.register_failure("192.0.2.10", "gono").as_secs(), 10);
        assert_eq!(limiter.register_failure("192.0.2.11", "gono").as_secs(), 5);

        let stats = limiter.stats();
        assert_eq!(stats.active_keys, 2);
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.max_delay_secs, 10);

        limiter.clear("192.0.2.10", "gono");
        let stats = limiter.stats();
        assert_eq!(stats.active_keys, 1);
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.max_delay_secs, 5);
    }
}
