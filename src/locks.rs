use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use dav_server::{
    davpath::DavPath,
    ls::{DavLock, DavLockSystem, LsFuture},
};
use futures_util::FutureExt;
use sqlx::{Row, SqlitePool};
use tokio::sync::Mutex;
use tracing::error;
use uuid::Uuid;
use xmltree::{Element, Namespace, XMLNode};

use crate::db;

#[derive(Debug, Clone)]
pub struct SqliteLs {
    pool: SqlitePool,
    gate: Arc<Mutex<()>>,
}

impl SqliteLs {
    pub fn new(pool: SqlitePool) -> Box<Self> {
        Box::new(Self {
            pool,
            gate: Arc::new(Mutex::new(())),
        })
    }

    async fn lock_inner(
        &self,
        path: DavPath,
        principal: Option<String>,
        owner: Option<Element>,
        timeout: Option<Duration>,
        shared: bool,
        deep: bool,
    ) -> anyhow::Result<Result<DavLock, DavLock>> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        let locks = self.load_locks().await?;
        if let Err(lock) = check_locks_to_path(&locks, &path, None, true, &[], shared) {
            return Ok(Err(lock));
        }
        if deep {
            if let Err(lock) = check_locks_from_path(&locks, &path, None, true, &[], shared) {
                return Ok(Err(lock));
            }
        }

        let timeout_at = timeout.map(|duration| SystemTime::now() + duration);
        let lock = DavLock {
            token: Uuid::new_v4().urn().to_string(),
            path: Box::new(path),
            principal,
            owner: owner.map(Box::new),
            timeout_at,
            timeout,
            shared,
            deep,
        };
        self.insert_lock(&lock).await?;
        Ok(Ok(lock))
    }

    async fn unlock_inner(&self, path: DavPath, token: String) -> anyhow::Result<Result<(), ()>> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        let locks = self.load_locks().await?;
        if !locks
            .iter()
            .any(|lock| lock.token == token && path_is_ancestor_or_same(&lock.path, &path))
        {
            return Ok(Err(()));
        }

        let affected = sqlx::query("DELETE FROM webdav_locks WHERE token = ?1")
            .bind(token)
            .execute(&self.pool)
            .await
            .context("delete WebDAV lock")?
            .rows_affected();
        Ok(if affected == 0 { Err(()) } else { Ok(()) })
    }

    async fn refresh_inner(
        &self,
        path: DavPath,
        token: String,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Result<DavLock, ()>> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        let mut locks = self.load_locks().await?;
        let Some(lock) = locks
            .iter_mut()
            .find(|lock| lock.token == token && path_is_ancestor_or_same(&lock.path, &path))
        else {
            return Ok(Err(()));
        };

        lock.timeout = timeout;
        lock.timeout_at = timeout.map(|duration| SystemTime::now() + duration);
        sqlx::query(
            r#"
            UPDATE webdav_locks
            SET timeout_at = ?1, timeout_secs = ?2
            WHERE token = ?3
            "#,
        )
        .bind(lock.timeout_at.map(system_time_to_unix))
        .bind(lock.timeout.map(|duration| duration.as_secs() as i64))
        .bind(&token)
        .execute(&self.pool)
        .await
        .context("refresh WebDAV lock")?;
        Ok(Ok(lock.clone()))
    }

    async fn check_inner(
        &self,
        path: DavPath,
        principal: Option<String>,
        ignore_principal: bool,
        deep: bool,
        submitted_tokens: Vec<String>,
    ) -> anyhow::Result<Result<(), DavLock>> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        let locks = self.load_locks().await?;
        if let Err(lock) = check_locks_to_path(
            &locks,
            &path,
            principal.as_deref(),
            ignore_principal,
            &submitted_tokens,
            false,
        ) {
            return Ok(Err(lock));
        }
        if deep {
            if let Err(lock) = check_locks_from_path(
                &locks,
                &path,
                principal.as_deref(),
                ignore_principal,
                &submitted_tokens,
                false,
            ) {
                return Ok(Err(lock));
            }
        }
        Ok(Ok(()))
    }

    async fn discover_inner(&self, path: DavPath) -> anyhow::Result<Vec<DavLock>> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        Ok(self
            .load_locks()
            .await?
            .into_iter()
            .filter(|lock| path_is_ancestor_or_same(&lock.path, &path))
            .collect())
    }

    async fn delete_inner(&self, path: DavPath) -> anyhow::Result<()> {
        let _guard = self.gate.lock().await;
        self.prune_expired().await?;
        let locks = self.load_locks().await?;
        for token in locks
            .into_iter()
            .filter(|lock| path_is_same_or_descendant(&lock.path, &path))
            .map(|lock| lock.token)
        {
            sqlx::query("DELETE FROM webdav_locks WHERE token = ?1")
                .bind(token)
                .execute(&self.pool)
                .await
                .context("delete WebDAV lock subtree entry")?;
        }
        Ok(())
    }

    async fn insert_lock(&self, lock: &DavLock) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO webdav_locks(
                token, path, principal, owner_xml, timeout_at, timeout_secs,
                shared, deep, created_at
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
        )
        .bind(&lock.token)
        .bind(lock.path.as_url_string())
        .bind(lock.principal.as_deref())
        .bind(serialize_owner(lock.owner.as_deref())?)
        .bind(lock.timeout_at.map(system_time_to_unix))
        .bind(lock.timeout.map(|duration| duration.as_secs() as i64))
        .bind(if lock.shared { 1 } else { 0 })
        .bind(if lock.deep { 1 } else { 0 })
        .bind(db::unix_timestamp())
        .execute(&self.pool)
        .await
        .context("insert WebDAV lock")?;
        Ok(())
    }

    async fn load_locks(&self) -> anyhow::Result<Vec<DavLock>> {
        let now = db::unix_timestamp();
        let rows = sqlx::query(
            r#"
            SELECT token, path, principal, owner_xml, timeout_at, timeout_secs, shared, deep
            FROM webdav_locks
            WHERE timeout_at IS NULL OR timeout_at > ?1
            ORDER BY path ASC
            "#,
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("load WebDAV locks")?;

        rows.into_iter()
            .map(|row| {
                let path: String = row.try_get("path")?;
                let owner_xml: Option<String> = row.try_get("owner_xml")?;
                Ok(DavLock {
                    token: row.try_get("token")?,
                    path: Box::new(
                        DavPath::new(&path).with_context(|| format!("parse lock path {path}"))?,
                    ),
                    principal: row.try_get("principal")?,
                    owner: owner_xml.and_then(|xml| match Element::parse(xml.as_bytes()) {
                        Ok(owner) => Some(Box::new(owner)),
                        Err(err) => {
                            error!(?err, "discarding invalid persisted WebDAV lock owner XML");
                            None
                        }
                    }),
                    timeout_at: row
                        .try_get::<Option<i64>, _>("timeout_at")?
                        .map(unix_to_system_time),
                    timeout: row
                        .try_get::<Option<i64>, _>("timeout_secs")?
                        .map(|secs| Duration::from_secs(secs.max(0) as u64)),
                    shared: row.try_get::<i64, _>("shared")? != 0,
                    deep: row.try_get::<i64, _>("deep")? != 0,
                })
            })
            .collect()
    }

    async fn prune_expired(&self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM webdav_locks WHERE timeout_at IS NOT NULL AND timeout_at <= ?1")
            .bind(db::unix_timestamp())
            .execute(&self.pool)
            .await
            .context("prune expired WebDAV locks")?;
        Ok(())
    }
}

impl DavLockSystem for SqliteLs {
    fn lock(
        &'_ self,
        path: &DavPath,
        principal: Option<&str>,
        owner: Option<&Element>,
        timeout: Option<Duration>,
        shared: bool,
        deep: bool,
    ) -> LsFuture<'_, Result<DavLock, DavLock>> {
        let this = self.clone();
        let path = path.clone();
        let principal = principal.map(str::to_owned);
        let owner = owner.cloned();
        async move {
            match this
                .lock_inner(
                    path.clone(),
                    principal.clone(),
                    owner,
                    timeout,
                    shared,
                    deep,
                )
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    error!(?err, "failed to create WebDAV lock");
                    Err(synthetic_conflict_lock(path, principal))
                }
            }
        }
        .boxed()
    }

    fn unlock(&'_ self, path: &DavPath, token: &str) -> LsFuture<'_, Result<(), ()>> {
        let this = self.clone();
        let path = path.clone();
        let token = token.to_owned();
        async move {
            match this.unlock_inner(path, token).await {
                Ok(result) => result,
                Err(err) => {
                    error!(?err, "failed to unlock WebDAV lock");
                    Err(())
                }
            }
        }
        .boxed()
    }

    fn refresh(
        &'_ self,
        path: &DavPath,
        token: &str,
        timeout: Option<Duration>,
    ) -> LsFuture<'_, Result<DavLock, ()>> {
        let this = self.clone();
        let path = path.clone();
        let token = token.to_owned();
        async move {
            match this.refresh_inner(path, token, timeout).await {
                Ok(result) => result,
                Err(err) => {
                    error!(?err, "failed to refresh WebDAV lock");
                    Err(())
                }
            }
        }
        .boxed()
    }

    fn check(
        &'_ self,
        path: &DavPath,
        principal: Option<&str>,
        ignore_principal: bool,
        deep: bool,
        submitted_tokens: &[String],
    ) -> LsFuture<'_, Result<(), DavLock>> {
        let this = self.clone();
        let path = path.clone();
        let principal = principal.map(str::to_owned);
        let submitted_tokens = submitted_tokens.to_vec();
        async move {
            match this
                .check_inner(
                    path.clone(),
                    principal.clone(),
                    ignore_principal,
                    deep,
                    submitted_tokens,
                )
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    error!(?err, "failed to check WebDAV locks");
                    Err(synthetic_conflict_lock(path, principal))
                }
            }
        }
        .boxed()
    }

    fn discover(&'_ self, path: &DavPath) -> LsFuture<'_, Vec<DavLock>> {
        let this = self.clone();
        let path = path.clone();
        async move {
            match this.discover_inner(path).await {
                Ok(locks) => locks,
                Err(err) => {
                    error!(?err, "failed to discover WebDAV locks");
                    Vec::new()
                }
            }
        }
        .boxed()
    }

    fn delete(&'_ self, path: &DavPath) -> LsFuture<'_, Result<(), ()>> {
        let this = self.clone();
        let path = path.clone();
        async move {
            match this.delete_inner(path).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    error!(?err, "failed to delete WebDAV lock subtree");
                    Err(())
                }
            }
        }
        .boxed()
    }
}

fn check_locks_to_path(
    locks: &[DavLock],
    path: &DavPath,
    principal: Option<&str>,
    ignore_principal: bool,
    submitted_tokens: &[String],
    shared_ok: bool,
) -> Result<(), DavLock> {
    let mut holds_lock = false;
    let mut first_shared_lock = None;

    for lock in locks {
        if !path_is_ancestor_or_same(&lock.path, path) {
            continue;
        }
        if !path_is_same(&lock.path, path) && !lock.deep {
            continue;
        }
        if lock_is_owned(lock, principal, ignore_principal, submitted_tokens) {
            holds_lock = true;
        } else if !lock.shared {
            return Err(lock.clone());
        } else if !shared_ok {
            first_shared_lock.get_or_insert_with(|| lock.clone());
        }
    }

    if !holds_lock {
        if let Some(lock) = first_shared_lock {
            return Err(lock);
        }
    }

    Ok(())
}

fn check_locks_from_path(
    locks: &[DavLock],
    path: &DavPath,
    principal: Option<&str>,
    ignore_principal: bool,
    submitted_tokens: &[String],
    shared_ok: bool,
) -> Result<(), DavLock> {
    for lock in locks {
        if path_is_same_or_descendant(&lock.path, path)
            && (!lock.shared || !shared_ok)
            && !lock_is_owned(lock, principal, ignore_principal, submitted_tokens)
        {
            return Err(lock.clone());
        }
    }
    Ok(())
}

fn lock_is_owned(
    lock: &DavLock,
    principal: Option<&str>,
    ignore_principal: bool,
    submitted_tokens: &[String],
) -> bool {
    submitted_tokens.iter().any(|token| token == &lock.token)
        && (ignore_principal || principal == lock.principal.as_deref())
}

fn path_is_same(left: &DavPath, right: &DavPath) -> bool {
    path_segments(left) == path_segments(right)
}

fn path_is_ancestor_or_same(ancestor: &DavPath, path: &DavPath) -> bool {
    let ancestor = path_segments(ancestor);
    let path = path_segments(path);
    ancestor.len() <= path.len() && ancestor.iter().zip(path.iter()).all(|(a, b)| a == b)
}

fn path_is_same_or_descendant(path: &DavPath, root: &DavPath) -> bool {
    path_is_ancestor_or_same(root, path)
}

fn path_segments(path: &DavPath) -> Vec<&[u8]> {
    path.as_bytes()
        .split(|byte| *byte == b'/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn serialize_owner(owner: Option<&Element>) -> anyhow::Result<Option<String>> {
    let Some(owner) = owner else {
        return Ok(None);
    };
    let mut owner = owner.clone();
    ensure_dav_namespace(&mut owner);
    let mut buffer = Vec::new();
    owner
        .write(&mut buffer)
        .context("serialize WebDAV lock owner XML")?;
    Ok(Some(
        String::from_utf8(buffer).context("owner XML is not UTF-8")?,
    ))
}

fn ensure_dav_namespace(element: &mut Element) {
    if element.prefix.as_deref() == Some("D") {
        if element.namespace.is_none() {
            element.namespace = Some("DAV:".to_owned());
        }
        let mut namespaces = element.namespaces.take().unwrap_or_else(Namespace::empty);
        namespaces.put("D", "DAV:");
        element.namespaces = Some(namespaces);
    }

    for child in &mut element.children {
        if let XMLNode::Element(child) = child {
            ensure_dav_namespace(child);
        }
    }
}

fn system_time_to_unix(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn unix_to_system_time(secs: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs.max(0) as u64)
}

fn synthetic_conflict_lock(path: DavPath, principal: Option<String>) -> DavLock {
    DavLock {
        token: "urn:uuid:00000000-0000-0000-0000-000000000000".to_owned(),
        path: Box::new(path),
        principal,
        owner: None,
        timeout_at: None,
        timeout: None,
        shared: false,
        deep: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DbConfig;

    async fn temp_pool(temp: &tempfile::TempDir) -> SqlitePool {
        let config = DbConfig {
            path: temp
                .path()
                .join("gono-cloud.db")
                .to_string_lossy()
                .into_owned(),
            max_connections: 1,
        };
        let pool = db::connect(&config).await.expect("connect sqlite");
        db::migrate(&pool).await.expect("migrate sqlite");
        pool
    }

    #[tokio::test]
    async fn locks_persist_across_locksystem_instances() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let first = SqliteLs::new(pool.clone());
        let path = DavPath::new("/locked.txt").expect("lock path");
        let lock = first
            .lock(
                &path,
                Some("gono"),
                None,
                Some(Duration::from_secs(300)),
                false,
                false,
            )
            .await
            .expect("create lock");

        let second = SqliteLs::new(pool);
        let conflict = second
            .check(&path, Some("gono"), false, false, &[])
            .await
            .expect_err("lock persisted");
        assert_eq!(conflict.token, lock.token);

        second
            .unlock(&path, &lock.token)
            .await
            .expect("unlock persisted lock");
        second
            .check(&path, Some("gono"), false, false, &[])
            .await
            .expect("lock removed");
    }

    #[tokio::test]
    async fn default_namespace_lock_owner_survives_persistence() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let first = SqliteLs::new(pool.clone());
        let path = DavPath::new("/lock-owner.txt").expect("lock path");
        let root = Element::parse(
            br#"<lockinfo xmlns="DAV:"><owner>litmus test suite</owner></lockinfo>"#.as_slice(),
        )
        .expect("parse lockinfo");
        let mut owner = root.get_child("owner").expect("owner element").clone();
        owner.prefix = Some("D".to_owned());

        let lock = first
            .lock(
                &path,
                Some("gono"),
                Some(&owner),
                Some(Duration::from_secs(300)),
                false,
                false,
            )
            .await
            .expect("create lock");

        let second = SqliteLs::new(pool);
        let locks = second.discover(&path).await;
        let discovered = locks
            .iter()
            .find(|candidate| candidate.token == lock.token)
            .expect("persisted lock");
        let owner = discovered.owner.as_deref().expect("persisted owner");
        assert_eq!(owner.get_text().as_deref(), Some("litmus test suite"));

        let mut rendered = Vec::new();
        owner.write(&mut rendered).expect("render owner XML");
        let rendered = String::from_utf8(rendered).expect("owner XML is UTF-8");
        assert!(rendered.contains("xmlns:D=\"DAV:\""));
    }

    #[tokio::test]
    async fn expired_locks_are_pruned() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let pool = temp_pool(&temp).await;
        let ls = SqliteLs::new(pool.clone());
        sqlx::query(
            r#"
            INSERT INTO webdav_locks(
                token, path, shared, deep, timeout_at, timeout_secs, created_at
            )
            VALUES(?1, ?2, 0, 0, ?3, 1, ?4)
            "#,
        )
        .bind("urn:uuid:expired")
        .bind("/expired.txt")
        .bind(db::unix_timestamp() - 1)
        .bind(db::unix_timestamp() - 2)
        .execute(&pool)
        .await
        .expect("insert expired lock");

        let path = DavPath::new("/expired.txt").expect("path");
        assert!(ls.discover(&path).await.is_empty());
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webdav_locks")
            .fetch_one(&pool)
            .await
            .expect("count lock rows");
        assert_eq!(rows, 0);
    }
}
