use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};

use crate::{config::DbConfig, storage};

pub const BOOTSTRAP_USER: &str = "gono";
const BOOTSTRAP_LABEL: &str = "bootstrap";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapOutcome {
    pub generated_password: Option<String>,
}

pub async fn connect(config: &DbConfig) -> anyhow::Result<SqlitePool> {
    let db_path = Path::new(&config.path);
    if let Some(parent) = db_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create database directory {}", parent.display()))?;
    }

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);

    SqlitePoolOptions::new()
        .max_connections(config.max_connections.max(1))
        .connect_with(options)
        .await
        .with_context(|| format!("open SQLite database {}", db_path.display()))
}

pub async fn migrate(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("run SQLite migrations")
}

pub async fn ensure_bootstrap_user(pool: &SqlitePool) -> anyhow::Result<BootstrapOutcome> {
    let mut tx = pool
        .begin()
        .await
        .context("begin bootstrap user transaction")?;
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT INTO users(username, display_name, enabled, created_at)
        VALUES(?1, ?1, 1, ?2)
        ON CONFLICT(username) DO UPDATE SET
            display_name = excluded.display_name,
            enabled = 1
        "#,
    )
    .bind(BOOTSTRAP_USER)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("ensure bootstrap user")?;

    let existing: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT id
        FROM app_passwords
        WHERE username = ?1 AND label = ?2
        "#,
    )
    .bind(BOOTSTRAP_USER)
    .bind(BOOTSTRAP_LABEL)
    .fetch_optional(&mut *tx)
    .await
    .context("lookup bootstrap app password")?;

    let generated_password = if existing.is_none() {
        let password = generate_app_password();
        let password_hash = hash_password(&password).context("hash bootstrap app password")?;

        sqlx::query(
            r#"
            INSERT INTO app_passwords(username, label, password_hash, created_at)
            VALUES(?1, ?2, ?3, ?4)
            "#,
        )
        .bind(BOOTSTRAP_USER)
        .bind(BOOTSTRAP_LABEL)
        .bind(password_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("insert bootstrap app password")?;

        Some(password)
    } else {
        None
    };

    tx.commit()
        .await
        .context("commit bootstrap user transaction")?;
    Ok(BootstrapOutcome { generated_password })
}

fn generate_app_password() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash_password(password: &str) -> anyhow::Result<String> {
    let mut salt_bytes = [0u8; 16];
    OsRng.fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|err| anyhow!("encode password salt: {err}"))?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|err| anyhow!("hash password: {err}"))?
        .to_string())
}

pub fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    pub id: i64,
    pub oc_file_id: String,
    pub etag: String,
    pub permissions: i64,
    pub favorite: bool,
    pub mtime_ns: i64,
    pub file_size: i64,
}

#[derive(Debug, Clone)]
pub struct FileRecordInput<'a> {
    pub owner: &'a str,
    pub rel_path: &'a Path,
    pub abs_path: &'a Path,
    pub instance_id: &'a str,
    pub xattr_ns: &'a str,
}

pub async fn ensure_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
) -> anyhow::Result<FileRecord> {
    upsert_file_record(pool, input, false).await
}

pub async fn assign_new_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
) -> anyhow::Result<FileRecord> {
    upsert_file_record(pool, input, true).await
}

pub async fn move_file_record(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;

    sqlx::query(
        r#"
        UPDATE file_ids
        SET rel_path = CASE
            WHEN rel_path = ?1 THEN ?2
            ELSE ?2 || substr(rel_path, length(?1) + 1)
        END
        WHERE owner = ?3 AND (rel_path = ?1 OR rel_path LIKE ?4)
        "#,
    )
    .bind(&from_rel)
    .bind(&to_rel)
    .bind(owner)
    .bind(format!("{from_rel}/%"))
    .execute(pool)
    .await
    .context("move file_id cache row")?;

    move_dead_props(pool, owner, from_rel_path, to_rel_path).await?;
    Ok(())
}

pub async fn delete_file_records(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let prefix = if rel_path.is_empty() {
        "%".to_owned()
    } else {
        format!("{rel_path}/%")
    };

    sqlx::query(
        r#"
        DELETE FROM file_ids
        WHERE owner = ?1 AND (rel_path = ?2 OR rel_path LIKE ?3)
        "#,
    )
    .bind(owner)
    .bind(&rel_path)
    .bind(&prefix)
    .execute(pool)
    .await
    .context("delete file_id cache rows")?;

    sqlx::query(
        r#"
        DELETE FROM dead_props
        WHERE owner = ?1 AND (rel_path = ?2 OR rel_path LIKE ?3)
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(prefix)
    .execute(pool)
    .await
    .context("delete dead property rows")?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadProp {
    pub namespace: String,
    pub name: String,
    pub xml: Vec<u8>,
}

pub async fn list_dead_props(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
) -> anyhow::Result<Vec<DeadProp>> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let rows = sqlx::query(
        r#"
        SELECT namespace, name, xml
        FROM dead_props
        WHERE owner = ?1 AND rel_path = ?2
        ORDER BY namespace, name
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .fetch_all(pool)
    .await
    .context("list dead props")?;

    rows.into_iter()
        .map(|row| {
            Ok(DeadProp {
                namespace: row.try_get("namespace")?,
                name: row.try_get("name")?,
                xml: row.try_get("xml")?,
            })
        })
        .collect()
}

pub async fn get_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");
    let xml = sqlx::query("SELECT xml FROM dead_props WHERE owner = ?1 AND rel_path = ?2 AND namespace = ?3 AND name = ?4")
        .bind(owner)
        .bind(rel_path)
        .bind(namespace)
        .bind(name)
        .fetch_optional(pool)
        .await
        .context("get dead prop")?
        .map(|row| row.try_get::<Vec<u8>, _>("xml"))
        .transpose()?;
    Ok(xml)
}

pub async fn set_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
    xml: &[u8],
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT INTO dead_props(owner, rel_path, namespace, name, xml, updated_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(owner, rel_path, namespace, name) DO UPDATE SET
            xml = excluded.xml,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(namespace)
    .bind(name)
    .bind(xml)
    .bind(now)
    .execute(pool)
    .await
    .context("set dead prop")?;
    Ok(())
}

pub async fn remove_dead_prop(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let namespace = namespace.unwrap_or("");

    sqlx::query("DELETE FROM dead_props WHERE owner = ?1 AND rel_path = ?2 AND namespace = ?3 AND name = ?4")
        .bind(owner)
        .bind(rel_path)
        .bind(namespace)
        .bind(name)
        .execute(pool)
        .await
        .context("remove dead prop")?;
    Ok(())
}

pub async fn copy_dead_props(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;
    let now = unix_timestamp();

    sqlx::query(
        r#"
        INSERT OR REPLACE INTO dead_props(owner, rel_path, namespace, name, xml, updated_at)
        SELECT owner, ?1, namespace, name, xml, ?2
        FROM dead_props
        WHERE owner = ?3 AND rel_path = ?4
        "#,
    )
    .bind(to_rel)
    .bind(now)
    .bind(owner)
    .bind(from_rel)
    .execute(pool)
    .await
    .context("copy dead props")?;
    Ok(())
}

async fn move_dead_props(
    pool: &SqlitePool,
    owner: &str,
    from_rel_path: &Path,
    to_rel_path: &Path,
) -> anyhow::Result<()> {
    let from_rel = storage::rel_path_string(from_rel_path)?;
    let to_rel = storage::rel_path_string(to_rel_path)?;
    let prefix = format!("{from_rel}/%");

    sqlx::query(
        r#"
        UPDATE dead_props
        SET rel_path = CASE
            WHEN rel_path = ?1 THEN ?2
            ELSE ?2 || substr(rel_path, length(?1) + 1)
        END
        WHERE owner = ?3 AND (rel_path = ?1 OR rel_path LIKE ?4)
        "#,
    )
    .bind(from_rel)
    .bind(to_rel)
    .bind(owner)
    .bind(prefix)
    .execute(pool)
    .await
    .context("move dead props")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogEntry {
    pub file_id: i64,
    pub rel_path: String,
    pub operation: String,
    pub sync_token: i64,
}

pub async fn record_change(
    pool: &SqlitePool,
    owner: &str,
    file_id: i64,
    rel_path: &Path,
    operation: &str,
) -> anyhow::Result<i64> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let now = unix_timestamp();
    let mut tx = pool
        .begin()
        .await
        .context("begin record_change transaction")?;

    let (token,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO sync_tokens(owner, token) VALUES(?1, 1)
        ON CONFLICT(owner) DO UPDATE SET token = token + 1
        RETURNING token
        "#,
    )
    .bind(owner)
    .fetch_one(&mut *tx)
    .await
    .context("allocate sync token")?;

    sqlx::query(
        r#"
        INSERT INTO change_log(owner, file_id, rel_path, operation, sync_token, changed_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        "#,
    )
    .bind(owner)
    .bind(file_id)
    .bind(rel_path)
    .bind(operation)
    .bind(token)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("insert change_log row")?;

    tx.commit()
        .await
        .context("commit record_change transaction")?;
    Ok(token)
}

pub async fn current_sync_token(pool: &SqlitePool, owner: &str) -> anyhow::Result<i64> {
    let token = sqlx::query("SELECT token FROM sync_tokens WHERE owner = ?1")
        .bind(owner)
        .fetch_optional(pool)
        .await
        .context("load current sync token")?
        .map(|row| row.try_get::<i64, _>("token"))
        .transpose()?
        .unwrap_or(0);
    Ok(token)
}

pub async fn list_change_log(
    pool: &SqlitePool,
    owner: &str,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    list_change_log_since(pool, owner, 0).await
}

pub async fn list_change_log_since(
    pool: &SqlitePool,
    owner: &str,
    since_token: i64,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    list_change_log_range(pool, owner, since_token, i64::MAX).await
}

pub async fn list_change_log_range(
    pool: &SqlitePool,
    owner: &str,
    since_token: i64,
    until_token: i64,
) -> anyhow::Result<Vec<ChangeLogEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT file_id, rel_path, operation, sync_token
        FROM change_log
        WHERE owner = ?1 AND sync_token > ?2 AND sync_token <= ?3
        ORDER BY sync_token ASC
        "#,
    )
    .bind(owner)
    .bind(since_token)
    .bind(until_token)
    .fetch_all(pool)
    .await
    .context("list change_log rows")?;

    rows.into_iter()
        .map(|row| {
            Ok(ChangeLogEntry {
                file_id: row.try_get("file_id")?,
                rel_path: row.try_get("rel_path")?,
                operation: row.try_get("operation")?,
                sync_token: row.try_get("sync_token")?,
            })
        })
        .collect()
}

const UPLOAD_SESSION_TTL_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSession {
    pub upload_id: String,
    pub owner: String,
    pub target_path: String,
    pub total_size: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: i64,
}

pub async fn upsert_upload_session(
    pool: &SqlitePool,
    upload_id: &str,
    owner: &str,
    target_path: &Path,
    total_size: i64,
) -> anyhow::Result<UploadSession> {
    let target_path = storage::rel_path_string(target_path)?;
    let now = unix_timestamp();
    let expires_at = now + UPLOAD_SESSION_TTL_SECS;

    sqlx::query(
        r#"
        INSERT INTO upload_sessions(upload_id, owner, target_path, total_size, created_at, updated_at, expires_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?5, ?6)
        ON CONFLICT(upload_id) DO UPDATE SET
            owner = excluded.owner,
            target_path = excluded.target_path,
            total_size = excluded.total_size,
            updated_at = excluded.updated_at,
            expires_at = excluded.expires_at
        "#,
    )
    .bind(upload_id)
    .bind(owner)
    .bind(&target_path)
    .bind(total_size)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await
    .context("upsert upload session")?;

    load_upload_session(pool, owner, upload_id)
        .await?
        .context("upload session missing after upsert")
}

pub async fn load_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
) -> anyhow::Result<Option<UploadSession>> {
    let row = sqlx::query(
        r#"
        SELECT upload_id, owner, target_path, total_size, created_at, updated_at, expires_at
        FROM upload_sessions
        WHERE owner = ?1 AND upload_id = ?2
        "#,
    )
    .bind(owner)
    .bind(upload_id)
    .fetch_optional(pool)
    .await
    .context("load upload session")?;

    row.map(upload_session_from_row).transpose()
}

pub async fn touch_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
    total_size: Option<i64>,
) -> anyhow::Result<()> {
    let now = unix_timestamp();
    let expires_at = now + UPLOAD_SESSION_TTL_SECS;

    sqlx::query(
        r#"
        UPDATE upload_sessions
        SET total_size = COALESCE(?1, total_size),
            updated_at = ?2,
            expires_at = ?3
        WHERE owner = ?4 AND upload_id = ?5
        "#,
    )
    .bind(total_size)
    .bind(now)
    .bind(expires_at)
    .bind(owner)
    .bind(upload_id)
    .execute(pool)
    .await
    .context("touch upload session")?;

    Ok(())
}

pub async fn delete_upload_session(
    pool: &SqlitePool,
    owner: &str,
    upload_id: &str,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM upload_sessions WHERE owner = ?1 AND upload_id = ?2")
        .bind(owner)
        .bind(upload_id)
        .execute(pool)
        .await
        .context("delete upload session")?;
    Ok(())
}

pub async fn list_expired_upload_sessions(
    pool: &SqlitePool,
    cutoff: i64,
) -> anyhow::Result<Vec<UploadSession>> {
    let rows = sqlx::query(
        r#"
        SELECT upload_id, owner, target_path, total_size, created_at, updated_at, expires_at
        FROM upload_sessions
        WHERE expires_at < ?1
        ORDER BY expires_at ASC
        "#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .context("list expired upload sessions")?;

    rows.into_iter().map(upload_session_from_row).collect()
}

fn upload_session_from_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<UploadSession> {
    Ok(UploadSession {
        upload_id: row.try_get("upload_id")?,
        owner: row.try_get("owner")?,
        target_path: row.try_get("target_path")?,
        total_size: row.try_get("total_size")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

async fn upsert_file_record(
    pool: &SqlitePool,
    input: FileRecordInput<'_>,
    force_new_id: bool,
) -> anyhow::Result<FileRecord> {
    let rel_path = storage::rel_path_string(input.rel_path)?;
    let (mtime_ns, file_size) = storage::metadata_fingerprint(input.abs_path)?;
    let etag = derive_etag(mtime_ns, file_size);
    let existing = load_by_rel_path(pool, input.owner, &rel_path).await?;

    if !force_new_id {
        if let Some(existing) = existing.as_ref() {
            if existing.is_fresh(mtime_ns, file_size) {
                return Ok(existing.to_file_record(input.instance_id));
            }
        }
    }

    let id = if force_new_id {
        delete_by_rel_path(pool, input.owner, &rel_path).await?;
        insert_file_record(pool, input.owner, &rel_path).await?
    } else if let Some(existing) = existing.as_ref() {
        existing.id
    } else if let Some(xattr_id) = read_i64_xattr(input.abs_path, input.xattr_ns, "fileid")? {
        attach_xattr_file_id(pool, input.owner, &rel_path, xattr_id).await?
    } else {
        insert_file_record(pool, input.owner, &rel_path).await?
    };

    let favorite = if force_new_id {
        false
    } else {
        read_bool_xattr(input.abs_path, input.xattr_ns, "favorite")?
            .or_else(|| existing.as_ref().map(|row| row.favorite))
            .unwrap_or(false)
    };
    let permissions = if force_new_id {
        0x3f
    } else {
        read_i64_xattr(input.abs_path, input.xattr_ns, "perms")?
            .or_else(|| existing.as_ref().and_then(|row| row.permissions))
            .unwrap_or(0x3f)
    };

    write_xattr(input.abs_path, input.xattr_ns, "fileid", &id.to_string())?;
    write_xattr(input.abs_path, input.xattr_ns, "etag", &etag)?;
    write_xattr(
        input.abs_path,
        input.xattr_ns,
        "favorite",
        if favorite { "1" } else { "0" },
    )?;
    write_xattr(
        input.abs_path,
        input.xattr_ns,
        "perms",
        &permissions.to_string(),
    )?;

    sqlx::query(
        r#"
        UPDATE file_ids
        SET etag = ?1,
            permissions = ?2,
            favorite = ?3,
            mtime_ns = ?4,
            file_size = ?5
        WHERE owner = ?6 AND rel_path = ?7
        "#,
    )
    .bind(&etag)
    .bind(permissions)
    .bind(if favorite { 1 } else { 0 })
    .bind(mtime_ns)
    .bind(file_size)
    .bind(input.owner)
    .bind(&rel_path)
    .execute(pool)
    .await
    .context("update file metadata cache")?;

    Ok(FileRecord {
        id,
        oc_file_id: format!("{id}{}", input.instance_id),
        etag,
        permissions,
        favorite,
        mtime_ns,
        file_size,
    })
}

pub async fn set_favorite(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &Path,
    abs_path: &Path,
    instance_id: &str,
    xattr_ns: &str,
    favorite: bool,
) -> anyhow::Result<FileRecord> {
    let mut record = ensure_file_record(
        pool,
        FileRecordInput {
            owner,
            rel_path,
            abs_path,
            instance_id,
            xattr_ns,
        },
    )
    .await?;
    let rel_path = storage::rel_path_string(rel_path)?;

    write_xattr(
        abs_path,
        xattr_ns,
        "favorite",
        if favorite { "1" } else { "0" },
    )?;
    sqlx::query(
        r#"
        UPDATE file_ids
        SET favorite = ?1
        WHERE owner = ?2 AND rel_path = ?3
        "#,
    )
    .bind(if favorite { 1 } else { 0 })
    .bind(owner)
    .bind(rel_path)
    .execute(pool)
    .await
    .context("update favorite")?;

    record.favorite = favorite;
    Ok(record)
}

async fn load_by_rel_path(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &str,
) -> anyhow::Result<Option<FileRecordRow>> {
    let row = sqlx::query(
        r#"
        SELECT id, etag, permissions, favorite, mtime_ns, file_size
        FROM file_ids
        WHERE owner = ?1 AND rel_path = ?2
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .fetch_optional(pool)
    .await
    .context("load file_id row")?;

    row.map(FileRecordRow::try_from_row).transpose()
}

async fn delete_by_rel_path(pool: &SqlitePool, owner: &str, rel_path: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM file_ids WHERE owner = ?1 AND rel_path = ?2")
        .bind(owner)
        .bind(rel_path)
        .execute(pool)
        .await
        .context("delete existing file_id row")?;
    Ok(())
}

async fn insert_file_record(pool: &SqlitePool, owner: &str, rel_path: &str) -> anyhow::Result<i64> {
    let now = unix_timestamp();
    let result = sqlx::query(
        r#"
        INSERT INTO file_ids(owner, rel_path, permissions, favorite, created_at)
        VALUES(?1, ?2, ?3, 0, ?4)
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .bind(0x3f_i64)
    .bind(now)
    .execute(pool)
    .await
    .context("insert file_id row")?;

    Ok(result.last_insert_rowid())
}

async fn attach_xattr_file_id(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &str,
    id: i64,
) -> anyhow::Result<i64> {
    let now = unix_timestamp();
    let updated = sqlx::query(
        r#"
        UPDATE file_ids
        SET rel_path = ?1
        WHERE owner = ?2 AND id = ?3
        "#,
    )
    .bind(rel_path)
    .bind(owner)
    .bind(id)
    .execute(pool)
    .await
    .context("reattach xattr file_id row")?
    .rows_affected();

    if updated == 0 {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO file_ids(id, owner, rel_path, permissions, favorite, created_at)
            VALUES(?1, ?2, ?3, ?4, 0, ?5)
            "#,
        )
        .bind(id)
        .bind(owner)
        .bind(rel_path)
        .bind(0x3f_i64)
        .bind(now)
        .execute(pool)
        .await
        .context("insert xattr file_id row")?;
    }

    Ok(id)
}

#[derive(Debug, Clone)]
struct FileRecordRow {
    id: i64,
    etag: Option<String>,
    permissions: Option<i64>,
    favorite: bool,
    mtime_ns: Option<i64>,
    file_size: Option<i64>,
}

impl FileRecordRow {
    fn try_from_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            etag: row.try_get("etag")?,
            permissions: row.try_get("permissions")?,
            favorite: row.try_get::<i64, _>("favorite")? != 0,
            mtime_ns: row.try_get("mtime_ns")?,
            file_size: row.try_get("file_size")?,
        })
    }

    fn is_fresh(&self, mtime_ns: i64, file_size: i64) -> bool {
        self.etag.is_some() && self.mtime_ns == Some(mtime_ns) && self.file_size == Some(file_size)
    }

    fn to_file_record(&self, instance_id: &str) -> FileRecord {
        let mtime_ns = self.mtime_ns.unwrap_or_default();
        let file_size = self.file_size.unwrap_or_default();
        FileRecord {
            id: self.id,
            oc_file_id: format!("{}{}", self.id, instance_id),
            etag: self
                .etag
                .clone()
                .unwrap_or_else(|| derive_etag(mtime_ns, file_size)),
            permissions: self.permissions.unwrap_or(0x3f),
            favorite: self.favorite,
            mtime_ns,
            file_size,
        }
    }
}

fn derive_etag(mtime_ns: i64, file_size: i64) -> String {
    format!("{file_size:x}-{mtime_ns:x}")
}

fn xattr_key(namespace: &str, name: &str) -> String {
    format!("{namespace}.{name}")
}

fn read_i64_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<i64>> {
    let Some(raw) = read_xattr(path, namespace, name)? else {
        return Ok(None);
    };
    let value = String::from_utf8(raw).context("xattr value is not UTF-8")?;
    value
        .parse::<i64>()
        .map(Some)
        .with_context(|| format!("parse xattr {name} as integer"))
}

fn read_bool_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<bool>> {
    let Some(raw) = read_xattr(path, namespace, name)? else {
        return Ok(None);
    };
    let value = String::from_utf8(raw).context("xattr value is not UTF-8")?;
    Ok(Some(value == "1" || value.eq_ignore_ascii_case("true")))
}

fn read_xattr(path: &Path, namespace: &str, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
    xattr::get(path, xattr_key(namespace, name))
        .with_context(|| format!("read xattr {namespace}.{name} from {}", path.display()))
}

fn write_xattr(path: &Path, namespace: &str, name: &str, value: &str) -> anyhow::Result<()> {
    xattr::set(path, xattr_key(namespace, name), value.as_bytes())
        .with_context(|| format!("write xattr {namespace}.{name} to {}", path.display()))
}
