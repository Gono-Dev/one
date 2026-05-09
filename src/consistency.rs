use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
    Row, SqlitePool,
};

use crate::{
    config::Config,
    db::{self, BOOTSTRAP_USER},
    storage,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyReport {
    pub stats: ConsistencyStats,
    pub issues: Vec<ConsistencyIssue>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsistencyStats {
    pub file_records: usize,
    pub filesystem_entries: usize,
    pub dead_props: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyIssue {
    pub kind: ConsistencyIssueKind,
    pub owner: Option<String>,
    pub rel_path: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    pub mode: RepairMode,
    pub before: ConsistencyReport,
    pub actions: Vec<RepairAction>,
    pub after: Option<ConsistencyReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairMode {
    DryRun,
    Apply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairAction {
    pub kind: RepairActionKind,
    pub owner: String,
    pub rel_path: String,
    pub detail: String,
    pub applied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RepairActionKind {
    DeleteFileRecord,
    EnsureFileRecord,
    RefreshFileMetadata,
    DeleteDeadProps,
    SkipUnsafe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyIssueKind {
    InvalidFileRecordPath,
    OrphanFileRecord,
    StaleFileRecordCache,
    MissingFileRecord,
    MissingXattr,
    InvalidXattr,
    XattrMismatch,
    XattrWithoutFileRecord,
    DeadPropWithoutFile,
    DeadPropWithoutFileRecord,
    InvalidDeadPropPath,
}

impl ConsistencyReport {
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn render_text(&self) -> String {
        let mut output = format!(
            concat!(
                "gono-one consistency check\n",
                "file_records: {}\n",
                "filesystem_entries: {}\n",
                "dead_props: {}\n",
                "issues: {}\n",
            ),
            self.stats.file_records,
            self.stats.filesystem_entries,
            self.stats.dead_props,
            self.issues.len(),
        );

        for issue in &self.issues {
            output.push_str(&format!(
                "- {:?} owner={} path={} detail={}\n",
                issue.kind,
                issue.owner.as_deref().unwrap_or("-"),
                issue.rel_path.as_deref().unwrap_or("-"),
                issue.detail,
            ));
        }

        output
    }
}

impl RepairReport {
    pub fn render_text(&self) -> String {
        let mut output = format!(
            concat!(
                "gono-one consistency repair ({})\n",
                "before_issues: {}\n",
                "actions: {}\n",
            ),
            match self.mode {
                RepairMode::DryRun => "dry-run",
                RepairMode::Apply => "apply",
            },
            self.before.issues.len(),
            self.actions.len(),
        );

        for action in &self.actions {
            output.push_str(&format!(
                "- {:?} applied={} owner={} path={} detail={}\n",
                action.kind, action.applied, action.owner, action.rel_path, action.detail,
            ));
        }

        if let Some(after) = &self.after {
            output.push_str(&format!("after_issues: {}\n", after.issues.len()));
            for issue in &after.issues {
                output.push_str(&format!(
                    "- remaining {:?} owner={} path={} detail={}\n",
                    issue.kind,
                    issue.owner.as_deref().unwrap_or("-"),
                    issue.rel_path.as_deref().unwrap_or("-"),
                    issue.detail,
                ));
            }
        }

        output
    }
}

#[derive(Debug, Clone)]
struct FileRecordRow {
    id: i64,
    owner: String,
    rel_path: String,
    etag: Option<String>,
    permissions: Option<i64>,
    favorite: bool,
    mtime_ns: Option<i64>,
    file_size: Option<i64>,
}

#[derive(Debug, Clone)]
struct DeadPropRow {
    owner: String,
    rel_path: String,
    namespace: String,
    name: String,
}

#[derive(Debug, Clone)]
struct FsEntry {
    rel_path: String,
    abs_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct EntryXattrs {
    file_id: Option<String>,
    etag: Option<String>,
    favorite: Option<String>,
    permissions: Option<String>,
}

pub async fn check(config: &Config) -> Result<ConsistencyReport> {
    let roots = ExistingStorageRoots::load(config)?;
    let pool = connect_read_only(&config.db.path, config.db.max_connections).await?;
    let file_records = load_file_records(&pool).await?;
    let dead_props = load_dead_props(&pool).await?;
    let filesystem_entries = collect_filesystem_entries(&roots.files_root)?;

    let mut report = ConsistencyReport {
        stats: ConsistencyStats {
            file_records: file_records.len(),
            filesystem_entries: filesystem_entries.len(),
            dead_props: dead_props.len(),
        },
        issues: Vec::new(),
    };

    let records_by_path: BTreeMap<(String, String), FileRecordRow> = file_records
        .iter()
        .cloned()
        .map(|row| ((row.owner.clone(), row.rel_path.clone()), row))
        .collect();
    let records_by_id: BTreeMap<(String, i64), FileRecordRow> = file_records
        .iter()
        .cloned()
        .map(|row| ((row.owner.clone(), row.id), row))
        .collect();
    let fs_by_path: BTreeMap<String, FsEntry> = filesystem_entries
        .iter()
        .cloned()
        .map(|entry| (entry.rel_path.clone(), entry))
        .collect();

    check_file_records(&roots, &config.storage.xattr_ns, &file_records, &mut report);
    check_filesystem_entries(
        &config.storage.xattr_ns,
        &records_by_path,
        &records_by_id,
        &filesystem_entries,
        &mut report,
    );
    check_dead_props(&dead_props, &records_by_path, &fs_by_path, &mut report);

    report.issues.sort_by(|left, right| {
        format!("{:?}", left.kind)
            .cmp(&format!("{:?}", right.kind))
            .then_with(|| left.owner.cmp(&right.owner))
            .then_with(|| left.rel_path.cmp(&right.rel_path))
            .then_with(|| left.detail.cmp(&right.detail))
    });

    Ok(report)
}

pub async fn repair(config: &Config, mode: RepairMode) -> Result<RepairReport> {
    let before = check(config).await?;
    let roots = ExistingStorageRoots::load(config)?;
    let pool = db::connect(&config.db).await?;
    db::migrate(&pool).await?;
    let file_records = load_file_records(&pool).await?;
    let records_by_path: BTreeMap<(String, String), FileRecordRow> = file_records
        .iter()
        .cloned()
        .map(|row| ((row.owner.clone(), row.rel_path.clone()), row))
        .collect();
    let records_by_id: BTreeMap<(String, i64), FileRecordRow> = file_records
        .iter()
        .cloned()
        .map(|row| ((row.owner.clone(), row.id), row))
        .collect();
    let actions = plan_repair_actions(
        &before,
        &roots,
        &config.storage.xattr_ns,
        &records_by_path,
        &records_by_id,
    );
    let actions = if mode == RepairMode::Apply {
        apply_repair_actions(&pool, &roots, &config.storage.xattr_ns, actions).await?
    } else {
        actions
    };
    let after = if mode == RepairMode::Apply {
        Some(check(config).await?)
    } else {
        None
    };

    Ok(RepairReport {
        mode,
        before,
        actions,
        after,
    })
}

fn check_file_records(
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    file_records: &[FileRecordRow],
    report: &mut ConsistencyReport,
) {
    for record in file_records {
        let rel_path = match normalized_rel_path_string(&record.rel_path) {
            Ok(rel_path) => rel_path,
            Err(err) => {
                report.issue(
                    ConsistencyIssueKind::InvalidFileRecordPath,
                    Some(&record.owner),
                    Some(&record.rel_path),
                    format!("invalid file_ids.rel_path: {err}"),
                );
                continue;
            }
        };
        let rel_path_buf = PathBuf::from(&rel_path);
        let abs_path = match storage::safe_existing_path(&roots.files_root, &rel_path_buf) {
            Ok(abs_path) => abs_path,
            Err(err) => {
                report.issue(
                    ConsistencyIssueKind::OrphanFileRecord,
                    Some(&record.owner),
                    Some(&record.rel_path),
                    format!("file_id {} has no readable file: {err}", record.id),
                );
                continue;
            }
        };

        if let Ok((mtime_ns, file_size)) = storage::metadata_fingerprint(&abs_path) {
            if record.mtime_ns.is_some_and(|cached| cached != mtime_ns)
                || record.file_size.is_some_and(|cached| cached != file_size)
            {
                report.issue(
                    ConsistencyIssueKind::StaleFileRecordCache,
                    Some(&record.owner),
                    Some(&record.rel_path),
                    format!(
                        "cached metadata mtime_ns={:?} file_size={:?}, actual mtime_ns={} file_size={}",
                        record.mtime_ns, record.file_size, mtime_ns, file_size
                    ),
                );
            }
        }

        match read_entry_xattrs(&abs_path, xattr_ns) {
            Ok(xattrs) => check_record_xattrs(record, &xattrs, report),
            Err(err) => report.issue(
                ConsistencyIssueKind::InvalidXattr,
                Some(&record.owner),
                Some(&record.rel_path),
                format!("could not read xattrs: {err}"),
            ),
        }
    }
}

fn check_record_xattrs(
    record: &FileRecordRow,
    xattrs: &EntryXattrs,
    report: &mut ConsistencyReport,
) {
    match parse_i64_xattr(xattrs.file_id.as_deref()) {
        Ok(Some(file_id)) if file_id == record.id => {}
        Ok(Some(file_id)) => report.issue(
            ConsistencyIssueKind::XattrMismatch,
            Some(&record.owner),
            Some(&record.rel_path),
            format!("xattr fileid={} but SQLite id={}", file_id, record.id),
        ),
        Ok(None) => report.issue(
            ConsistencyIssueKind::MissingXattr,
            Some(&record.owner),
            Some(&record.rel_path),
            "missing fileid xattr",
        ),
        Err(err) => report.issue(
            ConsistencyIssueKind::InvalidXattr,
            Some(&record.owner),
            Some(&record.rel_path),
            format!("invalid fileid xattr: {err}"),
        ),
    }

    if let Some(etag) = &record.etag {
        check_string_xattr(xattrs.etag.as_deref(), etag, "etag", record, report);
    } else if xattrs.etag.is_none() {
        report.issue(
            ConsistencyIssueKind::MissingXattr,
            Some(&record.owner),
            Some(&record.rel_path),
            "missing etag xattr",
        );
    }

    if let Some(permissions) = record.permissions {
        check_string_xattr(
            xattrs.permissions.as_deref(),
            &permissions.to_string(),
            "perms",
            record,
            report,
        );
    } else if xattrs.permissions.is_none() {
        report.issue(
            ConsistencyIssueKind::MissingXattr,
            Some(&record.owner),
            Some(&record.rel_path),
            "missing perms xattr",
        );
    }

    let favorite = if record.favorite { "1" } else { "0" };
    check_string_xattr(
        xattrs.favorite.as_deref(),
        favorite,
        "favorite",
        record,
        report,
    );
}

fn check_string_xattr(
    actual: Option<&str>,
    expected: &str,
    name: &str,
    record: &FileRecordRow,
    report: &mut ConsistencyReport,
) {
    match actual {
        Some(actual) if actual == expected => {}
        Some(actual) => report.issue(
            ConsistencyIssueKind::XattrMismatch,
            Some(&record.owner),
            Some(&record.rel_path),
            format!("xattr {name}={actual:?} but SQLite expects {expected:?}"),
        ),
        None => report.issue(
            ConsistencyIssueKind::MissingXattr,
            Some(&record.owner),
            Some(&record.rel_path),
            format!("missing {name} xattr"),
        ),
    }
}

fn check_filesystem_entries(
    xattr_ns: &str,
    records_by_path: &BTreeMap<(String, String), FileRecordRow>,
    records_by_id: &BTreeMap<(String, i64), FileRecordRow>,
    filesystem_entries: &[FsEntry],
    report: &mut ConsistencyReport,
) {
    for entry in filesystem_entries {
        if entry.rel_path.is_empty() {
            continue;
        }

        let key = (BOOTSTRAP_USER.to_owned(), entry.rel_path.clone());
        let xattrs = match read_entry_xattrs(&entry.abs_path, xattr_ns) {
            Ok(xattrs) => xattrs,
            Err(err) => {
                report.issue(
                    ConsistencyIssueKind::InvalidXattr,
                    Some(BOOTSTRAP_USER),
                    Some(&entry.rel_path),
                    format!("could not read xattrs: {err}"),
                );
                continue;
            }
        };

        if !records_by_path.contains_key(&key) {
            report.issue(
                ConsistencyIssueKind::MissingFileRecord,
                Some(BOOTSTRAP_USER),
                Some(&entry.rel_path),
                "filesystem entry has no SQLite file_ids row",
            );
        }

        if let Some(file_id) = parse_i64_xattr(xattrs.file_id.as_deref()).unwrap_or(None) {
            match records_by_id.get(&(BOOTSTRAP_USER.to_owned(), file_id)) {
                Some(record) if record.rel_path == entry.rel_path => {}
                Some(record) => report.issue(
                    ConsistencyIssueKind::XattrWithoutFileRecord,
                    Some(BOOTSTRAP_USER),
                    Some(&entry.rel_path),
                    format!(
                        "xattr fileid={} belongs to SQLite path {}",
                        file_id, record.rel_path
                    ),
                ),
                None => report.issue(
                    ConsistencyIssueKind::XattrWithoutFileRecord,
                    Some(BOOTSTRAP_USER),
                    Some(&entry.rel_path),
                    format!("xattr fileid={} has no SQLite row", file_id),
                ),
            }
        }
    }
}

fn check_dead_props(
    dead_props: &[DeadPropRow],
    records_by_path: &BTreeMap<(String, String), FileRecordRow>,
    fs_by_path: &BTreeMap<String, FsEntry>,
    report: &mut ConsistencyReport,
) {
    let mut seen = BTreeSet::new();
    for prop in dead_props {
        if !seen.insert((
            prop.owner.clone(),
            prop.rel_path.clone(),
            prop.namespace.clone(),
            prop.name.clone(),
        )) {
            continue;
        }

        let rel_path = match normalized_rel_path_string(&prop.rel_path) {
            Ok(rel_path) => rel_path,
            Err(err) => {
                report.issue(
                    ConsistencyIssueKind::InvalidDeadPropPath,
                    Some(&prop.owner),
                    Some(&prop.rel_path),
                    format!("invalid dead_props.rel_path: {err}"),
                );
                continue;
            }
        };

        if !fs_by_path.contains_key(&rel_path) {
            report.issue(
                ConsistencyIssueKind::DeadPropWithoutFile,
                Some(&prop.owner),
                Some(&prop.rel_path),
                format!(
                    "dead prop {}:{} points at missing file",
                    prop.namespace, prop.name
                ),
            );
        }

        if !records_by_path.contains_key(&(prop.owner.clone(), rel_path)) {
            report.issue(
                ConsistencyIssueKind::DeadPropWithoutFileRecord,
                Some(&prop.owner),
                Some(&prop.rel_path),
                format!(
                    "dead prop {}:{} has no matching file_ids row",
                    prop.namespace, prop.name
                ),
            );
        }
    }
}

fn plan_repair_actions(
    report: &ConsistencyReport,
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    records_by_path: &BTreeMap<(String, String), FileRecordRow>,
    records_by_id: &BTreeMap<(String, i64), FileRecordRow>,
) -> Vec<RepairAction> {
    let mut actions = Vec::new();
    let mut seen = BTreeSet::new();

    for issue in &report.issues {
        let owner = issue
            .owner
            .clone()
            .unwrap_or_else(|| BOOTSTRAP_USER.to_owned());
        let rel_path = match &issue.rel_path {
            Some(rel_path) => rel_path.clone(),
            None => continue,
        };
        let action = match issue.kind {
            ConsistencyIssueKind::InvalidFileRecordPath
            | ConsistencyIssueKind::OrphanFileRecord => RepairAction {
                kind: RepairActionKind::DeleteFileRecord,
                owner,
                rel_path,
                detail: "delete unusable file_ids row".to_owned(),
                applied: false,
            },
            ConsistencyIssueKind::MissingFileRecord => RepairAction {
                kind: RepairActionKind::EnsureFileRecord,
                owner,
                rel_path,
                detail: "create missing file_ids row and xattrs from filesystem".to_owned(),
                applied: false,
            },
            ConsistencyIssueKind::MissingXattr
            | ConsistencyIssueKind::InvalidXattr
            | ConsistencyIssueKind::XattrMismatch
            | ConsistencyIssueKind::StaleFileRecordCache => RepairAction {
                kind: RepairActionKind::RefreshFileMetadata,
                owner,
                rel_path,
                detail: "rewrite file xattrs and SQLite metadata cache".to_owned(),
                applied: false,
            },
            ConsistencyIssueKind::XattrWithoutFileRecord => plan_xattr_without_record_action(
                roots,
                xattr_ns,
                records_by_path,
                records_by_id,
                owner,
                rel_path,
            ),
            ConsistencyIssueKind::DeadPropWithoutFile
            | ConsistencyIssueKind::DeadPropWithoutFileRecord
            | ConsistencyIssueKind::InvalidDeadPropPath => RepairAction {
                kind: RepairActionKind::DeleteDeadProps,
                owner,
                rel_path,
                detail: "delete dead_props rows for missing or invalid target".to_owned(),
                applied: false,
            },
        };

        if seen.insert((action.kind, action.owner.clone(), action.rel_path.clone())) {
            actions.push(action);
        }
    }

    actions.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.owner.cmp(&right.owner))
            .then_with(|| left.rel_path.cmp(&right.rel_path))
    });
    actions
}

fn plan_xattr_without_record_action(
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    records_by_path: &BTreeMap<(String, String), FileRecordRow>,
    records_by_id: &BTreeMap<(String, i64), FileRecordRow>,
    owner: String,
    rel_path: String,
) -> RepairAction {
    let key = (owner.clone(), rel_path.clone());
    if records_by_path.contains_key(&key) {
        return RepairAction {
            kind: RepairActionKind::RefreshFileMetadata,
            owner,
            rel_path,
            detail: "rewrite xattrs to match existing SQLite row".to_owned(),
            applied: false,
        };
    }

    let rel_path_buf = PathBuf::from(&rel_path);
    let file_id = storage::safe_existing_path(&roots.files_root, &rel_path_buf)
        .ok()
        .and_then(|abs_path| read_entry_xattrs(&abs_path, xattr_ns).ok())
        .and_then(|xattrs| parse_i64_xattr(xattrs.file_id.as_deref()).ok().flatten());

    if let Some(file_id) = file_id {
        if records_by_id.contains_key(&(owner.clone(), file_id)) {
            return RepairAction {
                kind: RepairActionKind::SkipUnsafe,
                owner,
                rel_path,
                detail: format!(
                    "xattr fileid={file_id} belongs to another SQLite path; manual repair required"
                ),
                applied: false,
            };
        }
    }

    RepairAction {
        kind: RepairActionKind::EnsureFileRecord,
        owner,
        rel_path,
        detail: "attach orphan xattr fileid or create a file_ids row".to_owned(),
        applied: false,
    }
}

async fn apply_repair_actions(
    pool: &SqlitePool,
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    actions: Vec<RepairAction>,
) -> Result<Vec<RepairAction>> {
    let mut applied = Vec::with_capacity(actions.len());

    for mut action in actions {
        match action.kind {
            RepairActionKind::DeleteFileRecord => {
                delete_file_record(pool, &action.owner, &action.rel_path).await?;
                action.applied = true;
            }
            RepairActionKind::EnsureFileRecord => {
                ensure_file_record(pool, roots, xattr_ns, &action.owner, &action.rel_path).await?;
                action.applied = true;
            }
            RepairActionKind::RefreshFileMetadata => {
                refresh_file_metadata(pool, roots, xattr_ns, &action.owner, &action.rel_path)
                    .await?;
                action.applied = true;
            }
            RepairActionKind::DeleteDeadProps => {
                delete_dead_props(pool, &action.owner, &action.rel_path).await?;
                action.applied = true;
            }
            RepairActionKind::SkipUnsafe => {}
        }
        applied.push(action);
    }

    Ok(applied)
}

async fn delete_file_record(pool: &SqlitePool, owner: &str, rel_path: &str) -> Result<()> {
    sqlx::query("DELETE FROM file_ids WHERE owner = ?1 AND rel_path = ?2")
        .bind(owner)
        .bind(rel_path)
        .execute(pool)
        .await
        .context("delete orphan file_ids row")?;
    Ok(())
}

async fn delete_dead_props(pool: &SqlitePool, owner: &str, rel_path: &str) -> Result<()> {
    sqlx::query("DELETE FROM dead_props WHERE owner = ?1 AND rel_path = ?2")
        .bind(owner)
        .bind(rel_path)
        .execute(pool)
        .await
        .context("delete orphan dead_props rows")?;
    Ok(())
}

async fn ensure_file_record(
    pool: &SqlitePool,
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    owner: &str,
    rel_path: &str,
) -> Result<()> {
    let rel_path_buf = PathBuf::from(rel_path);
    let abs_path = storage::safe_existing_path(&roots.files_root, &rel_path_buf)
        .with_context(|| format!("resolve filesystem entry {rel_path}"))?;
    db::ensure_file_record(
        pool,
        db::FileRecordInput {
            owner,
            rel_path: &rel_path_buf,
            abs_path: &abs_path,
            instance_id: "phase1",
            xattr_ns,
        },
    )
    .await
    .with_context(|| format!("ensure file_ids row for {rel_path}"))?;
    Ok(())
}

async fn refresh_file_metadata(
    pool: &SqlitePool,
    roots: &ExistingStorageRoots,
    xattr_ns: &str,
    owner: &str,
    rel_path: &str,
) -> Result<()> {
    let record = load_file_record(pool, owner, rel_path)
        .await?
        .with_context(|| format!("load file_ids row for {rel_path}"))?;
    let rel_path_buf = PathBuf::from(rel_path);
    let abs_path = storage::safe_existing_path(&roots.files_root, &rel_path_buf)
        .with_context(|| format!("resolve filesystem entry {rel_path}"))?;
    let (mtime_ns, file_size) = storage::metadata_fingerprint(&abs_path)?;
    let etag = derive_etag(mtime_ns, file_size);
    let permissions = record.permissions.unwrap_or(0x3f);
    let favorite = if record.favorite { "1" } else { "0" };

    write_string_xattr(&abs_path, xattr_ns, "fileid", &record.id.to_string())?;
    write_string_xattr(&abs_path, xattr_ns, "etag", &etag)?;
    write_string_xattr(&abs_path, xattr_ns, "favorite", favorite)?;
    write_string_xattr(&abs_path, xattr_ns, "perms", &permissions.to_string())?;

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
    .bind(etag)
    .bind(permissions)
    .bind(if record.favorite { 1 } else { 0 })
    .bind(mtime_ns)
    .bind(file_size)
    .bind(owner)
    .bind(rel_path)
    .execute(pool)
    .await
    .context("refresh file_ids metadata cache")?;
    Ok(())
}

impl ConsistencyReport {
    fn issue(
        &mut self,
        kind: ConsistencyIssueKind,
        owner: Option<&str>,
        rel_path: Option<&str>,
        detail: impl Into<String>,
    ) {
        self.issues.push(ConsistencyIssue {
            kind,
            owner: owner.map(str::to_owned),
            rel_path: rel_path.map(str::to_owned),
            detail: detail.into(),
        });
    }
}

#[derive(Debug, Clone)]
struct ExistingStorageRoots {
    files_root: PathBuf,
}

impl ExistingStorageRoots {
    fn load(config: &Config) -> Result<Self> {
        let data_dir = Path::new(&config.storage.data_dir);
        let files_dir = data_dir.join("files");
        let files_root = files_dir
            .canonicalize()
            .with_context(|| format!("canonicalize files directory {}", files_dir.display()))?;
        Ok(Self { files_root })
    }
}

async fn connect_read_only(path: &str, max_connections: u32) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .read_only(true)
        .foreign_keys(true);

    SqlitePoolOptions::new()
        .max_connections(max_connections.max(1))
        .connect_with(options)
        .await
        .with_context(|| format!("open SQLite database read-only {path}"))
}

async fn load_file_records(pool: &SqlitePool) -> Result<Vec<FileRecordRow>> {
    let rows = sqlx::query(
        r#"
        SELECT id, owner, rel_path, etag, permissions, favorite, mtime_ns, file_size
        FROM file_ids
        ORDER BY owner, rel_path
        "#,
    )
    .fetch_all(pool)
    .await
    .context("load file_ids rows")?;

    rows.into_iter().map(file_record_from_row).collect()
}

async fn load_file_record(
    pool: &SqlitePool,
    owner: &str,
    rel_path: &str,
) -> Result<Option<FileRecordRow>> {
    let row = sqlx::query(
        r#"
        SELECT id, owner, rel_path, etag, permissions, favorite, mtime_ns, file_size
        FROM file_ids
        WHERE owner = ?1 AND rel_path = ?2
        "#,
    )
    .bind(owner)
    .bind(rel_path)
    .fetch_optional(pool)
    .await
    .context("load file_ids row")?;

    row.map(file_record_from_row).transpose()
}

fn file_record_from_row(row: SqliteRow) -> Result<FileRecordRow> {
    Ok(FileRecordRow {
        id: row.try_get("id")?,
        owner: row.try_get("owner")?,
        rel_path: row.try_get("rel_path")?,
        etag: row.try_get("etag")?,
        permissions: row.try_get("permissions")?,
        favorite: row.try_get::<i64, _>("favorite")? != 0,
        mtime_ns: row.try_get("mtime_ns")?,
        file_size: row.try_get("file_size")?,
    })
}

async fn load_dead_props(pool: &SqlitePool) -> Result<Vec<DeadPropRow>> {
    let rows = sqlx::query(
        r#"
        SELECT owner, rel_path, namespace, name
        FROM dead_props
        ORDER BY owner, rel_path, namespace, name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("load dead_props rows")?;

    rows.into_iter().map(dead_prop_from_row).collect()
}

fn dead_prop_from_row(row: SqliteRow) -> Result<DeadPropRow> {
    Ok(DeadPropRow {
        owner: row.try_get("owner")?,
        rel_path: row.try_get("rel_path")?,
        namespace: row.try_get("namespace")?,
        name: row.try_get("name")?,
    })
}

fn collect_filesystem_entries(files_root: &Path) -> Result<Vec<FsEntry>> {
    let mut entries = vec![FsEntry {
        rel_path: String::new(),
        abs_path: files_root.to_path_buf(),
    }];
    collect_child_entries(files_root, Path::new(""), &mut entries)?;
    entries.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));
    Ok(entries)
}

fn collect_child_entries(root: &Path, rel_path: &Path, entries: &mut Vec<FsEntry>) -> Result<()> {
    let dir = root.join(rel_path);
    for entry in
        std::fs::read_dir(&dir).with_context(|| format!("read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("read directory entry {}", dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("read file type for {}", entry.path().display()))?;
        let child_rel = rel_path.join(entry.file_name());
        let child_rel_string = storage::rel_path_string(&child_rel)?;
        entries.push(FsEntry {
            rel_path: child_rel_string,
            abs_path: entry.path(),
        });
        if file_type.is_dir() {
            collect_child_entries(root, &child_rel, entries)?;
        }
    }
    Ok(())
}

fn normalized_rel_path_string(rel_path: &str) -> Result<String> {
    storage::rel_path_string(Path::new(rel_path))
}

fn read_entry_xattrs(path: &Path, namespace: &str) -> Result<EntryXattrs> {
    Ok(EntryXattrs {
        file_id: read_string_xattr(path, namespace, "fileid")?,
        etag: read_string_xattr(path, namespace, "etag")?,
        favorite: read_string_xattr(path, namespace, "favorite")?,
        permissions: read_string_xattr(path, namespace, "perms")?,
    })
}

fn read_string_xattr(path: &Path, namespace: &str, name: &str) -> Result<Option<String>> {
    let Some(raw) = xattr::get(path, format!("{namespace}.{name}"))
        .with_context(|| format!("read xattr {namespace}.{name} from {}", path.display()))?
    else {
        return Ok(None);
    };
    String::from_utf8(raw).map(Some).with_context(|| {
        format!(
            "xattr {namespace}.{name} is not UTF-8 on {}",
            path.display()
        )
    })
}

fn write_string_xattr(path: &Path, namespace: &str, name: &str, value: &str) -> Result<()> {
    xattr::set(path, format!("{namespace}.{name}"), value.as_bytes())
        .with_context(|| format!("write xattr {namespace}.{name} to {}", path.display()))
}

fn parse_i64_xattr(value: Option<&str>) -> Result<Option<i64>> {
    value
        .map(|value| {
            value
                .parse::<i64>()
                .with_context(|| format!("parse integer xattr value {value:?}"))
        })
        .transpose()
}

fn derive_etag(mtime_ns: i64, file_size: i64) -> String {
    format!("{file_size:x}-{mtime_ns:x}")
}
