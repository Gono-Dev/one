use std::path::Path;

use gono_one::{
    consistency::{self, ConsistencyIssueKind},
    db, AppState, Config,
};
use tempfile::TempDir;

fn test_config(temp: &TempDir) -> Config {
    let mut config = Config::dev_default();
    config.storage.data_dir = temp.path().join("data").to_string_lossy().into_owned();
    config.db.path = temp
        .path()
        .join("gono-one.db")
        .to_string_lossy()
        .into_owned();
    config.server.cert_file = temp.path().join("cert.pem").to_string_lossy().into_owned();
    config.server.key_file = temp.path().join("key.pem").to_string_lossy().into_owned();
    config
}

#[tokio::test]
async fn consistency_check_accepts_clean_file_record() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);
    let initialized = AppState::initialize(config.clone())
        .await
        .expect("init app state");
    let state = initialized.state;
    let rel_path = Path::new("clean.txt");
    let abs_path = state.files_root.join(rel_path);
    std::fs::write(&abs_path, "clean").expect("write clean file");

    db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner: &state.owner,
            rel_path,
            abs_path: &abs_path,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await
    .expect("ensure file record");

    let report = consistency::check(&config)
        .await
        .expect("consistency check");
    assert!(report.is_clean(), "{}", report.render_text());
}

#[tokio::test]
async fn consistency_check_reports_orphans_and_missing_records() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);
    let initialized = AppState::initialize(config.clone())
        .await
        .expect("init app state");
    let state = initialized.state;
    std::fs::write(state.files_root.join("loose.txt"), "loose").expect("write loose file");

    sqlx::query(
        r#"
        INSERT INTO file_ids(owner, rel_path, permissions, favorite, created_at)
        VALUES(?1, 'ghost.txt', 63, 0, ?2)
        "#,
    )
    .bind(&state.owner)
    .bind(db::unix_timestamp())
    .execute(&state.db)
    .await
    .expect("insert orphan file_id");

    db::set_dead_prop(
        &state.db,
        &state.owner,
        Path::new("ghost.txt"),
        Some("http://example.com/ns"),
        "stale",
        b"<x:stale xmlns:x=\"http://example.com/ns\" />",
    )
    .await
    .expect("insert orphan dead prop");
    db::set_dead_prop(
        &state.db,
        &state.owner,
        Path::new("dead-only.txt"),
        Some("http://example.com/ns"),
        "stale",
        b"<x:stale xmlns:x=\"http://example.com/ns\" />",
    )
    .await
    .expect("insert dead prop without file record");

    let report = consistency::check(&config)
        .await
        .expect("consistency check");
    assert_issue(
        &report,
        ConsistencyIssueKind::MissingFileRecord,
        "loose.txt",
    );
    assert_issue(&report, ConsistencyIssueKind::OrphanFileRecord, "ghost.txt");
    assert_issue(
        &report,
        ConsistencyIssueKind::DeadPropWithoutFile,
        "ghost.txt",
    );
    assert_issue(
        &report,
        ConsistencyIssueKind::DeadPropWithoutFileRecord,
        "dead-only.txt",
    );
}

#[tokio::test]
async fn consistency_check_reports_missing_xattr() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);
    let initialized = AppState::initialize(config.clone())
        .await
        .expect("init app state");
    let state = initialized.state;
    let rel_path = Path::new("missing-xattr.txt");
    let abs_path = state.files_root.join(rel_path);
    std::fs::write(&abs_path, "xattr").expect("write xattr file");

    db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner: &state.owner,
            rel_path,
            abs_path: &abs_path,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await
    .expect("ensure file record");
    xattr::remove(&abs_path, format!("{}.fileid", state.xattr_ns)).expect("remove fileid xattr");

    let report = consistency::check(&config)
        .await
        .expect("consistency check");
    assert_issue(
        &report,
        ConsistencyIssueKind::MissingXattr,
        "missing-xattr.txt",
    );
}

fn assert_issue(report: &consistency::ConsistencyReport, kind: ConsistencyIssueKind, path: &str) {
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == kind && issue.rel_path.as_deref() == Some(path)),
        "expected {kind:?} for {path}; report:\n{}",
        report.render_text()
    );
}
