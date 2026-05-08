use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use gono_one::{build_router, db, db::BOOTSTRAP_USER, AppState, Config};
use tempfile::TempDir;
use tower::ServiceExt;

fn auth_header(password: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("gono:{password}")))
}

async fn app_with_temp_root() -> (axum::Router, TempDir, String) {
    let (app, temp, password, _state) = app_with_state().await;
    (app, temp, password)
}

async fn app_with_state() -> (axum::Router, TempDir, String, Arc<AppState>) {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);
    let initialized = AppState::initialize(config)
        .await
        .expect("phase1 app state");
    std::fs::write(initialized.state.files_root.join("hello.txt"), "hello").expect("seed file");
    let password = initialized
        .bootstrap
        .generated_password
        .expect("first bootstrap generates password");
    let state = initialized.state;
    (build_router(state.clone()), temp, password, state)
}

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
async fn status_php_is_public_and_nextcloud_shaped() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/status.php")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"installed\":true"));
    assert!(body.contains("\"productname\":\"Nextcloud\""));
}

#[tokio::test]
async fn capabilities_are_public() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"status\":\"ok\""));
    assert!(body.contains("\"chunking\":\"1.0\""));
}

#[tokio::test]
async fn webdav_requires_basic_auth() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
}

#[tokio::test]
async fn propfind_depth_0_injects_nextcloud_props() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("oc:fileid"));
    assert!(body.contains("oc:permissions"));
    assert!(body.contains("nc:has-preview"));
}

#[tokio::test]
async fn propfind_depth_1_lists_children() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "1")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("hello.txt"));
    assert!(body.contains("oc:fileid"));
}

#[tokio::test]
async fn depth_infinity_is_rejected_before_filesystem_walk() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "infinity")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn bootstrap_password_is_generated_once_and_reused() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);

    let first = AppState::initialize(config.clone())
        .await
        .expect("first init");
    let password = first
        .bootstrap
        .generated_password
        .clone()
        .expect("first init prints password");
    assert!(password.len() >= 40);
    assert!(first
        .state
        .user_store
        .verify(BOOTSTRAP_USER, &password)
        .await
        .expect("verify generated password")
        .is_some());

    let second = AppState::initialize(config).await.expect("second init");
    assert_eq!(second.bootstrap.generated_password, None);
    assert!(second
        .state
        .user_store
        .verify(BOOTSTRAP_USER, &password)
        .await
        .expect("old password still works")
        .is_some());
}

#[tokio::test]
async fn wrong_basic_auth_password_is_rejected() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header("wrong-password"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn initial_migration_does_not_install_sync_token_triggers() {
    let temp = TempDir::new().expect("tempdir");
    let initialized = AppState::initialize(test_config(&temp))
        .await
        .expect("init app state");

    let trigger_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger'")
            .fetch_one(&initialized.state.db)
            .await
            .expect("count triggers");

    assert_eq!(trigger_count.0, 0);
}

#[tokio::test]
async fn put_returns_nextcloud_metadata_headers() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/upload.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.headers().contains_key("oc-etag"));
    assert!(response.headers().contains_key("oc-fileid"));
    assert_eq!(
        response.headers().get("x-nc-ownerid").unwrap(),
        BOOTSTRAP_USER
    );
    assert_eq!(
        response.headers().get("x-nc-permissions").unwrap(),
        "RGDNVW"
    );
}

#[tokio::test]
async fn copy_allocates_a_new_file_id() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    let source_file_id = put_response
        .headers()
        .get("oc-fileid")
        .expect("source file id")
        .clone();

    let copy_response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/copied.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(copy_response.status().is_success());
    let copied_file_id = copy_response
        .headers()
        .get("oc-fileid")
        .expect("copied file id");
    assert_ne!(&source_file_id, copied_file_id);
}

#[tokio::test]
async fn move_preserves_file_id() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/move-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    let source_file_id = put_response
        .headers()
        .get("oc-fileid")
        .expect("source file id")
        .clone();

    let move_response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MOVE").unwrap())
                .uri("/remote.php/dav/move-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/moved.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(move_response.status().is_success());
    let moved_file_id = move_response
        .headers()
        .get("oc-fileid")
        .expect("moved file id");
    assert_eq!(&source_file_id, moved_file_id);
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_parent_escape_is_rejected_for_put() {
    use std::os::unix::fs::symlink;

    let (app, temp, password) = app_with_temp_root().await;
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&outside).expect("outside dir");
    symlink(&outside, temp.path().join("data/files/link")).expect("create symlink");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/link/escape.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("escape"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(!outside.join("escape.txt").exists());
}

#[tokio::test]
async fn put_writes_monotonic_sync_tokens() {
    let (app, _temp, password, state) = app_with_state().await;

    for body in ["first", "second"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/remote.php/dav/tokened.txt")
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    let token = db::current_sync_token(&state.db, BOOTSTRAP_USER)
        .await
        .expect("sync token");
    assert_eq!(token, 2);

    let changes = db::list_change_log(&state.db, BOOTSTRAP_USER)
        .await
        .expect("change log");
    let operations: Vec<_> = changes
        .iter()
        .map(|entry| {
            (
                entry.rel_path.as_str(),
                entry.operation.as_str(),
                entry.sync_token,
            )
        })
        .collect();
    assert_eq!(
        operations,
        vec![("tokened.txt", "create", 1), ("tokened.txt", "modify", 2)]
    );
}

#[tokio::test]
async fn copy_move_delete_write_expected_change_log_rows() {
    let (app, _temp, password, state) = app_with_state().await;

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/journal-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());

    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/journal-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/journal-copy.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(copy.status().is_success());

    let moved = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MOVE").unwrap())
                .uri("/remote.php/dav/journal-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/journal-moved.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(moved.status().is_success());

    let delete = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/remote.php/dav/journal-copy.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete.status().is_success());

    let changes = db::list_change_log(&state.db, BOOTSTRAP_USER)
        .await
        .expect("change log");
    let operations: Vec<_> = changes
        .iter()
        .map(|entry| {
            (
                entry.rel_path.as_str(),
                entry.operation.as_str(),
                entry.sync_token,
            )
        })
        .collect();
    assert_eq!(
        operations,
        vec![
            ("journal-source.txt", "create", 1),
            ("journal-copy.txt", "create", 2),
            ("journal-source.txt", "delete", 3),
            ("journal-moved.txt", "create", 4),
            ("journal-copy.txt", "delete", 5),
        ]
    );
}

#[tokio::test]
async fn report_sync_collection_returns_changes_since_old_token() {
    let (app, _temp, password, _state) = app_with_state().await;

    for (name, body) in [("sync-a.txt", "a"), ("sync-b.txt", "b")] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/remote.php/dav/{name}"))
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(sync_collection_body("1")))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("<d:sync-token>2</d:sync-token>"));
    assert!(body.contains("/remote.php/dav/sync-b.txt"));
    assert!(!body.contains("/remote.php/dav/sync-a.txt"));
    assert!(body.contains("<oc:fileid>"));
    assert!(body.contains("HTTP/1.1 200 OK"));
}

#[tokio::test]
async fn report_sync_collection_marks_deleted_paths_not_found() {
    let (app, _temp, password, state) = app_with_state().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/sync-delete.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("delete me"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());
    let after_put = db::current_sync_token(&state.db, BOOTSTRAP_USER)
        .await
        .expect("token after put");

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/remote.php/dav/sync-delete.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(sync_collection_body(&after_put.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/sync-delete.txt"));
    assert!(body.contains("HTTP/1.1 404 Not Found"));
}

#[tokio::test]
async fn proppatch_favorite_is_readable_from_propfind() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/favorite.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("favorite"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPPATCH").unwrap())
                .uri("/remote.php/dav/favorite.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_favorite_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/favorite.txt")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("<oc:favorite>1</oc:favorite>"));
}

#[tokio::test]
async fn report_filter_files_lists_favorites_in_scope_recursively() {
    let (app, _temp, password) = app_with_temp_root().await;

    for uri in [
        "/remote.php/dav/projects",
        "/remote.php/dav/projects/nested",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::from_bytes(b"MKCOL").unwrap())
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    for (uri, body) in [
        ("/remote.php/dav/root-favorite.txt", "root favorite"),
        ("/remote.php/dav/projects/plain.txt", "plain"),
        ("/remote.php/dav/projects/favorite.txt", "favorite"),
        (
            "/remote.php/dav/projects/nested/starred.md",
            "nested favorite",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    for uri in [
        "/remote.php/dav/root-favorite.txt",
        "/remote.php/dav/projects/favorite.txt",
        "/remote.php/dav/projects/nested/starred.md",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::from_bytes(b"PROPPATCH").unwrap())
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .header(header::CONTENT_TYPE, "application/xml")
                    .body(Body::from(proppatch_favorite_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/projects")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(filter_files_favorites_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/projects/favorite.txt"));
    assert!(body.contains("/remote.php/dav/projects/nested/starred.md"));
    assert!(!body.contains("/remote.php/dav/projects/plain.txt"));
    assert!(!body.contains("/remote.php/dav/root-favorite.txt"));
    assert!(body.contains("<oc:favorite>1</oc:favorite>"));
}

#[tokio::test]
async fn chunking_v2_mkcol_put_move_merges_chunks() {
    let (app, _temp, password, state) = app_with_state().await;
    let upload_base = "/remote.php/dav/uploads/gono/upload-session-1";
    let destination = "/remote.php/dav/chunked.txt";

    let mkcol = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri(upload_base)
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", destination)
                .header("OC-Total-Length", "11")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mkcol.status(), StatusCode::CREATED);

    for (chunk, body) in [("2", "world"), ("1", "hello ")] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("{upload_base}/{chunk}"))
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .header("Destination", destination)
                    .header("OC-Total-Length", "11")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let moved = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MOVE").unwrap())
                .uri(format!("{upload_base}/.file"))
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", destination)
                .header("X-OC-MTime", "1700000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(moved.status(), StatusCode::CREATED);
    assert!(moved.headers().contains_key("oc-fileid"));
    assert_eq!(moved.headers().get("x-oc-mtime").unwrap(), "accepted");
    assert!(!state
        .uploads_root
        .join("gono")
        .join("upload-session-1")
        .exists());

    let get = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(destination)
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"hello world");

    let changes = db::list_change_log(&state.db, BOOTSTRAP_USER)
        .await
        .expect("change log");
    assert!(changes
        .iter()
        .any(|entry| entry.rel_path == "chunked.txt" && entry.operation == "create"));
}

#[tokio::test]
async fn chunking_v2_rejects_invalid_chunk_names() {
    let (app, _temp, password) = app_with_temp_root().await;
    let upload_base = "/remote.php/dav/uploads/gono/upload-session-2";
    let destination = "/remote.php/dav/chunked-invalid.txt";

    let mkcol = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri(upload_base)
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", destination)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mkcol.status(), StatusCode::CREATED);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{upload_base}/not-a-number"))
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", destination)
                .body(Body::from("bad"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn chunking_v2_rejects_destination_mismatch() {
    let (app, _temp, password) = app_with_temp_root().await;
    let upload_base = "/remote.php/dav/uploads/gono/upload-session-3";

    let mkcol = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri(upload_base)
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/original.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mkcol.status(), StatusCode::CREATED);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{upload_base}/1"))
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/other.txt")
                .body(Body::from("bad"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn chunking_v2_rejects_impossible_total_length() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri("/remote.php/dav/uploads/gono/upload-too-large")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/huge.bin")
                .header("OC-Total-Length", i64::MAX.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);
}

fn propfind_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:resourcetype />
    <d:getetag />
    <oc:fileid />
    <oc:permissions />
    <oc:favorite />
    <nc:has-preview />
  </d:prop>
</d:propfind>"#
}

fn proppatch_favorite_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:set>
    <d:prop>
      <oc:favorite>1</oc:favorite>
    </d:prop>
  </d:set>
</d:propertyupdate>"#
}

fn sync_collection_body(sync_token: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<d:sync-collection xmlns:d="DAV:">
  <d:sync-token>{sync_token}</d:sync-token>
  <d:sync-level>1</d:sync-level>
  <d:prop>
    <d:getetag />
    <oc:fileid xmlns:oc="http://owncloud.org/ns" />
    <oc:favorite xmlns:oc="http://owncloud.org/ns" />
  </d:prop>
</d:sync-collection>"#
    )
}

fn filter_files_favorites_body() -> &'static str {
    r#"<?xml version="1.0"?>
<oc:filter-files xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <oc:filter-rules>
    <oc:favorite>1</oc:favorite>
  </oc:filter-rules>
  <d:prop>
    <d:getetag />
    <oc:fileid />
    <oc:favorite />
    <nc:has-preview />
  </d:prop>
</oc:filter-files>"#
}
