use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use nc_dav::{build_router, db::BOOTSTRAP_USER, AppState, Config};
use tempfile::TempDir;
use tower::ServiceExt;

fn auth_header(password: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("gono:{password}")))
}

async fn app_with_temp_root() -> (axum::Router, TempDir, String) {
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
    (build_router(initialized.state), temp, password)
}

fn test_config(temp: &TempDir) -> Config {
    let mut config = Config::dev_default();
    config.storage.data_dir = temp.path().join("data").to_string_lossy().into_owned();
    config.db.path = temp.path().join("nc-dav.db").to_string_lossy().into_owned();
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
