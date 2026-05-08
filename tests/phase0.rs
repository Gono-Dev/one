use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use nc_dav::{build_router, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

fn auth_header() -> String {
    format!("Basic {}", STANDARD.encode("gono:app-password"))
}

fn app_with_temp_root() -> (axum::Router, TempDir) {
    let temp = TempDir::new().expect("tempdir");
    std::fs::write(temp.path().join("hello.txt"), "hello").expect("seed file");
    let state = Arc::new(AppState::phase0(temp.path()).expect("phase0 app state"));
    (build_router(state), temp)
}

#[tokio::test]
async fn status_php_is_public_and_nextcloud_shaped() {
    let (app, _temp) = app_with_temp_root();
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
    let (app, _temp) = app_with_temp_root();
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
    let (app, _temp) = app_with_temp_root();
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
    let (app, _temp) = app_with_temp_root();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header())
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
    let (app, _temp) = app_with_temp_root();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "1")
                .header(header::AUTHORIZATION, auth_header())
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
    let (app, _temp) = app_with_temp_root();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/")
                .header("Depth", "infinity")
                .header(header::AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
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
