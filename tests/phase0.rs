use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use gono_cloud::{
    build_router, dav_handler::chunked_upload, db, db::BOOTSTRAP_USER,
    permissions::PermissionLevel, AppState, Config,
};
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tower::ServiceExt;

type TestWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn auth_header(password: &str) -> String {
    auth_header_for(BOOTSTRAP_USER, password)
}

fn auth_header_for(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    )
}

async fn spawn_app_server(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://{addr}"), handle)
}

async fn login_ws(base_url: &str, username: &str, password: &str) -> TestWebSocket {
    login_ws_at(base_url, "/push/ws", username, password).await
}

async fn login_ws_at(base_url: &str, path: &str, username: &str, password: &str) -> TestWebSocket {
    let (mut websocket, _response) = connect_async(ws_url(base_url, path))
        .await
        .expect("connect websocket");
    websocket
        .send(Message::Text(username.to_owned().into()))
        .await
        .expect("send websocket username");
    websocket
        .send(Message::Text(password.to_owned().into()))
        .await
        .expect("send websocket password");
    websocket
}

async fn next_ws_text(websocket: &mut TestWebSocket) -> String {
    loop {
        match websocket.next().await {
            Some(Ok(Message::Text(text))) => return text.to_string(),
            Some(Ok(Message::Ping(payload))) => {
                websocket
                    .send(Message::Pong(payload))
                    .await
                    .expect("reply pong");
            }
            Some(Ok(Message::Close(frame))) => panic!("websocket closed: {frame:?}"),
            Some(Ok(_)) => {}
            Some(Err(err)) => panic!("websocket error: {err}"),
            None => panic!("websocket ended"),
        }
    }
}

fn ws_url(base_url: &str, path: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    if let Some(rest) = base_url.strip_prefix("https://") {
        format!("wss://{rest}{path}")
    } else if let Some(rest) = base_url.strip_prefix("http://") {
        format!("ws://{rest}{path}")
    } else {
        format!("{base_url}{path}")
    }
}

async fn app_with_temp_root() -> (axum::Router, TempDir, String) {
    let (app, temp, password, _state) = app_with_state().await;
    (app, temp, password)
}

async fn app_with_state() -> (axum::Router, TempDir, String, Arc<AppState>) {
    app_with_config(|_config| {}).await
}

async fn app_with_config(
    configure: impl FnOnce(&mut Config),
) -> (axum::Router, TempDir, String, Arc<AppState>) {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp);
    configure(&mut config);
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
        .join("gono-cloud.db")
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
async fn desktop_connectivity_probe_is_public_no_content() {
    let (app, _temp, _password) = app_with_temp_root().await;
    for uri in ["/204", "/index.php/204"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty(), "{uri}");
    }
}

#[tokio::test]
async fn capabilities_are_public() {
    let (app, _temp, _password) = app_with_temp_root().await;
    for uri in [
        "/ocs/v1.php/cloud/capabilities",
        "/index.php/ocs/v1.php/cloud/capabilities",
        "/ocs/v2.php/cloud/capabilities",
        "/index.php/ocs/v2.php/cloud/capabilities",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"chunking\":\"1.0\""));
        assert!(body.contains("\"notify_push\""));
        assert!(body.contains("\"websocket\":\"wss://127.0.0.1:3000/push/ws\""));
        assert!(body.contains("\"pre_auth\":\"https://127.0.0.1:3000/apps/notify_push/pre_auth\""));
    }
}

#[tokio::test]
async fn ocs_user_endpoints_require_auth_and_return_profile() {
    let (app, _temp, password, state) = app_with_state().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    for uri in ["/ocs/v2.php/cloud/user", "/index.php/ocs/v2.php/cloud/user"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"id\":\"gono\""));
        assert!(body.contains("\"displayname\":\"gono\""));
        assert!(body.contains("\"quota\""));
        assert!(body.contains("\"storageLocation\":\"/remote.php/dav/files/gono\""));
        assert!(!body.contains(&state.files_root.to_string_lossy().into_owned()));
    }
}

#[tokio::test]
async fn ocs_user_lookup_is_limited_to_current_user() {
    let (app, _temp, _password, state) = app_with_state().await;
    let alice = db::create_local_user(&state.db, "alice", None)
        .await
        .expect("create alice");
    let alice_auth = auth_header_for("alice", &alice.app_password);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/users")
                .header(header::AUTHORIZATION, &alice_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"users\":[\"alice\"]"));
    assert!(!body.contains("\"gono\""));

    let forbidden = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/users/gono")
                .header(header::AUTHORIZATION, &alice_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let own = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/users/alice")
                .header(header::AUTHORIZATION, &alice_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(own.status(), StatusCode::OK);
    let body = to_bytes(own.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"id\":\"alice\""));
    assert!(body.contains("\"storageLocation\":\"/remote.php/dav/files/alice\""));
    assert!(!body.contains(&state.data_root.to_string_lossy().into_owned()));
}

#[tokio::test]
async fn minimal_ocs_v1_user_aliases_match_desktop_client_probes() {
    let (app, _temp, password) = app_with_temp_root().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v1.php/cloud/user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    for uri in ["/ocs/v1.php/cloud/user", "/index.php/ocs/v1.php/cloud/user"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"id\":\"gono\""));
        assert!(body.contains("\"quota\""));
    }
}

#[tokio::test]
async fn documented_ocs_v2_endpoints_are_covered_or_placeholdered() {
    let (app, _temp, password) = app_with_temp_root().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/apps/files_sharing/api/v1/shares")
                .header("OCS-APIRequest", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let cases = vec![
        (
            Method::GET,
            "/ocs/v2.php/cloud/users",
            StatusCode::OK,
            "\"users\":[\"gono\"]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/cloud/users/gono",
            StatusCode::OK,
            "\"id\":\"gono\"",
        ),
        (
            Method::GET,
            "/index.php/ocs/v2.php/cloud/users/gono",
            StatusCode::OK,
            "\"displayname\":\"gono\"",
        ),
        (
            Method::GET,
            "/ocs/v2.php/core/autocomplete/get?search=gono&shareTypes[]=0",
            StatusCode::OK,
            "\"exact\"",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/dav/api/v1/direct",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/terms_of_service/terms?format=json",
            StatusCode::NOT_FOUND,
            "\"statuscode\":404",
        ),
        (
            Method::GET,
            "/ocs/v2.php/core/navigation/apps?absolute=true&format=json",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/shares",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/files_sharing/api/v1/shares",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/shares/123",
            StatusCode::NOT_FOUND,
            "\"statuscode\":404",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/sharees?search=gono&itemType=file",
            StatusCode::OK,
            "\"users\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/sharees_recommended?itemType=file",
            StatusCode::OK,
            "\"lookup\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/remote_shares",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/files_sharing/api/v1/remote_shares/pending",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/files_sharing/api/v1/remote_shares/pending/1",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/notifications/api/v2/notifications",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/notifications/api/v2/notifications/exists",
            StatusCode::OK,
            "\"ids\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/notifications/api/v2/notifications/99",
            StatusCode::NOT_FOUND,
            "\"statuscode\":404",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/user_status/api/v1/user_status",
            StatusCode::OK,
            "\"status\":\"online\"",
        ),
        (
            Method::PUT,
            "/ocs/v2.php/apps/user_status/api/v1/user_status/status",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/user_status/api/v1/predefined_statuses",
            StatusCode::OK,
            "\"data\":[]",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/user_status/api/v1/statuses/gono",
            StatusCode::OK,
            "\"userId\":\"gono\"",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/recommendations/api/v1/recommendations",
            StatusCode::OK,
            "\"enabled\":false",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/recommendations/api/v1/recommendations/always",
            StatusCode::OK,
            "\"recommendations\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/provisioning_api/api/v1/config/users/files/favorite",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/translation/languages",
            StatusCode::OK,
            "\"languages\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/translation/translate",
            StatusCode::PRECONDITION_FAILED,
            "No translation provider",
        ),
        (
            Method::GET,
            "/ocs/v2.php/textprocessing/tasktypes",
            StatusCode::OK,
            "\"types\":[]",
        ),
        (
            Method::POST,
            "/ocs/v2.php/textprocessing/schedule",
            StatusCode::PRECONDITION_FAILED,
            "No processing provider",
        ),
        (
            Method::POST,
            "/ocs/v2.php/textprocessing/task/1",
            StatusCode::NOT_FOUND,
            "\"statuscode\":404",
        ),
        (
            Method::GET,
            "/ocs/v2.php/text2image/is_available",
            StatusCode::OK,
            "\"isAvailable\":false",
        ),
        (
            Method::POST,
            "/ocs/v2.php/text2image/schedule",
            StatusCode::PRECONDITION_FAILED,
            "No processing provider",
        ),
        (
            Method::DELETE,
            "/ocs/v2.php/text2image/task/1",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::POST,
            "/ocs/v2.php/taskprocessing",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::POST,
            "/ocs/v2.php/taskprocessing/provider/test",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/dav/api/v1/outOfOffice/gono",
            StatusCode::NOT_FOUND,
            "\"statuscode\":404",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/dav/api/v1/outOfOffice/gono",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::GET,
            "/ocs/v2.php/apps/fulltextsearch/collection/test/index",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
        (
            Method::POST,
            "/ocs/v2.php/apps/fulltextsearch/collection/test/document/files/1/done",
            StatusCode::NOT_IMPLEMENTED,
            "not implemented yet",
        ),
    ];

    for (method, uri, expected, needle) in cases {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header("OCS-APIRequest", "true")
                    .header(header::ACCEPT, "application/json")
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), expected, "{method} {uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("\"ocs\""), "{method} {uri}: {body}");
        assert!(body.contains(needle), "{method} {uri}: {body}");
    }
}

#[tokio::test]
async fn notify_push_can_be_disabled() {
    let (app, _temp, _password, state) = app_with_config(|config| {
        config.notify_push.enabled = false;
    })
    .await;
    assert!(state.notify_push.is_none());

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
    assert!(!body.contains("\"notify_push\""));
}

#[tokio::test]
async fn notify_push_custom_websocket_path_is_advertised_and_routed() {
    let (app, _temp, password, _state) = app_with_config(|config| {
        config.notify_push.path = "/events".to_owned();
    })
    .await;

    let capabilities = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ocs/v2.php/cloud/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(capabilities.status(), StatusCode::OK);
    let body = to_bytes(capabilities.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"websocket\":\"wss://127.0.0.1:3000/events/ws\""));

    let (base_url, server) = spawn_app_server(app).await;
    let mut websocket = login_ws_at(&base_url, "/events/ws", BOOTSTRAP_USER, &password).await;
    assert_eq!(next_ws_text(&mut websocket).await, "authenticated");
    let _ = websocket.close(None).await;

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn metrics_require_basic_auth_and_are_prometheus_shaped() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/metrics.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("# TYPE gono_cloud_sync_token gauge"));
    assert!(body.contains("gono_cloud_file_records_total 1\n"));
    assert!(body.contains("gono_cloud_change_log_entries_total 1\n"));
    assert!(body.contains("gono_cloud_upload_sessions_total 0\n"));
    assert!(body.contains("gono_cloud_sync_token 1\n"));
    assert!(body.contains("gono_cloud_change_log_floor_token 0\n"));
    assert!(body.contains("gono_cloud_storage_files_available_bytes "));
    assert!(body.contains("gono_cloud_notify_push_active_connections 0\n"));
    assert!(body.contains("gono_cloud_notify_push_events_total 1\n"));
}

#[tokio::test]
async fn metrics_are_filtered_by_app_password_scope() {
    let (app, _temp, _password, state) = app_with_state().await;
    let visible_rel = std::path::Path::new("Projects/readme.txt");
    let outside_rel = std::path::Path::new("Outside/readme.txt");
    let visible_abs = state.files_root.join(visible_rel);
    let outside_abs = state.files_root.join(outside_rel);
    std::fs::create_dir_all(visible_abs.parent().unwrap()).expect("create visible dir");
    std::fs::create_dir_all(outside_abs.parent().unwrap()).expect("create outside dir");
    std::fs::write(&visible_abs, "project docs").expect("write visible file");
    std::fs::write(&outside_abs, "outside docs").expect("write outside file");

    let visible_record = db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner: BOOTSTRAP_USER,
            rel_path: visible_rel,
            abs_path: &visible_abs,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await
    .expect("insert visible file record");
    let outside_record = db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner: BOOTSTRAP_USER,
            rel_path: outside_rel,
            abs_path: &outside_abs,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await
    .expect("insert outside file record");
    db::record_change(
        &state.db,
        BOOTSTRAP_USER,
        visible_record.id,
        visible_rel,
        "create",
    )
    .await
    .expect("record visible change");
    db::record_change(
        &state.db,
        BOOTSTRAP_USER,
        outside_record.id,
        outside_rel,
        "create",
    )
    .await
    .expect("record outside change");

    db::upsert_upload_session(
        &state.db,
        "visible-upload",
        BOOTSTRAP_USER,
        std::path::Path::new("Projects/upload.txt"),
        1,
    )
    .await
    .expect("insert visible upload session");
    db::upsert_upload_session(
        &state.db,
        "outside-upload",
        BOOTSTRAP_USER,
        std::path::Path::new("Outside/upload.txt"),
        1,
    )
    .await
    .expect("insert outside upload session");
    sqlx::query("UPDATE upload_sessions SET expires_at = ?1 WHERE owner = ?2")
        .bind(db::unix_timestamp() - 60)
        .bind(BOOTSTRAP_USER)
        .execute(&state.db)
        .await
        .expect("expire upload sessions");

    let scoped = db::create_local_app_password(
        &state.db,
        BOOTSTRAP_USER,
        "metrics-scope",
        None,
        &[db::AppPasswordScopeInput {
            mount_path: "/Docs".to_owned(),
            storage_path: "/Projects".to_owned(),
            permission: PermissionLevel::View,
        }],
    )
    .await
    .expect("create scoped app password");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for(BOOTSTRAP_USER, &scoped.app_password),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("gono_cloud_file_records_total 1\n"));
    assert!(body.contains("gono_cloud_change_log_entries_total 1\n"));
    assert!(body.contains("gono_cloud_upload_sessions_total 1\n"));
    assert!(body.contains("gono_cloud_upload_sessions_expired_total 1\n"));
    assert!(body.contains("gono_cloud_sync_token 1\n"));
    assert!(body.contains("gono_cloud_change_log_floor_token 0\n"));
}

#[tokio::test]
async fn notify_push_pre_auth_uid_and_test_routes_work() {
    let (app, _temp, password, state) = app_with_state().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/apps/notify_push/pre_auth")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let token_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/apps/notify_push/pre_auth")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let token = to_bytes(token_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token = std::str::from_utf8(&token).unwrap();
    assert!(token.len() >= 40);

    let uid = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/index.php/apps/notify_push/uid")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uid.status(), StatusCode::OK);
    let body = to_bytes(uid.into_body(), usize::MAX).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), BOOTSTRAP_USER);

    let runtime = state.notify_push.as_ref().expect("notify push runtime");
    runtime.set_test_token("test-token");
    runtime.set_test_cookie(4242);

    let forbidden = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/push/test/cookie")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    for (method, uri, expected) in [
        (Method::GET, "/push/test/cookie", "4242"),
        (Method::GET, "/push/test/reverse_cookie", "4242"),
        (Method::GET, "/push/test/mapping/arbitrary-id", "1"),
        (Method::GET, "/push/test/remote/192.0.2.10", "192.0.2.10"),
        (Method::POST, "/push/test/version", "set"),
        (Method::POST, "/push/test/trigger/activity", "sent"),
        (Method::POST, "/push/test/trigger/notification", "sent"),
        (Method::POST, "/push/test/trigger/custom", "sent"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header("token", "test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), expected);
    }
}

#[tokio::test]
async fn notify_push_websocket_basic_auth_success_and_failure() {
    let (app, _temp, password, _state) = app_with_state().await;
    let (base_url, server) = spawn_app_server(app).await;

    let mut websocket = login_ws(&base_url, BOOTSTRAP_USER, &password).await;
    assert_eq!(next_ws_text(&mut websocket).await, "authenticated");
    let _ = websocket.close(None).await;

    let mut bad = login_ws(&base_url, BOOTSTRAP_USER, "wrong-password").await;
    assert!(next_ws_text(&mut bad).await.starts_with("err:"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn notify_push_pre_auth_websocket_token_is_one_time() {
    let (app, _temp, password, _state) = app_with_state().await;
    let token_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/index.php/apps/notify_push/pre_auth")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let token = to_bytes(token_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token = std::str::from_utf8(&token).unwrap().to_owned();

    let (base_url, server) = spawn_app_server(app).await;

    let mut websocket = login_ws(&base_url, "", &token).await;
    assert_eq!(next_ws_text(&mut websocket).await, "authenticated");

    let mut reused = login_ws(&base_url, "", &token).await;
    assert!(next_ws_text(&mut reused).await.starts_with("err:"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn notify_push_expired_pre_auth_token_is_rejected_by_websocket() {
    let (app, _temp, password, _state) = app_with_config(|config| {
        config.notify_push.pre_auth_ttl_secs = 0;
    })
    .await;
    let token_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/apps/notify_push/pre_auth")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let token = to_bytes(token_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token = std::str::from_utf8(&token).unwrap().to_owned();
    tokio::time::sleep(Duration::from_millis(1)).await;

    let (base_url, server) = spawn_app_server(app).await;
    let mut websocket = login_ws(&base_url, "", &token).await;
    assert!(next_ws_text(&mut websocket)
        .await
        .starts_with("err: Invalid pre-auth token"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn notify_push_websocket_enforces_user_connection_limit() {
    let (app, _temp, password, _state) = app_with_config(|config| {
        config.notify_push.user_connection_limit = 1;
    })
    .await;
    let (base_url, server) = spawn_app_server(app).await;

    let mut first = login_ws(&base_url, BOOTSTRAP_USER, &password).await;
    assert_eq!(next_ws_text(&mut first).await, "authenticated");

    let mut second = login_ws(&base_url, BOOTSTRAP_USER, &password).await;
    assert!(next_ws_text(&mut second)
        .await
        .starts_with("err: Too many connections"));

    let _ = first.close(None).await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn notify_push_websocket_receives_file_id_then_sync_collection_lists_change() {
    let (app, _temp, password, state) = app_with_state().await;
    let (base_url, server) = spawn_app_server(app.clone()).await;

    let mut websocket = login_ws(&base_url, BOOTSTRAP_USER, &password).await;
    assert_eq!(next_ws_text(&mut websocket).await, "authenticated");
    websocket
        .send(Message::Text("listen notify_file_id".into()))
        .await
        .expect("send listen mode");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/ws-push.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("push me"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);
    let changes = db::list_change_log(&state.db, BOOTSTRAP_USER)
        .await
        .expect("change log");
    let file_id = changes.last().expect("last change").file_id;
    assert_eq!(
        next_ws_text(&mut websocket).await,
        format!("notify_file_id [{file_id}]")
    );

    let report = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(sync_collection_body("0")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(report.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(report.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/ws-push.txt"));
    assert!(body.contains("<d:sync-token>1</d:sync-token>"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn notify_push_events_are_emitted_for_all_file_mutations() {
    let (app, _temp, password, state) = app_with_state().await;

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/notify-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("source"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);

    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/notify-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/notify-copy.txt")
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
                .uri("/remote.php/dav/notify-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/notify-moved.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(moved.status().is_success());

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/remote.php/dav/notify-copy.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete.status().is_success());

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPPATCH").unwrap())
                .uri("/remote.php/dav/notify-moved.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_favorite_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch.status().is_success());

    let upload_base = "/remote.php/dav/uploads/gono/notify-upload";
    let destination = "/remote.php/dav/notify-chunked.txt";
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
    for (chunk, body) in [("1", "hello "), ("2", "world")] {
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
    let chunk_move = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MOVE").unwrap())
                .uri(format!("{upload_base}/.file"))
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", destination)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(chunk_move.status(), StatusCode::CREATED);

    let metrics = state
        .notify_push
        .as_ref()
        .expect("notify push runtime")
        .metrics();
    assert_eq!(metrics.events_received, 7);
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
async fn root_webdav_path_requires_basic_auth() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/")
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
async fn root_webdav_path_shares_storage_with_remote_php_dav() {
    let (app, _temp, password) = app_with_temp_root().await;

    let propfind = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(propfind.status(), StatusCode::MULTI_STATUS);
    let propfind_body = to_bytes(propfind.into_body(), usize::MAX).await.unwrap();
    let propfind_body = std::str::from_utf8(&propfind_body).unwrap();
    assert!(propfind_body.contains("oc:fileid"));

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/root-path.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("root compatible"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);
    assert!(put.headers().contains_key("oc-fileid"));

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/root-path.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"root compatible");
}

#[tokio::test]
async fn standard_nextcloud_files_webdav_path_shares_storage_with_remote_php_dav() {
    let (app, _temp, password) = app_with_temp_root().await;

    let propfind = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/gono/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(propfind.status(), StatusCode::MULTI_STATUS);
    let propfind_body = to_bytes(propfind.into_body(), usize::MAX).await.unwrap();
    let propfind_body = std::str::from_utf8(&propfind_body).unwrap();
    assert!(propfind_body.contains("oc:fileid"));

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/standard-path.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("standard nextcloud path"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);
    assert!(put.headers().contains_key("oc-fileid"));

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/standard-path.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"standard nextcloud path");

    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/files/gono/standard-path.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(
                    "Destination",
                    "/remote.php/dav/files/gono/standard-copy.txt",
                )
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
                .uri("/remote.php/dav/files/gono/standard-copy.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(
                    "Destination",
                    "/remote.php/dav/files/gono/standard-moved.txt",
                )
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
                .uri("/remote.php/dav/files/gono/standard-moved.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn app_password_scopes_map_paths_and_enforce_permissions() {
    let (app, _temp, _password, state) = app_with_state().await;
    std::fs::create_dir_all(state.files_root.join("Projects")).expect("create projects");
    std::fs::write(state.files_root.join("Projects/readme.txt"), "project docs")
        .expect("seed project file");
    std::fs::create_dir_all(state.files_root.join("Inbox/Uploads")).expect("create upload target");

    let scoped = db::create_local_app_password(
        &state.db,
        BOOTSTRAP_USER,
        "scoped",
        None,
        &[
            db::AppPasswordScopeInput {
                mount_path: "/Docs".to_owned(),
                storage_path: "/Projects".to_owned(),
                permission: PermissionLevel::View,
            },
            db::AppPasswordScopeInput {
                mount_path: "/Uploads".to_owned(),
                storage_path: "/Inbox/Uploads".to_owned(),
                permission: PermissionLevel::Full,
            },
        ],
    )
    .await
    .expect("create scoped app password");
    let scoped_auth = auth_header_for(BOOTSTRAP_USER, &scoped.app_password);

    let root = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/gono/")
                .header("Depth", "1")
                .header(header::AUTHORIZATION, &scoped_auth)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(root.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(root.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/files/gono/Docs"));
    assert!(body.contains("/remote.php/dav/files/gono/Uploads"));
    assert!(!body.contains("/remote.php/dav/files/gono/Projects"));

    let docs_propfind = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/gono/Docs/")
                .header("Depth", "1")
                .header(header::AUTHORIZATION, &scoped_auth)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(docs_propfind.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(docs_propfind.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/files/gono/Docs/readme.txt"));
    assert!(!body.contains("/remote.php/dav/files/gono/Projects/readme.txt"));

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/files/gono/Docs/readme.txt")
                .header(header::AUTHORIZATION, &scoped_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"project docs");

    let outside = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/files/gono/Projects/readme.txt")
                .header(header::AUTHORIZATION, &scoped_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(outside.status(), StatusCode::FORBIDDEN);

    let readonly_put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/Docs/new.txt")
                .header(header::AUTHORIZATION, &scoped_auth)
                .body(Body::from("blocked"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(readonly_put.status(), StatusCode::FORBIDDEN);

    let writable_put = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/Uploads/new.txt")
                .header(header::AUTHORIZATION, &scoped_auth)
                .body(Body::from("uploaded"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(writable_put.status(), StatusCode::CREATED);
    assert_eq!(
        std::fs::read_to_string(state.files_root.join("Inbox/Uploads/new.txt"))
            .expect("read mapped upload"),
        "uploaded"
    );
    assert!(!state.files_root.join("Uploads/new.txt").exists());
}

#[tokio::test]
async fn local_users_are_confined_to_their_own_nextcloud_files_root() {
    let (app, temp, gono_password, state) = app_with_state().await;
    let alice = db::create_local_user(&state.db, "alice", Some("Alice"))
        .await
        .expect("create alice");
    assert!(state
        .files_root
        .ends_with(std::path::Path::new("users").join("gono").join("files")));
    assert!(!temp.path().join("data/files").exists());

    let alice_on_gono = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/gono/")
                .header("Depth", "0")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for("alice", &alice.app_password),
                )
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(alice_on_gono.status(), StatusCode::FORBIDDEN);

    let alice_root = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/alice/")
                .header("Depth", "0")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for("alice", &alice.app_password),
                )
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(alice_root.status(), StatusCode::MULTI_STATUS);

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/alice/alice.txt")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for("alice", &alice.app_password),
                )
                .body(Body::from("alice only"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);
    assert_eq!(put.headers().get("x-nc-ownerid").unwrap(), "alice");
    assert!(temp
        .path()
        .join("data/users/alice/files/alice.txt")
        .exists());
    assert!(!state.files_root.join("alice.txt").exists());

    let alice_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM file_ids WHERE owner = ?1 AND rel_path = 'alice.txt'",
    )
    .bind("alice")
    .fetch_one(&state.db)
    .await
    .expect("count alice file rows");
    assert_eq!(alice_rows, 1);

    let gono_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM file_ids WHERE owner = ?1 AND rel_path = 'alice.txt'",
    )
    .bind(BOOTSTRAP_USER)
    .fetch_one(&state.db)
    .await
    .expect("count gono file rows");
    assert_eq!(gono_rows, 0);

    let gono_on_alice = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/files/alice/")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&gono_password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gono_on_alice.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn head_on_nextcloud_files_path_returns_metadata_without_body() {
    let (app, _temp, password) = app_with_temp_root().await;

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri("/remote.php/dav/files/gono/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("oc-etag"));
    assert!(response.headers().contains_key("oc-fileid"));
    assert_eq!(
        response.headers().get("x-nc-permissions").unwrap(),
        "RGDNVCK"
    );
    assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "0");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.is_empty());
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
    assert!(body.contains("RGDNVCK"));
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
    assert!(first.state.instance_id.starts_with('i'));
    assert_ne!(first.state.instance_id, "phase1");
    let bootstrap_passwords = db::list_local_app_passwords(&first.state.db, BOOTSTRAP_USER)
        .await
        .expect("list bootstrap app passwords");
    assert_eq!(bootstrap_passwords.len(), 1);
    assert_eq!(bootstrap_passwords[0].label, db::DEFAULT_APP_PASSWORD_LABEL);

    let second = AppState::initialize(config).await.expect("second init");
    assert_eq!(second.bootstrap.generated_password, None);
    assert_eq!(second.state.instance_id, first.state.instance_id);
    assert!(second
        .state
        .user_store
        .verify(BOOTSTRAP_USER, &password)
        .await
        .expect("old password still works")
        .is_some());
}

#[tokio::test]
async fn startup_prunes_change_log_for_all_enabled_users() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp);
    config.sync.change_log_retention_days = 0;
    config.sync.change_log_min_entries = 1;

    let initialized = AppState::initialize(config.clone())
        .await
        .expect("first init");
    db::create_local_user(&initialized.state.db, "alice", Some("Alice"))
        .await
        .expect("create alice");

    for (owner, file_id) in [(BOOTSTRAP_USER, 1_i64), ("alice", 2_i64)] {
        for index in 0..3 {
            let rel_path = format!("{owner}-{index}.txt");
            db::record_change(
                &initialized.state.db,
                owner,
                file_id,
                std::path::Path::new(&rel_path),
                "create",
            )
            .await
            .expect("record change");
        }
    }
    sqlx::query("UPDATE change_log SET changed_at = ?1")
        .bind(db::unix_timestamp() - 86_400)
        .execute(&initialized.state.db)
        .await
        .expect("age change log rows");
    drop(initialized);

    let restarted = AppState::initialize(config)
        .await
        .expect("second init prunes all users");

    for owner in [BOOTSTRAP_USER, "alice"] {
        let changes = db::list_change_log(&restarted.state.db, owner)
            .await
            .expect("list changes");
        assert_eq!(changes.len(), 1, "{owner}");
        assert_eq!(changes[0].sync_token, 3, "{owner}");
        let floor = db::change_log_floor_token(&restarted.state.db, owner, 3)
            .await
            .expect("floor token");
        assert_eq!(floor, 2, "{owner}");
    }
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
async fn admin_disabled_returns_not_found() {
    let (app, _temp, _password) = app_with_temp_root().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_users_page_requires_configured_admin() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;

    for uri in ["/admin", "/admin/"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/admin/users"
        );
    }

    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let alice = db::create_local_user(&state.db, "alice", None)
        .await
        .expect("create non-admin user");
    db::update_local_app_password_expiry(
        &state.db,
        "alice",
        db::DEFAULT_APP_PASSWORD_LABEL,
        Some(db::unix_timestamp() + 86_400),
    )
    .await
    .expect("set expiring app password");
    let forbidden = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/users")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for("alice", &alice.app_password),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let allowed = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/users")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let body = to_bytes(allowed.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("User Management"));
    assert!(body.contains("gono"));
    let expiration_values = expiration_input_values(&body);
    assert!(expiration_values.len() >= 2);
    assert!(expiration_values.iter().all(|value| {
        value.len() == 16
            && value.as_bytes()[4] == b'-'
            && value.as_bytes()[7] == b'-'
            && value.as_bytes()[10] == b'T'
            && value.as_bytes()[13] == b':'
    }));
    assert_eq!(hidden_expiration_field_count(&body), 2);
    assert_eq!(visible_expiration_field_count(&body), 1);
    assert!(body.contains("select.value !== 'at'"));
    assert!(body.contains("[hidden] { display: none !important; }"));
    assert!(body.contains("Set expiry"));
    assert!(!body.contains("Save expiry"));
}

#[tokio::test]
async fn admin_pages_warn_when_base_url_uses_http() {
    let (app, _temp, password, _state) = app_with_config(|config| {
        config.server.base_url = "http://files.example.test".to_owned();
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;

    for uri in ["/admin/users", "/admin/settings"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth_header(&password))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Admin is configured with an HTTP base URL"));
        assert!(body.contains("http://files.example.test"));
        assert!(body.contains("use HTTPS through Nginx or another reverse proxy"));
    }
}

#[tokio::test]
async fn admin_status_page_shows_runtime_state() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;
    let _receiver = state
        .notify_push
        .as_ref()
        .expect("notify runtime")
        .subscribe(BOOTSTRAP_USER)
        .expect("subscribe notify client");
    assert_eq!(
        state
            .auth_rate_limiter
            .register_failure("203.0.113.7", BOOTSTRAP_USER)
            .as_secs(),
        5
    );
    assert_eq!(
        state
            .auth_rate_limiter
            .register_failure("203.0.113.7", BOOTSTRAP_USER)
            .as_secs(),
        10
    );

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/status")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("System Status"));
    assert!(body.contains("Notify Push"));
    assert!(body.contains("Active connections"));
    assert!(body.contains("1 connection(s)"));
    assert!(body.contains("Notify Push Clients"));
    assert!(body.contains(BOOTSTRAP_USER));
    assert!(body.contains("Database"));
    assert!(body.contains("Storage"));
    assert!(body.contains("Auth Rate Limit"));
    assert!(body.contains("Active throttled keys"));
    assert!(body.contains("Total failed attempts"));
    assert!(body.contains("Max response delay"));
    assert!(body.contains(">1</strong>"));
    assert!(body.contains(">2</strong>"));
    assert!(body.contains(">10s</strong>"));
}

#[tokio::test]
async fn configured_admin_user_is_created_for_admin_access() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp);
    config.admin.enabled = true;
    config.admin.users = vec!["kimi".to_owned()];

    let initialized = AppState::initialize(config)
        .await
        .expect("initialize with configured admin user");
    let gono_password = initialized
        .bootstrap
        .generated_password
        .clone()
        .expect("bootstrap password");
    let admin_user = initialized
        .bootstrap
        .generated_admin_users
        .first()
        .expect("generated configured admin user")
        .clone();
    assert_eq!(admin_user.username, "kimi");

    let app = build_router(initialized.state.clone());
    let gono_forbidden = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/users")
                .header(header::AUTHORIZATION, auth_header(&gono_password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gono_forbidden.status(), StatusCode::FORBIDDEN);

    let allowed = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/users")
                .header(
                    header::AUTHORIZATION,
                    auth_header_for("kimi", &admin_user.app_password),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let body = to_bytes(allowed.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("User Management"));
    assert!(body.contains("kimi"));
}

#[tokio::test]
async fn admin_create_user_requires_csrf_and_shows_one_time_password() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;

    let invalid_csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/users")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "_csrf=wrong&username=alice&display_name=Alice+Example",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_csrf.status(), StatusCode::FORBIDDEN);

    let body = format!(
        "_csrf={}&username=alice&display_name=Alice+Example",
        state.admin_csrf_token
    );
    let created = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/users")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let body = to_bytes(created.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("App password created"));
    assert!(body.contains("Alice Example"));
}

#[tokio::test]
async fn admin_settings_page_saves_editable_settings_with_restart_notice() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;

    let page = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/settings")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let body = to_bytes(page.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Settings"));
    assert!(body.contains("server_base_url"));
    assert!(body.contains("readonly-field"));

    let invalid_csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/settings")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(settings_form_body(
                    "wrong",
                    "https://changed.example",
                    "/push",
                    BOOTSTRAP_USER,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_csrf.status(), StatusCode::FORBIDDEN);

    let saved = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/settings")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(settings_form_body(
                    &state.admin_csrf_token,
                    "https://changed.example",
                    "/push",
                    BOOTSTRAP_USER,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::OK);
    let body = to_bytes(saved.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Settings saved"));
    assert!(body.contains("Restart required"));
    assert!(body.contains("https://changed.example"));
    assert_ne!(state.base_url, "https://changed.example");

    let value: String =
        sqlx::query_scalar("SELECT value_json FROM settings WHERE key = 'server.base_url'")
            .fetch_one(&state.db)
            .await
            .expect("load saved setting");
    assert_eq!(value, "\"https://changed.example\"");
}

#[tokio::test]
async fn admin_settings_rejects_invalid_values() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.admin.enabled = true;
        config.admin.users = vec![BOOTSTRAP_USER.to_owned()];
    })
    .await;

    let invalid_url = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/settings")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(settings_form_body(
                    &state.admin_csrf_token,
                    "ftp://wrong.example",
                    "/push",
                    BOOTSTRAP_USER,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_url.status(), StatusCode::OK);
    let body = to_bytes(invalid_url.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("server.base_url must start"));

    let invalid_path = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/settings")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(settings_form_body(
                    &state.admin_csrf_token,
                    "https://changed.example",
                    "push",
                    BOOTSTRAP_USER,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_path.status(), StatusCode::OK);
    let body = to_bytes(invalid_path.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("notify_push.path must start"));
}

fn settings_form_body(csrf: &str, base_url: &str, notify_path: &str, admin_users: &str) -> String {
    format!(
        concat!(
            "_csrf={csrf}",
            "&server_base_url={base_url}",
            "&auth_realm=Gono Cloud",
            "&sync_change_log_retention_days=30",
            "&sync_change_log_min_entries=10000",
            "&notify_push_enabled=true",
            "&notify_push_path={notify_path}",
            "&notify_push_advertised_types=files,activities,notifications",
            "&notify_push_pre_auth_ttl_secs=15",
            "&notify_push_user_connection_limit=64",
            "&notify_push_max_debounce_secs=15",
            "&notify_push_ping_interval_secs=30",
            "&notify_push_auth_timeout_secs=15",
            "&notify_push_max_connection_secs=0",
            "&admin_enabled=true",
            "&admin_users={admin_users}",
        ),
        csrf = csrf,
        base_url = base_url,
        notify_path = notify_path,
        admin_users = admin_users,
    )
}

fn expiration_input_values(body: &str) -> Vec<String> {
    let marker = r#"name="expires_at" type="text" value=""#;
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(index) = rest.find(marker) {
        let value_start = index + marker.len();
        let value_rest = &rest[value_start..];
        let Some(value_end) = value_rest.find('"') else {
            break;
        };
        values.push(value_rest[..value_end].to_owned());
        rest = &value_rest[value_end..];
    }
    values
}

fn hidden_expiration_field_count(body: &str) -> usize {
    body.matches(r#"<div class="field" data-expiry-time-field hidden>"#)
        .count()
}

fn visible_expiration_field_count(body: &str) -> usize {
    body.matches(r#"<div class="field" data-expiry-time-field>"#)
        .count()
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

    let lock_table_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1")
            .bind("webdav_locks")
            .fetch_one(&initialized.state.db)
            .await
            .expect("count lock table");
    assert_eq!(lock_table_count.0, 1);
}

#[tokio::test]
async fn webdav_locks_survive_app_restart() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp);
    let initialized = AppState::initialize(config.clone())
        .await
        .expect("initial app state");
    let password = initialized
        .bootstrap
        .generated_password
        .expect("first bootstrap password");
    let app = build_router(initialized.state.clone());

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/persistent-lock.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("before lock"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());

    let lock = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"LOCK").unwrap())
                .uri("/remote.php/dav/persistent-lock.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .header("Depth", "0")
                .header("Timeout", "Second-300")
                .body(Body::from(lock_info_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lock.status(), StatusCode::OK);
    let lock_token = lock
        .headers()
        .get("lock-token")
        .expect("lock token")
        .to_str()
        .expect("lock token string")
        .to_owned();

    let restarted = AppState::initialize(config)
        .await
        .expect("restarted app state");
    assert!(restarted.bootstrap.generated_password.is_none());
    let restarted_app = build_router(restarted.state);

    let locked_put = restarted_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/persistent-lock.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("blocked"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(locked_put.status(), StatusCode::LOCKED);

    let unlock = restarted_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"UNLOCK").unwrap())
                .uri("/remote.php/dav/persistent-lock.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Lock-Token", lock_token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unlock.status(), StatusCode::NO_CONTENT);

    let unlocked_put = restarted_app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/persistent-lock.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("after unlock"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(unlocked_put.status().is_success());
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
    let oc_etag = response.headers().get("oc-etag").unwrap().to_str().unwrap();
    assert_eq!(
        response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("\"{oc_etag}\"")
    );
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
async fn put_accepts_nextcloud_mtime_header() {
    let (app, _temp, password, state) = app_with_state().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/mtime-upload.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("X-OC-MTime", "1700000000")
                .body(Body::from("mtime"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers().get("x-oc-mtime").unwrap(), "accepted");
    let oc_etag = response.headers().get("oc-etag").unwrap().to_str().unwrap();
    assert_eq!(
        response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("\"{oc_etag}\"")
    );

    let modified = std::fs::metadata(state.files_root.join("mtime-upload.txt"))
        .expect("uploaded metadata")
        .modified()
        .expect("uploaded modified time");
    assert_eq!(
        modified
            .duration_since(UNIX_EPOCH)
            .expect("mtime after epoch")
            .as_secs(),
        1_700_000_000
    );
}

#[tokio::test]
async fn put_with_nextcloud_auto_mkcol_creates_missing_parents() {
    let (app, _temp, password, state) = app_with_state().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/auto/a/b/file.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("X-NC-WebDAV-AutoMkcol", "1")
                .body(Body::from("auto parent"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(state.files_root.join("auto/a/b").is_dir());

    let get = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/auto/a/b/file.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"auto parent");
}

#[tokio::test]
async fn put_without_auto_mkcol_still_rejects_missing_parents() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/files/gono/missing/parent/file.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("missing parent"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
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
async fn copy_with_depth_infinity_is_allowed_for_litmus_compatibility() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/depth-copy-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("copy me"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_response.status(), StatusCode::CREATED);

    let copy_response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/depth-copy-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Depth", "infinity")
                .header("Destination", "/remote.php/dav/depth-copy-target.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(copy_response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn copy_does_not_inherit_favorite_metadata() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/favorite-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("favorite source"))
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
                .uri("/remote.php/dav/favorite-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_favorite_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch.status().is_success());

    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/favorite-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/favorite-copy.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(copy.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/favorite-copy.txt")
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
    assert!(body.contains("<oc:favorite>0</oc:favorite>"));
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
    symlink(&outside, temp.path().join("data/users/gono/files/link")).expect("create symlink");

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

#[cfg(unix)]
#[tokio::test]
async fn symlink_existing_target_escape_is_rejected_for_get() {
    use std::os::unix::fs::symlink;

    let (app, temp, password) = app_with_temp_root().await;
    let outside = temp.path().join("outside-secret.txt");
    std::fs::write(&outside, "secret").expect("outside secret");
    symlink(
        &outside,
        temp.path().join("data/users/gono/files/secret-link.txt"),
    )
    .expect("create symlink");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/remote.php/dav/secret-link.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn encoded_parent_segments_are_rejected_for_put() {
    let (app, temp, password) = app_with_temp_root().await;
    std::fs::create_dir_all(temp.path().join("data/users/gono/files/nested")).expect("nested dir");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/nested/%2e%2e/escape.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("escape"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(!temp
        .path()
        .join("data/users/gono/files/escape.txt")
        .exists());
}

#[tokio::test]
async fn destination_encoded_parent_and_nul_are_rejected_for_copy() {
    let (app, temp, password) = app_with_temp_root().await;
    std::fs::create_dir_all(temp.path().join("data/users/gono/files/nested")).expect("nested dir");

    let encoded_parent = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/hello.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/nested/%2e%2e/copied.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(encoded_parent.status(), StatusCode::BAD_REQUEST);
    assert!(!temp
        .path()
        .join("data/users/gono/files/copied.txt")
        .exists());

    let nul = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/hello.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/bad%00name.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(nul.status(), StatusCode::BAD_REQUEST);
}

#[cfg(unix)]
#[tokio::test]
async fn chunking_destination_under_symlink_parent_is_rejected() {
    use std::os::unix::fs::symlink;

    let (app, temp, password) = app_with_temp_root().await;
    let outside = temp.path().join("outside-chunks");
    std::fs::create_dir_all(&outside).expect("outside dir");
    symlink(
        &outside,
        temp.path().join("data/users/gono/files/chunk-link"),
    )
    .expect("create symlink");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri("/remote.php/dav/uploads/gono/symlink-destination")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/chunk-link/escape.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(!outside.join("escape.bin").exists());
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
async fn report_sync_collection_rejects_pruned_sync_tokens() {
    let (app, _temp, password, state) = app_with_config(|config| {
        config.sync.change_log_retention_days = 0;
        config.sync.change_log_min_entries = 1;
    })
    .await;

    for (name, body) in [("compact-a.txt", "a"), ("compact-b.txt", "b")] {
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

    sqlx::query("UPDATE change_log SET changed_at = ?1 WHERE owner = ?2")
        .bind(db::unix_timestamp() - 86_400)
        .bind(BOOTSTRAP_USER)
        .execute(&state.db)
        .await
        .expect("age change log rows");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/compact-c.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("c"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    let changes = db::list_change_log(&state.db, BOOTSTRAP_USER)
        .await
        .expect("change log");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].rel_path, "compact-c.txt");
    assert_eq!(changes[0].sync_token, 3);
    let floor_token = db::change_log_floor_token(&state.db, BOOTSTRAP_USER, 3)
        .await
        .expect("floor token");
    assert_eq!(floor_token, 2);

    let stale = app
        .clone()
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
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
    let stale_body = to_bytes(stale.into_body(), usize::MAX).await.unwrap();
    let stale_body = std::str::from_utf8(&stale_body).unwrap();
    assert!(stale_body.contains("<d:valid-sync-token/>"));
    assert!(stale_body.contains("<g:sync-token-floor>2</g:sync-token-floor>"));

    let fresh = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(sync_collection_body("2")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fresh.status(), StatusCode::MULTI_STATUS);
    let fresh_body = to_bytes(fresh.into_body(), usize::MAX).await.unwrap();
    let fresh_body = std::str::from_utf8(&fresh_body).unwrap();
    assert!(fresh_body.contains("<d:sync-token>3</d:sync-token>"));
    assert!(fresh_body.contains("/remote.php/dav/compact-c.txt"));
}

#[tokio::test]
async fn root_path_report_sync_collection_uses_root_hrefs() {
    let (app, _temp, password, _state) = app_with_state().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/root-report.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("root report"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(sync_collection_body("0")))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("<d:href>/root-report.txt</d:href>"));
    assert!(!body.contains("/remote.php/dav/root-report.txt"));
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
async fn report_sync_collection_marks_superseded_create_as_not_found() {
    let (app, _temp, password, _state) = app_with_state().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/sync-create-then-delete.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("short lived"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/remote.php/dav/sync-create-then-delete.txt")
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
                .body(Body::from(sync_collection_body("0")))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert_eq!(
        body.matches("/remote.php/dav/sync-create-then-delete.txt")
            .count(),
        2
    );
    assert!(body.matches("HTTP/1.1 404 Not Found").count() >= 2);
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
async fn dead_props_are_readable_and_copied() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/dead-prop-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("dead prop"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPPATCH").unwrap())
                .uri("/remote.php/dav/dead-prop-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_dead_prop_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch.status().is_success());

    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"COPY").unwrap())
                .uri("/remote.php/dav/dead-prop-source.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header("Destination", "/remote.php/dav/dead-prop-copy.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(copy.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/dead-prop-copy.txt")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_dead_prop_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("litmus-value"));
}

#[tokio::test]
async fn propfind_reports_404_for_removed_dead_props() {
    let (app, _temp, password) = app_with_temp_root().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/dead-prop-remove.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("dead prop remove"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPPATCH").unwrap())
                .uri("/remote.php/dav/dead-prop-remove.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_two_dead_props_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch.status().is_success());

    let remove = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPPATCH").unwrap())
                .uri("/remote.php/dav/dead-prop-remove.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(proppatch_remove_dead_prop_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(remove.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"PROPFIND").unwrap())
                .uri("/remote.php/dav/dead-prop-remove.txt")
                .header("Depth", "0")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(propfind_two_dead_props_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("HTTP/1.1 404 Not Found"));
    assert!(body.contains("prop0"));
    assert!(body.contains("prop1"));
    assert!(body.contains("value1"));
    assert!(!body.contains("value0"));
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
async fn report_filter_files_skips_deleted_index_rows() {
    let (app, _temp, password, state) = app_with_state().await;

    for (uri, body) in [
        ("/remote.php/dav/live-favorite.txt", "live"),
        ("/remote.php/dav/deleted-favorite.txt", "deleted"),
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

    std::fs::remove_file(state.files_root.join("deleted-favorite.txt"))
        .expect("remove indexed favorite file");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"REPORT").unwrap())
                .uri("/remote.php/dav/")
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
    assert!(body.contains("/remote.php/dav/live-favorite.txt"));
    assert!(!body.contains("/remote.php/dav/deleted-favorite.txt"));
}

#[tokio::test]
async fn search_by_file_id_returns_exact_match() {
    let (app, _temp, password) = app_with_temp_root().await;
    let target = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/search-id-target.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("target"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(target.status().is_success());
    let oc_file_id = target.headers().get("oc-fileid").unwrap().to_str().unwrap();
    let numeric_file_id: String = oc_file_id
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();

    let other = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/search-id-other.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("other"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(other.status().is_success());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_file_id_body(&numeric_file_id)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/search-id-target.txt"));
    assert!(!body.contains("/remote.php/dav/search-id-other.txt"));
    assert!(body.contains("<oc:fileid>"));
}

#[tokio::test]
async fn root_path_search_uses_root_hrefs() {
    let (app, _temp, password) = app_with_temp_root().await;
    let target = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/root-search.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("root search"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(target.status().is_success());
    let oc_file_id = target.headers().get("oc-fileid").unwrap().to_str().unwrap();
    let numeric_file_id: String = oc_file_id
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_file_id_body(&numeric_file_id)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("<d:href>/root-search.txt</d:href>"));
    assert!(!body.contains("/remote.php/dav/root-search.txt"));
}

#[tokio::test]
async fn search_by_favorite_respects_scope() {
    let (app, _temp, password) = app_with_temp_root().await;
    let mkcol = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"MKCOL").unwrap())
                .uri("/remote.php/dav/search-scope")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(mkcol.status().is_success());

    for (uri, body) in [
        ("/remote.php/dav/search-scope/starred.txt", "starred"),
        ("/remote.php/dav/search-scope/plain.txt", "plain"),
        ("/remote.php/dav/search-outside-starred.txt", "outside"),
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
        "/remote.php/dav/search-scope/starred.txt",
        "/remote.php/dav/search-outside-starred.txt",
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
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_favorite_body("/files/gono/search-scope")))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/search-scope/starred.txt"));
    assert!(!body.contains("/remote.php/dav/search-scope/plain.txt"));
    assert!(!body.contains("/remote.php/dav/search-outside-starred.txt"));
}

#[tokio::test]
async fn search_by_favorite_skips_deleted_index_rows() {
    let (app, _temp, password, state) = app_with_state().await;

    for (uri, body) in [
        ("/remote.php/dav/live-search-favorite.txt", "live"),
        ("/remote.php/dav/deleted-search-favorite.txt", "deleted"),
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

    std::fs::remove_file(state.files_root.join("deleted-search-favorite.txt"))
        .expect("remove indexed search favorite file");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_favorite_body("/files/gono")))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/live-search-favorite.txt"));
    assert!(!body.contains("/remote.php/dav/deleted-search-favorite.txt"));
}

#[tokio::test]
async fn search_rejects_unsupported_where_operator() {
    let (app, _temp, password) = app_with_temp_root().await;
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_unsupported_like_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fresh_metadata_cache_is_used_for_search_results() {
    let (app, _temp, password, state) = app_with_state().await;
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/remote.php/dav/cache-hit.txt")
                .header(header::AUTHORIZATION, auth_header(&password))
                .body(Body::from("cache me"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_success());
    let oc_file_id = put.headers().get("oc-fileid").unwrap().to_str().unwrap();
    let numeric_file_id: String = oc_file_id
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();

    sqlx::query(
        r#"
        UPDATE file_ids
        SET etag = 'cached-etag'
        WHERE owner = ?1 AND rel_path = ?2
        "#,
    )
    .bind(BOOTSTRAP_USER)
    .bind("cache-hit.txt")
    .execute(&state.db)
    .await
    .expect("patch cached etag");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::from_bytes(b"SEARCH").unwrap())
                .uri("/remote.php/dav/")
                .header(header::AUTHORIZATION, auth_header(&password))
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(search_file_id_body(&numeric_file_id)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("/remote.php/dav/cache-hit.txt"));
    assert!(body.contains("<d:getetag>cached-etag</d:getetag>"));
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

#[tokio::test]
async fn expired_chunk_upload_cleanup_removes_directory_then_session_row() {
    let (_app, _temp, _password, state) = app_with_state().await;
    let upload_id = "expired-session";
    let session_dir = state.uploads_root.join(BOOTSTRAP_USER).join(upload_id);
    std::fs::create_dir_all(&session_dir).expect("create expired upload dir");
    std::fs::write(session_dir.join("1"), "stale").expect("write stale chunk");

    db::upsert_upload_session(
        &state.db,
        upload_id,
        BOOTSTRAP_USER,
        std::path::Path::new("expired-target.txt"),
        5,
    )
    .await
    .expect("insert expired upload session");
    sqlx::query(
        r#"
        UPDATE upload_sessions
        SET expires_at = ?1
        WHERE owner = ?2 AND upload_id = ?3
        "#,
    )
    .bind(db::unix_timestamp() - 7_200)
    .bind(BOOTSTRAP_USER)
    .bind(upload_id)
    .execute(&state.db)
    .await
    .expect("expire upload session");

    let removed = chunked_upload::cleanup_expired_sessions(&state)
        .await
        .expect("cleanup expired session");

    assert_eq!(removed, 1);
    assert!(!session_dir.exists());
    assert!(
        db::load_upload_session(&state.db, BOOTSTRAP_USER, upload_id)
            .await
            .expect("load upload session")
            .is_none()
    );
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

fn proppatch_dead_prop_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:x="http://example.com/litmus">
  <d:set>
    <d:prop>
      <x:litmus-prop>litmus-value</x:litmus-prop>
    </d:prop>
  </d:set>
</d:propertyupdate>"#
}

fn proppatch_two_dead_props_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:x="http://example.com/litmus">
  <d:set>
    <d:prop>
      <x:prop0>value0</x:prop0>
      <x:prop1>value1</x:prop1>
    </d:prop>
  </d:set>
</d:propertyupdate>"#
}

fn proppatch_remove_dead_prop_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:x="http://example.com/litmus">
  <d:remove>
    <d:prop>
      <x:prop0 />
    </d:prop>
  </d:remove>
</d:propertyupdate>"#
}

fn propfind_dead_prop_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:x="http://example.com/litmus">
  <d:prop>
    <x:litmus-prop />
  </d:prop>
</d:propfind>"#
}

fn propfind_two_dead_props_body() -> &'static str {
    r#"<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:x="http://example.com/litmus">
  <d:prop>
    <x:prop0 />
    <x:prop1 />
  </d:prop>
</d:propfind>"#
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

fn lock_info_body() -> &'static str {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<d:lockinfo xmlns:d="DAV:">
  <d:lockscope><d:exclusive /></d:lockscope>
  <d:locktype><d:write /></d:locktype>
  <d:owner><d:href>gono</d:href></d:owner>
</d:lockinfo>"#
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

fn search_file_id_body(file_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<d:searchrequest xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:basicsearch>
    <d:select>
      <d:prop>
        <oc:fileid />
        <d:getetag />
        <oc:favorite />
      </d:prop>
    </d:select>
    <d:from>
      <d:scope>
        <d:href>/files/gono</d:href>
        <d:depth>infinity</d:depth>
      </d:scope>
    </d:from>
    <d:where>
      <d:eq>
        <d:prop><oc:fileid /></d:prop>
        <d:literal>{file_id}</d:literal>
      </d:eq>
    </d:where>
    <d:orderby />
  </d:basicsearch>
</d:searchrequest>"#
    )
}

fn search_favorite_body(scope: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<d:searchrequest xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:basicsearch>
    <d:select>
      <d:prop>
        <oc:fileid />
        <d:getetag />
        <oc:favorite />
      </d:prop>
    </d:select>
    <d:from>
      <d:scope>
        <d:href>{scope}</d:href>
        <d:depth>infinity</d:depth>
      </d:scope>
    </d:from>
    <d:where>
      <d:eq>
        <d:prop><oc:favorite /></d:prop>
        <d:literal>1</d:literal>
      </d:eq>
    </d:where>
    <d:orderby />
  </d:basicsearch>
</d:searchrequest>"#
    )
}

fn search_unsupported_like_body() -> &'static str {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<d:searchrequest xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:basicsearch>
    <d:select><d:prop><oc:fileid /></d:prop></d:select>
    <d:from>
      <d:scope>
        <d:href>/files/gono</d:href>
        <d:depth>infinity</d:depth>
      </d:scope>
    </d:from>
    <d:where>
      <d:like>
        <d:prop><d:displayname /></d:prop>
        <d:literal>%.txt</d:literal>
      </d:like>
    </d:where>
  </d:basicsearch>
</d:searchrequest>"#
}
