use std::{
    path::{Path as StdPath, PathBuf},
    sync::Arc,
    time::Instant,
};

use axum::{
    Router,
    extract::{Extension, Form, Path, State},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post},
};
use serde::Deserialize;
use tracing::info;

use crate::{
    admin::{
        auth::require_admin,
        html::{self, Notice, NoticeKind, OneTimePassword},
    },
    auth::Principal,
    dav_handler::upload_space,
    db::{self, AppPasswordScopeInput, BOOTSTRAP_USER},
    permissions::PermissionLevel,
    state::AppState,
};

pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let protected = Router::new()
        .route("/users", get(users_page).post(create_user))
        .route("/status", get(status_page))
        .route("/clients", get(clients_page))
        .route("/settings", get(settings_page))
        .route("/app-passwords", post(create_app_password))
        .route("/users/{username}/display-name", post(update_display_name))
        .route("/users/{username}/enable", post(enable_user))
        .route("/users/{username}/disable", post(disable_user))
        .route(
            "/users/{username}/reset-password",
            post(reset_user_password),
        )
        .route(
            "/users/{username}/app-passwords/{label}/reset-password",
            post(reset_app_password),
        )
        .route(
            "/users/{username}/app-passwords/{label}/delete",
            post(delete_app_password),
        )
        .route(
            "/users/{username}/app-passwords/{label}/expires-at",
            post(update_app_password_expiry),
        )
        .route("/users/{username}/delete", post(delete_user))
        .route_layer(middleware::from_fn_with_state(state, require_admin));

    Router::new()
        .route(
            "/gono-admin",
            get(|| async { Redirect::to("/gono-admin/users") }),
        )
        .route(
            "/gono-admin/",
            get(|| async { Redirect::to("/gono-admin/users") }),
        )
        .nest("/gono-admin", protected)
}

pub fn disabled_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/gono-admin", any(not_found))
        .route("/gono-admin/", any(not_found))
        .nest("/gono-admin", Router::new().fallback(not_found))
}

#[derive(Debug, Deserialize)]
struct CreateUserForm {
    #[serde(rename = "_csrf")]
    csrf: String,
    username: String,
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateAppPasswordForm {
    #[serde(rename = "_csrf")]
    csrf: String,
    username: String,
    label: String,
    expires_at_mode: Option<String>,
    expires_at: Option<String>,
    mount_path: Option<String>,
    storage_path: Option<String>,
    permission: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AppPasswordExpiryForm {
    #[serde(rename = "_csrf")]
    csrf: String,
    expires_at_mode: Option<String>,
    expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DisplayNameForm {
    #[serde(rename = "_csrf")]
    csrf: String,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct CsrfForm {
    #[serde(rename = "_csrf")]
    csrf: String,
}

async fn users_page(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    render_users(&state, &principal, None, None).await
}

async fn settings_page(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    render_settings(&state, &principal).await
}

async fn status_page(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    match load_status_page_data(&state).await {
        Ok(status) => Html(html::render_status_page(
            &principal.username,
            &state.config,
            &status,
        ))
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to load status: {err:#}"),
        )
            .into_response(),
    }
}

async fn clients_page(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    Html(html::render_clients_page(
        &principal.username,
        &state.config,
        &load_clients_page_data(&state),
    ))
    .into_response()
}

async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Form(form): Form<CreateUserForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::create_local_user(&state.db, &form.username, form.display_name.as_deref()).await {
        Ok(created) => {
            info!(
                admin = %principal.username,
                username = %created.username,
                "admin created local user"
            );
            let notice = Notice {
                kind: NoticeKind::Success,
                text: format!("Created local user {}.", created.username),
            };
            let secret = OneTimePassword {
                username: created.username,
                label: created.password_label,
                password: created.app_password,
            };
            render_users(&state, &principal, Some(notice), Some(secret)).await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn create_app_password(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Form(form): Form<CreateAppPasswordForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    let expires_at = match parse_expiry(form.expires_at_mode.as_deref(), form.expires_at.as_deref())
    {
        Ok(expires_at) => expires_at,
        Err(err) => return render_error(&state, &principal, err).await,
    };
    let scopes = match parse_scope_inputs(
        form.mount_path.as_deref(),
        form.storage_path.as_deref(),
        form.permission.as_deref(),
    ) {
        Ok(scopes) => scopes,
        Err(err) => return render_error(&state, &principal, err).await,
    };

    match db::create_local_app_password(&state.db, &form.username, &form.label, expires_at, &scopes)
        .await
    {
        Ok(created) => {
            info!(
                admin = %principal.username,
                username = %created.username,
                label = %created.password_label,
                "admin created app password"
            );
            let notice = Notice {
                kind: NoticeKind::Success,
                text: format!(
                    "Created app password {} for {}.",
                    created.password_label, created.username
                ),
            };
            let secret = OneTimePassword {
                username: created.username,
                label: created.password_label,
                password: created.app_password,
            };
            render_users(&state, &principal, Some(notice), Some(secret)).await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn update_display_name(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(username): Path<String>,
    Form(form): Form<DisplayNameForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::update_local_user_display_name(&state.db, &username, &form.display_name).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                "admin updated local user display name"
            );
            render_success(
                &state,
                &principal,
                format!("Updated display name for {username}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("Local user {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn enable_user(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(username): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::set_local_user_enabled(&state.db, &username, true).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                "admin enabled local user"
            );
            render_success(
                &state,
                &principal,
                format!("Enabled local user {username}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("Local user {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn disable_user(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(username): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }
    if let Err(response) = guard_last_admin(&state, &username).await {
        return response;
    }

    match db::set_local_user_enabled(&state.db, &username, false).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                "admin disabled local user"
            );
            render_success(
                &state,
                &principal,
                format!("Disabled local user {username}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("Local user {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn reset_user_password(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(username): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::reset_local_user_app_password(&state.db, &username).await {
        Ok(reset) => {
            info!(
                admin = %principal.username,
                username = %username,
                "admin reset local user app password"
            );
            let notice = Notice {
                kind: NoticeKind::Success,
                text: format!("Reset app password for {username}."),
            };
            let secret = OneTimePassword {
                username: reset.username,
                label: reset.password_label,
                password: reset.app_password,
            };
            render_users(&state, &principal, Some(notice), Some(secret)).await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn reset_app_password(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path((username, label)): Path<(String, String)>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::reset_local_app_password(&state.db, &username, &label).await {
        Ok(reset) => {
            info!(
                admin = %principal.username,
                username = %username,
                label = %label,
                "admin reset app password"
            );
            let notice = Notice {
                kind: NoticeKind::Success,
                text: format!("Reset app password {label} for {username}."),
            };
            let secret = OneTimePassword {
                username: reset.username,
                label: reset.password_label,
                password: reset.app_password,
            };
            render_users(&state, &principal, Some(notice), Some(secret)).await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn delete_app_password(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path((username, label)): Path<(String, String)>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    match db::delete_local_app_password(&state.db, &username, &label).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                label = %label,
                "admin deleted app password"
            );
            render_success(
                &state,
                &principal,
                format!("Deleted app password {label} for {username}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("App password {label} for {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn update_app_password_expiry(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path((username, label)): Path<(String, String)>,
    Form(form): Form<AppPasswordExpiryForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }
    let expires_at = match parse_expiry(form.expires_at_mode.as_deref(), form.expires_at.as_deref())
    {
        Ok(expires_at) => expires_at,
        Err(err) => return render_error(&state, &principal, err).await,
    };

    match db::update_local_app_password_expiry(&state.db, &username, &label, expires_at).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                label = %label,
                "admin updated app password expiry"
            );
            render_success(
                &state,
                &principal,
                format!("Updated expiry for app password {label}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("App password {label} for {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(username): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }
    if username == BOOTSTRAP_USER {
        return render_error_message(
            &state,
            &principal,
            "Refusing to delete bootstrap user gono.",
        )
        .await;
    }
    if let Err(response) = guard_last_admin(&state, &username).await {
        return response;
    }

    match db::delete_local_user(&state.db, &username).await {
        Ok(true) => {
            info!(
                admin = %principal.username,
                username = %username,
                "admin deleted local user"
            );
            render_success(
                &state,
                &principal,
                format!("Deleted local user {username}."),
            )
            .await
        }
        Ok(false) => {
            render_error_message(
                &state,
                &principal,
                format!("Local user {username} not found."),
            )
            .await
        }
        Err(err) => render_error(&state, &principal, err).await,
    }
}

async fn render_success(state: &Arc<AppState>, principal: &Principal, text: String) -> Response {
    render_users(
        state,
        principal,
        Some(Notice {
            kind: NoticeKind::Success,
            text,
        }),
        None,
    )
    .await
}

async fn render_error(
    state: &Arc<AppState>,
    principal: &Principal,
    err: anyhow::Error,
) -> Response {
    render_error_message(state, principal, format!("{err:#}")).await
}

async fn render_error_message(
    state: &Arc<AppState>,
    principal: &Principal,
    text: impl Into<String>,
) -> Response {
    render_users(
        state,
        principal,
        Some(Notice {
            kind: NoticeKind::Error,
            text: text.into(),
        }),
        None,
    )
    .await
}

async fn render_users(
    state: &Arc<AppState>,
    principal: &Principal,
    notice: Option<Notice>,
    one_time_password: Option<OneTimePassword>,
) -> Response {
    match db::list_local_users(&state.db).await {
        Ok(users) => Html(html::render_users_page(
            &principal.username,
            &state.admin_csrf_token,
            &state.config,
            &users,
            notice.as_ref(),
            one_time_password.as_ref(),
        ))
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to load users: {err:#}"),
        )
            .into_response(),
    }
}

async fn render_settings(state: &Arc<AppState>, principal: &Principal) -> Response {
    Html(html::render_settings_page(
        &principal.username,
        &state.config,
    ))
    .into_response()
}

async fn load_status_page_data(state: &AppState) -> anyhow::Result<html::StatusPageData> {
    let users = db::list_local_users(&state.db).await?;
    let enabled_users = users.iter().filter(|user| user.enabled).count();
    let app_passwords = users
        .iter()
        .map(|user| user.app_password_count)
        .sum::<i64>();
    let file_records = count_rows(&state.db, "SELECT COUNT(*) FROM file_ids").await?;
    let change_log_entries = count_rows(&state.db, "SELECT COUNT(*) FROM change_log").await?;
    let upload_sessions = count_rows(&state.db, "SELECT COUNT(*) FROM upload_sessions").await?;
    let expired_upload_sessions = count_rows_bound_i64(
        &state.db,
        "SELECT COUNT(*) FROM upload_sessions WHERE expires_at <= ?1",
        db::unix_timestamp(),
    )
    .await?;
    let sync_token = db::current_sync_token(&state.db, &state.owner).await?;
    let change_log_floor_token =
        db::change_log_floor_token(&state.db, &state.owner, sync_token).await?;
    let (storage_available, storage_available_ms) =
        measure_elapsed_millis(|| Ok(fs2::available_space(&state.data_root)?))?;
    let (storage_total, storage_total_ms) =
        measure_elapsed_millis(|| Ok(fs2::total_space(&state.data_root)?))?;
    let (upload_reserved, upload_reserved_ms) = measure_elapsed_millis(|| {
        Ok::<_, anyhow::Error>(upload_space::reserved_space_threshold(
            storage_total,
            &state.config.storage,
        ))
    })?;
    let db_path = StdPath::new(&state.config.db.path);
    let db_display_path = std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.to_path_buf());
    let sqlite_db_size = file_size_label(db_path);
    let sqlite_wal_size = file_size_label(&sqlite_sidecar_path(db_path, "-wal"));
    let sqlite_shm_size = file_size_label(&sqlite_sidecar_path(db_path, "-shm"));
    let auth_rate_limit = state.auth_rate_limiter.stats();

    let notify_rows = if let Some(runtime) = &state.notify_push {
        let metrics = runtime.metrics();
        vec![
            status_row("Runtime", "Enabled"),
            status_row("Active connections", metrics.active_connections),
            status_row("Active users", metrics.active_users),
            status_row("Total connections", metrics.total_connections),
            status_row("Events received", metrics.events_received),
            status_row("Auth failures", metrics.auth_failures),
            status_row("Messages sent", metrics.messages_sent),
            status_row("File messages sent", metrics.messages_sent_file),
            status_row("Activity messages sent", metrics.messages_sent_activity),
            status_row(
                "Notification messages sent",
                metrics.messages_sent_notification,
            ),
            status_row("Custom messages sent", metrics.messages_sent_custom),
            status_row("Test endpoint hits", metrics.test_endpoint_hits),
        ]
    } else {
        vec![status_row("Runtime", "Disabled")]
    };

    Ok(html::StatusPageData {
        sections: vec![
            html::StatusSection {
                title: "Server".to_owned(),
                rows: vec![status_row("Instance ID", &state.instance_id)],
            },
            html::StatusSection {
                title: "Storage".to_owned(),
                rows: vec![
                    status_row("Data root", state.data_root.display()),
                    status_row("Files root", state.files_root.display()),
                    status_row("Uploads root", state.uploads_root.display()),
                    status_row(
                        "Available space",
                        format_timed_value(format_bytes(storage_available), storage_available_ms),
                    ),
                    status_row(
                        "Total space",
                        format_timed_value(format_bytes(storage_total), storage_total_ms),
                    ),
                    status_row(
                        "Upload minimum free space",
                        format_timed_value(format_bytes(upload_reserved), upload_reserved_ms),
                    ),
                    status_row("SQLite database path", db_display_path.display()),
                    status_row("SQLite database size", sqlite_db_size),
                    status_row("SQLite WAL size", sqlite_wal_size),
                    status_row("SQLite SHM size", sqlite_shm_size),
                ],
            },
            html::StatusSection {
                title: "Database".to_owned(),
                rows: vec![
                    status_row("Local users", users.len()),
                    status_row("Enabled users", enabled_users),
                    status_row("App passwords", app_passwords),
                    status_row("File records", file_records),
                    status_row("Change log entries", change_log_entries),
                    status_row("Upload sessions", upload_sessions),
                    status_row("Expired upload sessions", expired_upload_sessions),
                ],
            },
            html::StatusSection {
                title: "Auth Rate Limit".to_owned(),
                rows: vec![
                    status_row("Active throttled keys", auth_rate_limit.active_keys),
                    status_row("Total failed attempts", auth_rate_limit.total_failures),
                    status_row(
                        "Max response delay",
                        format!("{}s", auth_rate_limit.max_delay_secs),
                    ),
                ],
            },
            html::StatusSection {
                title: "Sync".to_owned(),
                rows: vec![
                    status_row("Owner", &state.owner),
                    status_row("Current sync token", sync_token),
                    status_row("Change log floor token", change_log_floor_token),
                ],
            },
            html::StatusSection {
                title: "Notify Push".to_owned(),
                rows: notify_rows,
            },
        ],
    })
}

fn load_clients_page_data(state: &AppState) -> html::ClientsPageData {
    let webdav_clients = state
        .webdav_clients
        .recent_clients()
        .into_iter()
        .map(webdav_client_row)
        .collect::<Vec<_>>();
    let notify_connections = state
        .notify_push
        .as_ref()
        .map(|runtime| {
            runtime
                .active_connections()
                .into_iter()
                .map(notify_connection_row)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    html::ClientsPageData {
        webdav_clients,
        notify_connections,
    }
}

async fn count_rows(pool: &sqlx::SqlitePool, query: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(query).fetch_one(pool).await?)
}

async fn count_rows_bound_i64(
    pool: &sqlx::SqlitePool,
    query: &str,
    value: i64,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(query)
        .bind(value)
        .fetch_one(pool)
        .await?)
}

fn status_row(label: impl ToString, value: impl ToString) -> html::StatusRow {
    html::StatusRow {
        label: label.to_string(),
        value: value.to_string(),
    }
}

fn notify_connection_row(
    connection: crate::notify_push::NotifyConnectionSnapshot,
) -> html::StatusRow {
    let info = &connection.client_info;
    let label = info
        .device_name
        .as_ref()
        .or(info.hostname.as_ref())
        .unwrap_or(&connection.peer_addr)
        .to_owned();
    let mut details = vec![connection.user, connection.peer_addr.clone()];

    if let Some(hostname) = &info.hostname {
        if hostname != &label {
            details.push(hostname.clone());
        }
    }

    let client = match (&info.client_name, &info.client_version) {
        (Some(name), Some(version)) => Some(format!("{name} {version}")),
        (Some(name), None) => Some(name.clone()),
        (None, Some(version)) => Some(format!("version {version}")),
        (None, None) => None,
    };
    if let Some(client) = client {
        details.push(client);
    }

    let platform = match (&info.platform, &info.os) {
        (Some(platform), Some(os)) if platform != os => Some(format!("{platform} / {os}")),
        (Some(platform), _) => Some(platform.clone()),
        (_, Some(os)) => Some(os.clone()),
        (None, None) => None,
    };
    if let Some(platform) = platform {
        details.push(platform);
    }

    details.push(if connection.listen_file_id && !info.is_empty() {
        "listen_file_id".to_owned()
    } else if connection.listen_file_id {
        "listen_file_id -> notify_file".to_owned()
    } else {
        "notify_file".to_owned()
    });
    details.push(format!(
        "connected {} ago",
        format_duration(connection.connected_secs)
    ));

    status_row(label, details.join(" · "))
}

fn webdav_client_row(client: crate::webdav_clients::WebDavClientSnapshot) -> html::StatusRow {
    let info = &client.client_info;
    let label = info
        .device_name
        .as_ref()
        .or(info.hostname.as_ref())
        .or(info.client_name.as_ref())
        .unwrap_or(&client.peer_addr)
        .to_owned();
    let mut details = vec![client.user, client.peer_addr.clone()];

    if let Some(hostname) = &info.hostname {
        if hostname != &label {
            details.push(hostname.clone());
        }
    }

    let client_name = match (&info.client_name, &info.client_version) {
        (Some(name), Some(version)) => Some(format!("{name} {version}")),
        (Some(name), None) => Some(name.clone()),
        (None, Some(version)) => Some(format!("version {version}")),
        (None, None) => None,
    };
    if let Some(client_name) = client_name {
        if client_name != label {
            details.push(client_name);
        }
    }

    let platform = match (&info.platform, &info.os) {
        (Some(platform), Some(os)) if platform != os => Some(format!("{platform} / {os}")),
        (Some(platform), _) => Some(platform.clone()),
        (_, Some(os)) => Some(os.clone()),
        (None, None) => None,
    };
    if let Some(platform) = platform {
        details.push(platform);
    }

    if let Some(protocol) = &client.protocol {
        details.push(protocol.clone());
    }

    details.push(format!("last {}", client.last_method));
    details.push(format!("{} request(s)", client.request_count));
    details.push(format!(
        "last seen {} ago",
        format_duration(client.last_seen_secs)
    ));
    details.push(format!(
        "first seen {} ago",
        format_duration(client.first_seen_secs)
    ));

    status_row(label, details.join(" · "))
}

fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}h", seconds / 3600)
    }
}

fn measure_elapsed_millis<T>(
    operation: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<(T, f64)> {
    let started = Instant::now();
    let value = operation()?;
    Ok((value, started.elapsed().as_secs_f64() * 1000.0))
}

fn format_timed_value(value: String, elapsed_ms: f64) -> String {
    format!("{value} ({elapsed_ms:.3} ms)")
}

fn sqlite_sidecar_path(db_path: &StdPath, suffix: &str) -> PathBuf {
    let mut raw = db_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

fn file_size_label(path: &StdPath) -> String {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => format_bytes(metadata.len()),
        Ok(_) => "not a file".to_owned(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => "not present".to_owned(),
        Err(err) => format!("unavailable: {err}"),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in UNITS.iter().skip(1) {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }
    if unit == "B" {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {unit}")
    }
}

async fn guard_last_admin(state: &Arc<AppState>, username: &str) -> Result<(), Response> {
    if !state
        .admin_config
        .users
        .iter()
        .any(|admin_user| admin_user == username)
    {
        return Ok(());
    }

    match db::enabled_admin_count(&state.db, &state.admin_config.users).await {
        Ok(count) if count <= 1 => Err((
            StatusCode::BAD_REQUEST,
            "Refusing to remove the last enabled admin user.",
        )
            .into_response()),
        Ok(_) => Ok(()),
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to check admin lockout: {err:#}"),
        )
            .into_response()),
    }
}

fn parse_expiry(mode: Option<&str>, raw: Option<&str>) -> anyhow::Result<Option<i64>> {
    if mode.unwrap_or("never") == "never" {
        return Ok(None);
    }
    let value = raw.unwrap_or_default().trim();
    if value.is_empty() {
        anyhow::bail!("expiry time is required unless never expires is selected");
    }
    if let Ok(timestamp) = value.parse::<i64>() {
        return Ok(Some(timestamp));
    }
    let parsed = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .map_err(|_| {
            anyhow::anyhow!(
                "expiry time must be a UNIX timestamp, YYYY-MM-DDTHH:MM, or YYYY-MM-DD HH:MM"
            )
        })?;
    Ok(Some(parsed.and_utc().timestamp()))
}

fn parse_scope_inputs(
    mount_path: Option<&str>,
    storage_path: Option<&str>,
    permission: Option<&str>,
) -> anyhow::Result<Vec<AppPasswordScopeInput>> {
    let mount_path = mount_path.unwrap_or("/").trim();
    let storage_path = storage_path.unwrap_or("/").trim();
    let permission = parse_permission(permission.unwrap_or("full"))?;
    Ok(vec![AppPasswordScopeInput {
        mount_path: mount_path.to_owned(),
        storage_path: storage_path.to_owned(),
        permission,
    }])
}

fn parse_permission(value: &str) -> anyhow::Result<PermissionLevel> {
    match value {
        "view" => Ok(PermissionLevel::View),
        "full" => Ok(PermissionLevel::Full),
        _ => anyhow::bail!("permission must be view or full"),
    }
}

fn validate_csrf(state: &AppState, csrf: &str) -> Result<(), Response> {
    if csrf == state.admin_csrf_token {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "Invalid CSRF token").into_response())
    }
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
