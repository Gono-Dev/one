use std::sync::Arc;

use axum::{
    extract::{Extension, Form, Path, State},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post},
    Router,
};
use serde::Deserialize;
use tracing::info;

use crate::{
    admin::{
        auth::require_admin,
        html::{self, Notice, NoticeKind, OneTimePassword},
    },
    auth::Principal,
    db::{self, AppPasswordScopeInput, BOOTSTRAP_USER},
    permissions::PermissionLevel,
    settings::{self, SettingsUpdate},
    state::AppState,
};

pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let protected = Router::new()
        .route("/users", get(users_page).post(create_user))
        .route("/settings", get(settings_page).post(save_settings))
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
        .route("/admin", get(|| async { Redirect::to("/admin/users") }))
        .route("/admin/", get(|| async { Redirect::to("/admin/users") }))
        .nest("/admin", protected)
}

pub fn disabled_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin", any(not_found))
        .route("/admin/", any(not_found))
        .nest("/admin", Router::new().fallback(not_found))
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

#[derive(Debug, Deserialize)]
struct SettingsForm {
    #[serde(rename = "_csrf")]
    csrf: String,
    server_base_url: String,
    auth_realm: String,
    sync_change_log_retention_days: String,
    sync_change_log_min_entries: String,
    notify_push_enabled: String,
    notify_push_path: String,
    notify_push_advertised_types: String,
    notify_push_pre_auth_ttl_secs: String,
    notify_push_user_connection_limit: String,
    notify_push_max_debounce_secs: String,
    notify_push_ping_interval_secs: String,
    notify_push_auth_timeout_secs: String,
    notify_push_max_connection_secs: String,
    admin_enabled: String,
    admin_users: String,
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
    render_settings(&state, &principal, None, false).await
}

async fn save_settings(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Form(form): Form<SettingsForm>,
) -> Response {
    if let Err(response) = validate_csrf(&state, &form.csrf) {
        return response;
    }

    let update = match settings_update_from_form(form) {
        Ok(update) => update,
        Err(err) => return render_settings_error(&state, &principal, err).await,
    };

    match settings::save_settings_update(&state.db, &update, &principal.username).await {
        Ok(()) => {
            info!(admin = %principal.username, "admin updated settings");
            let config = settings::apply_saved_settings(&state.db, state.config.clone())
                .await
                .unwrap_or_else(|_| state.config.clone());
            Html(html::render_settings_page(
                &principal.username,
                &state.admin_csrf_token,
                &config,
                Some(&Notice {
                    kind: NoticeKind::Success,
                    text: "Settings saved. Restart gono-cloud for changes to take effect."
                        .to_owned(),
                }),
                true,
            ))
            .into_response()
        }
        Err(err) => render_settings_error(&state, &principal, err).await,
    }
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

async fn render_settings(
    state: &Arc<AppState>,
    principal: &Principal,
    notice: Option<Notice>,
    pending_restart: bool,
) -> Response {
    Html(html::render_settings_page(
        &principal.username,
        &state.admin_csrf_token,
        &state.config,
        notice.as_ref(),
        pending_restart,
    ))
    .into_response()
}

async fn render_settings_error(
    state: &Arc<AppState>,
    principal: &Principal,
    err: anyhow::Error,
) -> Response {
    Html(html::render_settings_page(
        &principal.username,
        &state.admin_csrf_token,
        &state.config,
        Some(&Notice {
            kind: NoticeKind::Error,
            text: format!("{err:#}"),
        }),
        false,
    ))
    .into_response()
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

fn settings_update_from_form(form: SettingsForm) -> anyhow::Result<SettingsUpdate> {
    Ok(SettingsUpdate {
        server_base_url: form.server_base_url.trim().to_owned(),
        auth_realm: form.auth_realm.trim().to_owned(),
        sync_change_log_retention_days: parse_u64(
            "sync.change_log_retention_days",
            &form.sync_change_log_retention_days,
        )?,
        sync_change_log_min_entries: parse_usize(
            "sync.change_log_min_entries",
            &form.sync_change_log_min_entries,
        )?,
        notify_push_enabled: parse_bool("notify_push.enabled", &form.notify_push_enabled)?,
        notify_push_path: form.notify_push_path.trim().to_owned(),
        notify_push_advertised_types: parse_list(&form.notify_push_advertised_types),
        notify_push_pre_auth_ttl_secs: parse_u64(
            "notify_push.pre_auth_ttl_secs",
            &form.notify_push_pre_auth_ttl_secs,
        )?,
        notify_push_user_connection_limit: parse_usize(
            "notify_push.user_connection_limit",
            &form.notify_push_user_connection_limit,
        )?,
        notify_push_max_debounce_secs: parse_u64(
            "notify_push.max_debounce_secs",
            &form.notify_push_max_debounce_secs,
        )?,
        notify_push_ping_interval_secs: parse_u64(
            "notify_push.ping_interval_secs",
            &form.notify_push_ping_interval_secs,
        )?,
        notify_push_auth_timeout_secs: parse_u64(
            "notify_push.auth_timeout_secs",
            &form.notify_push_auth_timeout_secs,
        )?,
        notify_push_max_connection_secs: parse_u64(
            "notify_push.max_connection_secs",
            &form.notify_push_max_connection_secs,
        )?,
        admin_enabled: parse_bool("admin.enabled", &form.admin_enabled)?,
        admin_users: parse_list(&form.admin_users),
    })
}

fn parse_bool(key: &str, value: &str) -> anyhow::Result<bool> {
    match value.trim() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => anyhow::bail!("{key} must be true or false"),
    }
}

fn parse_u64(key: &str, value: &str) -> anyhow::Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("{key} must be a non-negative integer"))
}

fn parse_usize(key: &str, value: &str) -> anyhow::Result<usize> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("{key} must be a non-negative integer"))
}

fn parse_list(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
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
