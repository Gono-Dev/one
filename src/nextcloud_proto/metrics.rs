use std::{path::Path, sync::Arc};

use anyhow::Context;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use tracing::error;

use crate::{auth::Principal, db, permissions, state::AppState};

pub async fn handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    match render_metrics(&state, &principal).await {
        Ok(body) => ([(header::CONTENT_TYPE, prometheus_content_type())], body).into_response(),
        Err(err) => {
            error!(?err, "collect metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics collection failed\n",
            )
                .into_response()
        }
    }
}

async fn render_metrics(state: &AppState, principal: &Principal) -> anyhow::Result<String> {
    let owner = principal.username.as_str();
    let file_records = count_visible_paths(
        &state.db,
        "SELECT rel_path FROM file_ids WHERE owner = ?1",
        owner,
        principal,
    )
    .await
    .context("count file records")?;
    let change_log_entries = count_visible_paths(
        &state.db,
        "SELECT rel_path FROM change_log WHERE owner = ?1",
        owner,
        principal,
    )
    .await
    .context("count change log entries")?;
    let upload_sessions = count_visible_paths(
        &state.db,
        "SELECT target_path FROM upload_sessions WHERE owner = ?1",
        owner,
        principal,
    )
    .await
    .context("count upload sessions")?;
    let expired_upload_sessions =
        count_visible_expired_upload_sessions(&state.db, owner, principal)
            .await
            .context("count expired upload sessions")?;
    let (sync_token, change_log_floor_token) =
        visible_change_log_tokens(&state.db, owner, principal)
            .await
            .context("load visible change log tokens")?;
    let files_root = state.files_root_for_owner(owner)?;
    let space_root = if files_root.exists() {
        files_root
    } else {
        state.data_root.clone()
    };
    let files_available = fs2::available_space(&space_root)
        .with_context(|| format!("read available space for {}", space_root.display()))?;
    let files_total = fs2::total_space(&space_root)
        .with_context(|| format!("read total space for {}", space_root.display()))?;

    let mut body = format!(
        concat!(
            "# HELP gono_cloud_file_records_total SQLite file metadata rows visible to the current app password.\n",
            "# TYPE gono_cloud_file_records_total gauge\n",
            "gono_cloud_file_records_total {}\n",
            "# HELP gono_cloud_change_log_entries_total SQLite change log rows visible to the current app password.\n",
            "# TYPE gono_cloud_change_log_entries_total gauge\n",
            "gono_cloud_change_log_entries_total {}\n",
            "# HELP gono_cloud_upload_sessions_total Active chunked upload sessions visible to the current app password.\n",
            "# TYPE gono_cloud_upload_sessions_total gauge\n",
            "gono_cloud_upload_sessions_total {}\n",
            "# HELP gono_cloud_upload_sessions_expired_total Expired chunked upload sessions visible to the current app password and pending cleanup.\n",
            "# TYPE gono_cloud_upload_sessions_expired_total gauge\n",
            "gono_cloud_upload_sessions_expired_total {}\n",
            "# HELP gono_cloud_sync_token Current WebDAV sync token visible to the current app password.\n",
            "# TYPE gono_cloud_sync_token gauge\n",
            "gono_cloud_sync_token {}\n",
            "# HELP gono_cloud_change_log_floor_token Oldest visible previous sync token that can be used without a full resync.\n",
            "# TYPE gono_cloud_change_log_floor_token gauge\n",
            "gono_cloud_change_log_floor_token {}\n",
            "# HELP gono_cloud_storage_files_available_bytes Available bytes in the files storage root.\n",
            "# TYPE gono_cloud_storage_files_available_bytes gauge\n",
            "gono_cloud_storage_files_available_bytes {}\n",
            "# HELP gono_cloud_storage_files_total_bytes Total bytes in the files storage root.\n",
            "# TYPE gono_cloud_storage_files_total_bytes gauge\n",
            "gono_cloud_storage_files_total_bytes {}\n",
        ),
        file_records,
        change_log_entries,
        upload_sessions,
        expired_upload_sessions,
        sync_token,
        change_log_floor_token,
        files_available,
        files_total,
    );

    if let Some(runtime) = &state.notify_push {
        let metrics = runtime.metrics();
        body.push_str(&format!(
            concat!(
                "# HELP gono_cloud_notify_push_active_connections Active notify_push WebSocket connections.\n",
                "# TYPE gono_cloud_notify_push_active_connections gauge\n",
                "gono_cloud_notify_push_active_connections {}\n",
                "# HELP gono_cloud_notify_push_active_users Users with at least one notify_push connection.\n",
                "# TYPE gono_cloud_notify_push_active_users gauge\n",
                "gono_cloud_notify_push_active_users {}\n",
                "# HELP gono_cloud_notify_push_total_connections Total accepted notify_push WebSocket connections.\n",
                "# TYPE gono_cloud_notify_push_total_connections counter\n",
                "gono_cloud_notify_push_total_connections {}\n",
                "# HELP gono_cloud_notify_push_events_total Notify push events received by the runtime.\n",
                "# TYPE gono_cloud_notify_push_events_total counter\n",
                "gono_cloud_notify_push_events_total {}\n",
                "# HELP gono_cloud_notify_push_auth_failures_total Notify push authentication failures.\n",
                "# TYPE gono_cloud_notify_push_auth_failures_total counter\n",
                "gono_cloud_notify_push_auth_failures_total {}\n",
                "# HELP gono_cloud_notify_push_messages_sent_total Notify push messages sent by type.\n",
                "# TYPE gono_cloud_notify_push_messages_sent_total counter\n",
                "gono_cloud_notify_push_messages_sent_total {}\n",
                "gono_cloud_notify_push_messages_sent_total{{type=\"file\"}} {}\n",
                "gono_cloud_notify_push_messages_sent_total{{type=\"activity\"}} {}\n",
                "gono_cloud_notify_push_messages_sent_total{{type=\"notification\"}} {}\n",
                "gono_cloud_notify_push_messages_sent_total{{type=\"custom\"}} {}\n",
                "# HELP gono_cloud_notify_push_test_endpoint_hits_total notify_push compatibility test endpoint hits.\n",
                "# TYPE gono_cloud_notify_push_test_endpoint_hits_total counter\n",
                "gono_cloud_notify_push_test_endpoint_hits_total {}\n",
            ),
            metrics.active_connections,
            metrics.active_users,
            metrics.total_connections,
            metrics.events_received,
            metrics.auth_failures,
            metrics.messages_sent,
            metrics.messages_sent_file,
            metrics.messages_sent_activity,
            metrics.messages_sent_notification,
            metrics.messages_sent_custom,
            metrics.test_endpoint_hits,
        ));
    }

    Ok(body)
}

async fn visible_change_log_tokens(
    pool: &sqlx::SqlitePool,
    owner: &str,
    principal: &Principal,
) -> anyhow::Result<(i64, i64)> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT rel_path, sync_token FROM change_log WHERE owner = ?1 ORDER BY sync_token ASC",
    )
    .bind(owner)
    .fetch_all(pool)
    .await?;

    let mut min_visible = None::<i64>;
    let mut max_visible = 0_i64;
    for (path, token) in rows {
        if permissions::resolve_scope_for_storage_path(principal, Path::new(&path))?.is_some() {
            min_visible.get_or_insert(token);
            max_visible = token;
        }
    }
    let floor_token = min_visible
        .map(|token| token.saturating_sub(1))
        .unwrap_or(max_visible);
    Ok((max_visible, floor_token))
}

async fn count_visible_paths(
    pool: &sqlx::SqlitePool,
    query: &str,
    owner: &str,
    principal: &Principal,
) -> anyhow::Result<i64> {
    let paths: Vec<String> = sqlx::query_scalar(query)
        .bind(owner)
        .fetch_all(pool)
        .await?;
    count_visible_path_strings(principal, paths)
}

async fn count_visible_expired_upload_sessions(
    pool: &sqlx::SqlitePool,
    owner: &str,
    principal: &Principal,
) -> anyhow::Result<i64> {
    let paths: Vec<String> = sqlx::query_scalar(
        "SELECT target_path FROM upload_sessions WHERE owner = ?1 AND expires_at < ?2",
    )
    .bind(owner)
    .bind(db::unix_timestamp())
    .fetch_all(pool)
    .await?;
    count_visible_path_strings(principal, paths)
}

fn count_visible_path_strings(
    principal: &Principal,
    paths: impl IntoIterator<Item = String>,
) -> anyhow::Result<i64> {
    let mut count = 0_i64;
    for path in paths {
        if permissions::resolve_scope_for_storage_path(principal, Path::new(&path))?.is_some() {
            count += 1;
        }
    }
    Ok(count)
}

fn prometheus_content_type() -> &'static str {
    "text/plain; version=0.0.4; charset=utf-8"
}
