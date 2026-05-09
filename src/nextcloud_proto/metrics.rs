use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use sqlx::SqlitePool;
use tracing::error;

use crate::{db, state::AppState};

pub async fn handler(State(state): State<Arc<AppState>>) -> Response {
    match render_metrics(&state).await {
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

async fn render_metrics(state: &AppState) -> anyhow::Result<String> {
    let file_records = count_owner_rows(
        &state.db,
        "SELECT COUNT(*) FROM file_ids WHERE owner = ?1",
        &state.owner,
    )
    .await
    .context("count file records")?;
    let change_log_entries = count_owner_rows(
        &state.db,
        "SELECT COUNT(*) FROM change_log WHERE owner = ?1",
        &state.owner,
    )
    .await
    .context("count change log entries")?;
    let upload_sessions = count_owner_rows(
        &state.db,
        "SELECT COUNT(*) FROM upload_sessions WHERE owner = ?1",
        &state.owner,
    )
    .await
    .context("count upload sessions")?;
    let expired_upload_sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upload_sessions WHERE expires_at < ?1")
            .bind(db::unix_timestamp())
            .fetch_one(&state.db)
            .await
            .context("count expired upload sessions")?;
    let sync_token = db::current_sync_token(&state.db, &state.owner)
        .await
        .context("load sync token")?;
    let change_log_floor_token = db::change_log_floor_token(&state.db, &state.owner, sync_token)
        .await
        .context("load change log floor token")?;
    let files_available = fs2::available_space(&state.files_root)
        .with_context(|| format!("read available space for {}", state.files_root.display()))?;
    let files_total = fs2::total_space(&state.files_root)
        .with_context(|| format!("read total space for {}", state.files_root.display()))?;

    let mut body = format!(
        concat!(
            "# HELP gono_one_file_records_total SQLite file metadata rows for the owner.\n",
            "# TYPE gono_one_file_records_total gauge\n",
            "gono_one_file_records_total {}\n",
            "# HELP gono_one_change_log_entries_total SQLite change log rows for the owner.\n",
            "# TYPE gono_one_change_log_entries_total gauge\n",
            "gono_one_change_log_entries_total {}\n",
            "# HELP gono_one_upload_sessions_total Active chunked upload sessions for the owner.\n",
            "# TYPE gono_one_upload_sessions_total gauge\n",
            "gono_one_upload_sessions_total {}\n",
            "# HELP gono_one_upload_sessions_expired_total Expired chunked upload sessions pending cleanup.\n",
            "# TYPE gono_one_upload_sessions_expired_total gauge\n",
            "gono_one_upload_sessions_expired_total {}\n",
            "# HELP gono_one_sync_token Current WebDAV sync token for the owner.\n",
            "# TYPE gono_one_sync_token gauge\n",
            "gono_one_sync_token {}\n",
            "# HELP gono_one_change_log_floor_token Oldest previous sync token that can be used without a full resync.\n",
            "# TYPE gono_one_change_log_floor_token gauge\n",
            "gono_one_change_log_floor_token {}\n",
            "# HELP gono_one_storage_files_available_bytes Available bytes in the files storage root.\n",
            "# TYPE gono_one_storage_files_available_bytes gauge\n",
            "gono_one_storage_files_available_bytes {}\n",
            "# HELP gono_one_storage_files_total_bytes Total bytes in the files storage root.\n",
            "# TYPE gono_one_storage_files_total_bytes gauge\n",
            "gono_one_storage_files_total_bytes {}\n",
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
                "# HELP gono_one_notify_push_active_connections Active notify_push WebSocket connections.\n",
                "# TYPE gono_one_notify_push_active_connections gauge\n",
                "gono_one_notify_push_active_connections {}\n",
                "# HELP gono_one_notify_push_active_users Users with at least one notify_push connection.\n",
                "# TYPE gono_one_notify_push_active_users gauge\n",
                "gono_one_notify_push_active_users {}\n",
                "# HELP gono_one_notify_push_total_connections Total accepted notify_push WebSocket connections.\n",
                "# TYPE gono_one_notify_push_total_connections counter\n",
                "gono_one_notify_push_total_connections {}\n",
                "# HELP gono_one_notify_push_events_total Notify push events received by the runtime.\n",
                "# TYPE gono_one_notify_push_events_total counter\n",
                "gono_one_notify_push_events_total {}\n",
                "# HELP gono_one_notify_push_auth_failures_total Notify push authentication failures.\n",
                "# TYPE gono_one_notify_push_auth_failures_total counter\n",
                "gono_one_notify_push_auth_failures_total {}\n",
                "# HELP gono_one_notify_push_messages_sent_total Notify push messages sent by type.\n",
                "# TYPE gono_one_notify_push_messages_sent_total counter\n",
                "gono_one_notify_push_messages_sent_total {}\n",
                "gono_one_notify_push_messages_sent_total{{type=\"file\"}} {}\n",
                "gono_one_notify_push_messages_sent_total{{type=\"activity\"}} {}\n",
                "gono_one_notify_push_messages_sent_total{{type=\"notification\"}} {}\n",
                "gono_one_notify_push_messages_sent_total{{type=\"custom\"}} {}\n",
                "# HELP gono_one_notify_push_test_endpoint_hits_total notify_push compatibility test endpoint hits.\n",
                "# TYPE gono_one_notify_push_test_endpoint_hits_total counter\n",
                "gono_one_notify_push_test_endpoint_hits_total {}\n",
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

async fn count_owner_rows(pool: &SqlitePool, query: &str, owner: &str) -> anyhow::Result<i64> {
    let count = sqlx::query_scalar(query)
        .bind(owner)
        .fetch_one(pool)
        .await?;
    Ok(count)
}

fn prometheus_content_type() -> &'static str {
    "text/plain; version=0.0.4; charset=utf-8"
}
