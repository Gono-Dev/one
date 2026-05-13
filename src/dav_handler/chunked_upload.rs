use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Request, Response, StatusCode, header::HeaderName},
    response::IntoResponse,
};
use filetime::FileTime;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::{
    auth::Principal,
    dav_handler::{
        fs::permissions_string,
        pathmap::{parse_rel_path, parse_rel_path_for_owner},
        upload_space,
    },
    db,
    permissions::{self, PermissionLevel},
    state::AppState,
    storage::{self, safe_create_path, safe_existing_path, safe_write_path},
};

const OC_ETAG: HeaderName = HeaderName::from_static("oc-etag");
const OC_FILEID: HeaderName = HeaderName::from_static("oc-fileid");
const OC_TOTAL_LENGTH: HeaderName = HeaderName::from_static("oc-total-length");
const X_NC_OWNER_ID: HeaderName = HeaderName::from_static("x-nc-ownerid");
const X_NC_PERMISSIONS: HeaderName = HeaderName::from_static("x-nc-permissions");
const X_OC_MTIME: HeaderName = HeaderName::from_static("x-oc-mtime");
const CLEANUP_GRACE_SECS: i64 = 60 * 60;
const CLEANUP_INTERVAL_SECS: u64 = 60 * 60;

type ChunkResult<T> = Result<T, Response<Body>>;

pub async fn handle(
    state: Arc<AppState>,
    principal: Principal,
    files_root: PathBuf,
    request: Request<Body>,
) -> Response<Body> {
    if let Err(err) = cleanup_expired_sessions(&state).await {
        return internal_error(err, "cleanup expired upload sessions");
    }

    match handle_inner(state, principal, files_root, request).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

pub fn spawn_cleanup_task(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            match cleanup_expired_sessions(&state).await {
                Ok(count) if count > 0 => {
                    info!(count, "removed expired upload sessions");
                }
                Ok(_) => {}
                Err(err) => {
                    error!(?err, "failed to remove expired upload sessions");
                }
            }
        }
    })
}

async fn handle_inner(
    state: Arc<AppState>,
    principal: Principal,
    files_root: PathBuf,
    request: Request<Body>,
) -> ChunkResult<Response<Body>> {
    let method = request.method().clone();
    let upload_path = UploadPath::parse(request.uri().path())?;

    if upload_path.owner != principal.username {
        return Err(text_response(
            StatusCode::FORBIDDEN,
            "Upload owner does not match authenticated user",
        ));
    }

    match method.as_str() {
        "MKCOL" => mkcol_session(state, &principal, &files_root, request, upload_path).await,
        "PUT" => put_chunk(state, &principal, &files_root, request, upload_path).await,
        "DELETE" => delete_session(state, upload_path).await,
        "MOVE" => move_file(state, &principal, &files_root, request, upload_path).await,
        _ => Err(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Unsupported chunking method",
        )),
    }
}

async fn mkcol_session(
    state: Arc<AppState>,
    principal: &Principal,
    files_root: &Path,
    request: Request<Body>,
    upload_path: UploadPath,
) -> ChunkResult<Response<Body>> {
    let owner = &principal.username;
    if upload_path.chunk_name.is_some() {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "MKCOL must target an upload session",
        ));
    }

    let target_rel = destination_rel_path(request.headers(), principal)?;
    validate_target_path(files_root, &target_rel)?;
    let total_size = parse_total_length(request.headers())?.unwrap_or(0);
    upload_space::ensure_upload_space(
        files_root,
        nonzero_i64_to_u64(total_size),
        &state.config.storage,
    )?;

    let owner_dir = safe_upload_create_path(&state, Path::new(&upload_path.owner))?;
    tokio::fs::create_dir_all(&owner_dir)
        .await
        .map_err(|err| internal_error(err, "create upload owner directory"))?;
    let session_dir = session_create_path(&state, &upload_path)?;
    tokio::fs::create_dir_all(&session_dir)
        .await
        .map_err(|err| internal_error(err, "create upload session directory"))?;

    db::upsert_upload_session(
        &state.db,
        &upload_path.upload_id,
        owner,
        &target_rel,
        total_size,
    )
    .await
    .map_err(|err| internal_error(err, "persist upload session"))?;

    Ok(StatusCode::CREATED.into_response())
}

async fn put_chunk(
    state: Arc<AppState>,
    principal: &Principal,
    files_root: &Path,
    request: Request<Body>,
    upload_path: UploadPath,
) -> ChunkResult<Response<Body>> {
    let owner = &principal.username;
    let chunk_name = upload_path.chunk_name.as_deref().ok_or_else(|| {
        text_response(
            StatusCode::BAD_REQUEST,
            "PUT must target a numeric upload chunk",
        )
    })?;
    parse_chunk_name(chunk_name)?;

    let total_size = parse_total_length(request.headers())?;
    let chunk_size = upload_space::content_length(request.headers())?;
    upload_space::ensure_upload_space(files_root, chunk_size, &state.config.storage)?;
    let destination = destination_rel_path(request.headers(), principal)?;
    let session = require_session(&state, &upload_path).await?;
    ensure_destination_matches(&destination, &session.target_path)?;

    let session_dir = session_existing_path(&state, &upload_path)?;
    let chunk_path = safe_upload_create_path(
        &state,
        &Path::new(&upload_path.owner)
            .join(&upload_path.upload_id)
            .join(chunk_name),
    )?;
    if !chunk_path.starts_with(&session_dir) {
        return Err(text_response(
            StatusCode::FORBIDDEN,
            "Chunk path escapes upload session",
        ));
    }

    write_body_to_file(request.into_body(), &chunk_path).await?;
    db::touch_upload_session(&state.db, owner, &upload_path.upload_id, total_size)
        .await
        .map_err(|err| internal_error(err, "touch upload session"))?;

    Ok(StatusCode::CREATED.into_response())
}

async fn move_file(
    state: Arc<AppState>,
    principal: &Principal,
    files_root: &Path,
    request: Request<Body>,
    upload_path: UploadPath,
) -> ChunkResult<Response<Body>> {
    let owner = &principal.username;
    if upload_path.chunk_name.as_deref() != Some(".file") {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "MOVE must target the .file sentinel",
        ));
    }

    let destination = destination_rel_path(request.headers(), principal)?;
    let session = require_session(&state, &upload_path).await?;
    ensure_destination_matches(&destination, &session.target_path)?;
    validate_target_path(files_root, &destination)?;

    let target_abs = safe_write_path(files_root, &destination).map_err(|err| {
        warn!(?err, "invalid chunked upload destination");
        text_response(StatusCode::CONFLICT, "Invalid upload destination")
    })?;
    let target_existed = std::fs::symlink_metadata(&target_abs).is_ok();
    let temp_target = temp_target_path(&target_abs, &upload_path.upload_id)?;
    let session_dir = session_existing_path(&state, &upload_path)?;
    let required_size = match nonzero_i64_to_u64(session.total_size) {
        Some(total_size) => Some(total_size),
        None => Some(uploaded_chunk_bytes(&session_dir).await?),
    };
    upload_space::ensure_upload_space(files_root, required_size, &state.config.storage)?;

    merge_chunks(&session_dir, &temp_target).await?;
    if let Some(mtime) = parse_mtime(request.headers())? {
        filetime::set_file_mtime(&temp_target, mtime)
            .map_err(|err| internal_error(err, "set uploaded file mtime"))?;
    }
    tokio::fs::rename(&temp_target, &target_abs)
        .await
        .map_err(|err| internal_error(err, "move merged upload into place"))?;

    let record = db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner,
            rel_path: &destination,
            abs_path: &target_abs,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await
    .map_err(|err| internal_error(err, "persist uploaded file metadata"))?;

    let _sync_token = db::record_change(
        &state.db,
        owner,
        record.id,
        &destination,
        if target_existed { "modify" } else { "create" },
    )
    .await
    .map_err(|err| internal_error(err, "record uploaded file change"))?;
    state.notify_file_changed_for_owner(owner, Some(record.id));
    state.compact_change_log_for_owner_throttled(owner).await;

    remove_session_dir(&state, &upload_path).await?;
    db::delete_upload_session(&state.db, owner, &upload_path.upload_id)
        .await
        .map_err(|err| internal_error(err, "delete upload session"))?;

    let mut response = if target_existed {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::CREATED.into_response()
    };
    let headers = response.headers_mut();
    insert_header(headers, OC_ETAG, &record.etag);
    insert_header(headers, OC_FILEID, &record.oc_file_id);
    insert_header(headers, X_NC_OWNER_ID, owner);
    insert_header(
        headers,
        X_NC_PERMISSIONS,
        permissions_string(record.permissions, false),
    );
    if request.headers().contains_key(X_OC_MTIME) {
        insert_header(headers, X_OC_MTIME, "accepted");
    }
    Ok(response)
}

async fn delete_session(
    state: Arc<AppState>,
    upload_path: UploadPath,
) -> ChunkResult<Response<Body>> {
    if upload_path.chunk_name.is_some() {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "DELETE must target an upload session",
        ));
    }

    remove_session_dir(&state, &upload_path).await?;
    db::delete_upload_session(&state.db, &upload_path.owner, &upload_path.upload_id)
        .await
        .map_err(|err| internal_error(err, "delete upload session"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn require_session(
    state: &AppState,
    upload_path: &UploadPath,
) -> ChunkResult<db::UploadSession> {
    db::load_upload_session(&state.db, &upload_path.owner, &upload_path.upload_id)
        .await
        .map_err(|err| internal_error(err, "load upload session"))?
        .ok_or_else(|| text_response(StatusCode::NOT_FOUND, "Upload session does not exist"))
}

async fn write_body_to_file(body: Body, path: &Path) -> ChunkResult<u64> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| internal_error(err, "create chunk file"))?;
    let mut stream = body.into_data_stream();
    let mut written = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| internal_error(err, "read chunk body"))?;
        file.write_all(&chunk)
            .await
            .map_err(|err| internal_error(err, "write chunk body"))?;
        written += chunk.len() as u64;
    }

    file.flush()
        .await
        .map_err(|err| internal_error(err, "flush chunk body"))?;
    Ok(written)
}

async fn merge_chunks(session_dir: &Path, target: &Path) -> ChunkResult<()> {
    let mut chunks = Vec::new();
    let mut entries = tokio::fs::read_dir(session_dir)
        .await
        .map_err(|err| internal_error(err, "read upload session directory"))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|err| internal_error(err, "read upload chunk entry"))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|err| internal_error(err, "read upload chunk file type"))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let number = parse_chunk_name(&name)?;
        chunks.push((number, entry.path()));
    }

    if chunks.is_empty() {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "Upload session has no chunks",
        ));
    }
    chunks.sort_by_key(|(number, _)| *number);

    let mut output = tokio::fs::File::create(target)
        .await
        .map_err(|err| internal_error(err, "create merged upload"))?;
    for (_, path) in chunks {
        let mut input = tokio::fs::File::open(&path)
            .await
            .map_err(|err| internal_error(err, "open upload chunk"))?;
        tokio::io::copy(&mut input, &mut output)
            .await
            .map_err(|err| internal_error(err, "append upload chunk"))?;
    }
    output
        .flush()
        .await
        .map_err(|err| internal_error(err, "flush merged upload"))?;
    Ok(())
}

async fn uploaded_chunk_bytes(session_dir: &Path) -> ChunkResult<u64> {
    let mut total = 0_u64;
    let mut entries = tokio::fs::read_dir(session_dir)
        .await
        .map_err(|err| internal_error(err, "read upload session directory"))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|err| internal_error(err, "read upload chunk entry"))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|err| internal_error(err, "read upload chunk file type"))?;
        if !file_type.is_file() {
            continue;
        }
        let metadata = entry
            .metadata()
            .await
            .map_err(|err| internal_error(err, "read upload chunk metadata"))?;
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

pub async fn cleanup_expired_sessions(state: &AppState) -> anyhow::Result<usize> {
    let cutoff = db::unix_timestamp() - CLEANUP_GRACE_SECS;
    let sessions = db::list_expired_upload_sessions(&state.db, cutoff)
        .await
        .context("list expired upload sessions")?;
    let count = sessions.len();

    for session in sessions {
        let upload_path = UploadPath {
            owner: session.owner.clone(),
            upload_id: session.upload_id.clone(),
            chunk_name: None,
        };
        remove_session_dir_inner(state, &upload_path).await?;
        db::delete_upload_session(&state.db, &session.owner, &session.upload_id)
            .await
            .context("delete expired upload session")?;
    }

    Ok(count)
}

async fn remove_session_dir(state: &AppState, upload_path: &UploadPath) -> ChunkResult<()> {
    remove_session_dir_inner(state, upload_path)
        .await
        .map_err(|err| internal_error(err, "remove upload session directory"))
}

async fn remove_session_dir_inner(
    state: &AppState,
    upload_path: &UploadPath,
) -> anyhow::Result<()> {
    let rel_path = Path::new(&upload_path.owner).join(&upload_path.upload_id);
    match safe_existing_path(&state.uploads_root, &rel_path) {
        Ok(path) => {
            if let Err(err) = tokio::fs::remove_dir_all(&path).await {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err).context("remove upload session directory");
                }
            }
        }
        Err(_) => {
            storage::normalize_rel_path(&rel_path).context("normalize upload session path")?;
        }
    }
    Ok(())
}

fn destination_rel_path(headers: &HeaderMap, principal: &Principal) -> ChunkResult<PathBuf> {
    let destination = headers
        .get("destination")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| text_response(StatusCode::BAD_REQUEST, "Destination header is required"))?;
    let client_rel_path =
        parse_rel_path_for_owner(destination, &principal.username).map_err(|err| {
            warn!(?err, "invalid chunked upload destination header");
            text_response(StatusCode::BAD_REQUEST, "Invalid Destination header")
        })?;
    let scope_match = permissions::resolve_scope_for_client_path(principal, &client_rel_path)
        .map_err(|err| {
            warn!(?err, path = %client_rel_path.display(), "chunked upload destination is outside app password scopes");
            text_response(StatusCode::FORBIDDEN, "Destination is outside app password scope")
        })?;
    if scope_match.scope.permission != PermissionLevel::Full {
        return Err(text_response(
            StatusCode::FORBIDDEN,
            "App password scope is view-only",
        ));
    }
    Ok(scope_match.storage_rel_path)
}

fn validate_target_path(files_root: &Path, target_rel: &Path) -> ChunkResult<()> {
    safe_write_path(files_root, target_rel)
        .map(|_| ())
        .map_err(|err| {
            warn!(?err, "invalid chunked upload target path");
            text_response(StatusCode::CONFLICT, "Invalid upload destination")
        })
}

fn ensure_destination_matches(destination: &Path, session_target: &str) -> ChunkResult<()> {
    let destination = storage::rel_path_string(destination).map_err(|err| {
        warn!(?err, "invalid destination path");
        text_response(StatusCode::BAD_REQUEST, "Invalid Destination header")
    })?;
    if destination == session_target {
        Ok(())
    } else {
        Err(text_response(
            StatusCode::CONFLICT,
            "Destination does not match upload session",
        ))
    }
}

fn parse_total_length(headers: &HeaderMap) -> ChunkResult<Option<i64>> {
    let Some(value) = headers.get(OC_TOTAL_LENGTH) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "Invalid OC-Total-Length header"))?;
    let total = raw
        .parse::<i64>()
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "Invalid OC-Total-Length header"))?;
    if total < 0 {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "Invalid OC-Total-Length header",
        ));
    }
    Ok(Some(total))
}

fn parse_mtime(headers: &HeaderMap) -> ChunkResult<Option<FileTime>> {
    let Some(value) = headers.get(X_OC_MTIME) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header"))?;
    let secs = raw
        .parse::<i64>()
        .map_err(|_| text_response(StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header"))?;
    if secs < 0 {
        return Err(text_response(
            StatusCode::BAD_REQUEST,
            "Invalid X-OC-MTime header",
        ));
    }
    let _ = UNIX_EPOCH
        .checked_add(Duration::from_secs(secs as u64))
        .ok_or_else(|| text_response(StatusCode::BAD_REQUEST, "Invalid X-OC-MTime header"))?;
    Ok(Some(FileTime::from_unix_time(secs, 0)))
}

fn nonzero_i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok().filter(|value| *value > 0)
}

fn parse_chunk_name(name: &str) -> ChunkResult<u16> {
    let number = name.parse::<u16>().map_err(|_| {
        text_response(
            StatusCode::BAD_REQUEST,
            "Chunk name must be a number from 1 to 10000",
        )
    })?;
    if (1..=10_000).contains(&number) {
        Ok(number)
    } else {
        Err(text_response(
            StatusCode::BAD_REQUEST,
            "Chunk name must be a number from 1 to 10000",
        ))
    }
}

fn session_existing_path(state: &AppState, upload_path: &UploadPath) -> ChunkResult<PathBuf> {
    safe_existing_path(
        &state.uploads_root,
        &Path::new(&upload_path.owner).join(&upload_path.upload_id),
    )
    .map_err(|err| {
        warn!(?err, "upload session path is not readable");
        text_response(StatusCode::NOT_FOUND, "Upload session does not exist")
    })
}

fn session_create_path(state: &AppState, upload_path: &UploadPath) -> ChunkResult<PathBuf> {
    safe_upload_create_path(
        state,
        &Path::new(&upload_path.owner).join(&upload_path.upload_id),
    )
}

fn safe_upload_create_path(state: &AppState, rel_path: &Path) -> ChunkResult<PathBuf> {
    safe_create_path(&state.uploads_root, rel_path).map_err(|err| {
        warn!(?err, "upload path escapes upload root");
        text_response(StatusCode::FORBIDDEN, "Upload path escapes storage root")
    })
}

fn temp_target_path(target_abs: &Path, upload_id: &str) -> ChunkResult<PathBuf> {
    let parent = target_abs.parent().ok_or_else(|| {
        text_response(
            StatusCode::CONFLICT,
            "Upload destination must have a parent directory",
        )
    })?;
    let file_name = target_abs
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| text_response(StatusCode::CONFLICT, "Invalid upload destination"))?;
    Ok(parent.join(format!(".{file_name}.{upload_id}.upload")))
}

fn text_response(status: StatusCode, message: &'static str) -> Response<Body> {
    (status, message).into_response()
}

fn internal_error(err: impl std::fmt::Debug, context: &'static str) -> Response<Body> {
    error!(?err, context, "chunked upload failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Chunked upload operation failed",
    )
        .into_response()
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UploadPath {
    owner: String,
    upload_id: String,
    chunk_name: Option<String>,
}

impl UploadPath {
    fn parse(path: &str) -> ChunkResult<Self> {
        let rel_path = parse_rel_path(path).map_err(|err| {
            warn!(?err, path, "invalid upload path");
            text_response(StatusCode::BAD_REQUEST, "Invalid upload path")
        })?;
        let rel_path = storage::rel_path_string(&rel_path).map_err(|err| {
            warn!(?err, path, "invalid upload path");
            text_response(StatusCode::BAD_REQUEST, "Invalid upload path")
        })?;
        let parts = rel_path.split('/').collect::<Vec<_>>();
        if parts.len() < 3 || parts.len() > 4 || parts[0] != "uploads" {
            return Err(text_response(
                StatusCode::BAD_REQUEST,
                "Invalid upload path",
            ));
        }
        if parts[1].is_empty() || parts[2].is_empty() {
            return Err(text_response(
                StatusCode::BAD_REQUEST,
                "Invalid upload path",
            ));
        }
        Ok(Self {
            owner: parts[1].to_owned(),
            upload_id: parts[2].to_owned(),
            chunk_name: parts.get(3).map(|chunk| (*chunk).to_owned()),
        })
    }
}
