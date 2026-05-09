use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, Response, StatusCode},
    response::IntoResponse,
};
use quick_xml::{escape::escape, events::Event, Reader};
use tracing::{debug, error};

use crate::{
    dav_handler::dispatch::{
        mount_prefix_for_path, original_request_uri, parse_rel_path_for_owner,
    },
    dav_handler::fs::permissions_string,
    db,
    state::AppState,
    storage,
};

const REPORT_BODY_LIMIT: usize = 1024 * 1024;

pub async fn handle(
    state: Arc<AppState>,
    owner: String,
    files_root: PathBuf,
    request: Request<Body>,
) -> Response<Body> {
    match handle_inner(state, owner, files_root, request).await {
        Ok(response) => response,
        Err(err) => {
            error!(?err, "failed to handle REPORT");
            (StatusCode::BAD_REQUEST, "Invalid REPORT request").into_response()
        }
    }
}

async fn handle_inner(
    state: Arc<AppState>,
    owner: String,
    files_root: PathBuf,
    request: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let request_path = original_request_uri(&request).path().to_owned();
    let href_prefix = mount_prefix_for_path(&request_path, &owner);
    let body = to_bytes(request.into_body(), REPORT_BODY_LIMIT)
        .await
        .context("read REPORT body")?;
    let report = parse_report_body(&body)?;

    if report.filter_files {
        return handle_filter_files(
            state,
            &owner,
            &files_root,
            &request_path,
            &href_prefix,
            &report,
        )
        .await;
    }

    if report.sync_collection {
        return handle_sync_collection(state, &owner, &files_root, &href_prefix, &report).await;
    }

    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "REPORT dispatch is reserved for a later phase",
    )
        .into_response())
}

async fn handle_sync_collection(
    state: Arc<AppState>,
    owner: &str,
    files_root: &Path,
    href_prefix: &str,
    report: &ParsedReport,
) -> anyhow::Result<Response<Body>> {
    let current_token = db::current_sync_token(&state.db, owner).await?;
    let since_token = report.sync_token.unwrap_or(0);
    let floor_token = db::change_log_floor_token(&state.db, owner, current_token).await?;
    if since_token < floor_token {
        return Ok(stale_sync_token_response(
            since_token,
            floor_token,
            current_token,
        ));
    }

    let entries = db::list_change_log_range(&state.db, owner, since_token, current_token).await?;
    let mut xml = multistatus_start();

    for entry in entries {
        append_change_response(&mut xml, &state, owner, files_root, href_prefix, &entry).await?;
    }

    xml.push_str("<d:sync-token>");
    xml.push_str(&current_token.to_string());
    xml.push_str("</d:sync-token></d:multistatus>");

    Ok(xml_response(xml))
}

fn stale_sync_token_response(
    since_token: i64,
    floor_token: i64,
    current_token: i64,
) -> Response<Body> {
    let xml = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<d:error xmlns:d=\"DAV:\" xmlns:g=\"https://gono.cloud/ns\">",
            "<d:valid-sync-token/>",
            "<g:sync-token>{}</g:sync-token>",
            "<g:sync-token-floor>{}</g:sync-token-floor>",
            "<g:current-sync-token>{}</g:current-sync-token>",
            "</d:error>"
        ),
        since_token, floor_token, current_token
    );
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
        xml,
    )
        .into_response()
}

async fn handle_filter_files(
    state: Arc<AppState>,
    owner: &str,
    files_root: &Path,
    request_path: &str,
    href_prefix: &str,
    report: &ParsedReport,
) -> anyhow::Result<Response<Body>> {
    let Some(favorite) = report.favorite_filter else {
        return Ok((
            StatusCode::BAD_REQUEST,
            "oc:filter-files requires an oc:favorite filter",
        )
            .into_response());
    };

    let scope_rel = parse_rel_path_for_owner(request_path, owner)?;
    let scope_abs = storage::safe_existing_path(files_root, &scope_rel)?;
    let mut xml = multistatus_start();
    append_filter_matches(
        &mut xml,
        &state,
        owner,
        files_root,
        href_prefix,
        scope_rel,
        scope_abs,
        favorite,
    )
    .await?;
    xml.push_str("</d:multistatus>");

    Ok(xml_response(xml))
}

async fn append_change_response(
    xml: &mut String,
    state: &AppState,
    owner: &str,
    files_root: &Path,
    href_prefix: &str,
    entry: &db::ChangeLogEntry,
) -> anyhow::Result<()> {
    xml.push_str("<d:response><d:href>");
    xml.push_str(&escape(&href_for_rel_path(href_prefix, &entry.rel_path)));
    xml.push_str("</d:href>");

    if entry.operation == "delete" {
        append_not_found_propstat(xml);
    } else {
        match current_record_for_rel_path(state, owner, files_root, Path::new(&entry.rel_path))
            .await
        {
            Ok((record, is_collection)) => append_ok_propstat(xml, &record, is_collection),
            Err(err) if is_not_found_error(&err) => {
                debug!(
                    rel_path = %entry.rel_path,
                    operation = %entry.operation,
                    "change log entry is no longer readable; reporting sync tombstone"
                );
                append_not_found_propstat(xml);
            }
            Err(err) => {
                error!(?err, rel_path = %entry.rel_path, "changed path is no longer readable");
                append_not_found_propstat(xml);
            }
        }
    }

    xml.push_str("</d:response>");
    Ok(())
}

async fn append_filter_matches(
    xml: &mut String,
    state: &AppState,
    owner: &str,
    files_root: &Path,
    href_prefix: &str,
    scope_rel: std::path::PathBuf,
    scope_abs: std::path::PathBuf,
    favorite: bool,
) -> anyhow::Result<()> {
    let mut stack = vec![(scope_rel, scope_abs)];

    while let Some((rel_path, abs_path)) = stack.pop() {
        let metadata = std::fs::metadata(&abs_path)
            .with_context(|| format!("read metadata for filter path {}", abs_path.display()))?;
        let record = db::ensure_file_record(
            &state.db,
            db::FileRecordInput {
                owner,
                rel_path: &rel_path,
                abs_path: &abs_path,
                instance_id: &state.instance_id,
                xattr_ns: &state.xattr_ns,
            },
        )
        .await?;

        if record.favorite == favorite {
            let href_rel_path = storage::rel_path_string(&rel_path)?;
            append_resource_response(xml, href_prefix, &href_rel_path, &record, metadata.is_dir());
        }

        if metadata.is_dir() {
            let mut children = std::fs::read_dir(&abs_path)
                .with_context(|| format!("read directory {}", abs_path.display()))?
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("collect directory {}", abs_path.display()))?;
            children.sort_by_key(|entry| entry.file_name());

            for child in children.into_iter().rev() {
                let child_rel = rel_path.join(child.file_name());
                let child_abs = storage::safe_existing_path(files_root, &child_rel)?;
                stack.push((child_rel, child_abs));
            }
        }
    }

    Ok(())
}

async fn current_record_for_rel_path(
    state: &AppState,
    owner: &str,
    files_root: &Path,
    rel_path: &Path,
) -> anyhow::Result<(db::FileRecord, bool)> {
    let abs_path = storage::safe_existing_path(files_root, rel_path)?;
    let metadata = std::fs::metadata(&abs_path)
        .with_context(|| format!("read metadata for changed path {}", abs_path.display()))?;
    let record = db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner,
            rel_path,
            abs_path: &abs_path,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await?;
    Ok((record, metadata.is_dir()))
}

fn is_not_found_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::NotFound)
    })
}

fn append_resource_response(
    xml: &mut String,
    href_prefix: &str,
    rel_path: &str,
    record: &db::FileRecord,
    is_collection: bool,
) {
    xml.push_str("<d:response><d:href>");
    xml.push_str(&escape(&href_for_rel_path(href_prefix, rel_path)));
    xml.push_str("</d:href>");
    append_ok_propstat(xml, record, is_collection);
    xml.push_str("</d:response>");
}

fn append_ok_propstat(xml: &mut String, record: &db::FileRecord, is_collection: bool) {
    xml.push_str("<d:propstat><d:prop>");
    if is_collection {
        xml.push_str("<d:resourcetype><d:collection/></d:resourcetype>");
    } else {
        xml.push_str("<d:resourcetype/>");
        xml.push_str("<d:getcontentlength>");
        xml.push_str(&record.file_size.to_string());
        xml.push_str("</d:getcontentlength>");
    }
    xml.push_str("<d:getetag>");
    xml.push_str(&escape(&record.etag));
    xml.push_str("</d:getetag><oc:fileid>");
    xml.push_str(&escape(&record.oc_file_id));
    xml.push_str("</oc:fileid><oc:permissions>");
    xml.push_str(permissions_string(record.permissions, is_collection));
    xml.push_str("</oc:permissions><oc:favorite>");
    xml.push_str(if record.favorite { "1" } else { "0" });
    xml.push_str("</oc:favorite><nc:has-preview>false</nc:has-preview>");
    xml.push_str("</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>");
}

fn append_not_found_propstat(xml: &mut String) {
    xml.push_str("<d:propstat><d:prop/><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>");
}

#[derive(Debug, Default)]
struct ParsedReport {
    sync_collection: bool,
    filter_files: bool,
    sync_token: Option<i64>,
    favorite_filter: Option<bool>,
}

fn parse_report_body(body: &[u8]) -> anyhow::Result<ParsedReport> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut report = ParsedReport::default();
    let mut in_sync_token = false;
    let mut in_favorite_filter = false;

    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                let name = element.name();
                let local = name.local_name();
                match local.as_ref() {
                    b"sync-collection" => report.sync_collection = true,
                    b"filter-files" => report.filter_files = true,
                    b"sync-token" => in_sync_token = true,
                    b"favorite" if report.filter_files => in_favorite_filter = true,
                    _ => {}
                }
            }
            Event::Empty(element) => match element.name().local_name().as_ref() {
                b"sync-collection" => report.sync_collection = true,
                b"filter-files" => report.filter_files = true,
                _ => {}
            },
            Event::Text(text) if in_sync_token => {
                let raw = text.unescape().context("decode sync-token")?;
                report.sync_token = Some(parse_sync_token(raw.as_ref())?);
            }
            Event::Text(text) if in_favorite_filter => {
                let raw = text.unescape().context("decode favorite filter")?;
                report.favorite_filter = Some(parse_bool_filter(raw.as_ref())?);
            }
            Event::End(element) => match element.name().local_name().as_ref() {
                b"sync-token" => in_sync_token = false,
                b"favorite" => in_favorite_filter = false,
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(report)
}

fn multistatus_start() -> String {
    String::from(
        r#"<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">"#,
    )
}

fn xml_response(xml: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::MULTI_STATUS)
        .header(header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .body(Body::from(xml))
        .expect("valid REPORT response")
}

fn parse_sync_token(raw: &str) -> anyhow::Result<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let token = trimmed
        .rsplit(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())
        .context("sync-token does not contain a numeric token")?;
    let parsed = token.parse::<i64>().context("parse sync-token")?;
    if parsed < 0 {
        bail!("sync-token must not be negative");
    }
    Ok(parsed)
}

fn parse_bool_filter(raw: &str) -> anyhow::Result<bool> {
    match raw.trim() {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        value => bail!("unsupported boolean filter value {value:?}"),
    }
}

fn href_for_rel_path(href_prefix: &str, rel_path: &str) -> String {
    let encoded = percent_encode_path(rel_path);
    if href_prefix.is_empty() {
        if encoded.is_empty() {
            "/".to_owned()
        } else {
            format!("/{encoded}")
        }
    } else if encoded.is_empty() {
        format!("{href_prefix}/")
    } else {
        format!("{href_prefix}/{encoded}")
    }
}

fn percent_encode_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(*byte as char)
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}
