use std::{path::Path, sync::Arc};

use anyhow::{bail, Context};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, Response, StatusCode},
    response::IntoResponse,
};
use quick_xml::{escape::escape, events::Event, Reader};
use tracing::error;

use crate::{db, state::AppState, storage};

const REPORT_BODY_LIMIT: usize = 1024 * 1024;

pub async fn handle(state: Arc<AppState>, request: Request<Body>) -> Response<Body> {
    match handle_inner(state, request).await {
        Ok(response) => response,
        Err(err) => {
            error!(?err, "failed to handle REPORT");
            (StatusCode::BAD_REQUEST, "Invalid REPORT request").into_response()
        }
    }
}

async fn handle_inner(
    state: Arc<AppState>,
    request: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body = to_bytes(request.into_body(), REPORT_BODY_LIMIT)
        .await
        .context("read REPORT body")?;
    let report = parse_report_body(&body)?;

    if !report.sync_collection {
        return Ok((
            StatusCode::NOT_IMPLEMENTED,
            "REPORT dispatch is reserved for a later phase",
        )
            .into_response());
    }

    let current_token = db::current_sync_token(&state.db, &state.owner).await?;
    let since_token = report.sync_token.unwrap_or(0);
    let entries =
        db::list_change_log_range(&state.db, &state.owner, since_token, current_token).await?;
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">"#,
    );

    for entry in entries {
        append_change_response(&mut xml, &state, &entry).await?;
    }

    xml.push_str("<d:sync-token>");
    xml.push_str(&current_token.to_string());
    xml.push_str("</d:sync-token></d:multistatus>");

    Ok(Response::builder()
        .status(StatusCode::MULTI_STATUS)
        .header(header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .body(Body::from(xml))
        .expect("valid sync-collection response"))
}

async fn append_change_response(
    xml: &mut String,
    state: &AppState,
    entry: &db::ChangeLogEntry,
) -> anyhow::Result<()> {
    xml.push_str("<d:response><d:href>");
    xml.push_str(&escape(&href_for_rel_path(&entry.rel_path)));
    xml.push_str("</d:href>");

    if entry.operation == "delete" {
        append_not_found_propstat(xml);
    } else {
        match current_record_for_entry(state, entry).await {
            Ok((record, is_collection)) => append_ok_propstat(xml, &record, is_collection),
            Err(err) => {
                error!(?err, rel_path = %entry.rel_path, "changed path is no longer readable");
                append_not_found_propstat(xml);
            }
        }
    }

    xml.push_str("</d:response>");
    Ok(())
}

async fn current_record_for_entry(
    state: &AppState,
    entry: &db::ChangeLogEntry,
) -> anyhow::Result<(db::FileRecord, bool)> {
    let rel_path = Path::new(&entry.rel_path);
    let abs_path = storage::safe_existing_path(&state.files_root, rel_path)?;
    let metadata = std::fs::metadata(&abs_path)
        .with_context(|| format!("read metadata for changed path {}", abs_path.display()))?;
    let record = db::ensure_file_record(
        &state.db,
        db::FileRecordInput {
            owner: &state.owner,
            rel_path,
            abs_path: &abs_path,
            instance_id: &state.instance_id,
            xattr_ns: &state.xattr_ns,
        },
    )
    .await?;
    Ok((record, metadata.is_dir()))
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
    xml.push_str("</oc:fileid><oc:permissions>RGDNVW</oc:permissions><oc:favorite>");
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
    sync_token: Option<i64>,
}

fn parse_report_body(body: &[u8]) -> anyhow::Result<ParsedReport> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut report = ParsedReport::default();
    let mut in_sync_token = false;

    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                let name = element.name();
                let local = name.local_name();
                match local.as_ref() {
                    b"sync-collection" => report.sync_collection = true,
                    b"sync-token" => in_sync_token = true,
                    _ => {}
                }
            }
            Event::Empty(element) => {
                if element.name().local_name().as_ref() == b"sync-collection" {
                    report.sync_collection = true;
                }
            }
            Event::Text(text) if in_sync_token => {
                let raw = text.unescape().context("decode sync-token")?;
                report.sync_token = Some(parse_sync_token(raw.as_ref())?);
            }
            Event::End(element) => {
                if element.name().local_name().as_ref() == b"sync-token" {
                    in_sync_token = false;
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(report)
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

fn href_for_rel_path(rel_path: &str) -> String {
    let encoded = percent_encode_path(rel_path);
    if encoded.is_empty() {
        "/remote.php/dav/".to_owned()
    } else {
        format!("/remote.php/dav/{encoded}")
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
