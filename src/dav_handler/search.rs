use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, Response, StatusCode, Uri},
    response::IntoResponse,
};
use dav_server::davpath::DavPath;
use quick_xml::{escape::escape, events::Event, Reader};
use tracing::{error, warn};

use crate::{
    dav_handler::dispatch::{mount_prefix_for_path, original_request_uri},
    db,
    state::AppState,
    storage,
};

const SEARCH_BODY_LIMIT: usize = 1024 * 1024;

pub async fn handle(state: Arc<AppState>, request: Request<Body>) -> Response<Body> {
    match handle_inner(state, request).await {
        Ok(response) => response,
        Err(err) => {
            error!(?err, "failed to handle SEARCH");
            (StatusCode::BAD_REQUEST, "Invalid SEARCH request").into_response()
        }
    }
}

async fn handle_inner(
    state: Arc<AppState>,
    request: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let request_path = original_request_uri(&request).path().to_owned();
    let href_prefix = mount_prefix_for_path(&request_path);
    let body = to_bytes(request.into_body(), SEARCH_BODY_LIMIT)
        .await
        .context("read SEARCH body")?;
    let search = parse_search_body(&body)?;
    if !search.basicsearch {
        bail!("SEARCH request must contain d:basicsearch");
    }
    if let Some(operator) = search.unsupported_operator {
        bail!("unsupported SEARCH where operator {operator}");
    }

    let scope_rel = scope_rel_path(search.scope_href.as_deref(), &state.owner)?;
    let scope_abs = storage::safe_existing_path(&state.files_root, &scope_rel)?;
    let mut xml = multistatus_start();
    append_search_matches(
        &mut xml,
        &state,
        href_prefix,
        scope_rel,
        scope_abs,
        search.depth,
        &search.filters,
    )
    .await?;
    xml.push_str("</d:multistatus>");

    Ok(xml_response(xml))
}

async fn append_search_matches(
    xml: &mut String,
    state: &AppState,
    href_prefix: &str,
    scope_rel: PathBuf,
    scope_abs: PathBuf,
    depth: SearchDepth,
    filters: &[SearchFilter],
) -> anyhow::Result<()> {
    let mut stack = vec![(scope_rel, scope_abs, 0_usize)];

    while let Some((rel_path, abs_path, level)) = stack.pop() {
        let metadata = std::fs::metadata(&abs_path)
            .with_context(|| format!("read metadata for search path {}", abs_path.display()))?;
        let record = db::ensure_file_record(
            &state.db,
            db::FileRecordInput {
                owner: &state.owner,
                rel_path: &rel_path,
                abs_path: &abs_path,
                instance_id: &state.instance_id,
                xattr_ns: &state.xattr_ns,
            },
        )
        .await?;

        if matches_filters(&record, filters) {
            let rel_path = storage::rel_path_string(&rel_path)?;
            append_resource_response(xml, href_prefix, &rel_path, &record, metadata.is_dir());
        }

        if metadata.is_dir() && depth.allows_children_at(level) {
            let mut children = std::fs::read_dir(&abs_path)
                .with_context(|| format!("read directory {}", abs_path.display()))?
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("collect directory {}", abs_path.display()))?;
            children.sort_by_key(|entry| entry.file_name());

            for child in children.into_iter().rev() {
                let child_rel = rel_path.join(child.file_name());
                let child_abs = storage::safe_existing_path(&state.files_root, &child_rel)?;
                stack.push((child_rel, child_abs, level + 1));
            }
        }
    }

    Ok(())
}

fn matches_filters(record: &db::FileRecord, filters: &[SearchFilter]) -> bool {
    filters.iter().all(|filter| match filter {
        SearchFilter::FileId(value) => {
            value == &record.id.to_string() || value == &record.oc_file_id
        }
        SearchFilter::Favorite(value) => record.favorite == *value,
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
    xml.push_str("</d:href><d:propstat><d:prop>");
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
    xml.push_str("</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");
}

#[derive(Debug, Default)]
struct SearchRequest {
    basicsearch: bool,
    scope_href: Option<String>,
    depth: SearchDepth,
    filters: Vec<SearchFilter>,
    unsupported_operator: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SearchFilter {
    FileId(String),
    Favorite(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchDepth {
    Zero,
    One,
    Infinity,
}

impl SearchDepth {
    fn allows_children_at(self, level: usize) -> bool {
        match self {
            SearchDepth::Zero => false,
            SearchDepth::One => level == 0,
            SearchDepth::Infinity => true,
        }
    }
}

impl Default for SearchDepth {
    fn default() -> Self {
        Self::Infinity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextTarget {
    ScopeHref,
    ScopeDepth,
    EqLiteral,
}

#[derive(Debug, Default)]
struct EqBuilder {
    prop: Option<SearchProp>,
    literal: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchProp {
    FileId,
    Favorite,
}

fn parse_search_body(body: &[u8]) -> anyhow::Result<SearchRequest> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut search = SearchRequest::default();
    let mut text_target = None;
    let mut in_scope = false;
    let mut in_prop = false;
    let mut in_where = false;
    let mut eq_builder = None::<EqBuilder>;

    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                let name = element.name();
                let local = name.local_name();
                match local.as_ref() {
                    b"basicsearch" => search.basicsearch = true,
                    b"scope" => in_scope = true,
                    b"where" => in_where = true,
                    b"and" if in_where => {}
                    b"href" if in_scope => text_target = Some(TextTarget::ScopeHref),
                    b"depth" if in_scope => text_target = Some(TextTarget::ScopeDepth),
                    b"eq" => eq_builder = Some(EqBuilder::default()),
                    b"prop" => in_prop = true,
                    b"literal" if eq_builder.is_some() => text_target = Some(TextTarget::EqLiteral),
                    prop if in_prop && eq_builder.is_some() => {
                        if let Some(builder) = &mut eq_builder {
                            builder.prop = search_prop_from_local_name(prop);
                        }
                    }
                    b"or" | b"like" | b"lt" | b"lte" | b"gt" | b"gte" | b"not" if in_where => {
                        search.unsupported_operator =
                            Some(String::from_utf8_lossy(local.as_ref()).into_owned());
                    }
                    _ => {}
                }
            }
            Event::Empty(element) => {
                let name = element.name();
                let local = name.local_name();
                if in_prop {
                    if let Some(builder) = &mut eq_builder {
                        builder.prop = search_prop_from_local_name(local.as_ref());
                    }
                }
            }
            Event::Text(text) => {
                if let Some(target) = text_target {
                    let raw = text.unescape().context("decode SEARCH text")?;
                    match target {
                        TextTarget::ScopeHref => search.scope_href = Some(raw.into_owned()),
                        TextTarget::ScopeDepth => search.depth = parse_depth(raw.as_ref())?,
                        TextTarget::EqLiteral => {
                            if let Some(builder) = &mut eq_builder {
                                builder.literal = Some(raw.into_owned());
                            }
                        }
                    }
                }
            }
            Event::End(element) => {
                let name = element.name();
                let local = name.local_name();
                match local.as_ref() {
                    b"scope" => in_scope = false,
                    b"where" => in_where = false,
                    b"href" | b"depth" | b"literal" => text_target = None,
                    b"prop" => in_prop = false,
                    b"eq" => {
                        let Some(builder) = eq_builder.take() else {
                            continue;
                        };
                        search.filters.push(builder.into_filter()?);
                    }
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(search)
}

impl EqBuilder {
    fn into_filter(self) -> anyhow::Result<SearchFilter> {
        let prop = self
            .prop
            .context("SEARCH eq filter is missing a property")?;
        let literal = self
            .literal
            .context("SEARCH eq filter is missing a literal")?;
        match prop {
            SearchProp::FileId => Ok(SearchFilter::FileId(literal.trim().to_owned())),
            SearchProp::Favorite => Ok(SearchFilter::Favorite(parse_bool(literal.trim())?)),
        }
    }
}

fn search_prop_from_local_name(local: &[u8]) -> Option<SearchProp> {
    match local {
        b"fileid" => Some(SearchProp::FileId),
        b"favorite" => Some(SearchProp::Favorite),
        other => {
            warn!(property = ?String::from_utf8_lossy(other), "unsupported SEARCH property");
            None
        }
    }
}

fn parse_depth(raw: &str) -> anyhow::Result<SearchDepth> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "0" => Ok(SearchDepth::Zero),
        "1" => Ok(SearchDepth::One),
        "infinity" => Ok(SearchDepth::Infinity),
        value => bail!("unsupported SEARCH depth {value:?}"),
    }
}

fn parse_bool(raw: &str) -> anyhow::Result<bool> {
    match raw {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        value => bail!("unsupported boolean literal {value:?}"),
    }
}

fn scope_rel_path(href: Option<&str>, owner: &str) -> anyhow::Result<PathBuf> {
    let href = href.unwrap_or("/");
    let path = if href.starts_with("http://") || href.starts_with("https://") {
        href.parse::<Uri>()?.path().to_owned()
    } else {
        href.to_owned()
    };
    let path = path
        .strip_prefix("/remote.php/dav")
        .or_else(|| path.strip_prefix("/remote.php/webdav"))
        .unwrap_or(&path);
    let owner_prefix = format!("/files/{owner}");
    let rel_path = if path == owner_prefix {
        "/"
    } else if let Some(rest) = path.strip_prefix(&(owner_prefix + "/")) {
        rest
    } else if path == "/" {
        "/"
    } else {
        bail!("SEARCH scope must be rooted at /files/{owner}");
    };
    let rel_path = if rel_path.starts_with('/') {
        rel_path.to_owned()
    } else {
        format!("/{rel_path}")
    };
    let dav_path = DavPath::new(&rel_path)?;
    Ok(Path::new(dav_path.as_rel_ospath()).to_path_buf())
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
        .expect("valid SEARCH response")
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
