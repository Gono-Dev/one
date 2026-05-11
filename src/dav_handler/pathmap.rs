use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use axum::{
    body::Body,
    extract::OriginalUri,
    http::{uri::PathAndQuery, Request, Uri},
};
use dav_server::davpath::DavPath;

use crate::storage;

pub(crate) fn request_target_has_fragment(uri: &Uri) -> bool {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().contains('#'))
        .unwrap_or_else(|| uri.to_string().contains('#'))
}

pub(crate) fn path_for_request_style(
    request_path: &str,
    owner: &str,
    rel_path: &Path,
) -> anyhow::Result<String> {
    path_for_rel_path(&mount_prefix_for_path(request_path, owner), rel_path)
}

pub(crate) fn path_for_rel_path(prefix: &str, rel_path: &Path) -> anyhow::Result<String> {
    let rel_path = storage::rel_path_string(rel_path)?;
    let encoded = percent_encode_path(&rel_path);
    if prefix.is_empty() {
        if encoded.is_empty() {
            Ok("/".to_owned())
        } else {
            Ok(format!("/{encoded}"))
        }
    } else if encoded.is_empty() {
        Ok(format!("{prefix}/"))
    } else {
        Ok(format!("{prefix}/{encoded}"))
    }
}

pub(crate) fn uri_with_replaced_path(uri: &Uri, path: &str) -> anyhow::Result<Uri> {
    let path_and_query = if let Some(query) = uri.query() {
        format!("{path}?{query}")
    } else {
        path.to_owned()
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(PathAndQuery::from_str(&path_and_query)?);
    Ok(Uri::from_parts(parts)?)
}

pub(crate) fn path_part(path_or_uri: &str) -> anyhow::Result<String> {
    if path_or_uri.starts_with("http://") || path_or_uri.starts_with("https://") {
        Ok(path_or_uri.parse::<Uri>()?.path().to_owned())
    } else {
        Ok(path_or_uri.to_owned())
    }
}

pub(crate) fn original_request_uri(request: &Request<Body>) -> Uri {
    request
        .extensions()
        .get::<OriginalUri>()
        .map(|uri| uri.0.clone())
        .unwrap_or_else(|| request.uri().clone())
}

pub(crate) fn mount_prefix_for_path(path: &str, owner: &str) -> String {
    let remote_dav_files = format!("/remote.php/dav/files/{owner}");
    if has_path_prefix(path, &remote_dav_files) {
        return remote_dav_files;
    }
    if has_path_prefix(path, "/remote.php/webdav") {
        return "/remote.php/webdav".to_owned();
    }
    if has_path_prefix(path, "/remote.php/dav") {
        return "/remote.php/dav".to_owned();
    }
    String::new()
}

pub(crate) fn parse_rel_path(path_or_uri: &str) -> anyhow::Result<PathBuf> {
    parse_rel_path_for_owner(path_or_uri, "gono")
}

pub(crate) fn parse_rel_path_for_owner(path_or_uri: &str, owner: &str) -> anyhow::Result<PathBuf> {
    let path = if path_or_uri.starts_with("http://") || path_or_uri.starts_with("https://") {
        path_or_uri.parse::<Uri>()?.path().to_owned()
    } else {
        path_or_uri.to_owned()
    };
    reject_parent_segments(&path)?;
    let (path, supports_owner_files_mount) =
        if let Some(path) = path.strip_prefix("/remote.php/dav") {
            (path, true)
        } else if let Some(path) = path.strip_prefix("/remote.php/webdav") {
            (path, false)
        } else {
            (path.as_str(), false)
        };
    let owner_prefix = format!("/files/{owner}");
    let path = if supports_owner_files_mount && (path == "/files" || path == "/files/") {
        anyhow::bail!("WebDAV files mount requires the authenticated owner segment");
    } else if supports_owner_files_mount && path == owner_prefix {
        "/"
    } else if supports_owner_files_mount {
        let owner_child_prefix = owner_prefix + "/";
        if let Some(rest) = path.strip_prefix(&owner_child_prefix) {
            rest
        } else if path
            .strip_prefix("/files/")
            .is_some_and(|rest| !rest.is_empty())
        {
            anyhow::bail!("WebDAV owner does not match authenticated user");
        } else {
            path
        }
    } else {
        path
    };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let dav_path = DavPath::new(&path)?;
    Ok(Path::new(dav_path.as_rel_ospath()).to_path_buf())
}

pub(crate) fn reject_parent_segments(path: &str) -> anyhow::Result<()> {
    for segment in path.split('/') {
        let decoded = percent_decode_segment(segment)?;
        if decoded == b".." {
            anyhow::bail!("path contains parent segment");
        }
        if decoded.iter().any(|byte| *byte == 0 || *byte == b'/') {
            anyhow::bail!("path contains forbidden decoded byte");
        }
    }
    Ok(())
}

fn has_path_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
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

fn percent_decode_segment(segment: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    anyhow::bail!("invalid percent escape");
                }
                let high = hex_value(bytes[index + 1])
                    .ok_or_else(|| anyhow::anyhow!("invalid percent escape"))?;
                let low = hex_value(bytes[index + 2])
                    .ok_or_else(|| anyhow::anyhow!("invalid percent escape"))?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    Ok(decoded)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
