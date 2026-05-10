use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context};
use axum::http::Method;

use crate::auth::Principal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionLevel {
    View,
    Full,
}

impl PermissionLevel {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "view" => Ok(Self::View),
            "full" => Ok(Self::Full),
            _ => bail!("invalid permission {value:?}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Full => "full",
        }
    }

    pub fn display_label(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Full => "Full access",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPasswordScope {
    pub id: i64,
    pub mount_path: PathBuf,
    pub storage_path: PathBuf,
    pub permission: PermissionLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeMatch {
    pub scope: AppPasswordScope,
    pub client_rel_path: PathBuf,
    pub storage_rel_path: PathBuf,
}

pub fn normalize_scope_path(input: &str) -> anyhow::Result<PathBuf> {
    if input.is_empty() {
        bail!("scope path cannot be empty");
    }
    if !input.starts_with('/') {
        bail!("scope path must start with /");
    }
    if input.len() > 1 && input.ends_with('/') {
        bail!("scope path must not end with /");
    }
    if input.contains("//") {
        bail!("scope path must not contain duplicate slashes");
    }

    let mut normalized = PathBuf::new();
    for segment in input.trim_start_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." {
            bail!("scope path must not contain . or .. segments");
        }
        if segment.as_bytes().contains(&0) {
            bail!("scope path must not contain NUL bytes");
        }
        normalized.push(segment);
    }
    Ok(normalized)
}

pub fn scope_path_to_db(path: &Path) -> anyhow::Result<String> {
    validate_normalized_rel_path(path)?;
    if path.as_os_str().is_empty() {
        return Ok("/".to_owned());
    }
    Ok(format!("/{}", path_to_slash_string(path)?))
}

pub fn validate_scope_set(scopes: &[AppPasswordScope]) -> anyhow::Result<()> {
    for scope in scopes {
        validate_normalized_rel_path(&scope.mount_path)?;
        validate_normalized_rel_path(&scope.storage_path)?;
    }
    for (index, left) in scopes.iter().enumerate() {
        for right in scopes.iter().skip(index + 1) {
            if path_contains(&left.mount_path, &right.mount_path)
                || path_contains(&right.mount_path, &left.mount_path)
            {
                bail!(
                    "overlapping app password mount paths: {} and {}",
                    scope_path_to_db(&left.mount_path)?,
                    scope_path_to_db(&right.mount_path)?
                );
            }
        }
    }
    Ok(())
}

pub fn default_scope() -> AppPasswordScope {
    AppPasswordScope {
        id: 0,
        mount_path: PathBuf::new(),
        storage_path: PathBuf::new(),
        permission: PermissionLevel::Full,
    }
}

pub fn resolve_scope_for_client_path(
    principal: &Principal,
    client_rel_path: &Path,
) -> anyhow::Result<ScopeMatch> {
    validate_normalized_rel_path(client_rel_path)?;
    principal
        .scopes
        .iter()
        .filter(|scope| path_contains(&scope.mount_path, client_rel_path))
        .max_by_key(|scope| scope.mount_path.components().count())
        .map(|scope| {
            let suffix = strip_prefix_rel(client_rel_path, &scope.mount_path)?;
            Ok::<ScopeMatch, anyhow::Error>(ScopeMatch {
                scope: scope.clone(),
                client_rel_path: client_rel_path.to_path_buf(),
                storage_rel_path: scope.storage_path.join(suffix),
            })
        })
        .transpose()?
        .with_context(|| {
            format!(
                "path {} is outside app password scopes",
                path_to_slash_string(client_rel_path).unwrap_or_else(|_| "<invalid>".to_owned())
            )
        })
}

pub fn resolve_scope_for_storage_path(
    principal: &Principal,
    storage_rel_path: &Path,
) -> anyhow::Result<Option<ScopeMatch>> {
    validate_normalized_rel_path(storage_rel_path)?;
    principal
        .scopes
        .iter()
        .filter(|scope| path_contains(&scope.storage_path, storage_rel_path))
        .max_by_key(|scope| scope.storage_path.components().count())
        .map(|scope| {
            let suffix = strip_prefix_rel(storage_rel_path, &scope.storage_path)?;
            Ok::<ScopeMatch, anyhow::Error>(ScopeMatch {
                scope: scope.clone(),
                client_rel_path: scope.mount_path.join(suffix),
                storage_rel_path: storage_rel_path.to_path_buf(),
            })
        })
        .transpose()
}

pub fn is_method_allowed(scope_match: &ScopeMatch, method: &Method) -> bool {
    scope_match.scope.permission == PermissionLevel::Full || !is_write_method(method)
}

pub fn is_write_method(method: &Method) -> bool {
    matches!(
        method.as_str(),
        "PUT" | "MKCOL" | "DELETE" | "MOVE" | "COPY" | "PROPPATCH" | "LOCK" | "UNLOCK"
    )
}

pub fn storage_path_to_client_path(
    principal: &Principal,
    storage_rel_path: &Path,
) -> anyhow::Result<Option<PathBuf>> {
    Ok(resolve_scope_for_storage_path(principal, storage_rel_path)?
        .map(|matched| matched.client_rel_path))
}

pub fn is_virtual_collection_path(principal: &Principal, client_rel_path: &Path) -> bool {
    validate_normalized_rel_path(client_rel_path).is_ok()
        && principal.scopes.iter().any(|scope| {
            client_rel_path != scope.mount_path && path_contains(client_rel_path, &scope.mount_path)
        })
}

pub fn virtual_collection_children(
    principal: &Principal,
    client_rel_path: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    validate_normalized_rel_path(client_rel_path)?;
    let mut children = Vec::<PathBuf>::new();
    for scope in &principal.scopes {
        if !path_contains(client_rel_path, &scope.mount_path) || client_rel_path == scope.mount_path
        {
            continue;
        }
        let suffix = strip_prefix_rel(&scope.mount_path, client_rel_path)?;
        let Some(next) = suffix.components().next() else {
            continue;
        };
        let Component::Normal(segment) = next else {
            continue;
        };
        let child = client_rel_path.join(segment);
        if !children.iter().any(|existing| existing == &child) {
            children.push(child);
        }
    }
    children.sort();
    Ok(children)
}

pub fn path_to_display(path: &Path) -> anyhow::Result<String> {
    scope_path_to_db(path)
}

fn validate_normalized_rel_path(path: &Path) -> anyhow::Result<()> {
    for component in path.components() {
        match component {
            Component::Normal(value) if !value.is_empty() => {}
            _ => bail!("path must be a normalized relative path"),
        }
    }
    Ok(())
}

fn strip_prefix_rel<'a>(path: &'a Path, prefix: &Path) -> anyhow::Result<&'a Path> {
    if prefix.as_os_str().is_empty() {
        return Ok(path);
    }
    path.strip_prefix(prefix).with_context(|| {
        format!(
            "path {} does not contain prefix {}",
            path.display(),
            prefix.display()
        )
    })
}

fn path_contains(prefix: &Path, path: &Path) -> bool {
    prefix.as_os_str().is_empty() || path == prefix || path.starts_with(prefix)
}

fn path_to_slash_string(path: &Path) -> anyhow::Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => parts.push(
                value
                    .to_str()
                    .context("scope path must be valid UTF-8")?
                    .to_owned(),
            ),
            _ => bail!("path must be a normalized relative path"),
        }
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(scopes: Vec<AppPasswordScope>) -> Principal {
        Principal {
            username: "alice".to_owned(),
            app_password_id: 1,
            app_password_label: "test".to_owned(),
            expires_at: None,
            scopes,
        }
    }

    #[test]
    fn normalizes_scope_paths() {
        assert_eq!(normalize_scope_path("/").unwrap(), PathBuf::new());
        assert_eq!(
            normalize_scope_path("/Docs").unwrap(),
            PathBuf::from("Docs")
        );
        assert!(normalize_scope_path("").is_err());
        assert!(normalize_scope_path("Docs").is_err());
        assert!(normalize_scope_path("/Docs/").is_err());
        assert!(normalize_scope_path("/Docs//A").is_err());
        assert!(normalize_scope_path("/../A").is_err());
    }

    #[test]
    fn resolves_client_paths_to_storage_paths() {
        let principal = principal(vec![
            AppPasswordScope {
                id: 1,
                mount_path: PathBuf::from("Docs"),
                storage_path: PathBuf::from("Projects"),
                permission: PermissionLevel::View,
            },
            AppPasswordScope {
                id: 2,
                mount_path: PathBuf::from("Uploads"),
                storage_path: PathBuf::from("Inbox/Uploads"),
                permission: PermissionLevel::Full,
            },
        ]);

        let docs = resolve_scope_for_client_path(&principal, Path::new("Docs/a.txt")).unwrap();
        assert_eq!(docs.storage_rel_path, PathBuf::from("Projects/a.txt"));
        assert_eq!(docs.scope.permission, PermissionLevel::View);
        assert!(resolve_scope_for_client_path(&principal, Path::new("Other/a.txt")).is_err());

        let client = storage_path_to_client_path(&principal, Path::new("Inbox/Uploads/a.txt"))
            .unwrap()
            .unwrap();
        assert_eq!(client, PathBuf::from("Uploads/a.txt"));
    }

    #[test]
    fn rejects_overlapping_mount_paths() {
        let scopes = vec![
            AppPasswordScope {
                id: 1,
                mount_path: PathBuf::from("Docs"),
                storage_path: PathBuf::from("Projects"),
                permission: PermissionLevel::View,
            },
            AppPasswordScope {
                id: 2,
                mount_path: PathBuf::from("Docs/Sub"),
                storage_path: PathBuf::from("Other"),
                permission: PermissionLevel::Full,
            },
        ];
        assert!(validate_scope_set(&scopes).is_err());
    }
}
