use std::{
    path::{Component, Path, PathBuf},
    time::UNIX_EPOCH,
};

use crate::config::StorageConfig;
use anyhow::{bail, Context};

#[derive(Debug, Clone)]
pub struct StorageLayout {
    pub data_root: PathBuf,
    pub files_root: PathBuf,
    pub uploads_root: PathBuf,
}

impl StorageLayout {
    pub fn prepare(config: &StorageConfig) -> anyhow::Result<Self> {
        let data_dir = Path::new(&config.data_dir);
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create data directory {}", data_dir.display()))?;
        let files_dir = data_dir.join("files");
        let uploads_dir = data_dir.join("uploads");
        std::fs::create_dir_all(&files_dir)
            .with_context(|| format!("create files directory {}", files_dir.display()))?;
        std::fs::create_dir_all(&uploads_dir)
            .with_context(|| format!("create uploads directory {}", uploads_dir.display()))?;

        let data_root = data_dir
            .canonicalize()
            .with_context(|| format!("canonicalize data directory {}", data_dir.display()))?;
        let files_root = files_dir
            .canonicalize()
            .with_context(|| format!("canonicalize files directory {}", files_dir.display()))?;
        let uploads_root = uploads_dir
            .canonicalize()
            .with_context(|| format!("canonicalize uploads directory {}", uploads_dir.display()))?;

        probe_xattr(&files_root, &config.xattr_ns)?;
        ensure_same_partition(&files_root, &uploads_root)?;

        Ok(Self {
            data_root,
            files_root,
            uploads_root,
        })
    }
}

fn probe_xattr(root: &Path, namespace: &str) -> anyhow::Result<()> {
    let probe_path = root.join(".nc-dav-xattr-probe");
    std::fs::write(&probe_path, b"x")
        .with_context(|| format!("create xattr probe file {}", probe_path.display()))?;

    let key = format!("{namespace}.probe");
    let payload = vec![b'x'; 3800];
    let result = (|| -> anyhow::Result<()> {
        xattr::set(&probe_path, &key, &payload)
            .with_context(|| format!("write xattr probe key {key}"))?;
        let read_back = xattr::get(&probe_path, &key)
            .with_context(|| format!("read xattr probe key {key}"))?
            .context("xattr probe key was not readable after write")?;
        if read_back.len() != payload.len() {
            bail!(
                "xattr probe length mismatch: wrote {}, read {}",
                payload.len(),
                read_back.len()
            );
        }
        xattr::remove(&probe_path, &key)
            .with_context(|| format!("remove xattr probe key {key}"))?;
        Ok(())
    })();

    let cleanup = std::fs::remove_file(&probe_path)
        .with_context(|| format!("remove xattr probe file {}", probe_path.display()));

    result.and(cleanup)
}

#[cfg(unix)]
fn ensure_same_partition(files_root: &Path, uploads_root: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let files_dev = std::fs::metadata(files_root)
        .with_context(|| format!("stat files directory {}", files_root.display()))?
        .dev();
    let uploads_dev = std::fs::metadata(uploads_root)
        .with_context(|| format!("stat uploads directory {}", uploads_root.display()))?
        .dev();

    if files_dev != uploads_dev {
        bail!(
            "files and uploads must be on the same partition: {} dev={} {} dev={}",
            files_root.display(),
            files_dev,
            uploads_root.display(),
            uploads_dev
        );
    }

    Ok(())
}

#[cfg(not(unix))]
fn ensure_same_partition(_files_root: &Path, _uploads_root: &Path) -> anyhow::Result<()> {
    Ok(())
}

pub fn normalize_rel_path(rel_path: &Path) -> anyhow::Result<PathBuf> {
    let mut rel = PathBuf::new();

    for component in rel_path.components() {
        match component {
            Component::Normal(name) => {
                if name.to_string_lossy().as_bytes().contains(&0) {
                    bail!("path contains NUL byte");
                }
                rel.push(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("path escapes storage root");
            }
        }
    }

    Ok(rel)
}

pub fn safe_existing_path(canonical_root: &Path, rel_path: &Path) -> anyhow::Result<PathBuf> {
    let rel = normalize_rel_path(rel_path)?;
    if rel.as_os_str().is_empty() {
        return Ok(canonical_root.to_path_buf());
    }

    let joined = canonical_root.join(rel);
    let real = joined
        .canonicalize()
        .with_context(|| format!("canonicalize existing path {}", joined.display()))?;
    ensure_inside(canonical_root, &real)?;
    Ok(real)
}

pub fn safe_create_path(canonical_root: &Path, rel_path: &Path) -> anyhow::Result<PathBuf> {
    let rel = normalize_rel_path(rel_path)?;
    if rel.as_os_str().is_empty() {
        bail!("cannot create storage root");
    }

    let joined = canonical_root.join(rel);
    if std::fs::symlink_metadata(&joined).is_ok() {
        return safe_existing_path(canonical_root, joined.strip_prefix(canonical_root)?);
    }

    let parent = joined
        .parent()
        .context("new path must have a parent directory")?;
    let real_parent = parent
        .canonicalize()
        .with_context(|| format!("canonicalize parent path {}", parent.display()))?;
    ensure_inside(canonical_root, &real_parent)?;

    let file_name = joined
        .file_name()
        .context("new path must have a final component")?;
    Ok(real_parent.join(file_name))
}

pub fn safe_write_path(canonical_root: &Path, rel_path: &Path) -> anyhow::Result<PathBuf> {
    let rel = normalize_rel_path(rel_path)?;
    if rel.as_os_str().is_empty() {
        bail!("cannot write storage root");
    }

    let joined = canonical_root.join(rel);
    if std::fs::symlink_metadata(&joined).is_ok() {
        safe_existing_path(canonical_root, joined.strip_prefix(canonical_root)?)
    } else {
        safe_create_path(canonical_root, joined.strip_prefix(canonical_root)?)
    }
}

pub fn rel_path_string(rel_path: &Path) -> anyhow::Result<String> {
    let rel = normalize_rel_path(rel_path)?;
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

pub fn metadata_fingerprint(path: &Path) -> anyhow::Result<(i64, i64)> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("read metadata for {}", path.display()))?;
    let size = metadata.len() as i64;
    let mtime_ns = metadata
        .modified()
        .context("read modified time")?
        .duration_since(UNIX_EPOCH)
        .context("modified time before UNIX epoch")?
        .as_nanos() as i64;
    Ok((mtime_ns, size))
}

fn ensure_inside(canonical_root: &Path, path: &Path) -> anyhow::Result<()> {
    if path.starts_with(canonical_root) {
        Ok(())
    } else {
        bail!(
            "path escapes storage root: root={} path={}",
            canonical_root.display(),
            path.display()
        )
    }
}
