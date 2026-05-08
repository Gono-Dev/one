use std::path::{Path, PathBuf};

use anyhow::{bail, Context};

use crate::config::StorageConfig;

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
