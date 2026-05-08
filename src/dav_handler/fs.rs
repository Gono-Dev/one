use std::{
    future,
    path::{Path, PathBuf},
    pin::Pin,
};

use dav_server::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsStream,
        OpenOptions, ReadDirMeta,
    },
    localfs::LocalFs,
};
use futures_util::{stream, FutureExt, StreamExt};
use http::StatusCode;
use sqlx::SqlitePool;
use std::time::SystemTime;
use tracing::debug;

use crate::{db, storage};

const OC_NS: &str = "http://owncloud.org/ns";
const NC_NS: &str = "http://nextcloud.org/ns";
const PROPFIND_PREFETCH_CONCURRENCY: usize = 64;

#[derive(Clone)]
pub struct NcLocalFs {
    inner: LocalFs,
    root: PathBuf,
    db: SqlitePool,
    owner: String,
    instance_id: String,
    xattr_ns: String,
}

impl NcLocalFs {
    pub fn new(
        root: impl AsRef<Path>,
        db: SqlitePool,
        owner: impl Into<String>,
        instance_id: impl Into<String>,
        xattr_ns: impl Into<String>,
    ) -> Self {
        let root = root.as_ref().to_path_buf();
        let inner = *LocalFs::new(&root, false, false, cfg!(target_os = "macos"));
        Self {
            inner,
            root,
            db,
            owner: owner.into(),
            instance_id: instance_id.into(),
            xattr_ns: xattr_ns.into(),
        }
    }

    async fn props_for_path(
        &self,
        path: &DavPath,
        do_content: bool,
    ) -> anyhow::Result<Vec<DavProp>> {
        let record = self.ensure_record(path).await?;
        let mut props = vec![
            nc_prop("has-preview", "false", do_content),
            oc_prop("fileid", &record.oc_file_id, do_content),
            oc_prop(
                "id",
                &format!("{}:{}", self.owner, record.oc_file_id),
                do_content,
            ),
            oc_prop(
                "permissions",
                permissions_string(record.permissions),
                do_content,
            ),
            oc_prop(
                "favorite",
                if record.favorite { "1" } else { "0" },
                do_content,
            ),
            oc_prop("owner-id", &self.owner, do_content),
            oc_prop("owner-display-name", &self.owner, do_content),
        ];

        if !do_content {
            for prop in &mut props {
                prop.xml = None;
            }
        }

        Ok(props)
    }

    async fn ensure_record(&self, path: &DavPath) -> anyhow::Result<db::FileRecord> {
        self.ensure_record_for_rel_path(path.as_rel_ospath()).await
    }

    async fn ensure_record_for_rel_path(&self, rel_path: &Path) -> anyhow::Result<db::FileRecord> {
        let abs_path = storage::safe_existing_path(&self.root, rel_path)?;
        db::ensure_file_record(
            &self.db,
            db::FileRecordInput {
                owner: &self.owner,
                rel_path,
                abs_path: &abs_path,
                instance_id: &self.instance_id,
                xattr_ns: &self.xattr_ns,
            },
        )
        .await
    }

    async fn assign_new_record(&self, path: &DavPath) -> anyhow::Result<db::FileRecord> {
        let rel_path = path.as_rel_ospath();
        let abs_path = storage::safe_existing_path(&self.root, rel_path)?;
        db::assign_new_file_record(
            &self.db,
            db::FileRecordInput {
                owner: &self.owner,
                rel_path,
                abs_path: &abs_path,
                instance_id: &self.instance_id,
                xattr_ns: &self.xattr_ns,
            },
        )
        .await
    }

    async fn set_favorite(&self, path: &DavPath, favorite: bool) -> anyhow::Result<db::FileRecord> {
        let rel_path = path.as_rel_ospath();
        let abs_path = storage::safe_existing_path(&self.root, rel_path)?;
        db::set_favorite(
            &self.db,
            &self.owner,
            rel_path,
            &abs_path,
            &self.instance_id,
            &self.xattr_ns,
            favorite,
        )
        .await
    }

    fn validate_existing(&self, path: &DavPath) -> Result<(), FsError> {
        storage::safe_existing_path(&self.root, path.as_rel_ospath())
            .map(|_| ())
            .map_err(map_fs_error)
    }

    fn validate_create(&self, path: &DavPath) -> Result<(), FsError> {
        storage::safe_create_path(&self.root, path.as_rel_ospath())
            .map(|_| ())
            .map_err(map_fs_error)
    }

    fn validate_write(&self, path: &DavPath) -> Result<(), FsError> {
        storage::safe_write_path(&self.root, path.as_rel_ospath())
            .map(|_| ())
            .map_err(map_fs_error)
    }

    async fn prefetch_child_records(&self, path: &DavPath) -> anyhow::Result<()> {
        let rel_path = path.as_rel_ospath().to_path_buf();
        let abs_path = storage::safe_existing_path(&self.root, &rel_path)?;
        let mut entries = tokio::fs::read_dir(&abs_path).await?;
        let mut children = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            children.push(rel_path.join(entry.file_name()));
        }

        stream::iter(children)
            .for_each_concurrent(PROPFIND_PREFETCH_CONCURRENCY, |child_rel| {
                let fs = self.clone();
                async move {
                    if let Err(err) = fs.ensure_record_for_rel_path(&child_rel).await {
                        debug!(
                            ?err,
                            rel_path = %child_rel.display(),
                            "failed to prefetch child file metadata"
                        );
                    }
                }
            })
            .await;

        Ok(())
    }
}

impl DavFileSystem for NcLocalFs {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        async move {
            if options.write || options.append || options.create || options.create_new {
                self.validate_write(path)?;
            } else {
                self.validate_existing(path)?;
            }
            DavFileSystem::open(&self.inner, path, options).await
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            self.validate_existing(path)?;
            if meta != ReadDirMeta::None {
                if let Err(err) = self.prefetch_child_records(path).await {
                    debug!(?err, path = %path, "failed to prefetch directory metadata");
                }
            }
            DavFileSystem::read_dir(&self.inner, path, meta).await
        }
        .boxed()
    }

    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            self.validate_existing(path)?;
            DavFileSystem::metadata(&self.inner, path).await
        }
        .boxed()
    }

    fn symlink_metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            self.validate_existing(path)?;
            DavFileSystem::symlink_metadata(&self.inner, path).await
        }
        .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        async move {
            self.validate_create(path)?;
            DavFileSystem::create_dir(&self.inner, path).await?;
            self.ensure_record(path).await.map_err(map_fs_error)?;
            Ok(())
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        async move {
            self.validate_existing(path)?;
            DavFileSystem::remove_dir(&self.inner, path).await?;
            db::delete_file_records(&self.db, &self.owner, path.as_rel_ospath())
                .await
                .map_err(map_fs_error)?;
            Ok(())
        }
        .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        async move {
            self.validate_existing(path)?;
            DavFileSystem::remove_file(&self.inner, path).await?;
            db::delete_file_records(&self.db, &self.owner, path.as_rel_ospath())
                .await
                .map_err(map_fs_error)?;
            Ok(())
        }
        .boxed()
    }

    fn rename<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, ()> {
        async move {
            self.validate_existing(from)?;
            self.validate_write(to)?;
            self.ensure_record(from).await.map_err(map_fs_error)?;
            DavFileSystem::rename(&self.inner, from, to).await?;
            db::move_file_record(
                &self.db,
                &self.owner,
                from.as_rel_ospath(),
                to.as_rel_ospath(),
            )
            .await
            .map_err(map_fs_error)?;
            self.ensure_record(to).await.map_err(map_fs_error)?;
            Ok(())
        }
        .boxed()
    }

    fn copy<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, ()> {
        async move {
            self.validate_existing(from)?;
            self.validate_write(to)?;
            DavFileSystem::copy(&self.inner, from, to).await?;
            self.assign_new_record(to).await.map_err(map_fs_error)?;
            Ok(())
        }
        .boxed()
    }

    fn set_accessed<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        tm: SystemTime,
    ) -> FsFuture<'a, ()> {
        DavFileSystem::set_accessed(&self.inner, path, tm)
    }

    fn set_modified<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        tm: SystemTime,
    ) -> FsFuture<'a, ()> {
        DavFileSystem::set_modified(&self.inner, path, tm)
    }

    fn have_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> Pin<Box<dyn future::Future<Output = bool> + Send + 'a>> {
        Box::pin(future::ready(true))
    }

    fn patch_props<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<'a, Vec<(StatusCode, DavProp)>> {
        async move {
            DavFileSystem::metadata(&self.inner, path).await?;
            let mut results = Vec::with_capacity(patch.len());

            for (set, prop) in patch {
                let status = if prop.namespace.as_deref() == Some(OC_NS) && prop.name == "favorite"
                {
                    match self.set_favorite(path, set && prop_is_truthy(&prop)).await {
                        Ok(_) => StatusCode::OK,
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                } else {
                    StatusCode::FORBIDDEN
                };

                results.push((status, prop_without_content(prop)));
            }

            Ok(results)
        }
        .boxed()
    }

    fn get_props<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        do_content: bool,
    ) -> FsFuture<'a, Vec<DavProp>> {
        async move {
            self.props_for_path(path, do_content)
                .await
                .map_err(map_fs_error)
        }
        .boxed()
    }

    fn get_prop<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        prop: DavProp,
    ) -> FsFuture<'a, Vec<u8>> {
        async move {
            self.props_for_path(path, true)
                .await
                .map_err(map_fs_error)?
                .into_iter()
                .find(|candidate| {
                    candidate.name == prop.name && candidate.namespace == prop.namespace
                })
                .and_then(|prop| prop.xml)
                .ok_or(dav_server::fs::FsError::NotFound)
        }
        .boxed()
    }

    fn get_quota(&'_ self) -> FsFuture<'_, (u64, Option<u64>)> {
        DavFileSystem::get_quota(&self.inner)
    }
}

fn oc_prop(name: &str, value: &str, do_content: bool) -> DavProp {
    custom_prop(name, "oc", OC_NS, value, do_content)
}

fn nc_prop(name: &str, value: &str, do_content: bool) -> DavProp {
    custom_prop(name, "nc", NC_NS, value, do_content)
}

fn custom_prop(
    name: &str,
    prefix: &str,
    namespace: &str,
    value: &str,
    do_content: bool,
) -> DavProp {
    let mut prop = DavProp::new(
        name.to_owned(),
        prefix.to_owned(),
        namespace.to_owned(),
        value.to_owned(),
    );
    if !do_content {
        prop.xml = None;
    }
    prop
}

fn prop_is_truthy(prop: &DavProp) -> bool {
    prop.xml
        .as_deref()
        .and_then(|xml| std::str::from_utf8(xml).ok())
        .map(|xml| xml.contains(">1<") || xml.contains(">true<"))
        .unwrap_or(false)
}

fn permissions_string(_permissions: i64) -> &'static str {
    "RGDNVW"
}

fn map_fs_error(err: anyhow::Error) -> FsError {
    if let Some(io) = err.downcast_ref::<std::io::Error>() {
        return io.into();
    }
    if err.to_string().contains("escapes storage root") {
        return FsError::Forbidden;
    }
    FsError::GeneralFailure
}

fn prop_without_content(prop: DavProp) -> DavProp {
    DavProp {
        name: prop.name,
        prefix: prop.prefix,
        namespace: prop.namespace,
        xml: None,
    }
}
