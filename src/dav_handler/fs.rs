use std::{
    future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use dav_server::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsStream,
        OpenOptions, ReadDirMeta,
    },
    localfs::LocalFs,
};
use futures_util::FutureExt;
use http::StatusCode;
use sqlx::SqlitePool;
use std::time::SystemTime;

use crate::{db, storage};

const OC_NS: &str = "http://owncloud.org/ns";
const NC_NS: &str = "http://nextcloud.org/ns";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropfindLiveProps {
    Include,
    RequestedOnly,
}

#[derive(Clone)]
pub struct NcLocalFs {
    inner: LocalFs,
    root: PathBuf,
    db: SqlitePool,
    owner: String,
    instance_id: String,
    xattr_ns: String,
    dead_props_may_exist: Arc<Mutex<Option<bool>>>,
    propfind_live_props: PropfindLiveProps,
    record_cache: Arc<DashMap<PathBuf, CachedFileRecord>>,
}

#[derive(Clone)]
struct CachedFileRecord {
    record: db::FileRecord,
    is_collection: bool,
}

impl NcLocalFs {
    pub fn new(
        root: impl AsRef<Path>,
        db: SqlitePool,
        owner: impl Into<String>,
        instance_id: impl Into<String>,
        xattr_ns: impl Into<String>,
        propfind_live_props: PropfindLiveProps,
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
            dead_props_may_exist: Arc::new(Mutex::new(None)),
            propfind_live_props,
            record_cache: Arc::new(DashMap::new()),
        }
    }

    async fn props_for_path(
        &self,
        path: &DavPath,
        do_content: bool,
    ) -> anyhow::Result<Vec<DavProp>> {
        if self.propfind_live_props == PropfindLiveProps::RequestedOnly {
            return Ok(Vec::new());
        }

        let cached = self.cached_record(path).await?;
        let record = &cached.record;
        let is_collection = cached.is_collection;
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
                permissions_string(record.permissions, is_collection),
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

        if self.dead_props_may_exist().await? {
            for dead_prop in
                db::list_dead_props(&self.db, &self.owner, path.as_rel_ospath()).await?
            {
                props.push(dead_prop_to_dav_prop(dead_prop, do_content));
            }
        }

        Ok(props)
    }

    async fn prop_for_path(&self, path: &DavPath, prop: &DavProp) -> anyhow::Result<Vec<u8>> {
        if prop.namespace.as_deref() == Some(NC_NS) && prop.name == "has-preview" {
            return Ok(nc_prop("has-preview", "false", true)
                .xml
                .expect("known prop has xml"));
        }

        if prop.namespace.as_deref() == Some(OC_NS) {
            match prop.name.as_str() {
                "owner-id" => {
                    return Ok(oc_prop("owner-id", &self.owner, true)
                        .xml
                        .expect("known prop has xml"));
                }
                "owner-display-name" => {
                    return Ok(oc_prop("owner-display-name", &self.owner, true)
                        .xml
                        .expect("known prop has xml"));
                }
                "fileid" | "id" | "permissions" | "favorite" => {
                    let cached = self.cached_record(path).await?;
                    let record = &cached.record;
                    let is_collection = cached.is_collection;
                    let prop = match prop.name.as_str() {
                        "fileid" => oc_prop("fileid", &record.oc_file_id, true),
                        "id" => {
                            oc_prop("id", &format!("{}:{}", self.owner, record.oc_file_id), true)
                        }
                        "permissions" => oc_prop(
                            "permissions",
                            permissions_string(record.permissions, is_collection),
                            true,
                        ),
                        "favorite" => {
                            oc_prop("favorite", if record.favorite { "1" } else { "0" }, true)
                        }
                        _ => unreachable!("known prop match"),
                    };
                    return Ok(prop.xml.expect("known prop has xml"));
                }
                _ => {}
            }
        }

        db::get_dead_prop(
            &self.db,
            &self.owner,
            path.as_rel_ospath(),
            prop.namespace.as_deref(),
            &prop.name,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("property not found"))
    }

    async fn dead_props_may_exist(&self) -> anyhow::Result<bool> {
        if let Some(value) = *self
            .dead_props_may_exist
            .lock()
            .expect("dead props cache poisoned")
        {
            return Ok(value);
        }

        let value = db::owner_has_dead_props(&self.db, &self.owner).await?;
        *self
            .dead_props_may_exist
            .lock()
            .expect("dead props cache poisoned") = Some(value);
        Ok(value)
    }

    fn mark_dead_props_may_exist(&self) {
        *self
            .dead_props_may_exist
            .lock()
            .expect("dead props cache poisoned") = Some(true);
    }

    async fn ensure_record(&self, path: &DavPath) -> anyhow::Result<db::FileRecord> {
        self.ensure_record_for_rel_path(path.as_rel_ospath()).await
    }

    async fn cached_record(&self, path: &DavPath) -> anyhow::Result<CachedFileRecord> {
        self.cached_record_for_rel_path(path.as_rel_ospath()).await
    }

    async fn cached_record_for_rel_path(
        &self,
        rel_path: &Path,
    ) -> anyhow::Result<CachedFileRecord> {
        if let Some(record) = self.record_cache.get(rel_path) {
            return Ok(record.clone());
        }

        let abs_path = storage::safe_existing_path(&self.root, rel_path)?;
        let metadata = std::fs::metadata(&abs_path)?;
        let record = db::ensure_file_record(
            &self.db,
            db::FileRecordInput {
                owner: &self.owner,
                rel_path,
                abs_path: &abs_path,
                instance_id: &self.instance_id,
                xattr_ns: &self.xattr_ns,
            },
        )
        .await?;
        let cached = CachedFileRecord {
            record,
            is_collection: metadata.is_dir(),
        };
        self.record_cache
            .insert(rel_path.to_path_buf(), cached.clone());
        Ok(cached)
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
            db::copy_dead_props(
                &self.db,
                &self.owner,
                from.as_rel_ospath(),
                to.as_rel_ospath(),
            )
            .await
            .map_err(map_fs_error)?;
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
                } else if prop.namespace.as_deref() == Some("DAV:") {
                    StatusCode::FORBIDDEN
                } else if set {
                    match prop.xml.as_deref() {
                        Some(xml) => match db::set_dead_prop(
                            &self.db,
                            &self.owner,
                            path.as_rel_ospath(),
                            prop.namespace.as_deref(),
                            &prop.name,
                            xml,
                        )
                        .await
                        {
                            Ok(_) => {
                                self.mark_dead_props_may_exist();
                                StatusCode::OK
                            }
                            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                        },
                        None => StatusCode::BAD_REQUEST,
                    }
                } else {
                    match db::remove_dead_prop(
                        &self.db,
                        &self.owner,
                        path.as_rel_ospath(),
                        prop.namespace.as_deref(),
                        &prop.name,
                    )
                    .await
                    {
                        Ok(_) => StatusCode::OK,
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
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
            self.prop_for_path(path, &prop)
                .await
                .map_err(|_| dav_server::fs::FsError::NotFound)
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

fn dead_prop_to_dav_prop(prop: db::DeadProp, do_content: bool) -> DavProp {
    DavProp {
        name: prop.name,
        prefix: None,
        namespace: if prop.namespace.is_empty() {
            None
        } else {
            Some(prop.namespace)
        },
        xml: if do_content { Some(prop.xml) } else { None },
    }
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

pub(crate) fn permissions_string(_permissions: i64, is_collection: bool) -> &'static str {
    if is_collection { "RGDNVCK" } else { "RGDNVW" }
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
