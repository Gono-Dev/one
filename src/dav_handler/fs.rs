use std::{
    collections::HashMap,
    future,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
};

use dav_server::{
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsFuture, FsStream, OpenOptions,
        ReadDirMeta,
    },
    localfs::LocalFs,
};
use futures_util::FutureExt;
use http::StatusCode;
use sha2::{Digest, Sha256};
use std::time::SystemTime;

const OC_NS: &str = "http://owncloud.org/ns";
const NC_NS: &str = "http://nextcloud.org/ns";

#[derive(Clone)]
pub struct NcLocalFs {
    inner: LocalFs,
    owner: String,
    instance_id: String,
    favorites: Arc<Mutex<HashMap<String, bool>>>,
}

impl NcLocalFs {
    pub fn new(
        root: impl AsRef<Path>,
        owner: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Self {
        let inner = *LocalFs::new(root, false, false, cfg!(target_os = "macos"));
        Self {
            inner,
            owner: owner.into(),
            instance_id: instance_id.into(),
            favorites: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn props_for_path(
        &self,
        path: &dav_server::davpath::DavPath,
        do_content: bool,
    ) -> Vec<DavProp> {
        let key = path.as_url_string();
        let favorite = self
            .favorites
            .lock()
            .ok()
            .and_then(|favorites| favorites.get(&key).copied())
            .unwrap_or(false);
        let fileid = stable_file_id(&key, &self.instance_id);

        let mut props = vec![
            nc_prop("has-preview", "false", do_content),
            oc_prop("fileid", &fileid, do_content),
            oc_prop("id", &format!("{}:{}", self.owner, fileid), do_content),
            oc_prop("permissions", "RGDNVW", do_content),
            oc_prop("favorite", if favorite { "1" } else { "0" }, do_content),
            oc_prop("owner-id", &self.owner, do_content),
            oc_prop("owner-display-name", &self.owner, do_content),
        ];

        if !do_content {
            for prop in &mut props {
                prop.xml = None;
            }
        }

        props
    }
}

impl DavFileSystem for NcLocalFs {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        DavFileSystem::open(&self.inner, path, options)
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        DavFileSystem::read_dir(&self.inner, path, meta)
    }

    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        DavFileSystem::metadata(&self.inner, path)
    }

    fn symlink_metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        DavFileSystem::symlink_metadata(&self.inner, path)
    }

    fn create_dir<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        DavFileSystem::create_dir(&self.inner, path)
    }

    fn remove_dir<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        DavFileSystem::remove_dir(&self.inner, path)
    }

    fn remove_file<'a>(&'a self, path: &'a dav_server::davpath::DavPath) -> FsFuture<'a, ()> {
        DavFileSystem::remove_file(&self.inner, path)
    }

    fn rename<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, ()> {
        DavFileSystem::rename(&self.inner, from, to)
    }

    fn copy<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, ()> {
        DavFileSystem::copy(&self.inner, from, to)
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
            let key = path.as_url_string();
            let mut results = Vec::with_capacity(patch.len());

            for (set, prop) in patch {
                let status = if prop.namespace.as_deref() == Some(OC_NS) && prop.name == "favorite"
                {
                    if let Ok(mut favorites) = self.favorites.lock() {
                        favorites.insert(key.clone(), set && prop_is_truthy(&prop));
                    }
                    StatusCode::OK
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
            DavFileSystem::metadata(&self.inner, path).await?;
            Ok(self.props_for_path(path, do_content))
        }
        .boxed()
    }

    fn get_prop<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        prop: DavProp,
    ) -> FsFuture<'a, Vec<u8>> {
        async move {
            DavFileSystem::metadata(&self.inner, path).await?;
            self.props_for_path(path, true)
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

fn stable_file_id(path: &str, instance_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(instance_id.as_bytes());
    hasher.update([0]);
    hasher.update(path.as_bytes());
    let digest = hasher.finalize();
    format!("{}{}", hex::encode(&digest[..8]), instance_id)
}

fn prop_is_truthy(prop: &DavProp) -> bool {
    prop.xml
        .as_deref()
        .and_then(|xml| std::str::from_utf8(xml).ok())
        .map(|xml| xml.contains(">1<") || xml.contains(">true<"))
        .unwrap_or(false)
}

fn prop_without_content(prop: DavProp) -> DavProp {
    DavProp {
        name: prop.name,
        prefix: prop.prefix,
        namespace: prop.namespace,
        xml: None,
    }
}
