use crate::{
    meta::{FailureMeta, MetaData, MetaStore, MetaStoreError},
    zdb::ZdbConnectionInfo,
};
use actix::prelude::*;
use std::{path::PathBuf, sync::Arc};

#[derive(Message)]
#[rtype(result = "Result<(), MetaStoreError>")]
/// Message for saving metadata in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct SaveMeta {
    /// Path of the file that was uploaded.
    pub path: PathBuf,
    /// MetaData of the upload.
    pub meta: MetaData,
}

#[derive(Message)]
#[rtype(result = "Result<(), MetaStoreError>")]
/// Message for saving metadata by key in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct SaveMetaByKey {
    /// Key to use to save the MetaData.
    pub key: String,
    /// MetaData of the upload.
    pub meta: MetaData,
}

#[derive(Message)]
#[rtype(result = "Result<Option<MetaData>, MetaStoreError>")]
/// Message for loading metadata in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct LoadMeta {
    /// Path of the file to load the MetaData for.
    pub path: PathBuf,
}

#[derive(Message)]
#[rtype(result = "Result<Option<MetaData>, MetaStoreError>")]
/// Message for loading metadata by its key in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct LoadMetaByKey {
    /// Key of the MetaData to load.
    pub key: String,
}

#[derive(Message)]
#[rtype(result = "Result<(), MetaStoreError>")]
/// Message for marking a 0-db as replaced in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct SetReplaced {
    /// Connection info for the 0-db which was replaced.
    pub ci: ZdbConnectionInfo,
}

#[derive(Message)]
#[rtype(result = "Result<bool, MetaStoreError>")]
/// Message for checking if a 0-db is marked as replaced in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct IsReplaced {
    /// Connection info for the 0-db to check if it has been replaced.
    pub ci: ZdbConnectionInfo,
}

#[derive(Message)]
#[rtype(result = "Result<Vec<(String, MetaData)>, MetaStoreError>")]
/// Message for retrieving all [`MetaData`] objects in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct ObjectMetas;

#[derive(Message)]
#[rtype(result = "Result<(), MetaStoreError>")]
/// Message for saving upload failure info in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct SaveFailure {
    /// Path of the actual file that should be uploaded.
    pub data_path: PathBuf,
    /// Fake path which should be used to generate the key for the MetaData upload.
    pub key_dir_path: Option<PathBuf>,
    /// Whether the file should be deleted if the upload is succesful.
    pub should_delete: bool,
}

#[derive(Message)]
#[rtype(result = "Result<(), MetaStoreError>")]
/// Message for deleting an upload failure info in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct DeleteFailure {
    /// The metadata of a failed upload
    pub fm: FailureMeta,
}

#[derive(Message)]
#[rtype(result = "Result<Vec<FailureMeta>, MetaStoreError>")]
/// Message for retrieving all [`FailureMeta`] objects in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct GetFailures;

/// Actor for a metastore
#[derive(Debug)]
pub struct MetaStoreActor<T> {
    meta_store: Arc<T>,
}

impl<T> MetaStoreActor<T>
where
    T: MetaStore + Send,
{
    /// Create a new [`MetaStoreActor`] from a given [`MetaStore`].
    pub fn new(meta_store: T) -> MetaStoreActor<T> {
        Self {
            meta_store: Arc::new(meta_store),
        }
    }
}

impl<T> Actor for MetaStoreActor<T>
where
    T: Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<T> Handler<SaveMeta> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveMeta, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.save_meta(&msg.path, &msg.meta).await })
    }
}

impl<T> Handler<SaveMetaByKey> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveMetaByKey, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.save_meta_by_key(&msg.key, &msg.meta).await })
    }
}

impl<T> Handler<LoadMeta> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<Option<MetaData>, MetaStoreError>>;

    fn handle(&mut self, msg: LoadMeta, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.load_meta(&msg.path).await })
    }
}

impl<T> Handler<LoadMetaByKey> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<Option<MetaData>, MetaStoreError>>;

    fn handle(&mut self, msg: LoadMetaByKey, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.load_meta_by_key(&msg.key).await })
    }
}

impl<T> Handler<SetReplaced> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SetReplaced, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.set_replaced(&msg.ci).await })
    }
}

impl<T> Handler<IsReplaced> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<bool, MetaStoreError>>;

    fn handle(&mut self, msg: IsReplaced, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.is_replaced(&msg.ci).await })
    }
}

impl<T> Handler<ObjectMetas> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<Vec<(String, MetaData)>, MetaStoreError>>;

    fn handle(&mut self, _: ObjectMetas, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.object_metas().await })
    }
}

impl<T> Handler<SaveFailure> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveFailure, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            meta_store
                .save_failure(&msg.data_path, &msg.key_dir_path, msg.should_delete)
                .await
        })
    }
}

impl<T> Handler<DeleteFailure> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: DeleteFailure, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.delete_failure(&msg.fm).await })
    }
}

impl<T> Handler<GetFailures> for MetaStoreActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = ResponseFuture<Result<Vec<FailureMeta>, MetaStoreError>>;

    fn handle(&mut self, _: GetFailures, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.get_failures().await })
    }
}
