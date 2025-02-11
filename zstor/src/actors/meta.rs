use crate::{
    meta::{FailureMeta, MetaData, MetaStore, MetaStoreError},
    zdb::ZdbConnectionInfo,
};
use actix::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
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
#[rtype(result = "Result<(usize, Option<Vec<u8>>, Vec<(String, MetaData)>), MetaStoreError>")]
/// Message for  scan [`MetaData`] objects in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct ScanMeta {
    /// Cursor to start scanning from.
    pub cursor: Option<Vec<u8>>,

    /// Backend index to scan from.
    /// If none, it will use the backend with most keys
    pub backend_idx: Option<usize>,

    /// Maximum timestamp to scan until.
    pub max_timestamp: Option<u64>,
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
#[rtype(result = "bool")]
/// Message to check if the metastore is writable.
pub struct CheckWritable;

#[derive(Message)]
#[rtype(result = "Result<Vec<FailureMeta>, MetaStoreError>")]
/// Message for retrieving all [`FailureMeta`] objects in a [`MetaStore`] managed by a [`MetaStoreActor`].
pub struct GetFailures;

#[derive(Message)]
#[rtype(result = "()")]
/// Message for setting the `writeable` state of the metastore.
pub struct MarkWriteable {
    /// Indicates if the metastore is now writable or not.
    pub writeable: bool,
}

#[derive(Message)]
#[rtype(result = "()")]
/// Message to replace the metastore in use.
pub struct RebuildAllMeta;
#[derive(Message)]
#[rtype(result = "()")]
/// Message to replace the metastore in use.
pub struct ReplaceMetaStore {
    /// The new metastore to set
    pub new_store: Box<dyn MetaStore + Send>,

    /// writeable flag
    pub writeable: bool,
}

/// Actor for a metastore
pub struct MetaStoreActor {
    meta_store: Arc<dyn MetaStore>,
    writeable: bool,
    rebuild_all_meta_counter: Arc<AtomicU64>,
}

impl MetaStoreActor {
    /// Create a new [`MetaStoreActor`] from a given [`MetaStore`].
    pub fn new(meta_store: Box<dyn MetaStore>, writeable: bool) -> MetaStoreActor {
        log::info!("metastore actor writeable: {}", writeable);
        Self {
            meta_store: Arc::from(meta_store),
            writeable,
            rebuild_all_meta_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// create a guard for the rebuild meta operation.
    /// This guard is used to check if a newer rebuild operation has started
    /// and the current one should be stopped.
    /// We stop the current one because the newer one will have the latest meta store configuration,
    /// so there is no point to continue the current one.
    fn create_rebuild_meta_guard(&self) -> RebuildAllMetaGuard {
        let new_gen = self
            .rebuild_all_meta_counter
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        RebuildAllMetaGuard {
            generation: new_gen,
            current_gen: self.rebuild_all_meta_counter.clone(),
        }
    }
}

impl Actor for MetaStoreActor {
    type Context = Context<Self>;
}

impl Handler<SaveMeta> for MetaStoreActor {
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveMeta, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            if !writeable {
                return Err(MetaStoreError::not_writeable());
            }
            meta_store.save_meta(&msg.path, &msg.meta).await
        })
    }
}

impl Handler<SaveMetaByKey> for MetaStoreActor {
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveMetaByKey, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            if !writeable {
                return Err(MetaStoreError::not_writeable());
            }
            meta_store.save_meta_by_key(&msg.key, &msg.meta).await
        })
    }
}

impl Handler<LoadMeta> for MetaStoreActor {
    type Result = ResponseFuture<Result<Option<MetaData>, MetaStoreError>>;

    fn handle(&mut self, msg: LoadMeta, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.load_meta(&msg.path).await })
    }
}

impl Handler<LoadMetaByKey> for MetaStoreActor {
    type Result = ResponseFuture<Result<Option<MetaData>, MetaStoreError>>;

    fn handle(&mut self, msg: LoadMetaByKey, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.load_meta_by_key(&msg.key).await })
    }
}

impl Handler<SetReplaced> for MetaStoreActor {
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SetReplaced, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            if !writeable {
                return Err(MetaStoreError::not_writeable());
            }
            meta_store.set_replaced(&msg.ci).await
        })
    }
}

impl Handler<IsReplaced> for MetaStoreActor {
    type Result = ResponseFuture<Result<bool, MetaStoreError>>;

    fn handle(&mut self, msg: IsReplaced, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.is_replaced(&msg.ci).await })
    }
}

impl Handler<ScanMeta> for MetaStoreActor {
    type Result =
        ResponseFuture<Result<(usize, Option<Vec<u8>>, Vec<(String, MetaData)>), MetaStoreError>>;

    fn handle(&mut self, msg: ScanMeta, ctx: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        let addr = ctx.address();

        Box::pin(async move {
            let (new_cursor, backend_idx, keys) = match meta_store
                .scan_meta_keys(msg.cursor, msg.backend_idx, msg.max_timestamp)
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    return Err(e);
                }
            };

            let mut metas = Vec::with_capacity(keys.len());

            for key in keys {
                let meta: MetaData = match addr.send(LoadMetaByKey { key: key.clone() }).await {
                    Ok(Ok(m)) => m.unwrap(),
                    Ok(Err(e)) => {
                        log::error!("Error loading meta by key:{} -  {}", key, e);
                        continue;
                    }
                    Err(e) => {
                        log::error!("Error loading meta by key:{} -  {}", key, e);
                        continue;
                    }
                };
                metas.push((key, meta));
            }
            Ok((new_cursor, backend_idx, metas))
        })
    }
}

impl Handler<ObjectMetas> for MetaStoreActor {
    type Result = ResponseFuture<Result<Vec<(String, MetaData)>, MetaStoreError>>;

    fn handle(&mut self, _: ObjectMetas, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.object_metas().await })
    }
}

impl Handler<SaveFailure> for MetaStoreActor {
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: SaveFailure, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            if !writeable {
                return Err(MetaStoreError::not_writeable());
            }
            meta_store
                .save_failure(&msg.data_path, &msg.key_dir_path, msg.should_delete)
                .await
        })
    }
}

impl Handler<CheckWritable> for MetaStoreActor {
    type Result = ResponseFuture<bool>;

    fn handle(&mut self, _: CheckWritable, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        Box::pin(async move { writeable })
    }
}

impl Handler<DeleteFailure> for MetaStoreActor {
    type Result = ResponseFuture<Result<(), MetaStoreError>>;

    fn handle(&mut self, msg: DeleteFailure, _: &mut Self::Context) -> Self::Result {
        let writeable = self.writeable;
        let meta_store = self.meta_store.clone();
        Box::pin(async move {
            if !writeable {
                return Err(MetaStoreError::not_writeable());
            }
            meta_store.delete_failure(&msg.fm).await
        })
    }
}

impl Handler<GetFailures> for MetaStoreActor {
    type Result = ResponseFuture<Result<Vec<FailureMeta>, MetaStoreError>>;

    fn handle(&mut self, _: GetFailures, _: &mut Self::Context) -> Self::Result {
        let meta_store = self.meta_store.clone();
        Box::pin(async move { meta_store.get_failures().await })
    }
}

struct RebuildAllMetaGuard {
    generation: u64,
    current_gen: Arc<AtomicU64>,
}

impl RebuildAllMetaGuard {
    fn is_current(&self) -> bool {
        self.generation == self.current_gen.load(Ordering::SeqCst)
    }
}
/// Rebuild all meta data in the metastore:
/// - scan all keys in the metastore before current timestamp
/// - load meta by key
/// - save meta by key
impl Handler<RebuildAllMeta> for MetaStoreActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: RebuildAllMeta, ctx: &mut Self::Context) -> Self::Result {
        let metastore = self.meta_store.clone();
        let addr = ctx.address();
        log::info!("Rebuilding all meta handler");
        let rebuild_guard = self.create_rebuild_meta_guard();

        Box::pin(async move {
            let mut cursor = None;
            let mut backend_idx = None;

            // get current timestamp
            // we use this timestamp to prevent rebuilding meta that created after this timestamp,
            // otherwise we will have endless loop of meta rebuild
            use std::time::{SystemTime, UNIX_EPOCH};

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            log::info!("Starting rebuild at timestamp: {}", timestamp);
            loop {
                let (idx, new_cursor, keys) = match metastore
                    .scan_meta_keys(cursor.clone(), backend_idx, Some(timestamp))
                    .await
                {
                    Ok((idx, c, keys)) => (idx, c, keys),
                    Err(e) => {
                        log::error!("Error scanning keys: {}", e);
                        break;
                    }
                };

                for key in keys {
                    if !rebuild_guard.is_current() {
                        log::info!("Newer rebuild started, stopping current one");
                        break;
                    }
                    log::info!("Rebuilding meta key: {}", key);
                    let meta: MetaData = match addr.send(LoadMetaByKey { key: key.clone() }).await {
                        Ok(Ok(m)) => m.unwrap(),
                        Ok(Err(e)) => {
                            log::error!("Error loading meta by key:{} -  {}", key, e);
                            continue;
                        }
                        Err(e) => {
                            log::error!("Error loading meta by key:{} -  {}", key, e);
                            continue;
                        }
                    };

                    // save meta by key
                    match addr
                        .send(SaveMetaByKey {
                            key: key.clone(),
                            meta,
                        })
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            log::error!("Error saving meta by key:{} -  {}", key, e);
                        }
                        Err(e) => {
                            log::error!("Error saving meta by key:{} -  {}", key, e);
                        }
                    }
                }
                if cursor.is_none() {
                    break;
                }
                cursor = new_cursor;
                backend_idx = Some(idx);
            }
        })
    }
}

impl Handler<MarkWriteable> for MetaStoreActor {
    type Result = ();

    fn handle(&mut self, msg: MarkWriteable, _: &mut Self::Context) -> Self::Result {
        self.writeable = msg.writeable;
    }
}

impl Handler<ReplaceMetaStore> for MetaStoreActor {
    type Result = ();

    fn handle(&mut self, msg: ReplaceMetaStore, _: &mut Self::Context) -> Self::Result {
        log::info!("ReplaceMetaStore writeable: {}", msg.writeable);
        self.meta_store = Arc::from(msg.new_store as Box<dyn MetaStore>);
        self.writeable = msg.writeable;
    }
}
