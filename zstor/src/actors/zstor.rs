use crate::actors::{
    config::{ConfigActor, GetConfig},
    meta::{LoadMeta, MetaStoreActor, SaveMeta},
    pipeline::{PipelineActor, RecoverFile, StoreFile},
};
use crate::{
    erasure::Shard,
    meta::{MetaStore, ShardInfo},
    zdb::{SequentialZdb, ZdbError, ZdbResult},
    ZstorError, ZstorResult,
};
use actix::prelude::*;
use futures::future::{join_all, try_join_all};
use log::{debug, error, trace, warn};
use std::{ops::Deref, path::PathBuf};
use tokio::{fs, task::JoinHandle};

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message for the store command of zstor.
pub struct Store {
    /// Path to the file to store.
    pub file: PathBuf,
    /// Optional different path to use when computing the key. If set, the key is generated as if
    /// the file is saved in this path.
    pub key_path: Option<PathBuf>,
    /// Remember failure metadata to later retry the upload.
    pub save_failure: bool,
    /// Attempt to delete the file after a successful upload.
    pub delete: bool,
}

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message for the retrieve command of zstor.
pub struct Retrieve {
    /// Path of the file to retrieve.
    pub file: PathBuf,
}

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message for the rebuild command of zstor.
pub struct Rebuild {
    /// Path to the file to rebuild
    ///
    /// The path to the file to rebuild. The path is used to create a metadata key (by hashing
    /// the full path). The original data is decoded, and then reencoded as per the provided
    /// config. The new metadata is then used to replace the old metadata in the metadata
    /// store.
    pub file: Option<PathBuf>,
    /// Raw key to reconstruct
    ///
    /// The raw key to reconstruct. If this argument is given, the metadata store is checked
    /// for this key. If it exists, the data will be reconstructed according to the new policy,
    /// and the old metadata is replaced with the new metadata.
    pub key: Option<String>,
}

/// Actor for the main zstor object encoding and decoding.
pub struct ZstorActor<T: Unpin + 'static> {
    cfg: Addr<ConfigActor>,
    pipeline: Addr<PipelineActor>,
    meta: Addr<MetaStoreActor<T>>,
}

impl<T> ZstorActor<T>
where
    T: Unpin + 'static,
{
    /// new
    pub fn new(
        cfg: Addr<ConfigActor>,
        pipeline: Addr<PipelineActor>,
        meta: Addr<MetaStoreActor<T>>,
    ) -> ZstorActor<T> {
        Self {
            cfg,
            pipeline,
            meta,
        }
    }
}

impl<T> Actor for ZstorActor<T>
where
    T: Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<T> Handler<Store> for ZstorActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Store, _: &mut Self::Context) -> Self::Result {
        let pipeline = self.pipeline.clone();
        let config = self.cfg.clone();
        let meta = self.meta.clone();
        AtomicResponse::new(Box::pin(
            async move {
                let running_cfg = config.send(GetConfig).await?;
                // Explicity clone out the current config so we can modify it in the loop later
                let mut cfg = running_cfg.deref().clone();
                let (mut metadata, key_path, shards) = pipeline
                    .send(StoreFile {
                        file: msg.file.clone(),
                        key_path: msg.key_path,
                        cfg: running_cfg,
                    })
                    .await??;

                let shard_len = if shards.is_empty() {
                    0
                } else {
                    shards[0].len()
                };

                let dbs = loop {
                    debug!("Finding backend config");
                    let backends = cfg.shard_stores()?;

                    let mut failed_shards: usize = 0;
                    let mut handles: Vec<JoinHandle<ZdbResult<_>>> =
                        Vec::with_capacity(shards.len());

                    for backend in backends {
                        handles.push(tokio::spawn(async move {
                            let db = SequentialZdb::new(backend.clone()).await?;
                            // check space in backend
                            let ns_info = db.ns_info().await?;
                            match ns_info.free_space() {
                                insufficient if (insufficient as usize) < shard_len => {
                                    Err(ZdbError::new_storage_size(
                                        *db.connection_info().address(),
                                        shard_len,
                                        ns_info.free_space() as usize,
                                    ))
                                }
                                _ => Ok(db),
                            }
                        }));
                    }

                    let mut dbs = Vec::new();
                    for db in join_all(handles).await {
                        match db? {
                            Err(zdbe) => {
                                debug!("could not connect to 0-db: {}", zdbe);
                                cfg.remove_shard(zdbe.address());
                                failed_shards += 1;
                            }
                            Ok(db) => dbs.push(db), // no error so healthy db backend
                        }
                    }

                    // if we find one we are good
                    if failed_shards == 0 {
                        debug!("found valid backend configuration");
                        break dbs;
                    }

                    debug!("Backend config failed");
                };

                trace!("store shards in backends");

                let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(shards.len());
                for (db, (shard_idx, shard)) in dbs.into_iter().zip(shards.into_iter().enumerate())
                {
                    handles.push(tokio::spawn(async move {
                        let keys = db.set(&shard).await?;
                        Ok(ShardInfo::new(
                            shard_idx,
                            shard.checksum(),
                            keys,
                            db.connection_info().clone(),
                        ))
                    }));
                }

                for shard_info in try_join_all(handles).await? {
                    metadata.add_shard(shard_info?);
                }

                meta.send(SaveMeta {
                    path: key_path,
                    meta: metadata,
                })
                .await??;

                if msg.delete {
                    if let Err(e) = fs::remove_file(&msg.file).await {
                        // Log an error however it is not fatal, delete is done on a best effort
                        // basis.
                        error!("Failed to delete file {:?}: {}", &msg.file, e);
                    }
                }

                Ok(())
            }
            .into_actor(self),
        ))
    }
}

impl<T> Handler<Retrieve> for ZstorActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Retrieve, _: &mut Self::Context) -> Self::Result {
        let pipeline = self.pipeline.clone();
        let config = self.cfg.clone();
        let meta = self.meta.clone();
        AtomicResponse::new(Box::pin(
            async move {
                let cfg = config.send(GetConfig).await?;
                let metadata = meta
                    .send(LoadMeta {
                        path: msg.file.clone(),
                    })
                    .await??
                    .ok_or_else(|| {
                        ZstorError::new_io(
                            "no metadata found for file".to_string(),
                            std::io::Error::from(std::io::ErrorKind::NotFound),
                        )
                    })?;
                // attempt to retrieve all shards
                let mut shard_loads: Vec<JoinHandle<(usize, Result<(_, _), ZstorError>)>> =
                    Vec::with_capacity(metadata.shards().len());
                for si in metadata.shards().iter().cloned() {
                    shard_loads.push(tokio::spawn(async move {
                        let db = match SequentialZdb::new(si.zdb().clone()).await {
                            Ok(ok) => ok,
                            Err(e) => return (si.index(), Err(e.into())),
                        };
                        match db.get(si.key()).await {
                            Ok(potential_shard) => match potential_shard {
                                Some(shard) => (si.index(), Ok((shard, *si.checksum()))),
                                None => (
                                    si.index(),
                                    // TODO: Proper error here?
                                    Err(ZstorError::new_io(
                                        "shard not found".to_string(),
                                        std::io::Error::from(std::io::ErrorKind::NotFound),
                                    )),
                                ),
                            },
                            Err(e) => (si.index(), Err(e.into())),
                        }
                    }));
                }

                // Since this is the amount of actual shards needed to pass to the encoder, we calculate the
                // amount we will have from the amount of parity and data shards. Reason is that the `shards()`
                // might not have all data shards, due to a bug on our end, or later in case we allow for
                // degraded writes.
                let mut shards: Vec<Option<Vec<u8>>> =
                    vec![None; metadata.data_shards() + metadata.parity_shards()];
                for shard_info in join_all(shard_loads).await {
                    let (idx, shard) = shard_info?;
                    match shard {
                        Err(e) => warn!("could not download shard {}: {}", idx, e),
                        Ok((raw_shard, saved_checksum)) => {
                            let shard = Shard::from(raw_shard);
                            let checksum = shard.checksum();
                            if saved_checksum != checksum {
                                warn!("shard {} checksum verification failed", idx);
                                continue;
                            }
                            shards[idx] = Some(shard.into_inner());
                        }
                    }
                }

                pipeline
                    .send(RecoverFile {
                        path: msg.file,
                        shards,
                        cfg,
                        meta: metadata,
                    })
                    .await?
            }
            .into_actor(self),
        ))
    }
}

impl<T> Handler<Rebuild> for ZstorActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Rebuild, ctx: &mut Self::Context) -> Self::Result {
        todo!();
    }
}
