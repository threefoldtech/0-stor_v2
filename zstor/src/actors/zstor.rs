use crate::actors::{
    config::{ConfigActor, GetConfig},
    meta::{MetaStoreActor, SaveMeta},
    pipeline::{PipelineActor, StoreFile},
};
use crate::{
    meta::{MetaStore, ShardInfo},
    zdb::{SequentialZdb, ZdbError, ZdbResult},
    ZstorError, ZstorResult,
};
use actix::prelude::*;
use futures::future::{join_all, try_join_all};
use log::{debug, trace};
use std::{ops::Deref, path::PathBuf};
use tokio::task::JoinHandle;

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
                        file: msg.file,
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

                Ok(())
            }
            .into_actor(self),
        ))
    }
}
