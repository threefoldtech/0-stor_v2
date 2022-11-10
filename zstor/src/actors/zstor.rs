use crate::actors::{
    config::{ConfigActor, GetConfig},
    meta::{LoadMeta, LoadMetaByKey, MetaStoreActor, SaveMeta, SaveMetaByKey},
    metrics::{MetricsActor, ZstorCommandFinsihed, ZstorCommandId},
    pipeline::{PipelineActor, RebuildData, RecoverFile, StoreFile},
};
use crate::{
    config::Config,
    erasure::Shard,
    meta::{Checksum, MetaData, ShardInfo},
    zdb::{SequentialZdb, ZdbError, ZdbResult},
    ZstorError, ZstorResult,
};
use actix::prelude::*;
use futures::future::{join_all, try_join_all};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    path::{Path, PathBuf},
};
use tokio::{fs, io, task::JoinHandle};

use super::config::ReloadConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// All possible commands zstor operates on.
pub enum ZstorCommand {
    /// Command to store a file.
    Store(Store),
    /// Command to retrieve a file.
    Retrieve(Retrieve),
    /// Command to rebuild file data in the backend.
    Rebuild(Rebuild),
    /// Command to check if a file exists in the backend.
    Check(Check),
}

#[derive(Serialize, Deserialize, Debug)]
/// All possible responses zstor can send.
pub enum ZstorResponse {
    /// Success without any returned data,
    Success,
    /// An error, the error message is included.
    Err(String),
    /// A checksum of a file.
    Checksum(Checksum),
}

#[derive(Serialize, Deserialize, Debug, Message, Clone)]
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
    /// Wait for upload to finish before returning (used only by the scheduler, zstor always blocks)
    pub blocking: bool,
}

#[derive(Serialize, Deserialize, Debug, Message, Clone)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message for the retrieve command of zstor.
pub struct Retrieve {
    /// Path of the file to retrieve.
    pub file: PathBuf,
}

#[derive(Serialize, Deserialize, Debug, Message, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Message, Clone)]
#[rtype(result = "Result<Option<Checksum>, ZstorError>")]
/// Message for the check command of zstor.
pub struct Check {
    /// The path to check for the presence of a file.
    pub path: PathBuf,
}

/// Actor for the main zstor object encoding and decoding.
pub struct ZstorActor {
    cfg: Addr<ConfigActor>,
    pipeline: Addr<PipelineActor>,
    meta: Addr<MetaStoreActor>,
    metrics: Addr<MetricsActor>,
}

impl ZstorActor {
    /// new
    pub fn new(
        cfg: Addr<ConfigActor>,
        pipeline: Addr<PipelineActor>,
        meta: Addr<MetaStoreActor>,
        metrics: Addr<MetricsActor>,
    ) -> ZstorActor {
        Self {
            cfg,
            pipeline,
            meta,
            metrics,
        }
    }
}

impl Actor for ZstorActor {
    type Context = Context<Self>;
}

impl Handler<Store> for ZstorActor {
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Store, _: &mut Self::Context) -> Self::Result {
        let pipeline = self.pipeline.clone();
        let config = self.cfg.clone();
        let meta = self.meta.clone();
        AtomicResponse::new(Box::pin(
            async move {
                let ft = fs::metadata(&msg.file)
                    .await
                    .map_err(|e| ZstorError::new_io("Could not load file metadata".into(), e))?
                    .file_type();
                let files = if ft.is_file() {
                    vec![msg.file]
                } else if ft.is_dir() {
                    get_dir_entries(&msg.file)
                        .await
                        .map_err(|e| ZstorError::new_io("Could not load dir entries".into(), e))?
                } else {
                    return Err(ZstorError::new_io(
                        format!("Unsupported file type {:?}", ft),
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Unsupported file type",
                        ),
                    ));
                };
                let running_cfg = config.send(GetConfig).await?;
                // Explicitly clone out the current config so we can modify it in the loop later
                let mut cfg = running_cfg.deref().clone();

                for file in files {
                    let (mut metadata, key_path, shards) = pipeline
                        .send(StoreFile {
                            file: file.clone(),
                            key_path: msg.key_path.clone(),
                            cfg: running_cfg.clone(),
                        })
                        .await??;

                    if let Ok(meta_result) = meta
                        .send(LoadMeta {
                            path: key_path.clone(),
                        })
                        .await?
                    {
                        if let Some(stored_metadata) = meta_result {
                            if *stored_metadata.checksum() == *metadata.checksum() {
                                debug!(
                                    "Skipping {:?} for upload because it's already uploaded",
                                    key_path,
                                );
                                continue;
                            }
                        }
                    };

                    save_data(&mut cfg, shards, &mut metadata).await?;

                    meta.send(SaveMeta {
                        path: key_path,
                        meta: metadata,
                    })
                    .await??;

                    if msg.delete {
                        if let Err(e) = fs::remove_file(&file).await {
                            // Log an error however it is not fatal, delete is done on a best effort
                            // basis.
                            error!("Failed to delete file {:?}: {}", &file, e);
                        }
                    }
                }

                Ok(())
            }
            .into_actor(self)
            .then(|res, actor, _| {
                actor.metrics.do_send(ZstorCommandFinsihed {
                    id: ZstorCommandId::Store,
                    success: res.is_ok(),
                });
                async move { res }.into_actor(actor)
            }),
        ))
    }
}

impl Handler<Retrieve> for ZstorActor {
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

                let shards = load_data(&metadata).await?;

                pipeline
                    .send(RecoverFile {
                        path: msg.file,
                        shards,
                        cfg,
                        meta: metadata,
                    })
                    .await?
            }
            .into_actor(self)
            .then(|res, actor, _| {
                actor.metrics.do_send(ZstorCommandFinsihed {
                    id: ZstorCommandId::Retrieve,
                    success: res.is_ok(),
                });
                async move { res }.into_actor(actor)
            }),
        ))
    }
}

impl Handler<Rebuild> for ZstorActor {
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Rebuild, _: &mut Self::Context) -> Self::Result {
        let pipeline = self.pipeline.clone();
        let config = self.cfg.clone();
        let meta = self.meta.clone();
        AtomicResponse::new(Box::pin(
            async move {
                let cfg = config.send(GetConfig).await?;
                if msg.file.is_none() && msg.key.is_none() {
                    return Err(ZstorError::new_io(
                        "Either `file` or `key` argument must be set".to_string(),
                        std::io::Error::from(std::io::ErrorKind::InvalidInput),
                    ));
                }
                if msg.file.is_some() && msg.key.is_some() {
                    return Err(ZstorError::new_io(
                        "Only one of `file` or `key` argument must be set".to_string(),
                        std::io::Error::from(std::io::ErrorKind::InvalidInput),
                    ));
                }
                let old_metadata = if let Some(ref file) = msg.file {
                    meta.send(LoadMeta { path: file.clone() })
                        .await??
                        .ok_or_else(|| {
                            ZstorError::new_io(
                                "no metadata found for file".to_string(),
                                std::io::Error::from(std::io::ErrorKind::NotFound),
                            )
                        })?
                } else if let Some(ref key) = msg.key {
                    // key is set so the unwrap is safe
                    meta.send(LoadMetaByKey { key: key.clone() })
                        .await??
                        .ok_or_else(|| {
                            ZstorError::new_io(
                                "no metadata found for file".to_string(),
                                std::io::Error::from(std::io::ErrorKind::NotFound),
                            )
                        })?
                } else {
                    unreachable!();
                };

                let input = load_data(&old_metadata).await?;
                let (mut metadata, shards) = pipeline
                    .send(RebuildData {
                        input,
                        cfg: cfg.clone(),
                        input_meta: old_metadata.clone(),
                    })
                    .await??;

                save_data(&mut cfg.deref().clone(), shards, &mut metadata).await?;

                info!(
                    "Rebuild file from {} to {}",
                    old_metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );

                if let Some(file) = msg.file {
                    meta.send(SaveMeta {
                        path: file,
                        meta: metadata,
                    })
                    .await??;
                } else if let Some(key) = msg.key {
                    meta.send(SaveMetaByKey {
                        key,
                        meta: metadata,
                    })
                    .await??;
                };

                Ok(())
            }
            .into_actor(self)
            .then(|res, actor, _| {
                actor.metrics.do_send(ZstorCommandFinsihed {
                    id: ZstorCommandId::Rebuild,
                    success: res.is_ok(),
                });
                async move { res }.into_actor(actor)
            }),
        ))
    }
}

impl Handler<Check> for ZstorActor {
    type Result = ResponseActFuture<Self, Result<Option<Checksum>, ZstorError>>;

    fn handle(&mut self, msg: Check, _: &mut Self::Context) -> Self::Result {
        let meta = self.meta.clone();
        Box::pin(
            async move {
                Ok(meta
                    .send(LoadMeta { path: msg.path })
                    .await??
                    .map(|meta| *meta.checksum()))
            }
            .into_actor(self)
            .then(|res, actor, _| {
                actor.metrics.do_send(ZstorCommandFinsihed {
                    id: ZstorCommandId::Check,
                    success: res.is_ok(),
                });
                async move { res }.into_actor(actor)
            }),
        )
    }
}

impl Handler<ReloadConfig> for ZstorActor {
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, _: ReloadConfig, _: &mut Self::Context) -> Self::Result {
        let cfg = self.cfg.clone();
        Box::pin(async move { cfg.send(ReloadConfig).await? })
    }
}

async fn load_data(metadata: &MetaData) -> ZstorResult<Vec<Option<Vec<u8>>>> {
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
    // amount we will have from the amount of disposable and data shards. Reason is that the shards
    // might not have all data shards, due to a bug on our end, or later in case we allow for
    // degraded writes.
    let mut shards: Vec<Option<Vec<u8>>> =
        vec![None; metadata.data_shards() + metadata.disposable_shards()];
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

    Ok(shards)
}

async fn save_data(
    cfg: &mut Config,
    shards: Vec<Shard>,
    metadata: &mut MetaData,
) -> ZstorResult<()> {
    let shard_len = if shards.is_empty() {
        0
    } else {
        shards[0].len()
    };

    let dbs = loop {
        debug!("Finding backend config");
        let backends = cfg.shard_stores()?;

        let mut failed_shards: usize = 0;
        let mut handles: Vec<JoinHandle<ZdbResult<_>>> = Vec::with_capacity(shards.len());

        for backend in backends {
            handles.push(tokio::spawn(async move {
                let db = SequentialZdb::new(backend.clone()).await?;
                // check space in backend
                let ns_info = db.ns_info().await?;
                match ns_info.free_space() {
                    insufficient if (insufficient as usize) < shard_len => {
                        Err(ZdbError::new_storage_size(
                            db.connection_info().clone(),
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
                    cfg.remove_shard(zdbe.remote());
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
    for (db, (shard_idx, shard)) in dbs.into_iter().zip(shards.into_iter().enumerate()) {
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

    Ok(())
}

/// Get all file entries in a given directory
async fn get_dir_entries(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut dir_entries = Vec::new();
    let mut rd = fs::read_dir(&dir).await?;
    while let Some(dir_entry) = rd.next_entry().await? {
        let ft = dir_entry.file_type().await?;

        if !ft.is_file() {
            debug!(
                "Skipping entry {:?} for upload as it is not a file",
                dir_entry.path(),
            );
            continue;
        }

        dir_entries.push(dir_entry.path());
    }

    Ok(dir_entries)
}
