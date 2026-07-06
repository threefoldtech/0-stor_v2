use crate::actors::{
    config::{ConfigActor, GetConfig},
    meta::{CheckWritable, LoadMeta, LoadMetaByKey, MetaStoreActor, SaveMeta, SaveMetaByKey},
    metrics::{MetricsActor, ZstorCommandFinsihed, ZstorCommandId},
    pipeline::{PipelineActor, RebuildData, RecoverFile, StoreFile},
};
use crate::{
    config::Config,
    erasure::Shard,
    meta::{Checksum, MetaData, ShardInfo},
    zdb::{Key, SequentialZdb, ZdbConnectionInfo, ZdbError, ZdbResult},
    ZstorError, ZstorErrorKind, ZstorResult,
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

use super::{
    backends::BackendManagerActor,
    config::ReloadConfig,
    repairer::{ScanNow, ScanReport, SweepNow, SweepReport},
};

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
    /// Command to run a repair sweep over all stored objects.
    Sweep(SweepNow),
    /// Command to run a health scan over all stored objects, without repairing anything.
    Scan(ScanNow),
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
    /// The report of a completed repair sweep.
    Sweep(SweepReport),
    /// The report of a completed health scan.
    Scan(ScanReport),
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

    /// metadata of the file/key to rebuild
    pub metadata: Option<MetaData>,
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
    backend: Addr<BackendManagerActor>,
}

impl ZstorActor {
    /// new
    pub fn new(
        cfg: Addr<ConfigActor>,
        pipeline: Addr<PipelineActor>,
        meta: Addr<MetaStoreActor>,
        metrics: Addr<MetricsActor>,
        backend: Addr<BackendManagerActor>,
    ) -> ZstorActor {
        Self {
            cfg,
            pipeline,
            meta,
            metrics,
            backend,
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
                let meta_writeable = meta.send(CheckWritable).await.unwrap();
                if !meta_writeable {
                    return Err(ZstorError::new_io(
                        "Metastore is not writable".to_string(),
                        std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                    ));
                }

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

                    match meta
                        .send(LoadMeta {
                            path: key_path.clone(),
                        })
                        .await??
                    {
                        Some(stored_metadata)
                            if *stored_metadata.checksum() == *metadata.checksum() =>
                        {
                            debug!(
                                "Skipping {:?} for upload because it's already uploaded",
                                key_path,
                            );
                        }
                        meta_result => {
                            if meta_result.is_some() {
                                debug!("File {:?} changed.", key_path);
                            } else {
                                debug!("Metadata for file {:?} not found.", key_path);
                            }
                            save_data(&mut cfg, shards, &mut metadata).await?;
                            meta.send(SaveMeta {
                                path: key_path,
                                meta: metadata,
                            })
                            .await??;
                        }
                    };

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
                let mut metadata = meta
                    .send(LoadMeta {
                        path: msg.file.clone(),
                    })
                    .await??;
                if metadata.is_none() {
                    // The store pipeline keys metadata by the fully resolved path, so a file
                    // addressed through a symlinked or relative location would never find its
                    // own metadata under the path it was requested with.
                    if let Ok(resolved) = resolve_path(&msg.file).await {
                        if resolved != msg.file {
                            metadata = meta.send(LoadMeta { path: resolved }).await??;
                        }
                    }
                }
                let metadata = metadata.ok_or_else(|| {
                    ZstorError::new_io(
                        "no metadata found for file".to_string(),
                        std::io::Error::from(std::io::ErrorKind::NotFound),
                    )
                })?;

                let shards = load_data(&metadata, 1).await?;

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

                let old_metadata = match (msg.metadata, &msg.file, &msg.key) {
                    (Some(metadata), _, _) => {
                        debug!(
                            "Using provided metadata for rebuild file:{:?} key: {:?}",
                            &msg.file, &msg.key
                        );
                        metadata
                    }
                    (None, Some(file), _) => meta
                        .send(LoadMeta { path: file.clone() })
                        .await??
                        .ok_or_else(|| {
                            ZstorError::new_io(
                                "no metadata found for file".to_string(),
                                std::io::Error::from(std::io::ErrorKind::NotFound),
                            )
                        })?,
                    (None, None, Some(key)) => meta
                        .send(LoadMetaByKey { key: key.clone() })
                        .await??
                        .ok_or_else(|| {
                            ZstorError::new_io(
                                "no metadata found for file".to_string(),
                                std::io::Error::from(std::io::ErrorKind::NotFound),
                            )
                        })?,
                    _ => unreachable!(),
                };

                // load the data from the storage backends
                let input = load_data(&old_metadata, 3).await?;
                let existing_data = input.clone();

                // rebuild the data (in memory only)
                let (mut metadata, shards) = pipeline
                    .send(RebuildData {
                        input,
                        cfg: cfg.clone(),
                        input_meta: old_metadata.clone(),
                    })
                    .await??;

                // build a list of (key, backend used for the shards), indexed by shard
                // position:
                // - if the shard still exists in the backend, we set the backend to the old backend
                // - if the shard is missing (unreachable backend, corrupt data, or never placed
                //   because the object was written degraded), we set the backend to None
                let expected_shards = old_metadata.data_shards() + old_metadata.disposable_shards();
                let mut used_backends = Vec::with_capacity(expected_shards);
                for position in 0..expected_shards {
                    let old_shard = old_metadata
                        .shards()
                        .iter()
                        .find(|si| si.index() == position);
                    let key = old_shard.map(|si| si.key().to_vec()).unwrap_or_default();
                    match (old_shard, existing_data[position].as_ref()) {
                        (Some(si), Some(data)) if data.as_slice() == shards[position].as_ref() => {
                            used_backends.push((key, Some(si.zdb().clone())));
                        }
                        (Some(_), Some(_)) => {
                            error!("Shard {} is DIFFERENT", position);
                            used_backends.push((key, None));
                        }
                        _ => used_backends.push((key, None)),
                    }
                }

                rebuild_data(cfg.deref(), shards, &mut metadata, used_backends).await?;

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
        let backend = self.backend.clone();
        Box::pin(async move {
            let _ = cfg.send(ReloadConfig).await?;
            backend.send(ReloadConfig).await?
        })
    }
}

/// load data from the storage backends
async fn load_data(metadata: &MetaData, max_attempts: u64) -> ZstorResult<Vec<Option<Vec<u8>>>> {
    // attempt to retrieve all shards
    let mut shard_loads: Vec<JoinHandle<(usize, Result<(_, _), ZstorError>)>> =
        Vec::with_capacity(metadata.shards().len());
    for si in metadata.shards() {
        let idx = si.index();
        let key = si.key().to_vec();
        let zdb = si.zdb().clone();
        let chksum = *si.checksum();
        shard_loads.push(tokio::spawn(async move {
            let db = match SequentialZdb::new(zdb).await {
                Ok(ok) => ok,
                Err(e) => return (idx, Err(e.into())),
            };
            match db.get_with_retry(&key, max_attempts).await {
                Ok(potential_shard) => match potential_shard {
                    Some(shard) => (idx, Ok((shard, chksum))),
                    None => (
                        idx,
                        // TODO: Proper error here?
                        Err(ZstorError::new_io(
                            "shard not found".to_string(),
                            std::io::Error::from(std::io::ErrorKind::NotFound),
                        )),
                    ),
                },
                Err(e) => (idx, Err(e.into())),
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

async fn check_backend_space(
    backend: ZdbConnectionInfo,
    shard_len: usize,
) -> ZdbResult<SequentialZdb> {
    let db = SequentialZdb::new(backend.clone()).await?;
    let ns_info = db.ns_info().await?;
    match ns_info.free_space() {
        insufficient if (insufficient as usize) < shard_len => Err(ZdbError::new_storage_size(
            db.connection_info().clone(),
            shard_len,
            ns_info.free_space() as usize,
        )),
        _ => Ok(db),
    }
}

// Find valid backends for the shards
async fn find_valid_backends(
    cfg: &mut Config,
    shard_len: usize,
    needed_backends: usize,
) -> ZstorResult<Vec<SequentialZdb>> {
    loop {
        debug!("Finding backend config");
        let backends = cfg.shard_stores()?;
        let mut failed_shards = 0;
        let mut valid_dbs = Vec::new();

        let handles: Vec<_> = backends
            .into_iter()
            .map(|backend| {
                tokio::spawn(async move { check_backend_space(backend, shard_len).await })
            })
            .collect();

        for result in join_all(handles).await {
            match result? {
                Ok(db) => valid_dbs.push(db),
                Err(e) => {
                    debug!("Backend error: {}", e);
                    cfg.remove_shard(e.remote());
                    failed_shards += 1;
                }
            }
        }

        if valid_dbs.len() >= needed_backends && failed_shards == 0 {
            return Ok(valid_dbs);
        }

        debug!("Backend config failed, retrying...");
    }
}

/// Probe every backend in the config which is not in `used`, and return the healthy ones
/// tagged with their group index, together with the amount of `used` backends per group.
/// The group counts serve as the starting load for [`pick_least_loaded_groups`], so
/// follow-up picks keep the object spread over the configured groups.
async fn probe_spare_backends(
    cfg: &Config,
    shard_len: usize,
    used: &[&ZdbConnectionInfo],
) -> (Vec<(usize, SequentialZdb)>, Vec<usize>) {
    let mut group_load: Vec<usize> = vec![0; cfg.groups().len()];
    let mut candidates = Vec::new();
    for (group_idx, group) in cfg.groups().iter().enumerate() {
        for backend in group.backends() {
            if used.contains(&backend) {
                group_load[group_idx] += 1;
            } else {
                candidates.push((group_idx, backend.clone()));
            }
        }
    }

    let probes = candidates.into_iter().map(|(group_idx, ci)| async move {
        (group_idx, check_backend_space(ci, shard_len).await)
    });
    let mut healthy = Vec::new();
    for (group_idx, result) in join_all(probes).await {
        match result {
            Ok(db) => healthy.push((group_idx, db)),
            Err(e) => debug!("Skipping backend candidate: {}", e),
        }
    }

    (healthy, group_load)
}

/// Find backends to hold rebuilt shards.
///
/// Candidates are all backends in the config which do not already hold a live shard of the
/// object being rebuilt. Every candidate is probed, and the healthy ones are picked in an
/// order which prefers groups holding the fewest live shards of this object, so the rebuilt
/// object keeps its spread over the configured groups. Unlike the write path, this does not
/// require every configured backend to be healthy: as long as enough healthy candidates
/// exist to hold the missing shards, the rebuild can proceed - which is exactly the state a
/// fabric is in after losing a backend while a spare is configured.
async fn find_rebuild_backends(
    cfg: &Config,
    shard_len: usize,
    needed_backends: usize,
    used_backends: &[(Vec<Key>, Option<ZdbConnectionInfo>)],
) -> ZstorResult<Vec<SequentialZdb>> {
    let used: Vec<&ZdbConnectionInfo> = used_backends
        .iter()
        .filter_map(|(_, ci)| ci.as_ref())
        .collect();

    let (healthy, mut group_load) = probe_spare_backends(cfg, shard_len, &used).await;

    if healthy.len() < needed_backends {
        return Err(ZstorError::with_message(
            ZstorErrorKind::Storage,
            format!(
                "cannot rebuild to full redundancy: {} healthy spare backend(s) available, {} needed - provision replacement capacity or restore the missing backend(s)",
                healthy.len(),
                needed_backends
            ),
        ));
    }

    Ok(pick_least_loaded_groups(
        healthy,
        &mut group_load,
        needed_backends,
    ))
}

/// Find backends for a degraded write: as many healthy backends as available, up to
/// `wanted`, spread over the groups, as long as at least `minimum` can be found. Used when
/// the regular placement cannot be satisfied because backends are unreachable or full.
async fn find_degraded_write_backends(
    cfg: &Config,
    shard_len: usize,
    wanted: usize,
    minimum: usize,
) -> ZstorResult<Vec<SequentialZdb>> {
    let (healthy, mut group_load) = probe_spare_backends(cfg, shard_len, &[]).await;

    if healthy.len() < minimum {
        return Err(ZstorError::with_message(
            ZstorErrorKind::Storage,
            format!(
                "insufficient healthy backends even for a degraded write: {} healthy, need at least {} (minimal shards + degraded write margin)",
                healthy.len(),
                minimum
            ),
        ));
    }

    let amount = healthy.len().min(wanted);
    Ok(pick_least_loaded_groups(healthy, &mut group_load, amount))
}

/// Pick `needed` entries out of `candidates`, always taking a candidate from the group with the
/// lowest current load, and counting every pick towards that group's load. This keeps the picks
/// spread over the groups.
///
/// # Panics
///
/// Panics if `candidates` holds fewer than `needed` entries.
fn pick_least_loaded_groups<T>(
    mut candidates: Vec<(usize, T)>,
    group_load: &mut [usize],
    needed: usize,
) -> Vec<T> {
    let mut picked = Vec::with_capacity(needed);
    while picked.len() < needed {
        // Unwrap is safe: candidates holds at least the amount of entries still to be picked.
        let least_loaded = candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, (group_idx, _))| group_load[*group_idx])
            .map(|(idx, _)| idx)
            .unwrap();
        let (group_idx, entry) = candidates.swap_remove(least_loaded);
        group_load[group_idx] += 1;
        picked.push(entry);
    }
    picked
}

async fn rebuild_data(
    cfg: &Config,
    shards: Vec<Shard>,
    metadata: &mut MetaData,
    // used_backends specifies which backends are already used
    // which also means we don't need to check it again and the shard is not missing
    used_backends: Vec<(Vec<Key>, Option<ZdbConnectionInfo>)>,
) -> ZstorResult<()> {
    let shard_len = if shards.is_empty() {
        0
    } else {
        shards[0].len()
    };
    let mut existing_backends_num = 0;
    for (_, ci) in used_backends.iter() {
        if ci.is_some() {
            existing_backends_num += 1;
        }
    }

    let new_dbs = find_rebuild_backends(
        cfg,
        shard_len,
        shards.len() - existing_backends_num,
        &used_backends,
    )
    .await?;

    // create the key,connection_info, and db for the shard
    // - if the backend is already used, we don't need to set the shard
    //    hence the None db
    // - if the backend is not used, we need to set the shard
    //    hence the Some(db) which will be used the set the shard
    let mut new_dbs = new_dbs.into_iter();
    let mut key_dbs = Vec::new();
    for (key, ci) in used_backends {
        match ci {
            Some(ci) => key_dbs.push((key, ci, None)),
            None => {
                // unwrap is safe here because we know we have enough backends from the find_valid_backends
                let db = new_dbs.next().unwrap();
                key_dbs.push((key, db.connection_info().clone(), Some(db)));
            }
        }
    }

    let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(shards.len());
    for ((existing_key, existing_ci, db), (shard_idx, shard)) in
        key_dbs.into_iter().zip(shards.into_iter().enumerate())
    {
        handles.push(tokio::spawn(async move {
            if let Some(db) = db {
                let keys = db.set(&shard).await?;
                Ok(ShardInfo::new(
                    shard_idx,
                    shard.checksum(),
                    keys,
                    db.connection_info().clone(),
                ))
            } else {
                // no need to db.set if it is an already used backend (shard is not missing)
                Ok(ShardInfo::new(
                    shard_idx,
                    shard.checksum(),
                    existing_key.clone(),
                    existing_ci.clone(),
                ))
            }
        }));
    }

    for shard_info in try_join_all(handles).await? {
        metadata.add_shard(shard_info?);
    }

    Ok(())
}

/// Fully resolve a path on the filesystem, even when the file itself does not exist (the
/// parent directory must). This mirrors how the store pipeline canonicalizes the paths it
/// keys metadata under.
async fn resolve_path(path: &Path) -> io::Result<PathBuf> {
    match fs::canonicalize(path).await {
        Ok(resolved) => Ok(resolved),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
                return Err(e);
            };
            Ok(fs::canonicalize(parent).await?.join(name))
        }
        Err(e) => Err(e),
    }
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

    let wanted = shards.len();
    let dbs = match find_valid_backends(cfg, shard_len, wanted).await {
        Ok(dbs) => dbs,
        Err(e) => {
            // A full placement is not possible right now (backends unreachable or full).
            // Rather than refusing the write - which turns one dead backend into a stalled
            // fabric - degrade: place as many shards as healthy backends allow, as long as
            // the data stays recoverable with a margin. The missing shards are backfilled
            // by the repair sweep once capacity returns.
            let minimum = cfg.data_shards() + cfg.degraded_write_margin();
            let dbs = find_degraded_write_backends(cfg, shard_len, wanted, minimum).await?;
            warn!(
                "Degraded write: only {} of {} shards can be placed (full placement failed: {})",
                dbs.len(),
                wanted,
                e
            );
            dbs
        }
    };

    trace!("store shards in backends");

    let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(dbs.len());
    // In a degraded write fewer backends than shards are available; the zip drops the
    // unplaceable tail. Any `minimal_shards` shards recover the data, so which shards are
    // dropped does not matter.
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

#[cfg(test)]
mod tests {
    use super::pick_least_loaded_groups;

    #[test]
    fn picks_spread_over_empty_groups() {
        // Object lost its shard on a backend in group 0; groups 0 and 1 still hold one shard
        // each, group 2 holds none. The replacement should land in group 2.
        let candidates = vec![(0, "spare_g0"), (2, "spare_g2")];
        let mut load = vec![1, 1, 0];
        let picked = pick_least_loaded_groups(candidates, &mut load, 1);
        assert_eq!(picked, vec!["spare_g2"]);
        assert_eq!(load, vec![1, 1, 1]);
    }

    #[test]
    fn picks_count_towards_group_load() {
        // Two picks with two spares in the empty group 1 and one in group 0: the second pick
        // must not land in group 1 again, even though it started out least loaded.
        let candidates = vec![(0, "spare_g0"), (1, "spare_g1_a"), (1, "spare_g1_b")];
        let mut load = vec![1, 0];
        let picked = pick_least_loaded_groups(candidates, &mut load, 2);
        assert!(picked.contains(&"spare_g0"));
        assert_eq!(
            picked
                .iter()
                .filter(|name| name.starts_with("spare_g1"))
                .count(),
            1
        );
        assert_eq!(load, vec![2, 1]);
    }

    #[test]
    fn picks_exhaust_balanced_groups() {
        let candidates = vec![(0, "a"), (0, "b"), (1, "c"), (1, "d")];
        let mut load = vec![0, 0];
        let picked = pick_least_loaded_groups(candidates, &mut load, 4);
        assert_eq!(picked.len(), 4);
        assert_eq!(load, vec![2, 2]);
    }
}
