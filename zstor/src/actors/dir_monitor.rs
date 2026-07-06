use crate::actors::{
    config::{ConfigActor, GetConfig},
    metrics::{MetricsActor, SetDataDirStats},
    zstor::{Check, ZstorActor},
};
use crate::meta::{Checksum, CHECKSUM_LENGTH};
use crate::{ZstorError, ZstorResult};
use actix::prelude::*;
use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use log::{debug, error, info, warn};
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tokio::{fs, io};

/// 1 MiB.
const MIB: u64 = 1 << 20;

/// Actor to keep a directory within a size limit.
pub struct DirMonitorActor {
    cfg: Addr<ConfigActor>,
    zstor: Addr<ZstorActor>,
    metrics: Addr<MetricsActor>,
    interval: Duration,
}

/// Message requesting the actor to reduce the watched directory to the size limit.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckDir;

/// The result of a single eviction attempt.
enum RemovalOutcome {
    /// The file was evicted.
    Removed,
    /// The file must stay, with the reason why.
    Kept(&'static str),
    /// The file disappeared while it was being checked, so there is nothing left to evict.
    Vanished,
}

impl Actor for DirMonitorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Check immediately at startup: if the daemon comes (back) up under disk pressure,
        // eviction should not wait a full interval before reacting.
        ctx.notify(CheckDir);
        ctx.run_interval(self.interval, Self::check_dir);
    }
}

impl DirMonitorActor {
    /// Create a new [`DirMonitorActor`] using the given [`ConfigActor`], [`ZstorActor`] and
    /// [`MetricsActor`], checking the directory on the given interval.
    pub fn new(
        cfg: Addr<ConfigActor>,
        zstor: Addr<ZstorActor>,
        metrics: Addr<MetricsActor>,
        interval: Duration,
    ) -> DirMonitorActor {
        Self {
            cfg,
            zstor,
            metrics,
            interval,
        }
    }

    /// Trigger the directory check action.
    fn check_dir(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckDir)
    }
}

impl Handler<CheckDir> for DirMonitorActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: CheckDir, _: &mut Self::Context) -> Self::Result {
        let cfg_addr = self.cfg.clone();
        let zstor = self.zstor.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let cfg = match cfg_addr.send(GetConfig).await {
                Err(e) => {
                    error!("Failed to get currently active config: {}", e);
                    return;
                }
                Ok(cfg) => cfg,
            };
            if !(cfg.zdb_data_dir_path().is_some() && cfg.max_zdb_data_dir_size().is_some()) {
                return;
            }

            // Unwrap here is safe as we just checked that both are set.
            let dir_path = cfg.zdb_data_dir_path().unwrap();
            let size_limit = cfg.max_zdb_data_dir_size().unwrap() * MIB;

            debug!("checking data dir size");

            let mut entries = match get_dir_entries(dir_path).await {
                Ok(entries) => entries,
                Err(e) => {
                    error!("Couldn't get directory entries for {:?}: {}", dir_path, e);
                    return;
                }
            };

            let mut dir_size: u64 = entries.iter().map(|(_, meta)| meta.len()).sum();

            if dir_size < size_limit {
                debug!(
                    "Directory {:?} is within size limits ({} < {})",
                    dir_path, dir_size, size_limit
                );
                metrics.do_send(SetDataDirStats {
                    size_bytes: dir_size,
                    limit_bytes: size_limit,
                    evicted: 0,
                    failures: 0,
                });
                return;
            }

            info!("Data dir size is too large, try to delete");
            // sort files based on access time
            // small -> large i.e. accessed the longest ago first
            // unwraps are safe since the error only occurs if the platform does not
            // support `atime`, and we don't implicitly support this functionality on
            // those platforms.
            entries.sort_by(|(_, meta_1), (_, meta_2)| {
                meta_1.accessed().unwrap().cmp(&meta_2.accessed().unwrap())
            });

            let mut evicted: u64 = 0;
            let mut kept: u64 = 0;
            let mut failures: u64 = 0;
            for (path, meta) in entries {
                if dir_size < size_limit {
                    break;
                }
                debug!(
                    "Attempt to delete file {:?} (size: {}, last accessed: {:?})",
                    path,
                    meta.len(),
                    meta.accessed().unwrap()
                );
                match attempt_removal(&path, zstor.clone()).await {
                    Ok(RemovalOutcome::Removed) => {
                        evicted += 1;
                        dir_size = dir_size.saturating_sub(meta.len());
                    }
                    // The file is gone, so it no longer takes up space in the dir.
                    Ok(RemovalOutcome::Vanished) => {
                        dir_size = dir_size.saturating_sub(meta.len());
                    }
                    Ok(RemovalOutcome::Kept(reason)) => {
                        kept += 1;
                        debug!("Keeping file {:?}: {}", path, reason);
                    }
                    // A failure on one file must not starve the rest of the pass.
                    Err(e) => {
                        failures += 1;
                        error!("Could not evict file {:?}: {}", path, e);
                    }
                }
            }

            if dir_size < size_limit {
                info!(
                    "Sufficiently reduced data dir ({:?}) size (new size: {}, {} files evicted)",
                    dir_path, dir_size, evicted
                );
            } else {
                warn!(
                    "Data dir ({:?}) is still over its size limit after an eviction pass ({} >= {}): {} evicted, {} kept (not or not verifiably dispersed), {} failed — the disk will keep filling unless dispersal catches up or the limit is raised",
                    dir_path, dir_size, size_limit, evicted, kept, failures
                );
            }
            metrics.do_send(SetDataDirStats {
                size_bytes: dir_size,
                limit_bytes: size_limit,
                evicted,
                failures,
            });
        })
    }
}

/// Get all files in a directory. Only files are included, everything else is excluded. This
/// function does not recurse.
async fn get_dir_entries(path: &Path) -> io::Result<Vec<(PathBuf, Metadata)>> {
    let dir_meta = fs::metadata(&path).await?;
    if !dir_meta.is_dir() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    let mut entries = fs::read_dir(&path).await?;
    let mut file_entries = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        // failure to get one files metadata will be considered fatal
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }
        file_entries.push((entry.path(), meta));
    }

    Ok(file_entries)
}

/// Attempt to evict a single file. A file is only deleted when the metastore confirms that
/// exactly this content has been dispersed, so an eviction can always be undone by a retrieve.
async fn attempt_removal(path: &Path, zstor: Addr<ZstorActor>) -> ZstorResult<RemovalOutcome> {
    // The metastore keys files by the lexically cleaned path they were stored with, so query
    // that form first. Resolving the path on the filesystem instead (as older versions did)
    // breaks the lookup for every file stored through a symlinked location: the store side
    // never resolves symlinks, so the eviction side must not either.
    let clean_path = crate::meta::canonicalize(path)
        .map_err(|e| ZstorError::new_io("Could not clean path".into(), e))?;
    let mut stored_checksum = zstor
        .send(Check {
            path: clean_path.clone(),
        })
        .await??;
    if stored_checksum.is_none() {
        // Fall back to the fully resolved path, in case the file was stored through a
        // different spelling of the same location.
        match fs::canonicalize(path).await {
            Ok(resolved) => {
                if resolved != clean_path {
                    stored_checksum = zstor.send(Check { path: resolved }).await??;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(RemovalOutcome::Vanished),
            Err(e) => return Err(ZstorError::new_io("Could not canonicalize path".into(), e)),
        }
    }
    let Some(expected) = stored_checksum else {
        return Ok(RemovalOutcome::Kept("file is not (yet) dispersed"));
    };

    // Never delete content that doesn't match the dispersed copy: a retrieve would restore
    // different data than what is on disk now.
    match file_checksum(path).await {
        Ok(actual) if actual == expected => {}
        Ok(_) => {
            warn!(
                "Not evicting {:?}: local content diverged from the dispersed copy",
                path
            );
            return Ok(RemovalOutcome::Kept(
                "local content diverged from the dispersed copy",
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(RemovalOutcome::Vanished),
        Err(e) => return Err(ZstorError::new_io("Could not checksum file".into(), e)),
    }

    match fs::remove_file(path).await {
        Ok(()) => Ok(RemovalOutcome::Removed),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(RemovalOutcome::Vanished),
        Err(e) => Err(ZstorError::new_io("Could not remove file".into(), e)),
    }
}

/// Get a 16 byte blake2b checksum of a file, the same checksum the store pipeline records in
/// the metadata.
async fn file_checksum(path: &Path) -> io::Result<Checksum> {
    let mut file = fs::File::open(path).await?;
    // The unwrap here is safe since we know that 16 is a valid output size.
    let mut hasher = Blake2bVar::new(CHECKSUM_LENGTH).unwrap();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    // The expect is safe due to the static size, which is known to be valid.
    Ok(hasher
        .finalize_boxed()
        .as_ref()
        .try_into()
        .expect("Invalid hash size returned"))
}
