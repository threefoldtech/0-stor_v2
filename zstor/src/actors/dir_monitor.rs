use crate::actors::{
    config::{ConfigActor, GetConfig},
    zstor::{Check, ZstorActor},
};
use crate::{ZstorError, ZstorResult};
use actix::prelude::*;
use log::{debug, error, info, warn};
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, io};

/// Amount of seconds between every [`CheckDir`] command.
const DIR_MONITOR_INTERVAL_SECONDS: u64 = 60;
/// 1 MiB.
const MIB: u64 = 1 << 20;

/// Actor to keep a directory within a size limit.
pub struct DirMonitorActor {
    cfg: Addr<ConfigActor>,
    zstor: Addr<ZstorActor>,
}

/// Message requesting the actor to reduce the watched directory to the size limit.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckDir;

impl Actor for DirMonitorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            Duration::from_secs(DIR_MONITOR_INTERVAL_SECONDS),
            Self::check_dir,
        );
    }
}

impl DirMonitorActor {
    /// Create a new [`DirMonitorActor`] using the given [`ConfigActor`] and [`ZstorActor`].
    pub fn new(cfg: Addr<ConfigActor>, zstor: Addr<ZstorActor>) -> DirMonitorActor {
        Self { cfg, zstor }
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

            for (path, meta) in entries {
                debug!(
                    "Attempt to delete file {:?} (size: {}, last accessed: {:?})",
                    path,
                    meta.len(),
                    meta.accessed().unwrap()
                );
                let removed = match attempt_removal(&path, zstor.clone()).await {
                    Ok(removed) => removed,
                    Err(e) => {
                        error!("Could not remove file: {}", e);
                        return;
                    }
                };
                if removed {
                    dir_size -= meta.len();
                    if dir_size < size_limit {
                        return;
                    }
                }
            }

            if dir_size < size_limit {
                info!(
                    "Sufficiently reduced data dir ({:?}) size (new size: {})",
                    dir_path, dir_size
                );
            } else {
                warn!(
                    "Failed to reduce dir ({:?}) size to acceptable limit (currently {})",
                    dir_path, dir_size
                );
            }
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

/// Return true if the file was deleted
async fn attempt_removal(path: &Path, zstor: Addr<ZstorActor>) -> ZstorResult<bool> {
    let path = fs::canonicalize(path).await.map_err(|e| {
        ZstorError::new_io(
            "Could not canonicalize path".into(),
            io::Error::new(
                io::ErrorKind::Other,
                format!("Could not canonicalize path: {}", e),
            ),
        )
    })?;
    if zstor.send(Check { path: path.clone() }).await??.is_none() {
        return Ok(false);
    }

    // file is uploaded
    fs::remove_file(&path).await.map(|_| true).map_err(|e| {
        ZstorError::new_io(
            "Could not canonicalize path".into(),
            io::Error::new(
                io::ErrorKind::Other,
                format!("Could not canonicalize path: {}", e),
            ),
        )
    })
}
