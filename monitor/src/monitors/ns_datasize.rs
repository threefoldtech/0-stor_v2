use crate::config::Config;
use crate::MonitorResult;
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time::interval;

const BACKEND_MONITOR_INTERVAL_DURATION: u64 = 60; // 60 seconds => 1 minute
const BACKEND_MONITOR_INTERVAL: Duration = Duration::from_secs(BACKEND_MONITOR_INTERVAL_DURATION);
const MI_B: u64 = 1 << 20;

/// Monitor a data file dir and attempt to keep it within a certain size.
///
/// Returns None if the config does not have a limit set, otherwise returns the JoinHandle for
/// the actual monitoring task.
pub async fn monitor_ns_datasize(
    mut rx: Receiver<()>,
    ns: String,
    config: Config,
) -> Option<JoinHandle<MonitorResult<()>>> {
    let size_limit = match config.max_zdb_data_dir_size() {
        None => {
            warn!("No size limit set in config, abort monitoring data dir size");
            return None;
        }
        Some(limit) => limit as u64 * MI_B,
    };
    let mut dir_path = config.zdb_data_dir_path().clone();
    dir_path.push(ns);

    Some(tokio::spawn(async move {
        // steal the backend monitor interval for now, TODO
        let mut ticker = interval(BACKEND_MONITOR_INTERVAL);

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down failed write monitor");
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("checking data dir size");

                    let mut entries = match crate::get_dir_entries(&dir_path).await {
                        Ok(entries) => entries,
                        Err(e) => {
                            error!("Couldn't get directory entries for {:?}: {}", dir_path, e);
                            continue;
                        }
                    };

                    let mut dir_size: u64 = entries.iter().map(|(_, meta)| meta.len()).sum();

                    if dir_size < size_limit {
                        debug!("Directory {:?} is within size limits ({} < {})", dir_path, dir_size, size_limit);
                        continue
                    }

                    info!("Data dir size is too large, try to delete");
                    // sort files based on access time
                    // small -> large i.e. accessed the longest ago first
                    // unwraps are safe since the error only occurs if the platform does not
                    // support `atime`, and we don't implicitly support this functionality on
                    // those platforms.
                    entries.sort_by(|(_, meta_1), (_, meta_2)| meta_1.accessed().unwrap().cmp(&meta_2.accessed().unwrap()));

                    for (path, meta) in entries {
                        debug!("Attempt to delete file {:?} (size: {}, last accessed: {:?})", path, meta.len(), meta.accessed().unwrap());
                        let removed = match crate::attempt_removal(&path, &config).await {
                            Ok(removed) => removed,
                            Err(e) => {
                                error!("Could not remove file: {}", e);
                                continue
                            },
                        };
                        if removed {
                            dir_size -= meta.len();
                            if dir_size < size_limit {
                                break
                            }
                        }
                    }

                    if dir_size < size_limit {
                        info!("Sufficiently reduced data dir ({:?}) size (new size: {})",dir_path ,dir_size);
                    } else {
                        warn!("Failed to reduce dir ({:?}) size to acceptable limit (currently {})", dir_path, dir_size);
                    }

                }
            }
        }
    }))
}
