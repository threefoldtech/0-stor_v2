use crate::actors::metrics::{MetricsActor, UpdateZdbFsStats};
use crate::zdbfs::ZdbFsStats;
use crate::ZstorError;
use actix::prelude::*;
use log::debug;
use log::warn;
use std::path::PathBuf;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::time::sleep;
// use thread::{sleep};
const DEFAULT_ZDBFS_STATS_INTERVAL_SECS: u64 = 1;

/// An actor implementation of a periodic 0-db-fs statistic getter.
pub struct ZdbFsStatsActor {
    stats: Option<ZdbFsStats>,
}
impl Default for ZdbFsStatsActor {
    fn default() -> Self {
        ZdbFsStatsActor::new()
    }
}
impl ZdbFsStatsActor {
    /// Create a new ZdbFsStatsActor, which will get statistics and push them to the metrics actor
    /// at a given interval.
    pub fn new() -> Self {
        ZdbFsStatsActor { stats: None }
    }
    fn renew_fd(&mut self, path: PathBuf) {
        debug!("renewing stats fd");
        let zdbfs_stats = ZdbFsStats::try_new(path)
            .map_err(|e| ZstorError::new_io("Could not get 0-db-fs stats".into(), e));
        match zdbfs_stats {
            Ok(v) => {
                self.stats = Some(v);
            }
            Err(e) => {
                warn!("couldn't get a new zdbbfs stats {}", e);
                self.stats = None;
            }
        }
    }
}

/// Send stats to the metrics endpoint periodically
pub struct LoopOverStats {
    /// Path buffer to zdbfs mountpoint
    pub buf: PathBuf,
    /// Metrics actor address
    pub metrics: Addr<MetricsActor>,
}

impl Actor for ZdbFsStatsActor {
    type Context = SyncContext<Self>;
}
impl Message for LoopOverStats {
    type Result = Result<u64, ()>;
}

impl Handler<LoopOverStats> for ZdbFsStatsActor {
    type Result = Result<u64, ()>;
    fn handle(&mut self, msg: LoopOverStats, _: &mut Self::Context) -> Self::Result {
        let rt = Runtime::new().unwrap();
        loop {
            match &self.stats {
                None => self.renew_fd(msg.buf.clone()),
                Some(stats_obj) => {
                    let res = stats_obj.get_stats();
                    let metrics = msg.metrics.clone();
                    match res {
                        Err(ref e) => {
                            warn!("Could not get 0-db-fs stats: {}", e);
                            self.stats = None;
                        }
                        Ok(stats) => {
                            rt.block_on(async move {
                                if let Err(e) = metrics.send(UpdateZdbFsStats { stats }).await {
                                    warn!("Could not update 0-db-fs stats: {}", e);
                                };
                            });
                        }
                    }
                }
            }
            // would enter runtime work?
            rt.block_on(async move {
                sleep(Duration::from_secs(DEFAULT_ZDBFS_STATS_INTERVAL_SECS)).await;
            });
        }
    }
}
