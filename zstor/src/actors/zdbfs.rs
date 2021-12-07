use crate::actors::metrics::{MetricsActor, UpdateZdbFsStats};
use crate::zdbfs::ZdbFsStats;
use crate::ZstorError;
use actix::prelude::*;
use log::debug;
use log::warn;
use std::path::PathBuf;
use tokio::time;
// use thread::{sleep};
const DEFAULT_ZDBFS_STATS_INTERVAL_SECS: u64 = 1;
/// An async zdbfs stats actor for the sync actor
pub struct ZdbFsStatsProxyActor {
    /// Path buffer to zdbfs mountpoint
    pub buf: PathBuf,
    /// Metrics actor address
    pub metrics: Addr<MetricsActor>,
    /// ZdbFs stats actor address
    stats_actor: Addr<ZdbFsStatsActor>,
}

impl ZdbFsStatsProxyActor {
    /// Create a new zdbfs stats proxy actor
    pub fn new(buf: PathBuf, metrics: Addr<MetricsActor>) -> Self {
        let stats_actor = SyncArbiter::start(1, ZdbFsStatsActor::new);
        ZdbFsStatsProxyActor {
            buf,
            metrics,
            stats_actor,
        }
    }
    fn probe_stats(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(ProbeStatsActor);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
/// Probes the sync actor to process metrics
struct ProbeStatsActor;

impl Actor for ZdbFsStatsProxyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            time::Duration::from_secs(DEFAULT_ZDBFS_STATS_INTERVAL_SECS),
            Self::probe_stats,
        );
    }
}

impl Handler<ProbeStatsActor> for ZdbFsStatsProxyActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: ProbeStatsActor, _: &mut Self::Context) -> Self::Result {
        let buf = self.buf.clone();
        let metrics = self.metrics.clone();
        let stats_actor = self.stats_actor.clone();
        Box::pin(async move {
            if let Err(e) = stats_actor
                .send(ProcessStats {
                    buf: buf.clone(),
                    metrics: metrics.clone(),
                })
                .await
            {
                warn!("couldn't process zdbfs stats: {}", e);
            }
        })
    }
}

/// An actor implementation of a periodic 0-db-fs statistic getter.
struct ZdbFsStatsActor {
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
    fn new() -> Self {
        ZdbFsStatsActor { stats: None }
    }
    fn renew_fd(&mut self, path: PathBuf) -> Result<(), ZstorError> {
        debug!("renewing stats fd");
        let zdbfs_stats = ZdbFsStats::try_new(path)
            .map_err(|e| ZstorError::new_io("Could not get 0-db-fs stats fd".into(), e));
        match zdbfs_stats {
            Ok(v) => {
                self.stats = Some(v);
                Ok(())
            }
            Err(e) => {
                self.stats = None;
                Err(e)
            }
        }
    }
}

/// Send stats to the metrics endpoint periodically
struct ProcessStats {
    /// Path buffer to zdbfs mountpoint
    buf: PathBuf,
    /// Metrics actor address
    metrics: Addr<MetricsActor>,
}

impl Actor for ZdbFsStatsActor {
    type Context = SyncContext<Self>;
}

impl Message for ProcessStats {
    type Result = Result<(), ZstorError>;
}

impl Handler<ProcessStats> for ZdbFsStatsActor {
    type Result = Result<(), ZstorError>;
    fn handle(&mut self, msg: ProcessStats, _: &mut Self::Context) -> Self::Result {
        if self.stats.is_none() {
            self.renew_fd(msg.buf.clone())?;
        }
        match self
            .stats
            .as_ref()
            .unwrap()
            .get_stats()
            .map_err(|e| ZstorError::new_io("Could not get 0-db-fs stats".into(), e))
        {
            Err(e) => {
                self.stats = None;
                Err(e)
            }
            Ok(stats) => {
                msg.metrics.do_send(UpdateZdbFsStats { stats });
                Ok(())
            }
        }
    }
}
