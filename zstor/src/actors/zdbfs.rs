use crate::actors::metrics::{MetricsActor, UpdateZdbFsStats};
use crate::zdbfs::ZdbFsStats;
use actix::prelude::*;
use log::{error, warn};
use std::time::Duration;

const DEFAULT_ZDBFS_STATS_INTERVAL_SECS: u64 = 1;

/// An actor implementation of a periodic 0-db-fs statistic getter.
pub struct ZdbFsStatsActor {
    stats: ZdbFsStats,
    metrics: Addr<MetricsActor>,
}

impl ZdbFsStatsActor {
    /// Create a new ZdbFsStatsActor, which will get statistics and push them to the metrics actor
    /// at a given interval.
    pub fn new(stats: ZdbFsStats, metrics: Addr<MetricsActor>) -> Self {
        Self { stats, metrics }
    }

    fn get_stats(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(GetStats);
    }
}

/// Message requesting the actor to get the statistics of the 0-db-fs.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct GetStats;

impl Actor for ZdbFsStatsActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            Duration::from_secs(DEFAULT_ZDBFS_STATS_INTERVAL_SECS),
            Self::get_stats,
        );
    }
}

impl Handler<GetStats> for ZdbFsStatsActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: GetStats, _: &mut Self::Context) -> Self::Result {
        let res = self.stats.get_stats();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            match res {
                Err(e) => warn!("Could not get 0-db-fs stats: {}", e),
                Ok(stats) => {
                    if let Err(e) = metrics.send(UpdateZdbFsStats { stats }).await {
                        error!("Could not update 0-db-fs stats: {}", e);
                    };
                }
            }
        })
    }
}
