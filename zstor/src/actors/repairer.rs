use crate::actors::{
    backends::{BackendManagerActor, RequestBackends, StateInterest},
    config::{ConfigActor, GetConfig},
    meta::{MetaStoreActor, ScanMeta},
    zstor::{Rebuild, ZstorActor},
};
use crate::{ZstorError, ZstorErrorKind};
use actix::prelude::*;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Message)]
#[rtype(result = "()")]
/// Message to run a periodic sweep of all objects in the metastore, if unattended repair is
/// enabled in the running config. If one or more backends holding shards of an object are not
/// reachable, the object is repaired.
struct PeriodicSweep;

/// Message to run a sweep of all objects in the metastore right now, regardless of whether
/// unattended repair is enabled. This allows an external system which owns repair scheduling
/// to trigger a sweep on demand.
#[derive(Debug, Message, Serialize, Deserialize, Clone)]
#[rtype(result = "Result<SweepReport, ZstorError>")]
pub struct SweepNow;

/// The result of a repair sweep over all objects in the metastore.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SweepReport {
    /// Amount of objects inspected during the sweep.
    pub objects: u64,
    /// Amount of objects found with one or more shards on unreachable backends.
    pub degraded: u64,
    /// Amount of degraded objects successfully rebuilt onto healthy backends.
    pub rebuilt: u64,
    /// Amount of degraded objects which could not be rebuilt. Details are in the log.
    pub failed: u64,
}

/// Actor implementation of a repair queue. It periodically sweeps the metastore, verifies all
/// backends holding shards are still reachable, and rebuilds objects for which that is not the
/// case. Sweeps can also be requested on demand with [`SweepNow`].
pub struct RepairActor {
    meta: Addr<MetaStoreActor>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Addr<ZstorActor>,
    cfg: Addr<ConfigActor>,
    sweep_interval: Duration,
    handling_sweep_objects: Arc<AtomicBool>,
}

impl RepairActor {
    /// Create a new [`RepairActor`] checking objects in the provided metastore and using the given
    /// zstor to repair them if needed. The sweep interval controls how often automatic sweeps
    /// run; whether they run at all is read from the running config at every tick, so a config
    /// reload can enable or disable unattended repair without a restart.
    pub fn new(
        meta: Addr<MetaStoreActor>,
        backend_manager: Addr<BackendManagerActor>,
        zstor: Addr<ZstorActor>,
        cfg: Addr<ConfigActor>,
        sweep_interval: Duration,
    ) -> RepairActor {
        Self {
            meta,
            backend_manager,
            zstor,
            cfg,
            sweep_interval,
            handling_sweep_objects: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Send a [`PeriodicSweep`] command to the actor.
    fn periodic_sweep(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(PeriodicSweep);
    }
}

impl Actor for RepairActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(self.sweep_interval, Self::periodic_sweep);
    }
}

struct SweepGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for SweepGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Relaxed);
    }
}

/// Sweep the metastore for objects with shards on unreachable backends and rebuild them. The
/// caller must hold the sweep guard so no two sweeps run concurrently.
async fn sweep_objects(
    meta: Addr<MetaStoreActor>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Addr<ZstorActor>,
) -> Result<SweepReport, ZstorError> {
    let mut report = SweepReport {
        objects: 0,
        degraded: 0,
        rebuilt: 0,
        failed: 0,
    };

    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the unix epoch")
        .as_secs();

    // start scanning from the beginning (cursor == None) and let the metastore choose the
    // backend_id
    let mut cursor = None;
    let mut backend_idx = None;
    loop {
        // scan keys from the metastore
        let (idx, new_cursor, metas) = meta
            .send(ScanMeta {
                cursor: cursor.clone(),
                backend_idx,
                max_timestamp: Some(start_time),
            })
            .await
            .map_err(|e| {
                ZstorError::with_message(
                    ZstorErrorKind::Metadata,
                    format!("could not request meta keys from metastore: {}", e),
                )
            })??;

        // iterate over the keys and check if the backends are healthy
        // if not, rebuild the object
        for (key, metadata) in metas.into_iter() {
            report.objects += 1;
            let backend_requests = metadata
                .shards()
                .iter()
                .map(|shard_info| shard_info.zdb())
                .cloned()
                .collect::<Vec<_>>();
            let backends = backend_manager
                .send(RequestBackends {
                    backend_requests,
                    interest: StateInterest::Readable,
                })
                .await
                .map_err(|e| {
                    ZstorError::with_message(
                        ZstorErrorKind::Storage,
                        format!("failed to request backends: {}", e),
                    )
                })?;
            let must_rebuild = backends.into_iter().any(|b| !matches!(b, Ok(Some(_))));
            if must_rebuild {
                report.degraded += 1;
                match zstor
                    .send(Rebuild {
                        file: None,
                        key: Some(key.clone()),
                        metadata: Some(metadata),
                    })
                    .await
                {
                    Ok(Ok(())) => {
                        report.rebuilt += 1;
                        info!("Repaired object {}", key);
                    }
                    Ok(Err(e)) => {
                        report.failed += 1;
                        error!("Could not repair object {}: {}", key, e);
                    }
                    Err(e) => {
                        report.failed += 1;
                        error!("Could not deliver repair command for object {}: {}", key, e);
                    }
                }
            }
        }

        if new_cursor.is_none() {
            debug!("there is no more old data to rebuild");
            break;
        }

        cursor = new_cursor;
        backend_idx = Some(idx);
    }

    Ok(report)
}

/// Log the outcome of a finished sweep at a level matching its severity.
fn log_sweep_report(report: &SweepReport) {
    if report.failed > 0 {
        warn!(
            "Repair sweep finished with failures: {} objects checked, {} degraded, {} rebuilt, {} FAILED - the failed objects are still missing shards",
            report.objects, report.degraded, report.rebuilt, report.failed
        );
    } else {
        info!(
            "Repair sweep finished: {} objects checked, {} degraded, {} rebuilt",
            report.objects, report.degraded, report.rebuilt
        );
    }
}

impl Handler<PeriodicSweep> for RepairActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: PeriodicSweep, _: &mut Self::Context) -> Self::Result {
        let meta = self.meta.clone();
        let backend_manager = self.backend_manager.clone();
        let zstor = self.zstor.clone();
        let cfg = self.cfg.clone();
        let handling_sweep_objects = Arc::clone(&self.handling_sweep_objects);

        Box::pin(async move {
            match cfg.send(GetConfig).await {
                Ok(config) => {
                    if !config.unattended_repair() {
                        debug!("Skipping periodic repair sweep - unattended repair is disabled");
                        return;
                    }
                }
                Err(e) => {
                    error!("Could not get running config for periodic sweep: {}", e);
                    return;
                }
            }

            if handling_sweep_objects.swap(true, Ordering::Relaxed) {
                info!("Dropping periodic repair sweep - a sweep is already running");
                return;
            }
            let _guard = SweepGuard {
                flag: handling_sweep_objects,
            };

            info!("Starting periodic repair sweep");
            match sweep_objects(meta, backend_manager, zstor).await {
                Ok(report) => log_sweep_report(&report),
                Err(e) => error!("Repair sweep aborted: {}", e),
            }
        })
    }
}

impl Handler<SweepNow> for RepairActor {
    type Result = ResponseFuture<Result<SweepReport, ZstorError>>;

    fn handle(&mut self, _: SweepNow, _: &mut Self::Context) -> Self::Result {
        let meta = self.meta.clone();
        let backend_manager = self.backend_manager.clone();
        let zstor = self.zstor.clone();
        let handling_sweep_objects = Arc::clone(&self.handling_sweep_objects);

        Box::pin(async move {
            if handling_sweep_objects.swap(true, Ordering::Relaxed) {
                return Err(ZstorError::with_message(
                    ZstorErrorKind::Storage,
                    "a repair sweep is already running".to_string(),
                ));
            }
            let _guard = SweepGuard {
                flag: handling_sweep_objects,
            };

            info!("Starting requested repair sweep");
            let report = sweep_objects(meta, backend_manager, zstor).await?;
            log_sweep_report(&report);
            Ok(report)
        })
    }
}
