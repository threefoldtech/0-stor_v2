use crate::actors::{
    backends::{BackendManagerActor, RequestBackends, StateInterest},
    config::{ConfigActor, GetConfig},
    meta::{MetaStoreActor, ScanMeta},
    metrics::{MetricsActor, SetFabricStats},
    zstor::{Rebuild, ZstorActor},
};
use crate::zdb::ZdbConnectionInfo;
use crate::{ZstorError, ZstorErrorKind};
use actix::prelude::*;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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

/// Message to run a health scan of all objects in the metastore right now. The scan probes
/// the backends referenced by the object metadata and reports degraded objects and the
/// remaining redundancy margin, but never rebuilds anything, so fabric health can be
/// observed regardless of who owns repair scheduling.
#[derive(Debug, Message, Serialize, Deserialize, Clone)]
#[rtype(result = "Result<ScanReport, ZstorError>")]
pub struct ScanNow;

/// The result of a health scan over all objects in the metastore.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScanReport {
    /// Amount of objects inspected during the scan.
    pub objects: u64,
    /// Amount of objects with missing shards or shards on unreachable backends.
    pub degraded: u64,
    /// The redundancy margin of the worst object: how many of its shards can still become
    /// unreachable before the object can no longer be read. Negative when an object is
    /// already unreadable. None when there are no objects.
    pub margin: Option<i64>,
    /// Amount of distinct backends which are referenced by object metadata but absent from
    /// the running config. Their shards are still used for reads, but nothing monitors or
    /// repairs onto these backends anymore.
    pub unconfigured_backends: u64,
    /// Whether a repair sweep was running while this scan ran. Counts may be shifting.
    pub sweep_active: bool,
}

/// The full result of a metastore scan, shared between the repair sweep and the dry health
/// scan.
struct FabricScan {
    objects: u64,
    degraded: u64,
    rebuilt: u64,
    failed: u64,
    /// Worst-object redundancy margin, measured when the object was inspected. During a
    /// repair sweep rebuilds can improve objects after they were measured, so this is a
    /// pessimistic value; the next scan reflects the repairs.
    margin: Option<i64>,
    unconfigured_backends: u64,
}

/// Actor implementation of a repair queue. It periodically sweeps the metastore, verifies all
/// backends holding shards are still reachable, and rebuilds objects for which that is not the
/// case. Sweeps can also be requested on demand with [`SweepNow`].
pub struct RepairActor {
    meta: Addr<MetaStoreActor>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Addr<ZstorActor>,
    cfg: Addr<ConfigActor>,
    metrics: Addr<MetricsActor>,
    sweep_interval: Duration,
    handling_sweep_objects: Arc<AtomicBool>,
    handling_scan_objects: Arc<AtomicBool>,
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
        metrics: Addr<MetricsActor>,
        sweep_interval: Duration,
    ) -> RepairActor {
        Self {
            meta,
            backend_manager,
            zstor,
            cfg,
            metrics,
            sweep_interval,
            handling_sweep_objects: Arc::new(AtomicBool::new(false)),
            handling_scan_objects: Arc::new(AtomicBool::new(false)),
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

/// Scan the metastore for objects with missing shards or shards on unreachable backends.
/// With a zstor address the scan is a repair sweep and rebuilds what it finds; without one
/// it only observes. Backend reachability is probed against the connection info recorded in
/// the object metadata — not against the running config — so shards on backends which were
/// swapped out of the config still count as reachable while those backends live, and a
/// reconfiguration alone never looks like lost redundancy. Every distinct backend is probed
/// once per scan (a batch per metastore page), so dead backends cost one connection attempt
/// per scan instead of one per object referencing them. The caller must hold the relevant
/// guard so no two sweeps run concurrently.
async fn scan_objects(
    meta: Addr<MetaStoreActor>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Option<Addr<ZstorActor>>,
    configured: HashSet<ZdbConnectionInfo>,
) -> Result<FabricScan, ZstorError> {
    let mut scan = FabricScan {
        objects: 0,
        degraded: 0,
        rebuilt: 0,
        failed: 0,
        margin: None,
        unconfigured_backends: 0,
    };
    let mut probed: HashMap<ZdbConnectionInfo, bool> = HashMap::new();
    let mut unconfigured: HashSet<ZdbConnectionInfo> = HashSet::new();

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

        // Probe every backend referenced by this page which has not been probed yet, in one
        // concurrent batch.
        let new_cis: Vec<ZdbConnectionInfo> = metas
            .iter()
            .flat_map(|(_, metadata)| metadata.shards().iter().map(|shard| shard.zdb()))
            .filter(|ci| !probed.contains_key(*ci))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !new_cis.is_empty() {
            let results = backend_manager
                .send(RequestBackends {
                    backend_requests: new_cis.clone(),
                    interest: StateInterest::Readable,
                })
                .await
                .map_err(|e| {
                    ZstorError::with_message(
                        ZstorErrorKind::Storage,
                        format!("failed to request backends: {}", e),
                    )
                })?;
            for (ci, result) in new_cis.into_iter().zip(results) {
                probed.insert(ci, matches!(result, Ok(Some(_))));
            }
        }

        // iterate over the keys and check if all shards are placed and all backends holding
        // them are healthy; if not, the object is degraded (and rebuilt when sweeping)
        for (key, metadata) in metas.into_iter() {
            scan.objects += 1;
            let mut reachable: i64 = 0;
            for shard in metadata.shards() {
                let ci = shard.zdb();
                if probed.get(ci).copied().unwrap_or(false) {
                    reachable += 1;
                }
                if !configured.contains(ci) {
                    unconfigured.insert(ci.clone());
                }
            }
            let object_margin = reachable - metadata.data_shards() as i64;
            scan.margin = Some(match scan.margin {
                Some(margin) => margin.min(object_margin),
                None => object_margin,
            });
            // An object written while backends were down holds fewer shards than intended
            // (degraded write); rebuilding it backfills the missing shards.
            let missing_shards =
                metadata.data_shards() + metadata.disposable_shards() > metadata.shards().len();
            let unreachable_shards = (reachable as usize) < metadata.shards().len();
            if missing_shards || unreachable_shards {
                scan.degraded += 1;
                let Some(zstor) = &zstor else {
                    continue;
                };
                match zstor
                    .send(Rebuild {
                        file: None,
                        key: Some(key.clone()),
                        metadata: Some(metadata),
                    })
                    .await
                {
                    Ok(Ok(())) => {
                        scan.rebuilt += 1;
                        info!("Repaired object {}", key);
                    }
                    Ok(Err(e)) => {
                        scan.failed += 1;
                        error!("Could not repair object {}: {}", key, e);
                    }
                    Err(e) => {
                        scan.failed += 1;
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

    scan.unconfigured_backends = unconfigured.len() as u64;
    Ok(scan)
}

impl FabricScan {
    /// The repair-outcome part of the scan.
    fn sweep_report(&self) -> SweepReport {
        SweepReport {
            objects: self.objects,
            degraded: self.degraded,
            rebuilt: self.rebuilt,
            failed: self.failed,
        }
    }

    /// Push the fabric-health part of the scan to the metrics actor.
    fn push_metrics(&self, metrics: &Addr<MetricsActor>) {
        metrics.do_send(SetFabricStats {
            objects: self.objects,
            degraded: self.degraded,
            margin: self.margin,
            unconfigured_backends: self.unconfigured_backends,
        });
    }
}

/// The set of storage backends in the running config, to compare object metadata against.
async fn configured_backends(
    cfg: &Addr<ConfigActor>,
) -> Result<HashSet<ZdbConnectionInfo>, ZstorError> {
    let config = cfg.send(GetConfig).await.map_err(|e| {
        ZstorError::with_message(
            ZstorErrorKind::Config,
            format!("could not get running config: {}", e),
        )
    })?;
    Ok(config.backends().into_iter().cloned().collect())
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
        let metrics = self.metrics.clone();
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
            let configured = match configured_backends(&cfg).await {
                Ok(configured) => configured,
                Err(e) => {
                    error!("Could not get running config for periodic sweep: {}", e);
                    return;
                }
            };

            if handling_sweep_objects.swap(true, Ordering::Relaxed) {
                info!("Dropping periodic repair sweep - a sweep is already running");
                return;
            }
            let _guard = SweepGuard {
                flag: handling_sweep_objects,
            };

            info!("Starting periodic repair sweep");
            match scan_objects(meta, backend_manager, Some(zstor), configured).await {
                Ok(scan) => {
                    log_sweep_report(&scan.sweep_report());
                    scan.push_metrics(&metrics);
                }
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
        let cfg = self.cfg.clone();
        let metrics = self.metrics.clone();
        let handling_sweep_objects = Arc::clone(&self.handling_sweep_objects);

        Box::pin(async move {
            let configured = configured_backends(&cfg).await?;
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
            let scan = scan_objects(meta, backend_manager, Some(zstor), configured).await?;
            let report = scan.sweep_report();
            log_sweep_report(&report);
            scan.push_metrics(&metrics);
            Ok(report)
        })
    }
}

impl Handler<ScanNow> for RepairActor {
    type Result = ResponseFuture<Result<ScanReport, ZstorError>>;

    fn handle(&mut self, _: ScanNow, _: &mut Self::Context) -> Self::Result {
        let meta = self.meta.clone();
        let backend_manager = self.backend_manager.clone();
        let cfg = self.cfg.clone();
        let metrics = self.metrics.clone();
        let handling_scan_objects = Arc::clone(&self.handling_scan_objects);
        let handling_sweep_objects = Arc::clone(&self.handling_sweep_objects);

        Box::pin(async move {
            let configured = configured_backends(&cfg).await?;
            if handling_scan_objects.swap(true, Ordering::Relaxed) {
                return Err(ZstorError::with_message(
                    ZstorErrorKind::Storage,
                    "a health scan is already running".to_string(),
                ));
            }
            let _guard = SweepGuard {
                flag: handling_scan_objects,
            };

            // A scan may run next to a sweep: it only reads, and refusing would make health
            // unobservable exactly while repair is busy. The flag tells the caller the
            // numbers may be shifting under a concurrent sweep.
            let sweep_active = handling_sweep_objects.load(Ordering::Relaxed);
            debug!("Starting requested health scan");
            let scan = scan_objects(meta, backend_manager, None, configured).await?;
            scan.push_metrics(&metrics);
            Ok(ScanReport {
                objects: scan.objects,
                degraded: scan.degraded,
                margin: scan.margin,
                unconfigured_backends: scan.unconfigured_backends,
                sweep_active,
            })
        })
    }
}
