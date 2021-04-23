use crate::backend::BackendState;
use crate::config::Config;
use crate::zstor::SingleZstor;
use crate::MonitorResult;
use futures::future::join_all;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;
use tokio::time::interval;
use zstor_v2::config::Meta;
use zstor_v2::etcd::Etcd;
use zstor_v2::meta::MetaStore;
use zstor_v2::zdb::{SequentialZdb, ZdbConnectionInfo};
use zstor_v2::ZstorError;

const BACKEND_MONITOR_INTERVAL_DURATION: u64 = 60; // 60 seconds => 1 minute
const BACKEND_MONITOR_INTERVAL: Duration = Duration::from_secs(BACKEND_MONITOR_INTERVAL_DURATION);
const REPAIR_BACKLOG_RETRY_INTERVAL_DURATION: u64 = 60 * 5; // 5 minutes
const REPAIR_BACKLOG_RETRY_INTERVAL: Duration =
    Duration::from_secs(REPAIR_BACKLOG_RETRY_INTERVAL_DURATION);
const MAX_CONCURRENT_CONNECTIONS: usize = 10;

pub async fn monitor_backends(
    mut rx: Receiver<()>,
    zstor: SingleZstor,
    config: Config,
) -> JoinHandle<MonitorResult<()>> {
    let (repairer_tx, repairer_rx) = unbounded_channel::<String>();
    let repair_handle = spawn_repairer(repairer_rx, zstor).await;

    tokio::spawn(async move {
        let mut ticker = interval(BACKEND_MONITOR_INTERVAL);
        let mut backends = HashMap::<ZdbConnectionInfo, BackendState>::new();

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down backend monitor");
                    info!("waiting for repair queue shutdown");
                    drop(repairer_tx);
                    if let Err(e) = repair_handle.await {
                        error!("Error detected in repair queue shutdown: {}", e);
                    } else {
                        info!("repair queue shutdown completed");
                    }
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("Checking health of known backends");
                    let zstor_config = match crate::read_zstor_config(config.zstor_config_path()).await {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            error!("could not read zstor config: {}", e);
                            continue
                        }
                    };

                   for backend in zstor_config.backends() {
                       backends
                           .entry(backend.clone())
                           .or_insert_with(|| BackendState::Unknown(std::time::Instant::now()));
                   }

                    let keys = backends.keys().into_iter().cloned().collect::<Vec<_>>();
                    for backend_group in keys.chunks(MAX_CONCURRENT_CONNECTIONS) {
                        let mut futs: Vec<JoinHandle<Result<_,(_,ZstorError)>>> = Vec::with_capacity(backend_group.len());
                        for backend in backend_group {
                            let backend = backend.clone();
                            futs.push(tokio::spawn(async move{
                                // connect to backend and get size
                                let ns_info = SequentialZdb::new(backend.clone())
                                    .await
                                    .map_err(|e| (backend.clone(), e.into()))?
                                    .ns_info()
                                    .await
                                    .map_err(|e| (backend.clone(), e.into()))?;

                                Ok((backend, ns_info))

                            }));
                        }
                        for result in join_all(futs).await {
                            let (backend, info) = match result? {
                                Ok(succes) => succes,
                                Err((backend, e)) => {
                                    warn!("backend {} can not be reached {}", backend.address(), e);
                                    backends.entry(backend).and_modify(BackendState::mark_unreachable);
                                    continue;
                                }
                            };

                            if info.data_usage_percentage() > config.zdb_namespace_fill_treshold() {
                                warn!("backend {} has a high fill rate ({}%)", backend.address(), info.data_usage_percentage());
                                backends.entry(backend).and_modify(|bs| bs.mark_lowspace(info.data_usage_percentage()));
                            } else {
                                debug!("backend {} is healthy!", backend.address());
                                backends.entry(backend).and_modify(BackendState::mark_healthy);
                            }
                        }
                    }

                    let mut cluster = match zstor_config.meta() {
                        Meta::Etcd(etcdconf) => match Etcd::new(etcdconf, zstor_config.virtual_root().clone()).await {
                            Ok(cluster) => cluster,
                            Err(e) => {error!("could not create metadata cluster: {}", e); continue},
                        },
                    };

                    debug!("verifying objects to repair");
                    for (key, meta) in cluster.object_metas().await? {
                        let mut should_repair = false;
                        for shard in meta.shards() {
                            should_repair = match backends.get(shard.zdb()) {
                                Some(state) if !state.is_readable() => true,
                                // backend is readable, nothing to do
                                Some(_) => {continue},
                                None => {
                                    warn!("Object {} has shard on {} which has unknown state", key, shard.zdb().address());
                                    continue
                                },
                            };
                        }
                        if should_repair {
                            // unwrapping here is safe as an error would indicate a programming
                            // logic error
                            debug!("Requesting repair of object {}", key);
                            repairer_tx.send(key).unwrap();
                        }
                    }

                    if let Some(vdc_config) = config.vdc_config() {
                        debug!("attempt to replace unwriteable data backends if needed");
                        let vdc_client = reqwest::Client::new();
                        for backend in backends.keys() {
                            if cluster.is_replaced(backend).await? {
                                continue
                            }
                            if !backends[backend].is_writeable() {
                                debug!("attempt to replace backend {}", backend.address());
                                // replace backend
                                let res = match vdc_client.post(format!("{}/api/controller/zdb/add", vdc_config.url()))
                                    .json(&VdcZdbAddReqBody {
                                        password: vdc_config.password().to_string(),
                                        capacity: vdc_config.new_size(),
                                    })
                                    .send()
                                    .await {
                                        Ok(res) => res,
                                        Err(e) => {
                                            error!("could not contact evdc controller: {}", e);
                                            continue;
                                        },
                                    };
                                if !res.status().is_success() {
                                    error!("could not reserve new zdb, unexpected status code ({})", res.status());
                                    continue
                                }
                                // now mark backend as replaced
                                if let Err(e) = cluster.set_replaced(backend).await {
                                    error!("could not mark cluster as replaced: {}", e);
                                    continue
                                }
                            }
                        }
                    }

                }
            }
        }
    })
}

async fn spawn_repairer(mut rx: UnboundedReceiver<String>, zstor: SingleZstor) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(REPAIR_BACKLOG_RETRY_INTERVAL);
        let mut repair_backlog = Vec::new();
        loop {
            select! {
                key = rx.recv() => {
                    let key = match key {
                        None => {
                            info!("shutting down repairer");
                            return
                        },
                        Some(key) => key,
                    };
                    // attempt rebuild
                    if let Err(e) = zstor.rebuild_key(&key).await {
                        error!("Could not rebuild item {}: {}", key, e);
                        repair_backlog.push(key);
                        continue
                    };
                }
                _ = ticker.tick() => {
                    debug!("Processing repair backlog");
                    for key in std::mem::replace(&mut repair_backlog, Vec::new()).drain(..) {
                        debug!("Trying to rebuild key {}, which is in the repair backlog", key);
                        if let Err(e) = zstor.rebuild_key(&key).await {
                            error!("Could not rebuild item {}: {}", key, e);
                            repair_backlog.push(key);
                            continue
                        };
                    }
                }
            }
        }
    })
}

#[derive(Serialize)]
struct VdcZdbAddReqBody {
    password: String,
    capacity: usize,
}
