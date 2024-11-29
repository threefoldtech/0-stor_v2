use crate::{
    actors::{
        backends::{BackendManagerActor, RequestBackends, StateInterest},
        meta::{MetaStoreActor, ObjectMetas},
        zstor::{Rebuild, ZstorActor},
    },
    zdb::ZdbConnectionInfo,
};
use actix::prelude::*;
use log::{debug, error, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Duration;

/// Amount of time between starting a new sweep of the backend objects.
const OBJECT_SWEEP_INTERVAL_SECONDS: u64 = 60 * 10;

#[derive(Message)]
#[rtype(result = "()")]
/// Message to request a sweep of all objects in the [`MetaStore`]. If one or more backends are not
/// reachable, the object is repaired.
struct SweepObjects;

/// Actor implementation of a repair queue. It periodically sweeps the [`MetaStore`], and verifies
/// all backends are still reachable.
pub struct RepairActor {
    meta: Addr<MetaStoreActor>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Addr<ZstorActor>,
}

impl RepairActor {
    /// Create a new [`RepairActor`] checking objects in the provided metastore and using the given
    /// zstor to repair them if needed.
    pub fn new(
        meta: Addr<MetaStoreActor>,
        backend_manager: Addr<BackendManagerActor>,
        zstor: Addr<ZstorActor>,
    ) -> RepairActor {
        Self {
            meta,
            backend_manager,
            zstor,
        }
    }

    /// Send a [`SweepObjects`] command to the actor.
    fn sweep_objects(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(SweepObjects);
    }
}

impl Actor for RepairActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            Duration::from_secs(OBJECT_SWEEP_INTERVAL_SECONDS),
            Self::sweep_objects,
        );
    }
}

impl Handler<SweepObjects> for RepairActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: SweepObjects, _: &mut Self::Context) -> Self::Result {
        let meta = self.meta.clone();
        let backend_manager = self.backend_manager.clone();
        let zstor = self.zstor.clone();

        Box::pin(async move {
            let obj_metas = match meta.send(ObjectMetas).await {
                Err(e) => {
                    error!("Could not request object metas from metastore: {}", e);
                    return;
                }
                Ok(om) => match om {
                    Err(e) => {
                        error!("Could not get object metas from metastore: {}", e);
                        return;
                    }
                    Ok(om) => om,
                },
            };

            // prevent same backend from being checked multiple times
            let mut unique_zdbs = HashMap::new();
            for (key, metadata) in obj_metas.into_iter() {
                let mut need_rebuild_check = false;
                let backend_requests: Vec<ZdbConnectionInfo> = metadata
                    .shards()
                    .iter()
                    .map(|shard_info| shard_info.zdb().clone())
                    .filter(|zdb| match unique_zdbs.entry(zdb.clone()) {
                        Entry::Occupied(entry) => {
                            let is_healthy: &bool = entry.get();
                            if !*is_healthy {
                                need_rebuild_check = true;
                            }
                            false
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(false);
                            true
                        }
                    })
                    .collect();
                if backend_requests.is_empty() {
                    debug!("No unchecked backends found for object: {}", key);
                    continue;
                }
                debug!("number of backends to check = {}", backend_requests.len());

                if !need_rebuild_check {
                    continue;
                }
                let backends = match backend_manager
                    .send(RequestBackends {
                        backend_requests,
                        interest: StateInterest::Readable,
                    })
                    .await
                {
                    Err(e) => {
                        error!("Failed to request backends: {}", e);
                        return;
                    }
                    Ok(backends) => backends,
                };
                let must_rebuild = backends.into_iter().fold(false, |rebuild, b| {
                    let is_healthy = matches!(b, Ok(Some(_)));
                    if let Ok(Some(zdb)) = b {
                        unique_zdbs.insert(zdb.connection_info().clone(), is_healthy);
                    }
                    rebuild || !is_healthy
                });
                if must_rebuild {
                    if let Err(e) = zstor
                        .send(Rebuild {
                            file: None,
                            key: Some(key),
                        })
                        .await
                    {
                        warn!("Failed to rebuild data: {}", e);
                    }
                }
            }
        })
    }
}
