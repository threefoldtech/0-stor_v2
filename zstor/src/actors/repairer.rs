use crate::actors::{
    backends::{BackendManagerActor, RequestBackends, StateInterest},
    meta::{MetaStoreActor, ScanMeta},
    zstor::{Rebuild, ZstorActor},
};
use actix::prelude::*;
use log::{debug, error, warn};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

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
            let start_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // start scanning from the beginning (cursor == None) and let the metastore choose the backend_id
            let mut cursor = None;
            let mut backend_idx = None;
            loop {
                // scan keys from the metastore
                let (idx, new_cursor, metas) = match meta
                    .send(ScanMeta {
                        cursor: cursor.clone(),
                        backend_idx,
                        max_timestamp: Some(start_time),
                    })
                    .await
                {
                    Err(e) => {
                        error!("Could not request meta keys from metastore: {}", e);
                        return;
                    }
                    Ok(result) => match result {
                        Err(e) => {
                            error!("Could not get meta keys from metastore: {}", e);
                            return;
                        }
                        Ok(res) => res,
                    },
                };

                // iterate over the keys and check if the backends are healthy
                // if not, rebuild the object
                for (key, metadata) in metas.into_iter() {
                    let backend_requests = metadata
                        .shards()
                        .iter()
                        .map(|shard_info| shard_info.zdb())
                        .cloned()
                        .collect::<Vec<_>>();
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
                    let must_rebuild = backends.into_iter().any(|b| !matches!(b, Ok(Some(_))));
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

                if new_cursor.is_none() {
                    debug!("there is no more old data to rebuild");
                    break;
                }

                cursor = new_cursor;
                backend_idx = Some(idx);
            }
        })
    }
}
