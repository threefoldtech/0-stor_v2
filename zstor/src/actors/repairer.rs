use crate::actors::{
    backends::{BackendManagerActor, RequestBackends, StateInterest},
    meta::{MetaStoreActor, ObjectMetas},
    zstor::{Rebuild, ZstorActor},
};
use crate::meta::MetaStore;
use actix::prelude::*;
use log::{error, warn};
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
pub struct RepairActor<T>
where
    T: Unpin + 'static,
{
    meta: Addr<MetaStoreActor<T>>,
    backend_manager: Addr<BackendManagerActor>,
    zstor: Addr<ZstorActor<T>>,
}

impl<T> RepairActor<T>
where
    T: MetaStore + Unpin,
{
    /// Create a new [`RepairActor`] checking objects in the provided metastore and using the given
    /// zstor to repair them if needed.
    pub fn new(
        meta: Addr<MetaStoreActor<T>>,
        backend_manager: Addr<BackendManagerActor>,
        zstor: Addr<ZstorActor<T>>,
    ) -> RepairActor<T> {
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

impl<T> Actor for RepairActor<T>
where
    T: MetaStore + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            Duration::from_secs(OBJECT_SWEEP_INTERVAL_SECONDS),
            Self::sweep_objects,
        );
    }
}

impl<T> Handler<SweepObjects> for RepairActor<T>
where
    T: MetaStore + Unpin,
{
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

            for (key, metadata) in obj_metas.into_iter() {
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
                let must_rebuild = backends.into_iter().all(|b| matches!(b, Ok(Some(_))));
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
