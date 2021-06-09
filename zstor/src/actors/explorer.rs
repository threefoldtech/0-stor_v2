use crate::actors::config::ConfigActor;
use actix::prelude::*;
use chrono::{TimeZone, Utc};
use futures::future::join_all;
use grid_explorer_client::{
    reservation::{PoolData, ReservationData},
    ExplorerClient,
};
use log::{debug, error};
use std::{sync::Arc, time::Duration};

/// The amount of seconds between each pool check.
const POOL_CHECK_INTERVAL_SECONDS: u64 = 60 * 60;
/// The amount of seconds a pool should live after we extend it. I.e. the pool should be extended
/// so that pool.empty_at - time::now() >= this value.
const POOL_TARGET_LIFETIME_SECONDS: u64 = 60 * 60 * 24 * 7;
/// The amount of seconds a pool should live for, at most, before we try to extend it. I.e. the
/// pool should only be extended if pool.empty_at - time::now() <= this value.
const POOL_REFRESH_LIFETIME_SECONDS: u64 = 60 * 60 * 24 * 2;
/// The currency identifier for the TFT currency as expected by the explorer.
const TFT_CURRENCY_ID: &str = "TFT";

/// Actor managing reservatonson the threefold grid with an attached stellar wallet.
pub struct ExplorerActor {
    client: Arc<ExplorerClient>,
    // TODO
    _cfg_addr: Addr<ConfigActor>,
    managed_pools: Vec<PoolData>,
}

impl ExplorerActor {
    /// Create a new [`ExplorerActor`] from an existing [`ExplorerClient`], and a
    /// [`ConfigActor`].
    pub fn new(client: ExplorerClient, _cfg_addr: Addr<ConfigActor>) -> ExplorerActor {
        let client = Arc::new(client);
        Self {
            client,
            _cfg_addr,
            managed_pools: Vec::new(),
        }
    }

    /// Send a [`CheckPools`] command to this actor.
    fn check_pools(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckPools);
    }
}

/// Message requesting the actor checks the pools, and fills them if their expiration time is lower
/// than the threshold.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckPools;

impl Actor for ExplorerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("Explorer actor started, loading managed pools");

        let client = self.client.clone();

        // As part of actor startup, load __all__ pools we manage.
        ctx.wait(
            async move { client.pools_by_owner().await }
                .into_actor(self)
                .map(|res, actor, _| {
                    match res {
                        Err(e) => error!("Could not get capacity pools we currently own: {}", e),
                        Ok(pools) => {
                            debug!(
                                "Explorer response found {} pools (ids {})",
                                pools.len(),
                                pools
                                    .iter()
                                    .map(|pool| pool.pool_id.to_string())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                            actor.managed_pools = pools;
                        }
                    };
                }),
        );

        ctx.run_interval(
            Duration::from_secs(POOL_CHECK_INTERVAL_SECONDS),
            Self::check_pools,
        );

        debug!(
            "Explorer actor initialization finished, managing {} pools (ids {})",
            self.managed_pools.len(),
            self.managed_pools
                .iter()
                .map(|pool| pool.pool_id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

impl Handler<CheckPools> for ExplorerActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: CheckPools, _: &mut Self::Context) -> Self::Result {
        debug!("Attempting to refresh pool expiration");

        let client = self.client.clone();
        let now = Utc::now();

        let pools_to_extend = self
            .managed_pools
            .iter()
            .filter(|pool| {
                let expiration = Utc.timestamp(pool.empty_at, 0);
                if (expiration - now).num_seconds() <= POOL_REFRESH_LIFETIME_SECONDS as i64 {
                    // Found a pool which is about to expire, start refresh operaton
                    debug!(
                        "Pool {} is about to expire, attempting to refresh",
                        pool.pool_id
                    );
                    return true;
                };
                false
            })
            .cloned()
            .collect::<Vec<_>>();

        Box::pin(async move {
            let extensions = pools_to_extend.iter().map(|pool| {
                let missing_cu = pool.active_cu * POOL_TARGET_LIFETIME_SECONDS as f64 - pool.cus;
                let missing_su = pool.active_su * POOL_TARGET_LIFETIME_SECONDS as f64 - pool.sus;

                let rd = ReservationData {
                    pool_id: pool.pool_id,
                    cus: missing_cu.ceil() as u64,
                    sus: missing_su.ceil() as u64,
                    ipv4us: 0,
                    node_ids: pool.node_ids.clone(),
                    currencies: vec![String::from(TFT_CURRENCY_ID)],
                };

                client.create_capacity_pool(rd)
            });

            for result in join_all(extensions).await {
                match result {
                    Err(e) => error!("Failed to extend pool: {}", e),
                    Ok(success) if !success => {
                        error!("Got unexpected failed response from explorer")
                    }
                    Ok(success) if success => debug!("Successfully extended pool"),
                    // compiler does not understand that we exhauted al bool options above
                    Ok(_) => unreachable!(),
                }
            }
        })
    }
}
