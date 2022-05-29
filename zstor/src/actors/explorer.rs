use crate::actors::{
    config::{AddZdb, ConfigActor, GetConfig},
    metrics::{MetricsActor, UpdatePoolData, UpdateWalletBalances},
};
use crate::{
    config::Group,
    zdb::{ZdbConnectionInfo, ZdbRunMode},
    ZstorError, ZstorErrorKind,
};
use actix::prelude::*;
use chrono::{TimeZone, Utc};
use futures::future::join_all;
use grid_explorer_client::{
    reservation::{PoolData, ReservationData},
    types::Node,
    workload::{DiskType, WorkloadData, WorkloadType, ZdbInformationBuilder, ZdbMode},
    ExplorerClient,
};
use log::{debug, error, warn};
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

/// The amount of seconds between each pool check.
const POOL_CHECK_INTERVAL_SECONDS: u64 = 60 * 60;
/// The amount of seconds between each wallet balance check.
const BALANCE_CHECK_INTERVAL_SECONDS: u64 = 60 * 10;
/// The amount of seconds a pool should live after we extend it. I.e. the pool should be extended
/// so that pool.empty_at - time::now() >= this value.
const POOL_TARGET_LIFETIME_SECONDS: u64 = 60 * 60 * 24 * 7;
/// The amount of seconds a pool should live for, at most, before we try to extend it. I.e. the
/// pool should only be extended if pool.empty_at - time::now() <= this value.
const POOL_REFRESH_LIFETIME_SECONDS: u64 = 60 * 60 * 24 * 2;
/// The currency identifier for the TFT currency as expected by the explorer.
const TFT_CURRENCY_ID: &str = "TFT";
/// The factor to increate the size of old reservations with if a size increase is requested.
const SIZE_INCREASE: u64 = 2;

/// Actor managing reservatonson the threefold grid with an attached stellar wallet.
pub struct ExplorerActor {
    client: Arc<ExplorerClient>,
    cfg_addr: Addr<ConfigActor>,
    metrics_addr: Addr<MetricsActor>,
    managed_pools: HashMap<i64, PoolData>,
}

/// Drop in replacement for the [`ExplorerActor`] which does not connect to the threefold grid.
pub struct NopExplorerActor;

impl ExplorerActor {
    /// Create a new [`ExplorerActor`] from an existing [`ExplorerClient`], and a
    /// [`ConfigActor`].
    pub fn new(
        client: ExplorerClient,
        cfg_addr: Addr<ConfigActor>,
        metrics_addr: Addr<MetricsActor>,
    ) -> ExplorerActor {
        let client = Arc::new(client);
        Self {
            client,
            cfg_addr,
            metrics_addr,
            managed_pools: HashMap::new(),
        }
    }

    /// Send a [`CheckPools`] command to this actor.
    fn check_pools(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckPools);
    }

    /// Send a [`CheckWalletBalances`] command to this actor.
    fn check_balances(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckWalletBalances);
    }
}

impl NopExplorerActor {
    /// Create a new [`NopExplorerActor`].
    pub fn new() -> Self {
        Self
    }
}

impl Default for NopExplorerActor {
    fn default() -> Self {
        Self
    }
}

/// Message requesting the actor checks the pools, and fills them if their expiration time is lower
/// than the threshold.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckPools;

/// Message to trigger a balance check on the wallet used.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckWalletBalances;

/// Message requesting the actor to expand the backend storage. An optional existing reservation
/// ID can be passed. In this case, an attempt is made to reserve the storage on the same pool. If
/// the decomission flag is passed as well, the old reservation is attempted to be removed.
#[derive(Debug, Message)]
#[rtype(result = "Result<ZdbConnectionInfo, ZstorError>")]
pub struct ExpandStorage {
    /// An optional existing 0-db to remove.
    pub existing_zdb: Option<ZdbConnectionInfo>,
    /// Whether the existing reservation should be decomissioned.
    pub decomission: bool,
    /// Size request for the new 0-db namespace. The default size is specified in GiB.
    pub size_request: SizeRequest,
    /// The mode the new 0-db should run in.
    pub mode: ZdbRunMode,
}

/// Requested size for a new 0-db backend. The size will depend on the old reservation if that is
/// ossible, and only use the provided value if the old reservation could not be found.
#[derive(Debug)]
pub enum SizeRequest {
    /// Reserve the exact size of the existing reservation, if possible, otherwise use the provided
    /// default.
    Exact(u64),
    /// Increase the size compared to the existing reservation, if possible, otherwise use the
    /// provided default. The amount of increase is an implementation detail.
    Increase(u64),
}

impl SizeRequest {
    /// Extract the default size from the size request.
    fn default_size(&self) -> u64 {
        match *self {
            SizeRequest::Exact(size) => size,
            SizeRequest::Increase(size) => size,
        }
    }
}

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
                            actor.managed_pools =
                                pools.into_iter().map(|pd| (pd.pool_id, pd)).collect();
                        }
                    };
                }),
        );

        ctx.run_interval(
            Duration::from_secs(POOL_CHECK_INTERVAL_SECONDS),
            Self::check_pools,
        );

        ctx.run_interval(
            Duration::from_secs(BALANCE_CHECK_INTERVAL_SECONDS),
            Self::check_balances,
        );

        debug!(
            "Explorer actor initialization finished, managing {} pools (ids {})",
            self.managed_pools.len(),
            self.managed_pools
                .keys()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

impl Actor for NopExplorerActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        warn!("NopExplorer started, automatic capacity managment is disabled");
    }
}

impl Handler<CheckWalletBalances> for ExplorerActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: CheckWalletBalances, _: &mut Self::Context) -> Self::Result {
        let explorer = self.client.clone();
        let metrics_addr = self.metrics_addr.clone();

        Box::pin(async move {
            let balances = match explorer.stellar_client.get_balances().await {
                Err(e) => {
                    error!("Could not get wallet balances: {}", e);
                    return;
                }
                Ok(balances) => balances,
            };

            if let Err(e) = metrics_addr.send(UpdateWalletBalances { balances }).await {
                error!("Could not update wallet balance metrics: {}", e);
            }
        })
    }
}

impl Handler<CheckPools> for ExplorerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: CheckPools, _: &mut Self::Context) -> Self::Result {
        debug!("Attempting to refresh pool expiration");

        // First update all pools we own
        let client = self.client.clone();
        let metrics = self.metrics_addr.clone();

        Box::pin(
            async move {
                match client.pools_by_owner().await {
                    Err(e) => {
                        error!("Could not get capacity pools we currently own: {}", e);
                        None
                    }
                    Ok(pools) => {
                        for pool in pools.iter().cloned() {
                            if let Err(e) = metrics.send(UpdatePoolData { pool }).await {
                                warn!("Could not update pool data metrics: {}", e);
                            }
                        }
                        Some(pools)
                    }
                }
            }
            .into_actor(self)
            .then(|res, actor, _| {
                // If we managed to fetch the pools, update them first
                if let Some(pools) = res {
                    actor.managed_pools = pools.into_iter().map(|pd| (pd.pool_id, pd)).collect();
                }

                let client = actor.client.clone();
                let now = Utc::now();

                let pools_to_extend = actor
                    .managed_pools
                    .iter()
                    .filter_map(|(id, pool)| {
                        let expiration = Utc.timestamp(pool.empty_at, 0);
                        if (expiration - now).num_seconds() <= POOL_REFRESH_LIFETIME_SECONDS as i64
                        {
                            // Found a pool which is about to expire, start refresh operaton
                            debug!("Pool {} is about to expire, attempting to refresh", id);
                            return Some(pool);
                        };
                        None
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                async move {
                    let extensions = pools_to_extend.iter().map(|pool| {
                        let missing_cu =
                            pool.active_cu * POOL_TARGET_LIFETIME_SECONDS as f64 - pool.cus;
                        let missing_su =
                            pool.active_su * POOL_TARGET_LIFETIME_SECONDS as f64 - pool.sus;

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

                    // Fetch all pools again to get latest status.
                    client.pools_by_owner().await
                }
                .into_actor(actor)
            })
            .map(|pools, actor, _| match pools {
                Err(e) => error!("Failed to refresh pools: {}", e),
                Ok(pools) => {
                    debug!("Reloaded existing capacity pools");
                    actor.managed_pools = pools.into_iter().map(|pd| (pd.pool_id, pd)).collect();
                }
            }),
        )
    }
}

impl Handler<ExpandStorage> for ExplorerActor {
    type Result = ResponseFuture<Result<ZdbConnectionInfo, ZstorError>>;

    fn handle(&mut self, msg: ExpandStorage, _: &mut Self::Context) -> Self::Result {
        let client = self.client.clone();
        let own_pools = self.managed_pools.clone();
        let own_pool_ids = self.managed_pools.keys().copied().collect::<Vec<_>>();
        let cfg_actor = self.cfg_addr.clone();

        Box::pin(async move {
            if own_pool_ids.is_empty() {
                return Err(ZstorError::with_message(
                    ZstorErrorKind::Explorer,
                    "Can't reserve a new 0-db as we are currently not managing any pools".into(),
                ));
            }
            let existing_reservation_id = if let Some(ref ci) = msg.existing_zdb {
                ci.reservation_id()
            } else {
                None
            };
            // Try to select a new node from the same pool to deploy on.
            let (mut prefered_node_id, size) = if let Some(id) = existing_reservation_id {
                let old_workload = client.workload_get_by_id(id).await?;
                let new_db_size = match old_workload.data {
                    WorkloadData::Zdb(data) => match msg.size_request {
                        SizeRequest::Exact(_) => data.size as u64,
                        SizeRequest::Increase(_) => data.size as u64 * SIZE_INCREASE,
                    },
                    _ => msg.size_request.default_size(),
                };
                let pool_id = old_workload.pool_id;
                // prefer to work on the same pool
                if !own_pool_ids.contains(&pool_id) {
                    warn!("Attempting to get new storage from existing storage which is not managed by our own pools (pool_id: {})", pool_id);
                    (None, new_db_size)
                } else {
                    // unwrap here is safe since we selected a pool id we own above.
                    let mut pref = None;
                    for node_id in &own_pools.get(&pool_id).unwrap().node_ids {
                        if node_id == &old_workload.node_id && msg.decomission {
                            continue;
                        }
                        let node = client.node_get_by_id(node_id).await?;
                        if is_deployable(&node, new_db_size) {
                            pref = Some((pool_id, node_id.clone(), node.public_key_hex));
                            break;
                        }
                    }
                    (pref, new_db_size)
                }
            } else {
                (None, msg.size_request.default_size())
            };

            // TODO: Selecting a random pool here might break the redundancy configuration
            if prefered_node_id.is_none() {
                // Randomly select from all pools.
                // First create a list of (pool_id, node_id)
                let mut nodes = own_pools
                    .into_iter()
                    .flat_map(|(id, pool)| {
                        pool.node_ids
                            .into_iter()
                            .map(|nid| (id, nid))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();

                // Shuffle nodes list, so they are in random order
                nodes.shuffle(&mut rand::thread_rng());

                // Now pick the first acceptable node
                for (pool_id, node_id) in nodes.into_iter() {
                    let node = client.node_get_by_id(&node_id).await?;
                    if is_deployable(&node, size) {
                        prefered_node_id = Some((pool_id, node_id, node.public_key_hex));
                        break;
                    }
                }
            }

            if prefered_node_id.is_none() {
                error!(
                    "Unable to find valid node to deploy new 0-db on of size {}",
                    size
                );
                return Err(ZstorError::with_message(
                    ZstorErrorKind::Explorer,
                    "could not deploy new 0-db, no valid node found".into(),
                ));
            }

            // Unwrap is safe as we just checked that this is Some(_).
            let (pool_id, node_id, node_pubkey) = prefered_node_id.unwrap();
            // Generate a new password. This is included in the connection info later.
            let pwd: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();
            let res = ZdbInformationBuilder::new()
                .size(size as i64)
                .mode(match msg.mode {
                    ZdbRunMode::Seq => ZdbMode::ZdbModeSeq,
                    ZdbRunMode::User => ZdbMode::ZdbModeUser,
                })
                .public(false)
                .disk_type(DiskType::Hdd)
                .password(pwd.clone())
                .build(&client.user_identity, &node_pubkey)
                .expect("We should have filled in all 0-db information required");

            let wid = client.create_zdb_reservation(node_id, pool_id, res).await?;

            // Wait for 3 minutes for deployment, this should be more than sufficient
            let workload = match client.workload_poll(wid, 60 * 3).await {
                Err(e) => {
                    error!("Could not deploy workload (in 3 minutes): {}", e);
                    // Try to decommission the workload again.
                    if let Err(e) = client.workload_decommission(wid).await {
                        error!("Could not decommission failed workload: {}", e);
                    }
                    return Err(e.into());
                }
                Ok(w) => {
                    debug!("Workload {} deployed successfully", wid);
                    w
                }
            };

            // This unwrap is safe since the poll call above only returns Ok(_) if this is Some(_).
            let result = workload.result.unwrap();
            let res = match serde_json::value::from_value::<ZdbResultJson>(result.data_json) {
                Err(e) => {
                    error!("Failed to parse 0-db reservation result: {}", e);
                    // Try to decommission the workload again.
                    if let Err(e) = client.workload_decommission(wid).await {
                        error!("Could not decommission failed workload: {}", e);
                    }
                    return Err(ZstorError::with_message(
                        ZstorErrorKind::Explorer,
                        "Failed to parse 0-db reservation result".into(),
                    ));
                }
                Ok(res) => res,
            };

            if res.ips.is_empty() {
                error!("0-db reservation did not provide any IPs");
                // Try to decommission the workload again.
                if let Err(e) = client.workload_decommission(wid).await {
                    error!("Could not decommission failed workload: {}", e);
                }
                return Err(ZstorError::with_message(
                    ZstorErrorKind::Explorer,
                    "0-db reservation did not provide any IPs".into(),
                ));
            }

            let ci = ZdbConnectionInfo::new(
                SocketAddr::new(res.ips[0], res.port),
                Some(res.namespace),
                Some(pwd),
            );

            // delete old zdb if needed
            if let Some(wid) = existing_reservation_id {
                debug!("Decommissioning existing workload {}", wid);
                if let Err(e) = client.workload_decommission(wid).await {
                    error!("Could not decommission old workload {}: {}", wid, e);
                }
            }
            // Now we need to insert the CI in the proper config group
            let cfg = cfg_actor.send(GetConfig).await?;

            for (idx, group) in cfg.groups().iter().enumerate() {
                let group_pool = group_pool(client.clone(), group).await;
                if let Some(pid) = group_pool {
                    if pool_id == pid {
                        cfg_actor
                            .send(AddZdb {
                                group_idx: idx,
                                ci: ci.clone(),
                                replaced: msg.existing_zdb,
                            })
                            .await??;
                        return Ok(ci);
                    }
                }
            }

            // If we get here it means we couldn't identify the group, start a new one
            cfg_actor
                .send(AddZdb {
                    group_idx: cfg.groups().len(),
                    ci: ci.clone(),
                    replaced: msg.existing_zdb,
                })
                .await??;

            Ok(ci)
        })
    }
}

impl Handler<ExpandStorage> for NopExplorerActor {
    type Result = ResponseFuture<Result<ZdbConnectionInfo, ZstorError>>;

    fn handle(&mut self, msg: ExpandStorage, _: &mut Self::Context) -> Self::Result {
        Box::pin(async move {
            warn!(
                "Requesting storage expansion, but capacity managment is disabled. Request: {:?}",
                msg
            );
            Err(ZstorError::with_message(
                ZstorErrorKind::Explorer,
                "capacity management disabled".into(),
            ))
        })
    }
}

/// Attempt to find the primary pool of a group. If there are too many unknown 0-dbs in the group,
/// or they belong to too many different pools, [`Option::None`] is returned.
// TODO: returns farms here
async fn group_pool(client: Arc<ExplorerClient>, group: &Group) -> Option<i64> {
    let mut pools: HashMap<i64, usize> = HashMap::new();
    for ci in group.backends() {
        if let Some(res_id) = ci.reservation_id() {
            let reservation = match client.workload_get_by_id(res_id).await {
                Err(e) => {
                    warn!("Could not load 0-db reservation: {}", e);
                    continue;
                }
                Ok(res) => res,
            };

            if reservation.workload_type != WorkloadType::WorkloadTypeZdb {
                warn!("Reservation has wrong type {}", reservation.workload_type);
                continue;
            }

            *pools.entry(reservation.pool_id).or_insert(0) += 1;
        }
    }

    // Find the pool with the most attached 0-dbs in the group.
    let most_frequent_pool = pools.iter().max_by(|a, b| a.1.cmp(b.1)).map(|(k, _)| k);
    if let Some(id) = most_frequent_pool {
        // make sure at least half of the 0-dbs belong to the same pool, otherwise return [`None`]
        // to indicate that we don't know.
        if *id as usize >= group.backends().len() {
            return Some(*id);
        }
    }
    None
}

/// Check if a node is acceptable to deploy a new 0-db on with a given size.
fn is_deployable(node: &Node, size: u64) -> bool {
    if node.deleted {
        return false;
    }
    if node.reserved {
        return false;
    }
    // check node is online
    if (Utc::now() - Utc.timestamp(node.updated as i64, 0)).num_seconds() > 600 {
        return false;
    }

    // Make sure node has enough free hru
    if ((node.total_resources.hru - node.used_resources.hru - node.reserved_resources.hru).floor()
        as u64)
        < size
    {
        return false;
    }

    true
}

/// The json structure of the response in a 0-db reservation
#[derive(Debug, Serialize, Deserialize)]
struct ZdbResultJson {
    #[serde(rename = "Namespace")]
    namespace: String,
    #[serde(rename = "IPs")]
    ips: Vec<IpAddr>,
    #[serde(rename = "Port")]
    port: u16,
}
