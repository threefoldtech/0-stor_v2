use crate::actors::{
    config::{ConfigActor, GetConfig, ReloadConfig, ReplaceMetaBackend},
    explorer::{ExpandStorage, SizeRequest},
    meta::{MarkWriteable, MetaStoreActor, ReplaceMetaStore},
    metrics::{MetricsActor, SetDataBackendInfo, SetMetaBackendInfo},
};
use crate::{
    config::{Encryption, Meta},
    encryption::AesGcm,
    zdb::{NsInfo, SequentialZdb, UserKeyZdb, ZdbConnectionInfo, ZdbRunMode},
    zdb_meta::ZdbMetaStore,
    ZstorError,
};
use actix::prelude::*;
use futures::{
    future::{join, join_all},
    {stream, StreamExt},
};
use log::{debug, error, warn};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Amount of time a backend can be unreachable before it is actually considered unreachable.
/// Currently set to 15 minutes.
const MISSING_DURATION: Duration = Duration::from_secs(15 * 60);
/// The amount of free space left in a backend before it is considered to be full. Currently this is
/// 100 MiB.
const FREESPACE_TRESHOLD: u64 = 100 * (1 << 20);
/// Check backends every 3 seconds.
const BACKEND_CHECK_INTERVAL_SECONDS: u64 = 3;
/// Default to 0-db data backends of 100GiB in size.
const DEFAULT_DATA_BACKEND_SIZE_GIB: u64 = 100;
/// Default to 0-db metadata backends of 25GiB in size.
const DEFAULT_META_BACKEND_SIZE_GIB: u64 = 25;

/// An actor implementation of a backend manager.
pub struct BackendManagerActor {
    config_addr: Addr<ConfigActor>,
    explorer: Recipient<ExpandStorage>,
    metrics: Addr<MetricsActor>,
    metastore: Addr<MetaStoreActor>,
    managed_seq_dbs: HashMap<ZdbConnectionInfo, (Option<SequentialZdb>, BackendState)>,
    managed_meta_dbs: HashMap<ZdbConnectionInfo, (Option<UserKeyZdb>, BackendState)>,
}

impl BackendManagerActor {
    /// Create a new [`BackendManagerActor`].
    pub fn new(
        config_addr: Addr<ConfigActor>,
        explorer: Recipient<ExpandStorage>,
        metrics: Addr<MetricsActor>,
        metastore: Addr<MetaStoreActor>,
    ) -> BackendManagerActor {
        Self {
            config_addr,
            explorer,
            metrics,
            metastore,
            managed_seq_dbs: HashMap::new(),
            managed_meta_dbs: HashMap::new(),
        }
    }

    /// Send a [`CheckBackends`] command to this actor.
    fn check_backends(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckBackends);
    }

    /// check & returns new metadata backends if the cluster needs to be refreshed
    fn check_new_metastore(
        &self,
        meta_info: Vec<(
            ZdbConnectionInfo,
            Option<UserKeyZdb>,
            BackendState,
            Option<NsInfo>,
        )>,
    ) -> Option<Vec<UserKeyZdb>> {
        let mut need_refresh = false;
        for (ci, _, new_state, _) in &meta_info {
            if let Some((_, old_state)) = self.managed_meta_dbs.get(ci) {
                if old_state.is_writeable() != new_state.is_writeable() {
                    need_refresh = true;
                    break;
                }
            }
        }
        if !need_refresh {
            return None;
        }
        // get new backends:
        // - new_db
        // - old_db with state == writable
        let mut backends = Vec::new();
        let managed_db = self.managed_meta_dbs.clone();
        for (ci, new_db, new_state, _) in &meta_info {
            if let Some(new_db) = new_db {
                backends.push(new_db.clone());
            } else if new_state.is_writeable() {
                if let Some((Some(db), _)) = managed_db.get(ci) {
                    backends.push(db.clone());
                }
            }
        }
        Some(backends)
    }
}

/// Message requesting the actor checks the backends, and updates the state of all managed
/// backends.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckBackends;

/// Message requesting the actor checks the backends, and takes action if a 0-db has reached a
/// terminal state.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct ReplaceBackends;

/// Message to request connections to backends with a given capabitlity. If a healthy connection
/// is being managed to his backend, this is returned. If a connection to this backend is not
/// managed, or the connection is in an unhealthy state, a new connection is attempted.
#[derive(Debug, Message)]
#[rtype(result = "Vec<Result<Option<SequentialZdb>, ZstorError>>")]
pub struct RequestBackends {
    /// Connection info identifying the requested backend.
    pub backend_requests: Vec<ZdbConnectionInfo>,
    /// The required state of the connections.
    pub interest: StateInterest,
}

impl Actor for BackendManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("Backend manager actor started, loading backends to monitor");

        let cfg_addr = self.config_addr.clone();

        ctx.wait(
            async move {
                let (managed_seq_dbs, managed_meta_dbs) =
                    get_zdbs_from_config(cfg_addr.clone()).await;
                (managed_seq_dbs, managed_meta_dbs)
            }
            .into_actor(self)
            .map(|res, actor, _| {
                actor.managed_seq_dbs = res.0;
                actor.managed_meta_dbs = res.1;
            }),
        );

        ctx.run_interval(
            Duration::from_secs(BACKEND_CHECK_INTERVAL_SECONDS),
            Self::check_backends,
        );
    }
}

impl Handler<ReloadConfig> for BackendManagerActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, _: ReloadConfig, ctx: &mut Self::Context) -> Self::Result {
        let cfg_addr = self.config_addr.clone();
        let fut = Box::pin(
            async move {
                let (managed_seq_dbs, managed_meta_dbs) =
                    get_zdbs_from_config(cfg_addr.clone()).await;
                (managed_seq_dbs, managed_meta_dbs)
            }
            .into_actor(self)
            .map(|(seq_dbs, meta_dbs), actor, _| {
                // remove the data backends that are no longer managed from  the metrics
                for (ci, _) in actor.managed_seq_dbs.iter() {
                    if !seq_dbs.contains_key(ci) {
                        actor.metrics.do_send(SetDataBackendInfo {
                            ci: ci.clone(),
                            info: None,
                        });
                    }
                }

                // remove the meta backends that are no longer managed from  the metrics
                for (ci, _) in actor.managed_meta_dbs.iter() {
                    if !meta_dbs.contains_key(ci) {
                        actor.metrics.do_send(SetMetaBackendInfo {
                            ci: ci.clone(),
                            info: None,
                        });
                    }
                }
                actor.managed_seq_dbs = seq_dbs;
                actor.managed_meta_dbs = meta_dbs;
            }),
        );
        ctx.spawn(fut);
        Ok(())
    }
}

async fn get_zdbs_from_config(
    cfg_addr: Addr<ConfigActor>,
) -> (
    HashMap<ZdbConnectionInfo, (Option<SequentialZdb>, BackendState)>,
    HashMap<ZdbConnectionInfo, (Option<UserKeyZdb>, BackendState)>,
) {
    match cfg_addr.send(GetConfig).await {
        Err(e) => {
            error!("Could not get config: {}", e);
            // we won't manage the dbs
            (HashMap::new(), HashMap::new())
        }
        Ok(config) => {
            let managed_seq_dbs = stream::iter(config.backends())
                .filter_map(|ci| async move {
                    let db = match SequentialZdb::new(ci.clone()).await {
                        Ok(db) => db,
                        Err(e) => {
                            warn!("Could not connect to backend {} in config file: {}", ci, e);
                            return Some((ci.clone(), (None, BackendState::new())));
                        }
                    };
                    let ns_info = match db.ns_info().await {
                        Ok(info) => info,
                        Err(e) => {
                            warn!("Failed to get ns info from backend {}: {}", ci, e);
                            return Some((ci.clone(), (Some(db), BackendState::new())));
                        }
                    };

                    let free_space = ns_info.free_space();
                    let state = if free_space <= FREESPACE_TRESHOLD {
                        BackendState::LowSpace(free_space)
                    } else {
                        BackendState::Healthy
                    };
                    Some((ci.clone(), (Some(db), state)))
                })
                .collect()
                .await;

            let managed_meta_dbs = match config.meta() {
                Meta::Zdb(zdb_meta_cfg) => {
                    stream::iter(zdb_meta_cfg.backends())
                        .filter_map(|ci| async move {
                            let db = match UserKeyZdb::new(ci.clone()).await {
                                Ok(db) => db,
                                Err(e) => {
                                    warn!(
                                    "Failed to connect to metadata backend {} in config file: {}",
                                    ci, e
                                );
                                    return Some((ci.clone(), (None, BackendState::new())));
                                }
                            };
                            let ns_info = match db.ns_info().await {
                                Ok(info) => info,
                                Err(e) => {
                                    warn!(
                                        "Failed to get ns info from metadata backend {}: {}",
                                        ci, e
                                    );
                                    return Some((ci.clone(), (Some(db), BackendState::new())));
                                }
                            };
                            let free_space = ns_info.free_space();
                            let state = if free_space <= FREESPACE_TRESHOLD {
                                BackendState::LowSpace(free_space)
                            } else {
                                BackendState::Healthy
                            };

                            Some((ci.clone(), (Some(db), state)))
                        })
                        .collect()
                        .await
                }
            };
            (managed_seq_dbs, managed_meta_dbs)
        }
    }
}

impl Handler<CheckBackends> for BackendManagerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: CheckBackends, _: &mut Self::Context) -> Self::Result {
        let data_backend_info = self
            .managed_seq_dbs
            .iter()
            // Can't use `Cloned` here because it expects a ref to a type, while we have a tuple of
            // refs.
            .map(|(ci, (db, state))| (ci.clone(), (db.clone(), state.clone())))
            .collect::<Vec<_>>();
        let meta_backend_info = self
            .managed_meta_dbs
            .iter()
            // Can't use `Cloned` here because it expects a ref to a type, while we have a tuple of
            // refs.
            .map(|(ci, (db, state))| (ci.clone(), (db.clone(), state.clone())))
            .collect::<Vec<_>>();

        Box::pin(
            async move {
                let futs = data_backend_info
                    .into_iter()
                    .map(|(ci, (db, mut state))| async move {
                        if let Some(db) = db {
                            let info = match db.ns_info().await {
                                Err(e) => {
                                    warn!("Failed to get ns_info from {}: {}", ci, e);
                                    state.mark_unreachable();
                                    None
                                }
                                Ok(info) => {
                                    state.mark_healthy(info.free_space());
                                    Some(info)
                                }
                            };
                            (ci, None, state, info)
                        } else {
                            // Try and get a new connection to the db.
                            if let Ok(db) = SequentialZdb::new(ci.clone()).await {
                                let info = match db.ns_info().await {
                                    Err(e) => {
                                        warn!("Failed to get ns_info from {}: {}", ci, e);
                                        state.mark_unreachable();
                                        None
                                    }
                                    Ok(info) => {
                                        state.mark_healthy(info.free_space());
                                        Some(info)
                                    }
                                };
                                (ci, Some(db), state, info)
                            } else {
                                state.mark_unreachable();
                                (ci, None, state, None)
                            }
                        }
                    });
                let data_info = join_all(futs).await;
                let futs = meta_backend_info
                    .into_iter()
                    .map(|(ci, (db, mut state))| async move {
                        if let Some(db) = db {
                            let info = match db.ns_info().await {
                                Err(e) => {
                                    warn!("Failed to get ns_info from {}: {}", ci, e);
                                    state.mark_unreachable();
                                    None
                                }
                                Ok(info) => {
                                    state.mark_healthy(info.free_space());
                                    Some(info)
                                }
                            };
                            (ci, None, state, info)
                        } else {
                            // Try and get a new connection to the db.
                            if let Ok(db) = UserKeyZdb::new(ci.clone()).await {
                                let info = match db.ns_info().await {
                                    Err(e) => {
                                        warn!("Failed to get ns_info from {}: {}", ci, e);
                                        state.mark_unreachable();
                                        None
                                    }
                                    Ok(info) => {
                                        state.mark_healthy(info.free_space());
                                        Some(info)
                                    }
                                };
                                (ci, Some(db), state, info)
                            } else {
                                state.mark_unreachable();
                                (ci, None, state, None)
                            }
                        }
                    });
                let meta_info = join_all(futs).await;
                (data_info, meta_info)
            }
            .into_actor(self)
            .then(|(data_info, meta_info), actor, _| {
                let new_meta_backends = actor.check_new_metastore(meta_info.clone());

                let config_addr = actor.config_addr.clone();
                let metastore = actor.metastore.clone();
                async move {
                    if let Some(backends) = new_meta_backends {
                        let config = match config_addr.send(GetConfig).await {
                            Ok(cfg) => cfg,
                            Err(e) => {
                                error!("Failed to get running config: {}", e);
                                return (data_info, meta_info);
                            }
                        };
                        let Meta::Zdb(meta_config) = config.meta();
                        let encoder = meta_config.encoder();
                        let encryptor = match config.encryption() {
                            Encryption::Aes(key) => AesGcm::new(key.clone()),
                        };
                        let new_cluster = ZdbMetaStore::new(
                            backends,
                            encoder.clone(),
                            encryptor.clone(),
                            meta_config.prefix().to_owned(),
                            config.virtual_root().clone(),
                        );
                        let writeable = new_cluster.writable();
                        if let Err(e) = metastore
                            .send(ReplaceMetaStore {
                                new_store: Box::new(new_cluster),
                                writeable,
                            })
                            .await
                        {
                            error!("Failed to send ReplaceMetaStore message: {}", e);
                        }
                    }
                    (data_info, meta_info)
                }
                .into_actor(actor)
            })
            .map(|res, actor, _ctx| {
                for (ci, new_db, new_state, info) in res.0.into_iter() {
                    // update metrics
                    actor.metrics.do_send(SetDataBackendInfo {
                        ci: ci.clone(),
                        info,
                    });
                    // It _IS_ possible that the entry is gone here, if another future in the
                    // actor runs another future in between. But in this case, it was this actor
                    // which decided to remove the entry, so we don't really care about that
                    // anyway.
                    if let Some((possible_con, old_state)) = actor.managed_seq_dbs.get_mut(&ci) {
                        if new_db.is_some() {
                            debug!("Caching new connection to {}", ci);
                            *possible_con = new_db;
                        }
                        // If the backend is not readable, remove the cached connection to trigger
                        // a reconnect.
                        if !new_state.is_readable() {
                            if possible_con.is_some() {
                                debug!(
                                    "State is not readable, clearing existing connection to {}",
                                    ci
                                );
                            }
                            *possible_con = None;
                        }
                        *old_state = new_state
                    }
                }
                for (ci, new_db, new_state, info) in res.1.into_iter() {
                    // update metrics
                    actor.metrics.do_send(SetMetaBackendInfo {
                        ci: ci.clone(),
                        info,
                    });
                    // It _IS_ possible that the entry is gone here, if another future in the
                    // actor runs another future in between. But in this case, it was this actor
                    // which decided to remove the entry, so we don't really care about that
                    // anyway.
                    if let Some((possible_con, old_state)) = actor.managed_meta_dbs.get_mut(&ci) {
                        if new_db.is_some() {
                            debug!("Caching new connection to {}", ci);
                            *possible_con = new_db;
                        }
                        // If the backend is not readable, remove the cached connection to trigger
                        // a reconnect.
                        if !new_state.is_readable() {
                            if possible_con.is_some() {
                                debug!(
                                    "State is not readable, clearing existing connection to {}",
                                    ci
                                );
                            }
                            *possible_con = None;
                        }
                        *old_state = new_state
                    }
                }
            }),
        )
    }
}

impl Handler<ReplaceBackends> for BackendManagerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: ReplaceBackends, _: &mut Self::Context) -> Self::Result {
        debug!("Attempting to replace backends");
        let explorer = self.explorer.clone();
        // Grab all backends which must be replaced.
        let seq_replacements = self
            .managed_seq_dbs
            .iter()
            .filter_map(|(ci, (_, state))| {
                if state.is_terminal() {
                    Some((ci.clone(), state.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Now remove the backends to replace from the set of managed ones.
        for (key, _) in &seq_replacements {
            self.metrics.do_send(SetDataBackendInfo {
                ci: key.clone(),
                info: None,
            });
            self.managed_seq_dbs.remove(key);
        }

        // Finally, request new 0-dbs and decommission unreadable ones.
        let seq_replacements = seq_replacements
            .into_iter()
            .map(|(ci, state)| {
                let explorer = explorer.clone();
                get_seq_zdb(explorer, ci, state)
            })
            .collect::<Vec<_>>();

        // Same for meta 0-db's really.
        let user_replacements = self
            .managed_meta_dbs
            .iter()
            .filter_map(|(ci, (_, state))| {
                if state.is_terminal() {
                    Some((ci.clone(), state.clone()))
                } else {
                    None
                }
            })
            // Don't remove the meta db's from the managed set yet, wait untill the rebuild is complete
            // for that.
            // Finally, request new 0-dbs and decommission unreadable ones.
            .map(|(ci, state)| {
                let explorer = explorer.clone();
                get_user_zdb(explorer, ci, state)
            })
            .collect::<Vec<_>>();

        Box::pin(
            async move { join(join_all(user_replacements), join_all(seq_replacements)).await }
                .into_actor(self)
                .then(|res, actor, _| {
                    // Explicit destructure of the tuple. This is needed because:
                    // - The for loop below would result in a partial move of res.1
                    // - The async move of res.0 later would error because it moves the entirety of
                    // res, while its already partially moved.
                    let (meta_res, data_res) = res;
                    for result in data_res {
                        match result {
                            Err(e) => error!("Could not provision new 0-db: {}", e),
                            Ok((ci, db, state)) => {
                                actor.managed_seq_dbs.insert(ci, (db, state));
                            }
                        };
                    }

                    // Since we need a static return type, remember an eventual error here
                    let mut error = false;
                    // Verify new nodes
                    let mut meta_info = Vec::with_capacity(meta_res.len());
                    for result in meta_res {
                        match result {
                            Err(e) => {
                                error!("Could not provision new 0-db: {}", e);
                                error = true;
                            }
                            Ok((ci, db, state)) => {
                                meta_info.push((ci, (db, state)));
                            }
                        };
                    }
                    let explorer = actor.config_addr.clone();
                    let metastore = actor.metastore.clone();
                    let metrics = actor.metrics.clone();
                    let config_actor = actor.config_addr.clone();

                    let old_nodes = actor
                        .managed_meta_dbs
                        .values()
                        .filter_map(|(con, _)| con.clone())
                        .collect();
                    let new_nodes = actor
                        .managed_meta_dbs
                        .iter()
                        .filter_map(|(ci, (con, state))| {
                            if !state.is_terminal() {
                                Some((ci.clone(), (con.clone(), state.clone())))
                            } else {
                                None
                            }
                        })
                        .chain(meta_info)
                        .collect::<HashMap<_, _>>();
                    async move {
                        // If there was an error for one of the nodes, this is pointless.
                        if error {
                            return None;
                        }
                        // Get config
                        let config = match explorer.send(GetConfig).await {
                            Ok(cfg) => cfg,
                            Err(e) => {
                                error!("Failed to get running config: {}", e);
                                return None;
                            }
                        };
                        let Meta::Zdb(meta_config) = config.meta();
                        let encoder = meta_config.encoder();
                        let encryptor = match meta_config.encryption() {
                            Encryption::Aes(key) => AesGcm::new(key.clone()),
                        };
                        let old_cluster = ZdbMetaStore::new(
                            old_nodes,
                            encoder.clone(),
                            encryptor.clone(),
                            meta_config.prefix().to_owned(),
                            config.virtual_root().clone(),
                        );
                        let new_cluster = ZdbMetaStore::new(
                            new_nodes
                                .values()
                                .filter_map(|(con, _)| con.clone())
                                .collect(),
                            encoder.clone(),
                            encryptor.clone(),
                            meta_config.prefix().to_owned(),
                            config.virtual_root().clone(),
                        );
                        // Disable writes on the metastore during the rebuild.
                        if let Err(e) = metastore.send(MarkWriteable { writeable: false }).await {
                            error!("Could not mark metastore as read-only: {}", e);
                            return None;
                        }
                        if let Err(e) = new_cluster.rebuild_cluster(&old_cluster).await {
                            error!("Could not rebuild cluster: {}", e);
                            return None;
                        };

                        // TODO: send new nodes to config and save it
                        let metastore_writeable = new_cluster.writable();
                        if let Err(e) = metastore
                            .send(ReplaceMetaStore {
                                new_store: Box::new(new_cluster),
                                writeable: metastore_writeable,
                            })
                            .await
                        {
                            error!("Could not send new cluster to metastore: {}", e);
                            return None;
                        };

                        if let Err(e) = config_actor
                            .send(ReplaceMetaBackend {
                                new_nodes: new_nodes.keys().cloned().collect(),
                            })
                            .await
                        {
                            error!("Could not update metadata stores in config: {}", e);
                            return None;
                        };

                        // Enable writes on the metastore again.
                        if let Err(e) = metastore.send(MarkWriteable { writeable: true }).await {
                            error!("Could not mark metadata cluster as writeable: {}", e);
                            return None;
                        };

                        // Now remove the backends to replace from the set of managed ones.
                        for key in new_nodes.keys() {
                            metrics.do_send(SetDataBackendInfo {
                                ci: key.clone(),
                                info: None,
                            });
                        }

                        Some(new_nodes)
                    }
                    .into_actor(actor)
                })
                .map(|res, actor, _| {
                    // TODO: if an error occurred for metadata nodes, cancel them again.
                    if let Some(nodes) = res {
                        actor.managed_meta_dbs = nodes;
                    }
                }),
        )
    }
}

/// IntermediateRequestState for resolving cached connections
enum IrState {
    Cached((SequentialZdb, BackendState)),
    NotFound(ZdbConnectionInfo),
}
impl Handler<RequestBackends> for BackendManagerActor {
    type Result = ResponseFuture<Vec<Result<Option<SequentialZdb>, ZstorError>>>;

    fn handle(&mut self, msg: RequestBackends, _: &mut Self::Context) -> Self::Result {
        let mut cached_cons = Vec::with_capacity(msg.backend_requests.len());
        for request in &msg.backend_requests {
            let cached_con = if let Some((c, state)) = self.managed_seq_dbs.get(request) {
                match c {
                    Some(con) => IrState::Cached((con.clone(), state.clone())),
                    // None means there is no readily available connection to the backend, so create a
                    // new one. This means state is not healthy anyway.
                    None => IrState::NotFound(request.clone()),
                }
            } else {
                IrState::NotFound(request.clone())
            };
            cached_cons.push(cached_con);
        }

        Box::pin(async move {
            let interest = msg.interest;
            let futs = cached_cons.into_iter().map(|mut cc| async move {
                if let IrState::Cached((con, state)) = cc {
                    match interest {
                        StateInterest::Writeable if state.is_writeable() => return Ok(Some(con)),
                        StateInterest::Readable if state.is_readable() => return Ok(Some(con)),
                        // Connection doesn't have the proper state, extract the connection info so
                        // we can attempt a reconnect.
                        _ => {
                            cc = IrState::NotFound(con.connection_info().clone());
                        }
                    }
                }

                if let IrState::NotFound(ci) = cc {
                    // No existing connection, attempt a new one
                    let db = SequentialZdb::new(ci).await?;
                    if let StateInterest::Readable = interest {
                        return Ok(Some(db));
                    }

                    let mut state = BackendState::new();
                    let ns_info = db.ns_info().await?;
                    state.mark_healthy(ns_info.free_space());

                    // Interest must be writeable here.
                    if state.is_writeable() {
                        return Ok(Some(db));
                    }

                    Ok(None)
                } else {
                    unreachable!();
                }
            });
            join_all(futs).await
        })
    }
}

/// This function is a workaround for the fact that the compiler cannot properly infer the returned
/// type if it is inlined at the call site as an "async move { ... }". Because "impl Trait" is not
/// allowed in generics at call sited, the join_all call above will complain as it does not
/// understand the return type, which is hugely complex, as  it is a Map of an intoIter with a
/// FnMut which returns "something which impls Future", and this something has to be precisely
/// named sadly.
async fn get_seq_zdb(
    explorer: Recipient<ExpandStorage>,
    ci: ZdbConnectionInfo,
    state: BackendState,
) -> Result<(ZdbConnectionInfo, Option<SequentialZdb>, BackendState), ZstorError> {
    let res = explorer
        .send(ExpandStorage {
            existing_zdb: Some(ci),
            // Only try to decommission the old 0-db if it is no longer readable.
            // We don't want to decommission backends in an unknown state just yet, but those
            // should not be included here since [`BackendState::Unknown`] is not considered
            // terminal.
            decomission: !state.is_readable(),
            size_request: if state.is_readable() {
                SizeRequest::Increase(DEFAULT_DATA_BACKEND_SIZE_GIB)
            } else {
                SizeRequest::Exact(DEFAULT_DATA_BACKEND_SIZE_GIB)
            },
            mode: ZdbRunMode::Seq,
        })
        .await??;
    let mut state = BackendState::new();
    match SequentialZdb::new(res.clone()).await {
        Ok(db) => {
            match db.ns_info().await {
                Ok(info) => state.mark_healthy(info.free_space()),
                Err(_) => state.mark_unreachable(),
            };
            Ok((res, Some(db), state))
        }
        Err(e) => {
            error!("Could not connect to new 0-db: {}", e);
            Ok((res, None, state))
        }
    }
}

/// Reserve a new [`UserKeyZdb`] instance. This is a separate method for the same reasons as
/// [`get_seq_zdb`].
async fn get_user_zdb(
    explorer: Recipient<ExpandStorage>,
    ci: ZdbConnectionInfo,
    state: BackendState,
) -> Result<(ZdbConnectionInfo, Option<UserKeyZdb>, BackendState), ZstorError> {
    let res = explorer
        .send(ExpandStorage {
            existing_zdb: Some(ci),
            // Only try to decommission the old 0-db if it is no longer readable.
            // We don't want to decommission backends in an unknown state just yet, but those
            // should not be included here since [`BackendState::Unknown`] is not considered
            // terminal.
            // IMPORTANT: this also allows a rebuild if a majority of nodes are full.
            decomission: !state.is_readable(),
            size_request: if state.is_readable() {
                SizeRequest::Increase(DEFAULT_META_BACKEND_SIZE_GIB)
            } else {
                SizeRequest::Exact(DEFAULT_META_BACKEND_SIZE_GIB)
            },
            mode: ZdbRunMode::User,
        })
        .await??;
    let mut state = BackendState::new();
    match UserKeyZdb::new(res.clone()).await {
        Ok(db) => {
            match db.ns_info().await {
                Ok(info) => state.mark_healthy(info.free_space()),
                Err(_) => state.mark_unreachable(),
            };
            Ok((res, Some(db), state))
        }
        Err(e) => {
            error!("Could not connect to new 0-db: {}", e);
            Ok((res, None, state))
        }
    }
}

/// An interest in the state of a db connection when requesting one or multiple. Only connections
/// which are sufficiently capable are returned.
#[derive(Debug, Clone, Copy)]
pub enum StateInterest {
    /// The connection can be read from, i.e. a previously stored object can be recovered. This
    /// currently means an active / healthy connection.
    Readable,
    /// The connection can be written to, i.e. the backend has sufficient space left, and there is
    /// an active / healthy connection. This also implies the connection is
    /// [`StateInterest::Readable`].
    Writeable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Information about the state of a backend.
pub enum BackendState {
    /// The state can't be decided at the moment. The time at which we last saw this backend healthy
    /// is also recorded
    Unknown(Instant),
    /// The backend is healthy and has sufficient free space available.
    Healthy,
    /// The backend is (supposedly permanently) unreachable.
    Unreachable,
    /// The shard reports low space, amount of space left is included in bytes
    LowSpace(u64),
}

impl BackendState {
    /// Create a new [`BackendState`]. Since no check has been done at this point, it will be in
    /// the [`BackendState::Unknown`] state.
    pub fn new() -> BackendState {
        BackendState::Unknown(Instant::now())
    }

    /// Indicate that the backend is (currently) unreachable. Depending on the previous state, this
    /// might change the state.
    pub fn mark_unreachable(&mut self) {
        match self {
            BackendState::Unreachable => {}
            BackendState::Unknown(since) if since.elapsed() >= MISSING_DURATION => {
                debug!("Backend state changed to unreachable");
                *self = BackendState::Unreachable;
            }
            BackendState::Unknown(since) if since.elapsed() < MISSING_DURATION => (),
            _ => *self = BackendState::Unknown(Instant::now()),
        }
    }

    /// Indicate that the backend is healthy, and has the given amount of space left. Depending on
    /// the provided space, this might transition the backend into [`BackendState::LowSpace`] state.
    pub fn mark_healthy(&mut self, space_left: u64) {
        if space_left <= FREESPACE_TRESHOLD {
            *self = BackendState::LowSpace(space_left)
        } else {
            *self = BackendState::Healthy
        }
    }

    /// Identify if the backend is considered definitely readable.
    pub fn is_readable(&self) -> bool {
        matches!(self, BackendState::Healthy | BackendState::Unknown(_))
    }

    /// Identify if the backend is considered definitely writeable.
    pub fn is_writeable(&self) -> bool {
        matches!(self, BackendState::Healthy)
    }

    /// Identify if the backend is considered to be in a "terminal" state. A terminal state means
    /// that a new backend should be reserved to maintain system stability.
    pub fn is_terminal(&self) -> bool {
        matches!(self, BackendState::Unreachable | BackendState::LowSpace(_))
    }
}

impl Default for BackendState {
    fn default() -> Self {
        Self::new()
    }
}
