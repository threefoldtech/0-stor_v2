use crate::actors::{
    config::{ConfigActor, GetConfig},
    explorer::{ExpandStorage, ExplorerActor},
    metrics::{MetricsActor, SetDataBackendInfo, SetMetaBackendInfo},
};
use crate::{
    config::Meta,
    zdb::{SequentialZdb, UserKeyZdb, ZdbConnectionInfo, ZdbRunMode},
    ZstorError,
};
use actix::prelude::*;
use futures::{
    future::join_all,
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
/// Default to 0-db backends of 100GiB in size.
const DEFAULT_BACKEND_SIZE_GIB: u64 = 100;

/// An actor implementation of a backend manager.
pub struct BackendManagerActor {
    config_addr: Addr<ConfigActor>,
    explorer: Addr<ExplorerActor>,
    metrics: Addr<MetricsActor>,
    managed_seq_dbs: HashMap<ZdbConnectionInfo, (Option<SequentialZdb>, BackendState)>,
    managed_meta_dbs: HashMap<ZdbConnectionInfo, (Option<UserKeyZdb>, BackendState)>,
}

impl BackendManagerActor {
    /// Create a new [`BackendManagerActor`].
    pub fn new(
        config_addr: Addr<ConfigActor>,
        explorer: Addr<ExplorerActor>,
        metrics: Addr<MetricsActor>,
    ) -> BackendManagerActor {
        Self {
            config_addr,
            explorer,
            metrics,
            managed_seq_dbs: HashMap::new(),
            managed_meta_dbs: HashMap::new(),
        }
    }

    /// Send a [`CheckBackends`] command to this actor.
    fn check_backends(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckBackends);
    }

    /// Send a [`ReplaceBackends`] command to this actor
    fn replace_backends(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(ReplaceBackends);
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
                                        warn!(
                                            "Could not connect to backend {} in config file: {}",
                                            ci, e
                                        );
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
                                                warn!("Failed to connect to metadata backend {} in config file: {}", ci ,e);
                                                return Some((ci.clone(), (None, BackendState::new())));
                                            }
                                        };
                                        let ns_info = match db.ns_info().await {
                                            Ok(info) => info,
                                            Err(e) => {
                                                warn!("Failed to get ns info from metadata backend {}: {}", ci, e);
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
                let meta_info = join_all(futs).await;
                (data_info, meta_info)
            }
            .into_actor(self)
            .map(|res, actor, ctx| {
                let mut should_sweep = false;
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
                            *possible_con = new_db;
                        }
                        if !(new_state.is_readable() && new_state.is_writeable()) {
                            should_sweep = true;
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
                    if let Some((possible_con, old_state)) = actor.managed_seq_dbs.get_mut(&ci) {
                        if new_db.is_some() {
                            *possible_con = new_db;
                        }
                        if !(new_state.is_readable() && new_state.is_writeable()) {
                            should_sweep = true;
                        }
                        *old_state = new_state
                    }
                }

                if should_sweep {
                    // Trigger a sweep of all managed backends, removing those at the end of their
                    // (managed) lifetime, and try to reserve new ones in their place.
                    actor.replace_backends(ctx);
                }
            }),
        )
    }
}

impl Handler<ReplaceBackends> for BackendManagerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: ReplaceBackends, _: &mut Self::Context) -> Self::Result {
        let explorer = self.explorer.clone();
        // Grab all backends which must be replaced.
        let replacements = self
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
        for (key, _) in &replacements {
            self.metrics.do_send(SetDataBackendInfo {
                ci: key.clone(),
                info: None,
            });
            self.managed_seq_dbs.remove(key);
        }

        // Finally, request new 0-dbs and decommission unreadable ones.
        let replacements = replacements
            .into_iter()
            .map(|(ci, state)| {
                let explorer = explorer.clone();
                get_seq_zdb(explorer, ci, state)
            })
            .collect::<Vec<_>>();

        Box::pin(
            async move { join_all(replacements).await }
                .into_actor(self)
                .map(|res, actor, _| {
                    for result in res {
                        match result {
                            Err(e) => error!("Could not provision new 0-db: {}", e),
                            Ok((ci, db, state)) => {
                                actor.managed_seq_dbs.insert(ci, (db, state));
                            }
                        };
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
            let cached_con = if let Some((c, state)) = self.managed_seq_dbs.get(&request) {
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
    explorer: Addr<ExplorerActor>,
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
            size_gib: DEFAULT_BACKEND_SIZE_GIB,
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

#[derive(Debug, Clone, PartialEq)]
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
    /// that a new backend should be reserved to maintain system .
    pub fn is_terminal(&self) -> bool {
        matches!(self, BackendState::Unreachable | BackendState::LowSpace(_))
    }
}

impl Default for BackendState {
    fn default() -> Self {
        Self::new()
    }
}
