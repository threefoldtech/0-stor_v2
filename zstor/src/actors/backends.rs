use crate::actors::{
    config::{ConfigActor, GetConfig},
    explorer::{ExpandStorage, ExplorerActor},
};
use crate::{
    zdb::{SequentialZdb, ZdbConnectionInfo, ZdbRunMode},
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
    sync::Arc,
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
    managed_seq_dbs: HashMap<ZdbConnectionInfo, (Option<Arc<SequentialZdb>>, BackendState)>,
}

impl BackendManagerActor {
    /// Create a new [`BackendManagerActor`].
    pub fn new(
        config_addr: Addr<ConfigActor>,
        explorer: Addr<ExplorerActor>,
    ) -> BackendManagerActor {
        Self {
            config_addr,
            explorer,
            managed_seq_dbs: HashMap::new(),
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
                        HashMap::new()
                    }
                    Ok(config) => {
                        stream::iter(config.backends())
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
                                let db = Arc::new(db);
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
                            .await
                    }
                }
            }
            .into_actor(self)
            .map(|res, actor, _| actor.managed_seq_dbs = res),
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
        let backend_info = self
            .managed_seq_dbs
            .iter()
            // Can't use `Cloned` here because it expects a ref to a type, while we have a tuple of
            // refs.
            .map(|(ci, (db, state))| (ci.clone(), (db.clone(), state.clone())))
            .collect::<Vec<_>>();

        Box::pin(
            async move {
                let futs = backend_info
                    .into_iter()
                    .map(|(ci, (db, mut state))| async move {
                        if let Some(db) = db {
                            match db.ns_info().await {
                                Err(e) => {
                                    warn!("Failed to get ns_info from {}: {}", ci, e);
                                    state.mark_unreachable();
                                }
                                Ok(info) => {
                                    state.mark_healthy(info.free_space());
                                }
                            }
                            (ci, None, state)
                        } else {
                            // Try and get a new connection to the db.
                            if let Ok(db) = SequentialZdb::new(ci.clone()).await {
                                match db.ns_info().await {
                                    Err(e) => {
                                        warn!("Failed to get ns_info from {}: {}", ci, e);
                                        state.mark_unreachable();
                                    }
                                    Ok(info) => {
                                        state.mark_healthy(info.free_space());
                                    }
                                }
                                (ci, Some(Arc::new(db)), state)
                            } else {
                                state.mark_unreachable();
                                (ci, None, state)
                            }
                        }
                    });
                join_all(futs).await
            }
            .into_actor(self)
            .map(|res, actor, ctx| {
                let mut should_sweep = false;
                for (ci, new_db, new_state) in res.into_iter() {
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
) -> Result<(ZdbConnectionInfo, Option<Arc<SequentialZdb>>, BackendState), ZstorError> {
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
            Ok((res, Some(Arc::new(db)), state))
        }
        Err(e) => {
            error!("Could not connect to new 0-db: {}", e);
            Ok((res, None, state))
        }
    }
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
