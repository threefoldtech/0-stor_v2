use crate::actors::config::{ConfigActor, GetConfig};
use crate::zdb::{SequentialZdb, ZdbConnectionInfo};
use actix::prelude::*;
use futures::{stream, StreamExt};
use log::{debug, error, warn};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

/// Check backends every 3 seconds.
const BACKEND_CHECK_INTERVAL_SECONDS: u64 = 2;

/// An actor implementation of a backend manager.
pub struct BackendManagerActor {
    config_addr: Addr<ConfigActor>,
    managed_seq_dbs: HashMap<ZdbConnectionInfo, (Arc<SequentialZdb>, BackendState)>,
}

impl BackendManagerActor {
    /// Create a new [`BackendManagerActor`].
    pub fn new(config_addr: Addr<ConfigActor>) -> BackendManagerActor {
        Self {
            config_addr,
            managed_seq_dbs: HashMap::new(),
        }
    }

    /// Send a [`CheckBackends`] command to this actor.
    fn check_backends(&mut self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(CheckBackends);
    }
}

/// Message requesting the actor checks the backends, and takes action if a 0-db has reached a
/// terminal state.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CheckBackends;

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
                                        // We can't track the db here as we don't have an object to
                                        // track.
                                        // TODO
                                        return None;
                                    }
                                };
                                let db = Arc::new(db);
                                let ns_info = match db.ns_info().await {
                                    Ok(info) => info,
                                    Err(e) => {
                                        warn!("Failed to get ns info from backend {}: {}", ci, e);
                                        return Some((
                                            ci.clone(),
                                            (db, BackendState::Unknown(Instant::now())),
                                        ));
                                    }
                                };

                                let free_space = ns_info.free_space();
                                let state = if free_space <= FREESPACE_TRESHOLD {
                                    BackendState::LowSpace(free_space)
                                } else {
                                    BackendState::Healthy
                                };

                                Some((ci.clone(), (db, state)))
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
        todo!();
    }
}

/// Amount of time a backend can be unreachable before it is actually considered unreachable.
/// Currently set to 15 minutes.
const MISSING_DURATION: Duration = Duration::from_secs(15 * 60);
/// The amount of free space left in a backend before it is considered to be full. Currently this is
/// 100 MiB.
const FREESPACE_TRESHOLD: u64 = 100 * (1 << 20);

#[derive(Debug, PartialEq)]
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

    /// Identify if the backend is considered readable.
    pub fn is_readable(&self) -> bool {
        // we still consider unknown state to be readable.
        *self != BackendState::Unreachable
    }

    /// Identify if the backend is considered writeable.
    pub fn is_writeable(&self) -> bool {
        match self {
            BackendState::Unreachable => false,
            BackendState::LowSpace(size) if *size <= FREESPACE_TRESHOLD => false,
            _ => true,
        }
    }
}
