use crate::{
    zdb::{ZdbConnectionInfo, ZdbRunMode},
    ZstorError, ZstorErrorKind,
};
use actix::prelude::*;
use log::warn;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Drop in replacement for the [`ExplorerActor`] which does not connect to the threefold grid.
pub struct NopExplorerActor;

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

impl Actor for NopExplorerActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        warn!("NopExplorer started, automatic capacity managment is disabled");
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
