use crate::zdb::{NsInfo, ZdbConnectionInfo};
use actix::prelude::*;
use std::{collections::HashMap, fmt};

/// A metrics actor collecting metrics from the system.
pub struct MetricsActor {
    zdbs: HashMap<ZdbConnectionInfo, NsInfo>,
    successfull_zstor_commands: HashMap<ZstorCommandID, usize>,
    failed_zstor_commands: HashMap<ZstorCommandID, usize>,
}

impl MetricsActor {
    /// Create a new [`MetricsActor`].
    pub fn new() -> MetricsActor {
        Self {
            zdbs: HashMap::new(),
            successfull_zstor_commands: HashMap::new(),
            failed_zstor_commands: HashMap::new(),
        }
    }
}

impl Default for MetricsActor {
    fn default() -> Self {
        Self::new()
    }
}

/// Message updating the status of a backend.
#[derive(Message)]
#[rtype(result = "()")]
pub struct SetBackendInfo {
    /// Info identifying the backend.
    pub ci: ZdbConnectionInfo,
    /// The backend stats. If this is None, the backend is removed.
    pub info: Option<NsInfo>,
}

/// Message updating the amount of finished zstor commands.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ZstorCommandFinsihed {
    /// The command which finished.
    pub id: ZstorCommandID,
    /// Whether the command finished successfully or not.
    pub success: bool,
}

impl Actor for MetricsActor {
    type Context = Context<Self>;
}

impl Handler<SetBackendInfo> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: SetBackendInfo, _: &mut Self::Context) -> Self::Result {
        if let Some(info) = msg.info {
            self.zdbs.insert(msg.ci, info);
        } else {
            self.zdbs.remove(&msg.ci);
        }
    }
}

impl Handler<ZstorCommandFinsihed> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: ZstorCommandFinsihed, _: &mut Self::Context) -> Self::Result {
        if msg.success {
            *self.successfull_zstor_commands.entry(msg.id).or_insert(0) += 1;
        } else {
            *self.failed_zstor_commands.entry(msg.id).or_insert(0) += 1;
        }
    }
}

/// Possible zstor commands.
#[derive(Hash, PartialEq, Eq)]
pub enum ZstorCommandID {
    /// Store command.
    Store,
    /// Retrieve command.
    Retrieve,
    /// Rebuild command.
    Rebuild,
    /// Check command.
    Check,
}

impl fmt::Display for ZstorCommandID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ZstorCommandID::Store => "Store",
                ZstorCommandID::Retrieve => "Retrieve",
                ZstorCommandID::Rebuild => "Rebuild",
                ZstorCommandID::Check => "Check",
            }
        )
    }
}
