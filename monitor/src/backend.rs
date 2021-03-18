use std::time::Instant;

pub struct BackendMonitor {}

#[derive(Debug)]
pub enum BackendState {
    // The state can't be decided at the moment. The time at which we last saw this backend healthy
    // is also recorded
    Unknown(Instant),
    Healthy,
    Unreachable,
    // The shard reports low space, amount of space left is included in bytes
    LowSpace(usize),
}
