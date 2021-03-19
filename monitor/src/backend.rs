use std::time::{Duration, Instant};

// Amount of time a backend can be unreachable before it is actually considered unreachable
const MISSING_DURATION: Duration = Duration::from_secs(15 * 60);
const FULL_TRESHOLD: u8 = 95; // if a shard is 95% full we rotate it out for writing

#[derive(Debug, PartialEq)]
pub enum BackendState {
    // The state can't be decided at the moment. The time at which we last saw this backend healthy
    // is also recorded
    Unknown(Instant),
    Healthy,
    Unreachable,
    // The shard reports low space, amount of space left is included in bytes
    LowSpace(u8),
}

impl BackendState {
    pub fn mark_unreachable(&mut self) {
        match self {
            BackendState::Unknown(since) if since.elapsed() >= MISSING_DURATION => {
                *self = BackendState::Unreachable;
            }
            BackendState::Unknown(since) if since.elapsed() < MISSING_DURATION => (),
            _ => *self = BackendState::Unknown(Instant::now()),
        }
    }

    pub fn mark_healthy(&mut self) {
        *self = BackendState::Healthy;
    }

    pub fn mark_lowspace(&mut self, fill: u8) {
        *self = BackendState::LowSpace(fill);
    }

    pub fn is_readable(&self) -> bool {
        // we still consider unknown state to be readable
        return *self != BackendState::Unreachable;
    }

    pub fn is_writeable(&self) -> bool {
        match self {
            BackendState::Unreachable => false,
            BackendState::LowSpace(size) if *size >= FULL_TRESHOLD => false,
            _ => true,
        }
    }
}
