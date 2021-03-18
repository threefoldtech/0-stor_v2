use std::time::{Duration, Instant};

// Amount of time a backend can be unreachable before it is actually considered unreachable
const MISSING_DURATION: Duration = Duration::from_secs(15 * 60);

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

impl BackendState {
    pub fn is_unreachable(&mut self) {
        match self {
            BackendState::Unknown(since) if since.elapsed() >= MISSING_DURATION => {
                *self = BackendState::Unreachable;
            }
            BackendState::Unknown(since) if since.elapsed() < MISSING_DURATION => (),
            _ => *self = BackendState::Unknown(Instant::now()),
        }
    }

    pub fn is_healthy(&mut self) {
        *self = BackendState::Healthy;
    }

    pub fn is_lowspace(&mut self, left: usize) {
        *self = BackendState::LowSpace(left);
    }
}
