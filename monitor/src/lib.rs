use backend::BackendState;
use config::Config;
use log::{debug, error};
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;
use tokio::time::interval;
use zstor_v2::config::Config as ZStorConfig;
use zstor_v2::zdb::ZdbConnectionInfo;

pub mod backend;
pub mod config;

pub type MonitorResult<T> = Result<T, MonitorError>;

const BACKEND_MONITOR_INTERVAL_DURATION: u64 = 60 * 1; // 60 seconds => 1 minute
const BACKEND_MONITOR_INTERVAL: Duration = Duration::from_secs(BACKEND_MONITOR_INTERVAL_DURATION);
const MAX_CONCURRENT_CONNECTIONS: usize = 10;

pub struct Monitor {
    cfg: Config,
}

impl Monitor {
    /// Create a new monitor with a given config
    pub fn new(cfg: Config) -> Self {
        Monitor { cfg }
    }

    /// Start the monitor, consuming it. This does not return until the monitor itself returns.
    pub async fn start(self) -> MonitorResult<()> {
        let (tx, rx) = channel::<()>(1);
        // TODO
        // Monitor backend health: reachable + space
        //   - Unreachable backend: trigger rebuild
        // Monitor data dir size if required:
        //   - delete least used data file if size above treshold
        //   - make sure datafile is encoded
        // Monitor failed writes:
        //   - Periodically retry object write of failed writes TODO: how to
        let mut zstor_config_file = File::open(self.cfg.zstor_config_path())
            .await
            .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;
        let mut buf = Vec::new();
        zstor_config_file
            .read_to_end(&mut buf)
            .await
            .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;

        unimplemented!();
    }

    pub async fn monitor_backends(&self, mut rx: Receiver<()>) -> JoinHandle<()> {
        let config = self.cfg.clone();
        let mut backends = HashMap::<ZdbConnectionInfo, BackendState>::new();
        tokio::spawn(async move {
            let mut ticker = interval(BACKEND_MONITOR_INTERVAL);
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }
                    _ = ticker.tick() => {
                        let zstor_config = match read_zstor_config(config.zstor_config_path()).await {
                            Ok(cfg) => cfg,
                            Err(e) => {
                                error!("could not read zstor config: {}", e);
                                continue
                            }
                        };
                        for backend in zstor_config.backends() {
                            backends.entry(backend.clone()).or_insert(BackendState::Unknown(std::time::Instant::now()));
                        }

                        for backend_group in backends.keys().collect::<Vec<_>>().chunks(MAX_CONCURRENT_CONNECTIONS) {
                            let mut futs = Vec::new(); // JOINHANDLES
                            for backend in backend_group {

                            }
                        }

                        // Connect to backends and check state
                        // TODO
                    }
                }
            }
        })
    }
}

async fn read_zstor_config(cfg_path: &Path) -> MonitorResult<ZStorConfig> {
    let mut zstor_config_file = File::open(cfg_path)
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;
    let mut buf = Vec::new();
    zstor_config_file
        .read_to_end(&mut buf)
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;

    Ok(toml::from_slice(&buf).map_err(|e| MonitorError {
        kind: ErrorKind::Config,
        internal: InternalError::Format(e),
    })?)
}

#[derive(Debug)]
pub struct MonitorError {
    kind: ErrorKind,
    internal: InternalError,
}

impl fmt::Display for MonitorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "monitoring error in {}: {}", self.kind, self.internal)
    }
}

impl error::Error for MonitorError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self.internal {
            InternalError::IO(ref e) => Some(e),
            InternalError::Format(ref e) => Some(e),
        }
    }
}

impl MonitorError {
    pub fn new_io(kind: ErrorKind, e: io::Error) -> Self {
        MonitorError {
            kind,
            internal: InternalError::IO(e),
        }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Config,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorKind::Config => "Config",
            }
        )
    }
}

#[derive(Debug)]
pub enum InternalError {
    IO(io::Error),
    Format(toml::de::Error),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::IO(ref e) => e as &dyn fmt::Display,
                InternalError::Format(ref e) => e,
            }
        )
    }
}
