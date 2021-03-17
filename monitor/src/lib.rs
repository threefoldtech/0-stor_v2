use config::Config;
use std::error;
use std::fmt;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
use zstor_v2::config::Config as ZStorConfig;

pub mod config;

pub type MonitorResult<T> = Result<T, MonitorError>;

pub struct Monitor {}

impl Monitor {
    /// Start the monitor, consuming it. This does not return until the monitor itself returns.
    pub async fn start(self, cfg: Config) -> MonitorResult<()> {
        let mut zstor_config_file = File::open(cfg.zstor_config_path())
            .await
            .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;
        let mut buf = Vec::new();
        zstor_config_file
            .read_to_end(&mut buf)
            .await
            .map_err(|e| MonitorError::new_io(ErrorKind::Config, e))?;

        unimplemented!();
    }
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
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::IO(e) => e,
            }
        )
    }
}
