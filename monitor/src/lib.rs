use config::Config;
use futures::future::join_all;
use log::{debug, error, info};
use monitors::{monitor_backends, monitor_failed_writes, monitor_ns_datasize};
use std::error;
use std::fmt;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::fs::{self, File};
use tokio::io::{self, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::broadcast::{channel, Sender};
use tokio::task::{JoinError, JoinHandle};
use zstor_v2::config::Config as ZStorConfig;
use zstor_v2::etcd::EtcdError;
use zstor_v2::ZstorError;

pub mod backend;
pub mod config;
pub mod monitors;

pub type MonitorResult<T> = Result<T, MonitorError>;

const ZDBFS_META: &str = "zdbfs-meta";
const ZDBFS_DATA: &str = "zdbfs-data";

pub struct Monitor {
    cfg: Config,
}

impl Monitor {
    /// Create a new monitor with a given config
    pub fn new(cfg: Config) -> Self {
        Monitor { cfg }
    }

    /// Start the monitor, consuming it. This does not return until the monitor itself returns.
    pub async fn start(self) -> MonitorResult<(Sender<()>, JoinHandle<()>)> {
        let (tx, _) = channel::<()>(1);
        // clone sender because we move it into the task we spawn later. Drop again after all sub
        // tasks are spawned to avoid blocking on exits.
        let rx_factory = tx.clone();

        // TODO: Should an error here be fatal?
        if let Err(e) = self.recover_index(ZDBFS_META).await {
            error!("Could not recover {} index: {}", ZDBFS_META, e);
        }
        if let Err(e) = self.recover_index(ZDBFS_DATA).await {
            error!("Could not recover {} index: {}", ZDBFS_DATA, e);
        }

        let config = self.cfg.clone();

        let handle = tokio::spawn(async move {
            let mut handles = Vec::new();
            let rxc = rx_factory.subscribe();
            let cfge = config.clone();
            handles.push(tokio::spawn(async {
                monitor_backends(rxc, cfge).await.await
            }));
            let rxc = rx_factory.subscribe();
            let cfge = config.clone();
            handles.push(tokio::spawn(async {
                monitor_failed_writes(rxc, cfge).await.await
            }));
            if config.max_zdb_data_dir_size().is_some() {
                let rxc = rx_factory.subscribe();
                handles.push(tokio::spawn(async {
                    monitor_ns_datasize(rxc, ZDBFS_DATA.to_string(), config)
                        .await
                        .unwrap()
                        .await
                }));
            }

            // drop our duplicate sender so we can send a termination signal to all monitors by
            // dropping the single sender we return.
            drop(rx_factory);

            // should be safe to ignore errors here
            // TODO: maybe properly do this?
            join_all(handles).await;
        });

        Ok((tx, handle))
    }

    pub async fn recover_index(&self, ns: &str) -> MonitorResult<()> {
        let mut path = self.cfg.zdb_index_dir_path().clone();
        path.push(ns);

        debug!("Attempting to recover index data at {:?}", path);

        // try to recover namespace file
        // TODO: is this always present?
        let mut namespace_path = path.clone();
        namespace_path.push("zdb-namespace");

        // TODO: collapse this into separate function and call that
        if file_is_uploaded(&namespace_path, &self.cfg).await? {
            debug!("namespace file found in storage, checking local filesystem");
            // exists on Path is blocking, but it essentially just tests if a `metadata` call
            // returns ok.
            if fs::metadata(&namespace_path).await.is_err() {
                info!("index namespace file is encoded and not present locally, attempt recovery");
                // At this point we know that the file is uploaded and not present locally, so an
                // error here is terminal for the whole recovery process.
                download_file(&namespace_path, &self.cfg).await?;
            }
        }

        // Recover regular index files
        let mut file_idx = 0usize;
        loop {
            let mut index_path = path.clone();
            index_path.push(format!("zdb-index-{:05}", file_idx));

            if file_is_uploaded(&namespace_path, &self.cfg).await? {
                debug!("namespace file found in storage, checking local filesystem");
                // exists on Path is blocking, but it essentially just tests if a `metadata` call
                // returns ok.
                if fs::metadata(&namespace_path).await.is_err() {
                    info!(
                        "index namespace file is encoded and not present locally, attempt recovery"
                    );
                    // At this point we know that the file is uploaded and not present locally, so an
                    // error here is terminal for the whole recovery process.
                    download_file(&namespace_path, &self.cfg).await?;
                }
            } else {
                break;
            }

            file_idx += 1;
        }

        Ok(())
    }
}

async fn get_dir_entries(path: &Path) -> io::Result<Vec<(PathBuf, Metadata)>> {
    let dir_meta = fs::metadata(&path).await?;
    if !dir_meta.is_dir() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    let mut entries = fs::read_dir(&path).await?;
    let mut file_entries = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        // failure to get one files metadata will be considered fatal
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }
        file_entries.push((entry.path(), meta));
    }

    Ok(file_entries)
}

/// Return true if the file was deleted
async fn attempt_removal(path: &Path, cfg: &Config) -> MonitorResult<bool> {
    let path = fs::canonicalize(path)
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Fs, e))?;
    if !file_is_uploaded(&path, cfg).await? {
        return Ok(false);
    }

    // file is uploaded
    fs::remove_file(&path)
        .await
        .map(|_| true)
        .map_err(|e| MonitorError::new_io(ErrorKind::Fs, e))
}

/// Triggers the zstor binary to perform a rebuild command on the given key.
async fn rebuild_key(key: &str, cfg: &Config) -> MonitorResult<()> {
    if Command::new(cfg.zstor_bin_path())
        .arg("--config")
        .arg(cfg.zstor_config_path())
        .arg("rebuild")
        .arg("-k")
        .arg(key)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .wait()
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .success()
    {
        Ok(())
    } else {
        // TODO: proper error
        Err(MonitorError::new_io(
            ErrorKind::Exec,
            std::io::Error::from(std::io::ErrorKind::Other),
        ))
    }
}

/// Trigger the zstor binary to try and upload a file
async fn upload_file(
    data_path: &PathBuf,
    key_path: &Option<PathBuf>,
    should_delete: bool,
    cfg: &Config,
) -> MonitorResult<()> {
    let mut cmd = Command::new(cfg.zstor_bin_path());
    cmd.arg("--config")
        .arg(cfg.zstor_config_path())
        .arg("store");
    if should_delete {
        cmd.arg("--delete");
    }
    if let Some(kp) = key_path {
        cmd.arg("--key-path").arg(kp);
    };
    if cmd
        .arg("--file")
        .arg(data_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .wait()
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .success()
    {
        Ok(())
    } else {
        // TODO: proper error
        Err(MonitorError::new_io(
            ErrorKind::Exec,
            std::io::Error::from(std::io::ErrorKind::Other),
        ))
    }
}

/// Trigger the zstor binary to try and download a file
async fn download_file(path: &PathBuf, cfg: &Config) -> MonitorResult<()> {
    if Command::new(cfg.zstor_bin_path())
        .arg("--config")
        .arg(cfg.zstor_config_path())
        .arg("retrieve")
        .arg("-f")
        .arg(path.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .wait()
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .success()
    {
        Ok(())
    } else {
        // TODO: proper error
        Err(MonitorError::new_io(
            ErrorKind::Exec,
            std::io::Error::from(std::io::ErrorKind::Other),
        ))
    }
}

/// Trigger the zstor binary to perform a check on the file. If true, the file is uploaded
async fn file_is_uploaded(path: &Path, cfg: &Config) -> MonitorResult<bool> {
    Ok(Command::new(cfg.zstor_bin_path())
        .arg("--config")
        .arg(cfg.zstor_config_path())
        .arg("check")
        .arg("-f")
        .arg(path.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .wait()
        .await
        .map_err(|e| MonitorError::new_io(ErrorKind::Exec, e))?
        .success())
}

async fn read_zstor_config(cfg_path: &Path) -> MonitorResult<ZStorConfig> {
    debug!("reading zstor config at {:?}", cfg_path);
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
            InternalError::Task(ref e) => Some(e),
            InternalError::Zstor(ref e) => Some(e),
            InternalError::Etcd(ref e) => Some(e),
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

impl From<JoinError> for MonitorError {
    fn from(e: JoinError) -> Self {
        MonitorError {
            kind: ErrorKind::Task,
            internal: InternalError::Task(e),
        }
    }
}

impl From<ZstorError> for MonitorError {
    fn from(e: ZstorError) -> Self {
        MonitorError {
            kind: ErrorKind::Zstor,
            internal: InternalError::Zstor(e),
        }
    }
}

impl From<EtcdError> for MonitorError {
    fn from(e: EtcdError) -> Self {
        MonitorError {
            kind: ErrorKind::Meta,
            internal: InternalError::Etcd(e),
        }
    }
}

impl From<toml::de::Error> for MonitorError {
    fn from(e: toml::de::Error) -> Self {
        MonitorError {
            kind: ErrorKind::Config,
            internal: InternalError::Format(e),
        }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Config,
    Task,
    Zstor,
    Meta,
    Exec,
    Fs,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorKind::Config => "Config",
                ErrorKind::Task => "Unrecoverable failure in asynchronous task",
                ErrorKind::Zstor => "0-stor operation error",
                ErrorKind::Meta => "Metadata",
                ErrorKind::Exec => "Executing system binary",
                ErrorKind::Fs => "Filesystem error",
            }
        )
    }
}

#[derive(Debug)]
pub enum InternalError {
    IO(io::Error),
    Format(toml::de::Error),
    Task(tokio::task::JoinError),
    Zstor(ZstorError),
    Etcd(EtcdError),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::IO(ref e) => e as &dyn fmt::Display,
                InternalError::Format(ref e) => e,
                InternalError::Task(ref e) => e,
                InternalError::Zstor(ref e) => e,
                InternalError::Etcd(ref e) => e,
            }
        )
    }
}
