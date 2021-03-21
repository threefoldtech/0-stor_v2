use backend::BackendState;
use config::Config;
use futures::future::join_all;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::{self, AsyncReadExt};
use tokio::process::Command;
use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::interval;
use zstor_v2::config::{Config as ZStorConfig, Meta};
use zstor_v2::etcd::{Etcd, EtcdError};
use zstor_v2::zdb::{Zdb, ZdbConnectionInfo};
use zstor_v2::ZstorError;

pub mod backend;
pub mod config;

pub type MonitorResult<T> = Result<T, MonitorError>;

const BACKEND_MONITOR_INTERVAL_DURATION: u64 = 60; // 60 seconds => 1 minute
const BACKEND_MONITOR_INTERVAL: Duration = Duration::from_secs(BACKEND_MONITOR_INTERVAL_DURATION);
const REPAIR_BACKLOG_RETRY_INTERVAL_DURATION: u64 = 60 * 5; // 5 minutes
const REPAIR_BACKLOG_RETRY_INTERVAL: Duration =
    Duration::from_secs(REPAIR_BACKLOG_RETRY_INTERVAL_DURATION);
const MAX_CONCURRENT_CONNECTIONS: usize = 10;

const ZDBFS_META: &str = "zdbfs-meta";
const ZDBFS_DATA: &str = "zdbfs-data";

const MI_B: u64 = 1 << 20;

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
                monitor_failures(rxc, cfge).await.await
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

/// Monitor a data file dir and attempt to keep it within a certain size.
///
/// Returns None if the config does not have a limit set, otherwise returns the JoinHandle for
/// the actual monitoring task.
pub async fn monitor_ns_datasize(
    mut rx: Receiver<()>,
    ns: String,
    config: Config,
) -> Option<JoinHandle<MonitorResult<()>>> {
    let size_limit = match config.max_zdb_data_dir_size() {
        None => {
            warn!("No size limit set in config, abort monitoring data dir size");
            return None;
        }
        Some(limit) => limit as u64 * MI_B,
    };
    let mut dir_path = config.zdb_data_dir_path().clone();
    dir_path.push(ns);

    Some(tokio::spawn(async move {
        // steal the backend monitor interval for now, TODO
        let mut ticker = interval(BACKEND_MONITOR_INTERVAL);

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down failed write monitor");
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("checking data dir size");

                    let mut entries = match get_dir_entries(&dir_path).await {
                        Ok(entries) => entries,
                        Err(e) => {
                            error!("Couldn't get directory entries for {:?}: {}", dir_path, e);
                            continue;
                        }
                    };

                    let mut dir_size: u64 = entries.iter().map(|(_, meta)| meta.len()).sum();

                    if dir_size < size_limit {
                        debug!("Directory {:?} is within size limits ({} < {})", dir_path, dir_size, size_limit);
                        continue
                    }

                    info!("Data dir size is too large, try to delete");
                    // sort files based on access time
                    // small -> large i.e. accessed the longest ago first
                    // unwraps are safe since the error only occurs if the platform does not
                    // support `atime`, and we don't implicitly support this functionality on
                    // those platforms.
                    entries.sort_by(|(_, meta_1), (_, meta_2)| meta_1.accessed().unwrap().cmp(&meta_2.accessed().unwrap()));

                    for (path, meta) in entries {
                        debug!("Attempt to delete file {:?} (size: {}, last accessed: {:?})", path, meta.len(), meta.accessed().unwrap());
                        let removed = match attempt_removal(&path, &config).await {
                            Ok(removed) => removed,
                            Err(e) => {
                                error!("Could not remove file: {}", e);
                                continue
                            },
                        };
                        if removed {
                            dir_size -= meta.len();
                            if dir_size < size_limit {
                                break
                            }
                        }
                    }

                    if dir_size < size_limit {
                        info!("Sufficiently reduced data dir ({:?}) size (new size: {})",dir_path ,dir_size);
                    } else {
                        warn!("Failed to reduce dir ({:?}) size to acceptable limit (currently {})", dir_path, dir_size);
                    }

                }
            }
        }
    }))
}

async fn monitor_failures(mut rx: Receiver<()>, config: Config) -> JoinHandle<MonitorResult<()>> {
    tokio::spawn(async move {
        let mut ticker = interval(REPAIR_BACKLOG_RETRY_INTERVAL);

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down failed write monitor");
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("Checking for failed uploads");
                    // read zstor config
                    let zstor_config = match read_zstor_config(config.zstor_config_path()).await {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            error!("could not read zstor config: {}", e);
                            continue
                        }
                    };

                    // connect to meta store
                    let mut cluster = match zstor_config.meta() {
                        Meta::ETCD(etcdconf) => match Etcd::new(etcdconf, zstor_config.virtual_root().clone()).await {
                            Ok(cluster) => cluster,
                            Err(e) => {error!("could not create metadata cluster: {}", e); continue},
                        },
                    };

                    let failures = match cluster.get_failures().await {
                        Ok(failures) => failures,
                        Err(e) => {
                            error!("Could not get failed uploads from metastore: {}", e);
                            continue
                        }
                    };

                    for failure_data in failures {
                        debug!("Attempting to upload previously failed file {:?}", failure_data.data_path());
                        match upload_file(&failure_data.data_path(), failure_data.key_dir_path(), failure_data.should_delete(), &config).await {
                            Ok(_) => {
                                info!("Successfully uploaded {:?} after previous failure", failure_data.data_path());
                                match cluster.delete_failure(&failure_data).await {
                                    Ok(_) => debug!("Removed failed upload of {:?} from metastore", failure_data.data_path()),
                                    Err(e) => {
                                        error!("Could not delete failed upload of {:?} from metastore: {}", failure_data.data_path(), e);
                                    },
                                }
                            },
                            Err(e) => {
                                error!("Could not upload {:?}: {}", failure_data.data_path(), e);
                                continue
                            }
                        }
                    }
                }
            }
        }
    })
}

async fn monitor_backends(mut rx: Receiver<()>, config: Config) -> JoinHandle<MonitorResult<()>> {
    let (repairer_tx, repairer_rx) = unbounded_channel::<String>();
    let repair_handle = spawn_repairer(repairer_rx, config.clone()).await;

    tokio::spawn(async move {
        let mut ticker = interval(BACKEND_MONITOR_INTERVAL);
        let mut backends = HashMap::<ZdbConnectionInfo, BackendState>::new();

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down backend monitor");
                    info!("waiting for repair queue shutdown");
                    drop(repairer_tx);
                    if let Err(e) = repair_handle.await {
                        error!("Error detected in repair queue shutdown: {}", e);
                    } else {
                        info!("repair queue shutdown completed");
                    }
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("Checking health of known backends");
                    let zstor_config = match read_zstor_config(config.zstor_config_path()).await {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            error!("could not read zstor config: {}", e);
                            continue
                        }
                    };

                   for backend in zstor_config.backends() {
                       backends
                           .entry(backend.clone())
                           .or_insert_with(|| BackendState::Unknown(std::time::Instant::now()));
                   }

                    let keys = backends.keys().into_iter().cloned().collect::<Vec<_>>();
                    for backend_group in keys.chunks(MAX_CONCURRENT_CONNECTIONS) {
                        let mut futs: Vec<JoinHandle<Result<_,(_,ZstorError)>>> = Vec::with_capacity(backend_group.len());
                        for backend in backend_group {
                            let backend = backend.clone();
                            futs.push(tokio::spawn(async move{
                                // connect to backend and get size
                                let ns_info = Zdb::new(backend.clone())
                                    .await
                                    .map_err(|e| (backend.clone(), e.into()))?
                                    .ns_info()
                                    .await
                                    .map_err(|e| (backend.clone(), e.into()))?;

                                Ok((backend, ns_info))

                            }));
                        }
                        for result in join_all(futs).await {
                            let (backend, info) = match result? {
                                Ok(succes) => succes,
                                Err((backend, e)) => {
                                    warn!("backend {} can not be reached {}", backend.address(), e);
                                    backends.entry(backend).and_modify(BackendState::mark_unreachable);
                                    continue;
                                }
                            };

                            if info.data_usage_percentage() > config.zdb_namespace_fill_treshold() {
                                warn!("backend {} has a high fill rate ({}%)", backend.address(), info.data_usage_percentage());
                                backends.entry(backend).and_modify(|bs| bs.mark_lowspace(info.data_usage_percentage()));
                            } else {
                                debug!("backend {} is healthy!", backend.address());
                                backends.entry(backend).and_modify(BackendState::mark_healthy);
                            }
                        }
                    }

                    let mut cluster = match zstor_config.meta() {
                        Meta::ETCD(etcdconf) => match Etcd::new(etcdconf, zstor_config.virtual_root().clone()).await {
                            Ok(cluster) => cluster,
                            Err(e) => {error!("could not create metadata cluster: {}", e); continue},
                        },
                    };

                    debug!("verifying objects to repair");
                    for (key, meta) in cluster.object_metas().await? {
                        let mut should_repair = false;
                        for shard in meta.shards() {
                            should_repair = match backends.get(shard.zdb()) {
                                Some(state) if !state.is_readable() => true,
                                // backend is readable, nothing to do
                                Some(_) => {continue},
                                None => {
                                    warn!("Object {} has shard on {} which has unknown state", key, shard.zdb().address());
                                    continue
                                },
                            };
                        }
                        if should_repair {
                            // unwrapping here is safe as an error would indicate a programming
                            // logic error
                            debug!("Requesting repair of object {}", key);
                            repairer_tx.send(key).unwrap();
                        }
                    }

                    if let Some(vdc_config) = config.vdc_config() {
                        debug!("attempt to replace unwriteable data backends if needed");
                        let vdc_client = reqwest::Client::new();
                        for backend in backends.keys() {
                            if cluster.is_replaced(backend).await? {
                                continue
                            }
                            if !backends[backend].is_writeable() {
                                debug!("attempt to replace backend {}", backend.address());
                                // replace backend
                                let res = match vdc_client.post(format!("{}//api/controller/zdb/add", vdc_config.url()))
                                    .json(&VdcZdbAddReqBody {
                                        password: vdc_config.password().to_string(),
                                        capacity: vdc_config.new_size(),
                                    })
                                    .send()
                                    .await {
                                        Ok(res) => res,
                                        Err(e) => {
                                            error!("could not contact evdc controller: {}", e);
                                            continue;
                                        },
                                    };
                                if !res.status().is_success() {
                                    error!("could not reserve new zdb, unexpected status code ({})", res.status());
                                    continue
                                }
                                // now mark backend as replaced
                                if let Err(e) = cluster.set_replaced(backend).await {
                                    error!("could not mark cluster as replaced: {}", e);
                                    continue
                                }
                            }
                        }
                    }

                }
            }
        }
    })
}

async fn spawn_repairer(mut rx: UnboundedReceiver<String>, config: Config) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(REPAIR_BACKLOG_RETRY_INTERVAL);
        let mut repair_backlog = Vec::new();
        loop {
            select! {
                key = rx.recv() => {
                    let key = match key {
                        None => {
                            info!("shutting down repairer");
                            return
                        },
                        Some(key) => key,
                    };
                    // attempt rebuild
                    if let Err(e) = rebuild_key(&key, &config).await {
                        error!("Could not rebuild item {}: {}", key, e);
                        repair_backlog.push(key);
                        continue
                    };
                }
                _ = ticker.tick() => {
                    debug!("Processing repair backlog");
                    for key in std::mem::replace(&mut repair_backlog, Vec::new()).drain(..) {
                        debug!("Trying to rebuild key {}, which is in the repair backlog", key);
                        if let Err(e) = rebuild_key(&key, &config).await {
                            error!("Could not rebuild item {}: {}", key, e);
                            repair_backlog.push(key);
                            continue
                        };
                    }
                }
            }
        }
    })
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

#[derive(Serialize)]
struct VdcZdbAddReqBody {
    password: String,
    capacity: usize,
}
