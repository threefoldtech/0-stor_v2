use backend::BackendState;
use config::Config;
use futures::future::join_all;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
use tokio::process::Command;
use tokio::select;
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, UnboundedReceiver};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::interval;
use zstor_v2::config::{Config as ZStorConfig, Meta};
use zstor_v2::etcd::{Etcd, EtcdError};
use zstor_v2::zdb::{Zdb, ZdbConnectionInfo, ZdbError, ZdbResult};
use zstor_v2::{ZstorError, ZstorResult};

pub mod backend;
pub mod config;

pub type MonitorResult<T> = Result<T, MonitorError>;

const BACKEND_MONITOR_INTERVAL_DURATION: u64 = 60; // 60 seconds => 1 minute
const BACKEND_MONITOR_INTERVAL: Duration = Duration::from_secs(BACKEND_MONITOR_INTERVAL_DURATION);
const REPAIR_BACKLOG_RETRY_INTERVAL_DURATION: u64 = 60 * 5; // 5 minutes
const REPAIR_BACKLOG_RETRY_INTERVAL: Duration =
    Duration::from_secs(REPAIR_BACKLOG_RETRY_INTERVAL_DURATION);
const MAX_CONCURRENT_CONNECTIONS: usize = 10;
// TODO: make configurable
const LOWSPACE_TRESHOLD: u8 = 95; // at this point we consider the shard to be full

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

        unimplemented!();
    }

    pub async fn monitor_backends(&self, mut rx: Receiver<()>) -> JoinHandle<MonitorResult<()>> {
        let config = self.cfg.clone();
        let (repairer_tx, repairer_rx) = unbounded_channel::<String>();
        let repair_handle = self.spawn_repairer(repairer_rx).await;

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
                        debug!("reading zstor config at {:?}", config.zstor_config_path());
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
                               .or_insert(BackendState::Unknown(std::time::Instant::now()));
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

                                if info.data_usage_percentage() < LOWSPACE_TRESHOLD {
                                    warn!("backend {} has a high fill rate ({}%)", backend.address(), info.data_usage_percentage());
                                    backends.entry(backend).and_modify(|bs| bs.mark_lowspace(info.data_usage_percentage()));
                                } else {
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
                            debug!("attempt to replace unwriteable clusters");
                            let vdc_client = reqwest::Client::new();
                            for backend in backends.keys() {
                                if cluster.is_replaced(backend).await? {
                                    continue
                                }
                                if !backends[backend].is_writeable() {
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

    async fn spawn_repairer(&self, mut rx: UnboundedReceiver<String>) -> JoinHandle<()> {
        let config = self.cfg.clone();
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
}

/// Triggers the zstor binary to perform a rebuild command on the given key.
async fn rebuild_key(key: &str, cfg: &Config) -> MonitorResult<()> {
    if Command::new(cfg.zstor_bin_path())
        .arg("--config")
        .arg(cfg.zstor_config_path())
        .arg("rebuild")
        .arg("-k")
        .arg(key)
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

#[derive(Debug)]
pub enum ErrorKind {
    Config,
    Task,
    Zstor,
    Meta,
    Exec,
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
