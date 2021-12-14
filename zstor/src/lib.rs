#![deny(missing_docs)]
#![deny(unused_doc_comments)]
#![deny(dead_code)]

//! This crate contains the main implementations for the rstor library. This includes utilities to
//! write to 0-dbs, compress, encrypt and erasure code data, and write to etc. There are also
//! config structs available.

use crate::actors::{
    backends::BackendManagerActor,
    config::ConfigActor,
    dir_monitor::DirMonitorActor,
    explorer::{ExplorerActor, NopExplorerActor},
    meta::MetaStoreActor,
    metrics::{GetPrometheusMetrics, MetricsActor},
    pipeline::PipelineActor,
    repairer::RepairActor,
    zdbfs::ZdbFsStatsProxyActor,
    zstor::ZstorActor,
};
use actix::prelude::*;
use actix::{Addr, MailboxError};
use compression::CompressorError;
use config::ConfigError;
use encryption::EncryptionError;
use erasure::EncodingError;
use futures::future::try_join_all;
use grid_explorer_client::{
    identity::{Identity, IdentityError},
    ExplorerClient, ExplorerError,
};
use meta::MetaStoreError;
use std::fmt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::task::JoinError;
use zdb::ZdbError;
use {
    config::{Config, Encryption, Meta},
    zdb::UserKeyZdb,
    zdb_meta::ZdbMetaStore,
};
/// Implementations of all components as actors.
pub mod actors;
/// Contains a general compression interface and implementations.
pub mod compression;
/// Contains global configuration details.
pub mod config;
/// Contains a general encryption interface and implementations.
pub mod encryption;
/// Contains a general erasure encoder.
pub mod erasure;
/// Metadata for stored shards after encoding.
pub mod meta;
/// Very basic 0-db client, allowing to connect to a given (password protected) namespace, and
/// write and read data from it.
pub mod zdb;
/// A metadata implementation on top of zdb.
pub mod zdb_meta;
/// Zdbfs related tools.
pub mod zdbfs;

/// Global result type for zstor operations
pub type ZstorResult<T> = Result<T, ZstorError>;

/// Start the 0-stor monitor daemon
pub async fn setup_system(cfg_path: PathBuf, cfg: &Config) -> ZstorResult<Addr<ZstorActor>> {
    let metastore = match cfg.meta() {
        Meta::Zdb(zdb_cfg) => {
            let backends = try_join_all(
                zdb_cfg
                    .backends()
                    .iter()
                    .map(|ci| UserKeyZdb::new(ci.clone())),
            )
            .await?;
            let encryptor = match cfg.encryption() {
                Encryption::Aes(key) => encryption::AesGcm::new(key.clone()),
            };
            let encoder = zdb_cfg.encoder();
            Box::new(ZdbMetaStore::new(
                backends,
                encoder,
                encryptor,
                zdb_cfg.prefix().to_string(),
                cfg.virtual_root().clone(),
            ))
        }
    };
    let prom_port = cfg.prometheus_port();
    let metrics_addr = MetricsActor::new().start();
    if let Some(mountpoint) = cfg.zdbfs_mountpoint() {
        let _ = ZdbFsStatsProxyActor::new(mountpoint.to_path_buf(), metrics_addr.clone()).start();
    }
    let meta_addr = MetaStoreActor::new(metastore).start();
    let cfg_addr = ConfigActor::new(cfg_path, cfg.clone()).start();
    let pipeline_addr = SyncArbiter::start(1, || PipelineActor);

    let zstor = ZstorActor::new(
        cfg_addr.clone(),
        pipeline_addr,
        meta_addr.clone(),
        metrics_addr.clone(),
    )
    .start();

    let _ = DirMonitorActor::new(cfg_addr.clone(), zstor.clone()).start();

    let explorer = if let Some(explorer_cfg) = cfg.explorer() {
        let identity = Identity::new(
            explorer_cfg.identity_id() as i64,
            explorer_cfg.identity_mnemonic(),
        )?;
        let explorer_client = ExplorerClient::new(
            explorer_cfg.grid_network(),
            explorer_cfg.wallet_secret(),
            identity,
            explorer_cfg.horizon_url().map(|x| x.to_string()),
        );
        let explorer =
            ExplorerActor::new(explorer_client, cfg_addr.clone(), metrics_addr.clone()).start();
        explorer.recipient()
    } else {
        let ne = NopExplorerActor::new().start();
        ne.recipient()
    };

    let backends =
        BackendManagerActor::new(cfg_addr, explorer, metrics_addr.clone(), meta_addr.clone())
            .start();
    let _ = RepairActor::new(meta_addr, backends, zstor.clone()).start();

    // Setup prometheus endpoint if needed
    if let Some(port) = prom_port {
        use tide::Request;
        let mut app = tide::with_state(metrics_addr);
        app.at("/metrics")
            .get(|req: Request<Addr<MetricsActor>>| async move {
                Ok(req.state().send(GetPrometheusMetrics).await??)
            });
        tokio::spawn(async move {
            app.listen(format!("[::]:{}", port)).await.unwrap();
        });
    };

    Ok(zstor)
}

/// Load a TOML encoded config file from the given path.
pub async fn load_config(path: &Path) -> ZstorResult<Config> {
    let cfg_data = fs::read(path)
        .await
        .map_err(|e| ZstorError::new_io("Couldn't load config file".to_string(), e))?;
    Ok(toml::from_slice(&cfg_data)?)
}

/// An error originating in zstor
#[derive(Debug)]
pub struct ZstorError {
    kind: ZstorErrorKind,
    internal: InternalError,
}

impl fmt::Display for ZstorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error during {}: {}", self.kind, self.internal)
    }
}

impl std::error::Error for ZstorError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self.internal {
            InternalError::Zdb(ref e) => Some(e),
            InternalError::Other(ref e) => Some(e.as_ref()),
            InternalError::Msg(_) => None,
        }
    }
}

impl ZstorError {
    /// Create a new ZstorError from an IO error with an additional message.
    pub fn new_io(msg: String, e: std::io::Error) -> Self {
        ZstorError {
            kind: ZstorErrorKind::LocalIo(msg),
            internal: InternalError::Other(Box::new(e)),
        }
    }

    /// Create a new [`ZstorError`] from any kind, with the underlying error included.
    pub fn new(kind: ZstorErrorKind, internal: Box<dyn std::error::Error + Send>) -> ZstorError {
        Self {
            kind,
            internal: InternalError::Other(internal),
        }
    }

    /// Create a new [`ZstorError]` from a [`ZstorErrorKind`] and a custom message.
    pub fn with_message(kind: ZstorErrorKind, msg: String) -> ZstorError {
        Self {
            kind,
            internal: InternalError::Msg(msg),
        }
    }

    /// Return a reference to the embedded [`crate::zdb::ZdbError`], if this error is caused by
    /// a ZdbError, or nothing otherwise.
    pub fn zdb_error(&self) -> Option<&ZdbError> {
        match self.internal {
            InternalError::Zdb(ref e) => Some(e),
            _ => None,
        }
    }
}

/// Wrapper error for the ZstorError
#[derive(Debug)]
enum InternalError {
    Zdb(ZdbError),
    Other(Box<dyn std::error::Error + Send>),
    Msg(String),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::Zdb(ref e) => e as &dyn fmt::Display,
                InternalError::Other(e) => e,
                InternalError::Msg(string) => string,
            }
        )
    }
}

/// Information about where in the chain of operation the error occurred.
#[derive(Debug)]
pub enum ZstorErrorKind {
    /// An error in the compression or decompression step.
    Compression,
    /// An error in the encryption or decryption step.
    Encryption,
    /// An error in the data encoding or decoding step.
    Encoding,
    /// An error while setting up the storage connection, or when uploading to or downloading data
    /// from storage.
    Storage,
    /// An error while setting up the metadata connection, or when uploading to or downloading data
    /// from the metadata storage
    Metadata,
    /// An error wile reading or writing to the local storage.
    LocalIo(String),
    /// An error in the configuration,
    Config,
    /// An error while waiting for an asynchronous task to complete.
    Async,
    /// An error in the (binary) wire format of messages.
    Serialization,
    /// An error related to the explorer.
    Explorer,
}

impl fmt::Display for ZstorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ZstorErrorKind::Compression => "compression".to_string(),
                ZstorErrorKind::Encryption => "encryption".to_string(),
                ZstorErrorKind::Encoding => "encoding".to_string(),
                ZstorErrorKind::Storage => "storage".to_string(),
                ZstorErrorKind::Metadata => "metadata".to_string(),
                ZstorErrorKind::LocalIo(msg) => format!("accessing local storage for {}", msg),
                ZstorErrorKind::Config => "configuration".to_string(),
                ZstorErrorKind::Async => "waiting for async task completion".to_string(),
                ZstorErrorKind::Serialization => "error in the binary wire format".to_string(),
                ZstorErrorKind::Explorer => "error in the grid explorer".to_string(),
            }
        )
    }
}

impl From<ZdbError> for ZstorError {
    fn from(e: ZdbError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Storage,
            internal: InternalError::Zdb(e),
        }
    }
}

impl From<EncodingError> for ZstorError {
    fn from(e: EncodingError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Encoding,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<EncryptionError> for ZstorError {
    fn from(e: EncryptionError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Encryption,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<CompressorError> for ZstorError {
    fn from(e: CompressorError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Compression,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<ConfigError> for ZstorError {
    fn from(e: ConfigError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Config,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<JoinError> for ZstorError {
    fn from(e: JoinError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Async,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<MetaStoreError> for ZstorError {
    fn from(e: MetaStoreError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Metadata,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<toml::de::Error> for ZstorError {
    fn from(e: toml::de::Error) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Config,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<toml::ser::Error> for ZstorError {
    fn from(e: toml::ser::Error) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Config,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<bincode::Error> for ZstorError {
    fn from(e: bincode::Error) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Serialization,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<MailboxError> for ZstorError {
    fn from(e: MailboxError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Async,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<ExplorerError> for ZstorError {
    fn from(e: ExplorerError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Explorer,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}

impl From<IdentityError> for ZstorError {
    fn from(e: IdentityError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Explorer,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}
