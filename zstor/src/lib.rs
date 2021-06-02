#![deny(missing_docs)]
#![deny(unused_doc_comments)]
#![deny(dead_code)]

//! This crate contains the main implementations for the rstor library. This includes utilities to
//! write to 0-dbs, compress, encrypt and erasure code data, and write to etc. There are also
//! config structs available.

use actix::MailboxError;
use compression::CompressorError;
use config::ConfigError;
use encryption::EncryptionError;
use erasure::EncodingError;
use meta::MetaStoreError;
use std::fmt;
use tokio::task::JoinError;
use zdb::ZdbError;

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
/// Entrypoint for running 0-stor as a daemon.
pub mod monitor;
/// The main data pipeline to go from the raw data to the representation which can be saved in the
/// backend.
pub mod pipeline;
/// Very basic 0-db client, allowing to connect to a given (password protected) namespace, and
/// write and read data from it.
pub mod zdb;
/// A metadata implementation on top of zdb.
pub mod zdb_meta;

/// Global result type for zstor operations
pub type ZstorResult<T> = Result<T, ZstorError>;

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

    /// Create a new ZstorError from any kind, with the underlying error included
    pub fn new(kind: ZstorErrorKind, internal: Box<dyn std::error::Error + Send>) -> Self {
        ZstorError {
            kind,
            internal: InternalError::Other(internal),
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
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::Zdb(ref e) => e as &dyn std::error::Error,
                InternalError::Other(e) => e.as_ref(),
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

impl From<MailboxError> for ZstorError {
    fn from(e: MailboxError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Async,
            internal: InternalError::Other(Box::new(e)),
        }
    }
}
