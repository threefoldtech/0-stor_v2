#![deny(missing_docs)]
#![deny(unused_doc_comments)]
#![deny(dead_code)]

//! This crate contains the main implementations for the rstor library. This includes utilities to
//! write to 0-dbs, compress, encrypt and erasure code data, and write to etc. There are also
//! config structs available.

use compression::CompressorError;
use config::ConfigError;
use encryption::EncryptionError;
use erasure::EncodingError;
use etcd::EtcdError;
use std::fmt;
use tokio::task::JoinError;
use zdb::ZdbError;

/// Contains a general compression interface and implementations.
pub mod compression;
/// Contains global configuration details.
pub mod config;
/// Contains a general encryption interface and implementations.
pub mod encryption;
/// Contains a general erasure encoder.
pub mod erasure;
/// A small etcd cluster client to load and set metadata.
pub mod etcd;
/// Metadata for stored shards after encoding
pub mod meta;
/// Very basic 0-db client, allowing to connect to a given (password protected) namespace, and
/// write and read data from it.
pub mod zdb;

/// Global result type for zstor operations
pub type ZstorResult<T> = Result<T, ZstorError>;

/// An error originating in zstor
#[derive(Debug)]
pub struct ZstorError {
    kind: ZstorErrorKind,
    internal: Box<dyn std::error::Error + Send>,
}

impl fmt::Display for ZstorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error during {}: {}", self.kind, self.internal)
    }
}

impl std::error::Error for ZstorError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        Some(&*self.internal)
    }
}

impl ZstorError {
    /// Create a new ZstorError from an IO error with an additional message.
    pub fn new_io(msg: String, e: std::io::Error) -> Self {
        ZstorError {
            kind: ZstorErrorKind::LocalIO(msg),
            internal: Box::new(e),
        }
    }

    /// Create a new ZstorError from any kind, with the underlying error included
    pub fn new(kind: ZstorErrorKind, internal: Box<dyn std::error::Error + Send>) -> Self {
        ZstorError { kind, internal }
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
    LocalIO(String),
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
                ZstorErrorKind::LocalIO(msg) => format!("accessing local storage for {}", msg),
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
            internal: Box::new(e),
        }
    }
}

impl From<EtcdError> for ZstorError {
    fn from(e: EtcdError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Metadata,
            internal: Box::new(e),
        }
    }
}

impl From<EncodingError> for ZstorError {
    fn from(e: EncodingError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Encoding,
            internal: Box::new(e),
        }
    }
}

impl From<EncryptionError> for ZstorError {
    fn from(e: EncryptionError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Encryption,
            internal: Box::new(e),
        }
    }
}

impl From<CompressorError> for ZstorError {
    fn from(e: CompressorError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Compression,
            internal: Box::new(e),
        }
    }
}

impl From<ConfigError> for ZstorError {
    fn from(e: ConfigError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Config,
            internal: Box::new(e),
        }
    }
}

impl From<JoinError> for ZstorError {
    fn from(e: JoinError) -> Self {
        ZstorError {
            kind: ZstorErrorKind::Async,
            internal: Box::new(e),
        }
    }
}
