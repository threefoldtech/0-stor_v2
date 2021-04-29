use crate::{
    encryption::{EncryptionError, Encryptor},
    erasure::Encoder,
    meta::{MetaData, MetaStore},
    zdb::{UserKeyZdb, ZdbError},
};
use async_trait::async_trait;
use futures::future::try_join_all;
use log::{debug, trace};
use std::fmt;

/// ZdbMetaStore stores data in multiple 0-db, encrypted, with dispersed encoding
pub struct ZdbMetaStore<E: Encryptor> {
    // TODO: once const generics can be used in const expressions, we can turn this vec into an
    // array of length D + P
    backends: Vec<UserKeyZdb>,
    encoder: Encoder,
    encryptor: E,
}

impl<E> ZdbMetaStore<E>
where
    E: Encryptor,
{
    /// Create a new zdb metastorage client. Writing will be done to the provided 0-dbs, encrypting
    /// data with the provided encryptor, and chunking it with the provided encoder.
    pub fn new(backends: Vec<UserKeyZdb>, encoder: Encoder, encryptor: E) -> Self {
        Self {
            backends,
            encoder,
            encryptor,
        }
    }
}

#[async_trait]
impl<E: Encryptor + Send> MetaStore for ZdbMetaStore<E> {
    type Error = ZdbMetaStoreError;

    async fn save_meta_by_key(&mut self, key: &str, meta: &MetaData) -> Result<(), Self::Error> {
        debug!("Saving metadata for key {}", key);
        // binary encode data
        trace!("Binary encoding metadata");
        let bin_meta = bincode::serialize(meta)?;

        // encrypt metadata
        trace!("Encrypting metadata");
        let enc_meta = self.encryptor.encrypt(&bin_meta)?;

        // dispersed encoding
        trace!("Dispersed encoding of metadata");
        let mut chunks = self.encoder.encode(enc_meta);

        // Chunks must be kept in the correct order, but since we don't have any external
        // bookkeeping, we need to add the ordering in the chunk itself. Since we assume the
        // returned items from the encoding make use of their full allocation, adding an element
        // will probably always trigger a realloc, so we just use the intuitive approach of
        // prepending the data with the idx of the shard.
        // TODO: improve the encoding to optionally include the shard idx data in the shard
        //
        // We don't support redundant backends for the metadata so no point in randomizing which
        // ones are used, they all are.
        let mut store_requests = Vec::with_capacity(self.backends.len());
        for ((shard_idx, shard), backend) in
            chunks.iter_mut().enumerate().zip(self.backends.iter_mut())
        {
            shard.insert(0, shard_idx as u8);
            trace!(
                "Storing metadata chunk of size {} in backend {} with key {}",
                shard.len(),
                backend.connection_info().address(),
                key
            );
            store_requests.push(backend.set(key, shard));
        }

        // If any 1 db can't be written to we consider the whole write to be a failure
        // set operations return () so the result after `?` is a Vec<()>, which can be ignored
        try_join_all(store_requests).await?;

        Ok(())
    }
}

pub type ZdbMetaStoreResult<T> = Result<T, ZdbMetaStoreError>;

#[derive(Debug)]
pub struct ZdbMetaStoreError {
    kind: ErrorKind,
    internal: InternalError,
}

impl fmt::Display for ZdbMetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "zdb metadata error while: {}: {}",
            self.kind, self.internal
        )
    }
}

impl std::error::Error for ZdbMetaStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.internal {
            InternalError::Zdb(ref e) => Some(e as &dyn std::error::Error),
            InternalError::Encoding(ref e) => Some(e),
            InternalError::Encryption(ref e) => Some(e),
        }
    }
}

#[derive(Debug)]
enum ErrorKind {
    /// An error in the connection to the backend, or an operation on the backend.
    Zdb,
    /// An error while serializing or deserializing the value.
    Encoding,
    /// An error while encrypting or decrypting the data.
    Encryption,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorKind::Zdb => "ZDB BACKEND",
                ErrorKind::Encoding => "ENCODING",
                ErrorKind::Encryption => "ENCRYPTION",
            }
        )
    }
}

#[derive(Debug)]
enum InternalError {
    Zdb(ZdbError),
    Encoding(bincode::Error),
    Encryption(EncryptionError),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::Zdb(ref e) => e as &dyn fmt::Display,
                InternalError::Encoding(ref e) => e,
                InternalError::Encryption(ref e) => e,
            }
        )
    }
}

impl From<bincode::Error> for ZdbMetaStoreError {
    fn from(e: bincode::Error) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Encoding,
            internal: InternalError::Encoding(e),
        }
    }
}

impl From<EncryptionError> for ZdbMetaStoreError {
    fn from(e: EncryptionError) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Encryption,
            internal: InternalError::Encryption(e),
        }
    }
}

impl From<ZdbError> for ZdbMetaStoreError {
    fn from(e: ZdbError) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Zdb,
            internal: InternalError::Zdb(e),
        }
    }
}
