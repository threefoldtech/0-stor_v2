use crate::{
    encryption::{EncryptionError, Encryptor},
    erasure::{Encoder, EncodingError},
    meta::{FailureMeta, MetaData, MetaStore},
    zdb::{UserKeyZdb, ZdbConnectionInfo, ZdbError},
};
use async_trait::async_trait;
use futures::future::{join_all, try_join_all};
use log::{debug, error, trace, warn};
use std::fmt;
use std::path::{Path, PathBuf};

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

    // helper functions to write data to backends.
    async fn write_value(&mut self, key: &str, value: Vec<u8>) -> ZdbMetaStoreResult<()> {
        debug!("Writing data to zdb metastore");
        // dispersed encoding
        trace!("Dispersed encoding of metadata");
        let mut chunks = self.encoder.encode(value);

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
                "Storing data chunk of size {} in backend {} with key {}",
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

    // helper function read data from backends.
    async fn read_value(&mut self, key: &str) -> ZdbMetaStoreResult<Option<Vec<u8>>> {
        trace!("Reading data from zdb metastore");

        let mut read_requests = Vec::with_capacity(self.backends.len());
        for backend in self.backends.iter_mut() {
            read_requests.push(backend.get(key));
        }

        let mut shards: Vec<Option<Vec<u8>>> =
            vec![None; self.encoder.data_shards() + self.encoder.parity_shards()];
        for read_result in join_all(read_requests).await {
            if let Some(mut data) = read_result? {
                if data.is_empty() {
                    continue;
                }
                // data is not empty so index 0 is set, making this safe
                let idx = data[0];
                let shard = data.drain(..).skip(1).collect();
                if idx as usize >= shards.len() {
                    warn!(
                        "found shard at index {}, but only {} shards are expected for key {}",
                        idx,
                        shards.len(),
                        key
                    );
                    continue;
                }
                // TODO checksum
                shards[idx as usize] = Some(shard);
            };
        }

        let shard_count = shards.iter().filter(|x| x.is_some()).count();

        if shard_count == 0 {
            return Ok(None);
        }

        if shard_count < self.encoder.data_shards() {
            error!("key {} is corrupt and cannot be repaired", key);
            return Err(ZdbMetaStoreError {
                kind: ErrorKind::Encoding,
                internal: InternalError::Corruption(CorruptedKey {
                    available_shards: shard_count,
                    required_shards: self.encoder.data_shards(),
                }),
            });
        }

        Ok(Some(self.encoder.decode(shards)?))
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

        self.write_value(key, enc_meta).await
    }

    async fn load_meta_by_key(&mut self, key: &str) -> Result<Option<MetaData>, Self::Error> {
        debug!("Loading metadata for key {}", key);

        // read data back
        let read = match self.read_value(key).await? {
            Some(data) => data,
            None => return Ok(None),
        };

        // decrypt metadata
        trace!("Decrypting metadata");
        let dec_meta = self.encryptor.decrypt(&read)?;

        trace!("Binary decoding metadata");
        Ok(Some(bincode::deserialize(&dec_meta)?))
    }

    async fn load_meta(&mut self, path: &Path) -> Result<Option<MetaData>, Self::Error> {
        todo!();
    }

    async fn save_meta(&mut self, path: &Path, meta: &MetaData) -> Result<(), Self::Error> {
        todo!();
    }

    async fn object_metas(&mut self) -> Result<Vec<(String, MetaData)>, Self::Error> {
        todo!();
    }

    async fn set_replaced(&mut self, ci: &ZdbConnectionInfo) -> Result<(), Self::Error> {
        todo!();
    }

    async fn save_failure(
        &mut self,
        data_path: &Path,
        key_dir_path: &Option<PathBuf>,
        should_delete: bool,
    ) -> Result<(), Self::Error> {
        todo!();
    }

    async fn delete_failure(&mut self, fm: &FailureMeta) -> Result<(), Self::Error> {
        todo!();
    }

    async fn get_failures(&mut self) -> Result<Vec<FailureMeta>, Self::Error> {
        todo!();
    }

    async fn is_replaced(&mut self, ci: &ZdbConnectionInfo) -> Result<bool, Self::Error> {
        todo!();
    }
}

/// ZdbMetaStoreResult is the type returned by the Metadata interface on 0-db
pub type ZdbMetaStoreResult<T> = Result<T, ZdbMetaStoreError>;

/// ZdbMetaStoreError is the error returned by the 0-db meta store
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
            InternalError::Serialization(ref e) => Some(e),
            InternalError::Encryption(ref e) => Some(e),
            InternalError::Encoding(ref e) => Some(e),
            InternalError::Corruption(ref e) => Some(e),
        }
    }
}

#[derive(Debug)]
enum ErrorKind {
    /// An error in the connection to the backend, or an operation on the backend.
    Zdb,
    /// An error while serializing or deserializing the value.
    Serialize,
    /// An error while encrypting or decrypting the data.
    Encryption,
    /// An erorr while using dispersed encoding or decoding.
    Encoding,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorKind::Zdb => "ZDB BACKEND",
                ErrorKind::Serialize => "SERIALIZATION",
                ErrorKind::Encryption => "ENCRYPTION",
                ErrorKind::Encoding => "ENCODING",
            }
        )
    }
}

#[derive(Debug)]
enum InternalError {
    Zdb(ZdbError),
    Serialization(bincode::Error),
    Encryption(EncryptionError),
    Encoding(EncodingError),
    Corruption(CorruptedKey),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::Zdb(ref e) => e as &dyn fmt::Display,
                InternalError::Serialization(ref e) => e,
                InternalError::Encryption(ref e) => e,
                InternalError::Encoding(ref e) => e,
                InternalError::Corruption(ref e) => e,
            }
        )
    }
}

impl From<bincode::Error> for ZdbMetaStoreError {
    fn from(e: bincode::Error) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Serialize,
            internal: InternalError::Serialization(e),
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

impl From<EncodingError> for ZdbMetaStoreError {
    fn from(e: EncodingError) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Encoding,
            internal: InternalError::Encoding(e),
        }
    }
}

/// An error holding information about a corrupted key
#[derive(Debug)]
pub struct CorruptedKey {
    available_shards: usize,
    required_shards: usize,
}

impl fmt::Display for CorruptedKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "key reconstruction failed, data is corrupted. Need at least {} shards, only {} available", self.available_shards, self.required_shards)
    }
}

impl std::error::Error for CorruptedKey {}
