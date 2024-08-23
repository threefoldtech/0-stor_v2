use crate::{
    config,
    encryption::{EncryptionError, Encryptor},
    erasure::{Encoder, EncodingError, Shard},
    meta::{canonicalize, FailureMeta, MetaData, MetaStore, MetaStoreError},
    zdb::{UserKeyZdb, ZdbConnectionInfo, ZdbError},
};
use async_trait::async_trait;
use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use futures::{
    future::{join_all, try_join_all},
    stream::{Stream, StreamExt},
};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
};

/// Amount of data shards to use for the encoder used by the 0-db MetaStore.
const ZDB_METASTORE_DATA_SHARDS: usize = 2;
/// Amount of disposable shards to use for the encoder used by the 0-db MetaStore.
const ZDB_METASTORE_DISPOSABLE_SHARDS: usize = 2;

// TODO: find a good limit here
/// Concurrent amount of keys being rebuild in a rebuild operation.
const CONCURRENT_KEY_REBUILDS: usize = 10;

/// Configuration to create a 0-db based metadata store
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZdbMetaStoreConfig {
    prefix: String,
    encryption: config::Encryption,
    backends: [ZdbConnectionInfo; 4],
}

impl ZdbMetaStoreConfig {
    /// Create a new [`ZdbMetaStoreConfig`] with the given data.
    pub fn new(
        prefix: String,
        encryption: config::Encryption,
        backends: [ZdbConnectionInfo; 4],
    ) -> ZdbMetaStoreConfig {
        Self {
            prefix,
            encryption,
            backends,
        }
    }
    /// Get the connection info for all the specified backends.
    pub fn backends(&self) -> &[ZdbConnectionInfo] {
        &self.backends
    }

    /// Set the backends to the provided ones.
    pub fn set_backends(&mut self, backends: [ZdbConnectionInfo; 4]) {
        self.backends = backends;
    }

    /// Build an encoder from the config.
    pub fn encoder(&self) -> Encoder {
        Encoder::new(ZDB_METASTORE_DATA_SHARDS, ZDB_METASTORE_DISPOSABLE_SHARDS)
    }

    /// Get the encryption configuration from the config
    pub fn encryption(&self) -> &config::Encryption {
        &self.encryption
    }

    /// Get the prefix used for the metadata
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// ZdbMetaStore stores data in multiple 0-db, encrypted, with dispersed encoding
pub struct ZdbMetaStore<E: Encryptor> {
    // TODO: once const generics can be used in const expressions, we can turn this vec into an
    // array of length D + P
    backends: Vec<UserKeyZdb>,
    encoder: Encoder,
    encryptor: E,
    prefix: String,
    virtual_root: Option<PathBuf>,
    writeable: bool,
}

impl<E> ZdbMetaStore<E>
where
    E: Encryptor,
{
    /// Create a new zdb metastorage client. Writing will be done to the provided 0-dbs, encrypting
    /// data with the provided encryptor, and chunking it with the provided encoder.
    pub fn new(
        backends: Vec<UserKeyZdb>,
        encoder: Encoder,
        encryptor: E,
        prefix: String,
        virtual_root: Option<PathBuf>,
    ) -> Self {
        let writeable = backends.len() >= encoder.data_shards() + encoder.disposable_shards();
        Self {
            backends,
            encoder,
            encryptor,
            prefix,
            virtual_root,
            writeable,
        }
    }

    /// helper functions to encrypt and write data to backends.
    async fn write_value(&self, key: &str, value: &[u8]) -> ZdbMetaStoreResult<()> {
        debug!("Writing data to zdb metastore");
        trace!("Encrypt value");
        let value = self.encryptor.encrypt(value)?;
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
        for ((shard_idx, shard), backend) in chunks.iter_mut().enumerate().zip(self.backends.iter())
        {
            // grab the checksum before inserting the shard idx
            let checksum = shard.checksum();
            shard.insert(0, shard_idx as u8);
            trace!(
                "Storing data chunk of size {} in backend {} with key {}",
                shard.len(),
                backend.connection_info().address(),
                key
            );
            shard.extend(checksum);
            store_requests.push(backend.set(key, shard));
        }

        // If any 1 db can't be written to we consider the whole write to be a failure
        // set operations return () so the result after `?` is a Vec<()>, which can be ignored
        try_join_all(store_requests).await?;

        Ok(())
    }

    /// helper function read data from backends and decrypt it.
    async fn read_value(&self, key: &str) -> ZdbMetaStoreResult<Option<Vec<u8>>> {
        debug!("Reading data from zdb metastore for key {}", key);

        let mut read_requests = Vec::with_capacity(self.backends.len());
        for backend in self.backends.iter() {
            read_requests.push(backend.get(key));
        }

        let mut shards: Vec<Option<Vec<u8>>> =
            vec![None; self.encoder.data_shards() + self.encoder.disposable_shards()];
        for read_result in join_all(read_requests).await {
            if let Some(mut data) = read_result? {
                if data.is_empty() {
                    continue;
                }
                // data is not empty so index 0 is set, making this safe
                let idx = data[0];
                let mut shard: Vec<u8> = data.drain(..).skip(1).collect();
                if idx as usize >= shards.len() {
                    warn!(
                        "found shard at index {}, but only {} shards are expected for key {}",
                        idx,
                        shards.len(),
                        key
                    );
                    continue;
                }
                let saved_checksum = shard.split_off(shard.len() - 16);
                let shard = Shard::from(shard);
                if saved_checksum != shard.checksum() {
                    warn!("shard {} checksum verification failed", idx);
                    continue;
                }

                shards[idx as usize] = Some(shard.into_inner());
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

        let content = self.encoder.decode(shards)?;

        Ok(Some(self.encryptor.decrypt(&content)?))
    }

    /// Helper function to delete a value from backends
    async fn delete_value(&self, key: &str) -> ZdbMetaStoreResult<()> {
        debug!("Deleting data from zdb metastore for key {}", key);

        let mut delete_requests = Vec::with_capacity(self.backends.len());
        for backend in self.backends.iter() {
            delete_requests.push(backend.delete(key));
        }

        // If any 1 db can't be written to we consider the whole write to be a failure
        // set operations return () so the result after `?` is a Vec<()>, which can be ignored
        try_join_all(delete_requests).await?;

        Ok(())
    }

    /// Return a stream of all function with a given prefix
    async fn keys<'a>(
        &'a self,
        prefix: &'a str,
    ) -> ZdbMetaStoreResult<impl Stream<Item = String> + 'a> {
        debug!("Starting metastore key iteration with prefix {}", prefix);

        // First get the lengh of all the backends
        let mut ns_requests = Vec::with_capacity(self.backends.len());
        for backend in self.backends.iter() {
            ns_requests.push(backend.ns_info());
        }
        let mut most_keys_idx = 0;
        let mut highest_key_count = 0;
        let mut healthy_backend = false;
        for (idx, ns_info_res) in join_all(ns_requests).await.into_iter().enumerate() {
            // Only utilize reachable backends.
            if let Ok(ns_info) = ns_info_res {
                healthy_backend = true;
                if ns_info.entries() > highest_key_count {
                    most_keys_idx = idx;
                    highest_key_count = ns_info.entries();
                }
            }
        }

        // If there is no reachable backend, we can't list the keys.
        if !healthy_backend {
            return Err(ZdbMetaStoreError {
                kind: ErrorKind::InsufficientHealthBackends,
                internal: InternalError::Other("no healthy backend found to list keys".into()),
            });
        }

        // Now iterate over the keys of the longest backend
        Ok(self.backends[most_keys_idx]
            .keys()
            // yes both of these move keywords are really necessary
            .filter_map(move |raw_key| async move {
                if raw_key.starts_with(prefix.as_bytes()) {
                    return String::from_utf8(raw_key).ok();
                };
                None
            }))
    }

    /// Rebuild an old metdata cluster on a new one. This method does not return untill all known
    /// keys are rebuild.
    pub async fn rebuild_cluster(&self, old_cluster: &Self) -> ZdbMetaStoreResult<()> {
        info!("Start cluster rebuild");
        if !self.writeable {
            return Err(ZdbMetaStoreError {
                kind: ErrorKind::InsufficientHealthBackends,
                internal: InternalError::Other(
                    "Can't rebuild data to a cluster which is not writeable".into(),
                ),
            });
        }

        // Rebuild data, for now we skip keys which error but otherwise attempt to complete the
        // rebuild.
        old_cluster
            .keys(&format!("/{}/", &self.prefix))
            .await?
            .for_each_concurrent(CONCURRENT_KEY_REBUILDS, |key| async move {
                if old_cluster.encoder == self.encoder {
                    if let Err(e) = self.sparse_rebuild(old_cluster, &key).await {
                        error!("Failed to sparse rebuild key {} on new cluster: {}", key, e);
                    }
                } else {
                    trace!("plain rebuild of key {}", key);
                    let value = match old_cluster.read_value(&key).await {
                        Ok(value) => value,
                        Err(e) => {
                            error!("Failed to read key {} from old cluster: {}", key, e);
                            return;
                        }
                    };
                    if value.is_none() {
                        debug!("Skipping key {} with no value in rebuild", key);
                        return;
                    }
                    // unwrap here is safe as we just returned on the None variant, leaving only
                    // the Some variant as option.
                    if let Err(e) = self.write_value(&key, &value.unwrap()).await {
                        error!("Failed to write key {} to new cluster: {}", key, e);
                    }
                }
            })
            .await;
        Ok(())
    }

    /// Helper method which rebuilds a single key from an old cluster on a new cluster. It maps the
    /// available shards to the exact instance in the old cluster, so it can be reused if present
    /// in the new cluster. Existing shards are not written again to save bandwidth.
    ///
    /// # Panics
    ///
    /// Panics if the encoder configuration for the old and new cluster is different.
    async fn sparse_rebuild(&self, old_cluster: &Self, key: &str) -> ZdbMetaStoreResult<()> {
        trace!("Sparse rebuild of key {}", key);
        assert_eq!(old_cluster.encoder, self.encoder);

        let shard_count =
            old_cluster.encoder.data_shards() + old_cluster.encoder.disposable_shards();
        let mut shard_idx: HashMap<&ZdbConnectionInfo, usize> = HashMap::with_capacity(shard_count);
        let mut shards = vec![None; shard_count];
        for (ci, read_result) in join_all(
            old_cluster
                .backends
                .iter()
                .map(|backend| async move { (backend.connection_info(), backend.get(key).await) }),
        )
        .await
        {
            // Ignore errored shards
            if read_result.is_err() {
                // Don't log, as that might spamm the logfile.
                continue;
            }
            // Unwrap here is now safe as we only get here in case of the Ok variant.
            if let Some(mut data) = read_result.unwrap() {
                if data.is_empty() {
                    continue;
                }
                let idx = data[0];
                let mut shard: Vec<u8> = data.drain(..).skip(1).collect();
                // Sanity check
                if idx as usize >= shard_count {
                    warn!(
                        "found shard at index {}, but only {} shards are expected for key {}",
                        idx, shard_count, key
                    );
                    continue;
                }
                // Checksum verification
                let saved_checksum = shard.split_off(shard.len() - 16);
                let shard = Shard::from(shard);
                if saved_checksum != shard.checksum() {
                    warn!("shard {} checksum verification failed", idx);
                    continue;
                }

                shards[idx as usize] = Some(shard.into_inner());
                shard_idx.insert(ci, idx as usize);
            }
        }

        let shard_count = shards.iter().filter(|x| x.is_some()).count();

        // Nothing to do.
        // We skipped keys without data, meaning we won't rebuild a possible dataless key, TODO:
        // verify if this is an issue.
        if shard_count == 0 {
            return Ok(());
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

        let content = self.encoder.decode(shards)?;
        let new_shards = self.encoder.encode(content);

        let mut new_shards: Vec<Option<Shard>> = new_shards.into_iter().map(Some).collect();
        let mut writes = Vec::with_capacity(new_shards.len());

        // First iteration, map existing shards. These shards can be removed as we won't write them
        // to the backend, they are already present. This optimizes the outgoing bandwidth a bit.
        let writeable_backends = self
            .backends
            .iter()
            .filter(|backend| {
                if let Some(idx) = shard_idx.get(backend.connection_info()) {
                    // Remove the shard as it is already present.
                    new_shards[*idx] = None;
                    // Filter out the backend as we don't want to write to it again.
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();

        for backend in writeable_backends {
            // Grab the first shard available.
            for (idx, shard) in new_shards.iter_mut().enumerate() {
                if shard.is_some() {
                    // Unwrap is safe as we only enter this block in case of the Some variant.
                    let mut shard = shard.take().unwrap();
                    // Prepare shard
                    // Grab the checksum before inserting the shard idx
                    let checksum = shard.checksum();
                    shard.insert(0, idx as u8);
                    trace!(
                        "Storing data chunk of size {} in backend {} with key {}",
                        shard.len(),
                        backend.connection_info().address(),
                        key
                    );
                    shard.extend(checksum);
                    writes.push((backend, shard));
                    break;
                }
            }
        }

        let shards_left = new_shards.into_iter().filter(Option::is_some).count();
        if shards_left != 0 {
            return Err(ZdbMetaStoreError {
                kind: ErrorKind::Encoding,
                internal: InternalError::Other(
                    format!("Not all shards would be written in reconstruction, still have {} unassigned shards", shards_left),
                ),
            });
        }

        // Single write failure is considered an error
        try_join_all(
            writes
                .into_iter()
                .map(|(backend, shard)| async move { backend.set(key, &shard).await }),
        )
        .await?;

        Ok(())
    }

    /// hash a path using blake2b with 16 bytes of output, and hex encode the result
    /// the path is canonicalized before encoding so the full path is used
    #[allow(clippy::result_large_err)]
    fn build_key(&self, path: &Path) -> ZdbMetaStoreResult<String> {
        let canonical_path = canonicalize(path)?;

        // now strip the virtual_root, if one is set
        let actual_path = if let Some(ref virtual_root) = self.virtual_root {
            trace!("stripping path prefix {:?}", virtual_root);
            canonical_path
                .strip_prefix(virtual_root)
                .map_err(|e| ZdbMetaStoreError {
                    kind: ErrorKind::Key,
                    internal: InternalError::Other(format!("could not strip path prefix: {}", e)),
                })?
        } else {
            trace!("maintaining path");
            canonical_path.as_path()
        };

        trace!("hashing path {:?}", actual_path);
        // The unwrap here is safe since we know that 16 is a valid output size
        let mut hasher = Blake2bVar::new(16).unwrap();
        // TODO: might not need the move to a regular &str
        hasher.update(
            actual_path
                .as_os_str()
                .to_str()
                .ok_or(ZdbMetaStoreError {
                    kind: ErrorKind::Key,
                    internal: InternalError::Other(
                        "could not interpret path as utf-8 str".to_string(),
                    ),
                })?
                .as_bytes(),
        );

        let mut out = [0; 16];
        hasher.finalize_variable(&mut out);
        let r = hex::encode(out);
        trace!("hashed path: {}", r);
        let full_key = format!("/{}/meta/{}", self.prefix, r);
        trace!("full key: {}", full_key);
        Ok(full_key)
    }

    // This does not take into account the virtual root
    #[allow(clippy::result_large_err)]
    fn build_failure_key(&self, path: &Path) -> ZdbMetaStoreResult<String> {
        let mut hasher = Blake2bVar::new(16).unwrap();
        hasher.update(
            path.as_os_str()
                .to_str()
                .ok_or(ZdbMetaStoreError {
                    kind: ErrorKind::Key,
                    internal: InternalError::Other(
                        "could not interpret path as utf-8 str".to_string(),
                    ),
                })?
                .as_bytes(),
        );

        let mut out = [0; 16];
        hasher.finalize_variable(&mut out);

        Ok(format!("/{}/failures/{}", self.prefix, hex::encode(out)))
    }
}

#[async_trait]
impl<E> MetaStore for ZdbMetaStore<E>
where
    E: Encryptor + Send + Sync,
{
    async fn save_meta_by_key(&self, key: &str, meta: &MetaData) -> Result<(), MetaStoreError> {
        debug!("Saving metadata for key {}", key);
        // binary encode data
        trace!("Binary encoding metadata");
        let bin_meta = bincode::serialize(meta).map_err(ZdbMetaStoreError::from)?;

        Ok(self.write_value(key, &bin_meta).await?)
    }

    async fn load_meta_by_key(&self, key: &str) -> Result<Option<MetaData>, MetaStoreError> {
        debug!("Loading metadata for key {}", key);

        // read data back
        let read = match self.read_value(key).await? {
            Some(data) => data,
            None => return Ok(None),
        };

        trace!("Binary decoding metadata");
        Ok(Some(
            bincode::deserialize(&read).map_err(ZdbMetaStoreError::from)?,
        ))
    }

    async fn load_meta(&self, path: &Path) -> Result<Option<MetaData>, MetaStoreError> {
        Ok(self.load_meta_by_key(&self.build_key(path)?).await?)
    }

    async fn save_meta(&self, path: &Path, meta: &MetaData) -> Result<(), MetaStoreError> {
        Ok(self.save_meta_by_key(&self.build_key(path)?, meta).await?)
    }

    async fn object_metas(&self) -> Result<Vec<(String, MetaData)>, MetaStoreError> {
        // pin the stream on the heap for now
        let prefix = format!("/{}/meta/", self.prefix);
        let meta_keys = Box::pin(self.keys(&prefix).await?);
        let keys: Vec<String> = meta_keys.collect().await;
        let mut data = Vec::with_capacity(keys.len());
        for key in keys.into_iter() {
            let value = match self.read_value(&key).await {
                Ok(value) => match value {
                    Some(value) => value,
                    None => {
                        warn!("Empty value for key {}", key);
                        continue;
                    }
                },
                Err(e) => {
                    error!("error in key iteration: {}", e);
                    continue;
                }
            };

            let meta = match bincode::deserialize(&value) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Corrupted metadata at key {}: {}", key, e);
                    continue;
                }
            };
            data.push((key, meta));
        }

        Ok(data)
    }

    async fn set_replaced(&self, ci: &ZdbConnectionInfo) -> Result<(), MetaStoreError> {
        let hash = hex::encode(ci.blake2_hash());
        let key = format!("/{}/replaced_backends/{}", self.prefix, hash);
        Ok(self.write_value(&key, &[]).await?)
    }

    async fn is_replaced(&self, ci: &ZdbConnectionInfo) -> Result<bool, MetaStoreError> {
        let hash = hex::encode(ci.blake2_hash());
        let key = format!("/{}/replaced_backends/{}", self.prefix, hash);
        Ok(self.read_value(&key).await?.is_some())
    }

    async fn save_failure(
        &self,
        data_path: &Path,
        key_dir_path: &Option<PathBuf>,
        should_delete: bool,
    ) -> Result<(), MetaStoreError> {
        let abs_data_path = canonicalize(data_path).map_err(ZdbMetaStoreError::from)?;
        let key = self.build_failure_key(&abs_data_path)?;

        let meta = bincode::serialize(&FailureMeta::new(
            abs_data_path,
            match key_dir_path {
                None => None,
                Some(kp) => Some(canonicalize(kp).map_err(ZdbMetaStoreError::from)?),
            },
            should_delete,
        ))
        // unwrap here is fine since an error here is a programming error
        .unwrap();

        // unwrap here is safe as we already validated that to_str() returns some when building the
        // failure key
        Ok(self.write_value(&key, &meta).await?)
    }

    async fn delete_failure(&self, fm: &FailureMeta) -> Result<(), MetaStoreError> {
        let key = self.build_failure_key(fm.data_path())?;

        Ok(self.delete_value(&key).await?)
    }

    async fn get_failures(&self) -> Result<Vec<FailureMeta>, MetaStoreError> {
        // pin the stream on the heap for now
        let prefix = format!("/{}/failures/", self.prefix);
        let meta_keys = Box::pin(self.keys(&prefix).await?);
        let keys: Vec<String> = meta_keys.collect().await;
        let mut data = Vec::with_capacity(keys.len());
        for key in keys.into_iter() {
            let value = match self.read_value(&key).await {
                Ok(value) => match value {
                    Some(value) => value,
                    None => {
                        warn!("Empty value for key {}", key);
                        continue;
                    }
                },
                Err(e) => {
                    error!("error in key iteration: {}", e);
                    continue;
                }
            };

            let meta = match bincode::deserialize(&value) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Corrupted metadata at key {}: {}", key, e);
                    continue;
                }
            };
            data.push(meta);
        }

        Ok(data)
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
            InternalError::Io(ref e) => Some(e),
            InternalError::Other(_) => None,
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
    /// An error regarding a key.
    Key,
    /// An otherwise unspecified Io error.
    Io,
    /// Insufficient healthy backends to perform the request.
    InsufficientHealthBackends,
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
                ErrorKind::Key => "KEY",
                ErrorKind::Io => "IO",
                ErrorKind::InsufficientHealthBackends => "Insufficient healthy backends",
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
    Io(io::Error),
    Other(String),
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
                InternalError::Io(ref e) => e,
                InternalError::Other(ref e) => e,
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

impl From<io::Error> for ZdbMetaStoreError {
    fn from(e: io::Error) -> Self {
        ZdbMetaStoreError {
            kind: ErrorKind::Io,
            internal: InternalError::Io(e),
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
        write!(f, "key reconstruction failed, data is corrupted. Need at least {} shards, only {} available", self.required_shards, self.available_shards)
    }
}

impl std::error::Error for CorruptedKey {}

impl From<ZdbMetaStoreError> for MetaStoreError {
    fn from(e: ZdbMetaStoreError) -> Self {
        Self::new(Box::new(e))
    }
}
