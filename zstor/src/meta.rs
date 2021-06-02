use crate::config::{self, Compression, Encryption};
use crate::encryption;
use crate::zdb::{Key, UserKeyZdb, ZdbConnectionInfo};
use crate::zdb_meta::ZdbMetaStore;
use async_trait::async_trait;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{fmt, fs, io};

/// The length of file and shard checksums
pub const CHECKSUM_LENGTH: usize = 16;
/// A checksum of a data object
pub type Checksum = [u8; CHECKSUM_LENGTH];

/// MetaData holds all information needed to retrieve, decode, decrypt and decompress shards back
/// to the original data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetaData {
    /// The minimum amount of shards which are needed to recover the original data.
    data_shards: usize,
    /// The amount of redundant data shards which are generated when the data is encoded. Essentially,
    /// this many shards can be lost while still being able to recover the original data.
    parity_shards: usize,
    /// Checksum of the full file
    checksum: Checksum,
    /// configuration to use for the encryption stage
    encryption: Encryption,
    /// configuration to use for the compression stage
    compression: Compression,
    /// Information about where the shards are
    shards: Vec<ShardInfo>,
}

/// Information needed to store a single data shard
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardInfo {
    shard_idx: usize,
    checksum: Checksum,
    keys: Vec<Key>,
    ci: ZdbConnectionInfo,
}

impl MetaData {
    /// Create new encoding metadata.
    pub fn new(
        data_shards: usize,
        parity_shards: usize,
        checksum: Checksum,
        encryption: Encryption,
        compression: Compression,
    ) -> Self {
        Self {
            data_shards,
            parity_shards,
            checksum,
            encryption,
            compression,
            shards: Vec::with_capacity(data_shards + parity_shards),
        }
    }

    /// Add new shard information to the metadata. Since shard order is important for the recovery
    /// process, this must be done in order.
    pub fn add_shard(&mut self, si: ShardInfo) {
        self.shards.push(si)
    }

    /// Return the amount of data shards used for encoding this object.
    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// Return the amount of parity shards used for encoding this object.
    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    /// Return the encryption config used for encoding this object.
    pub fn encryption(&self) -> &Encryption {
        &self.encryption
    }

    /// Return the compression config used for encoding this object.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Return the information about where the shards are stored for this object.
    pub fn shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    /// Return the checksum of the file
    pub fn checksum(&self) -> &Checksum {
        &self.checksum
    }
}

impl ShardInfo {
    /// Create a new shardinfo, from the connectioninfo for the zdb (namespace) and the actual key
    /// in which the data is stored
    pub fn new(
        shard_idx: usize,
        checksum: Checksum,
        keys: Vec<Key>,
        ci: ZdbConnectionInfo,
    ) -> Self {
        Self {
            shard_idx,
            checksum,
            keys,
            ci,
        }
    }

    /// Get the index of this shard in the encoding sequence
    pub fn index(&self) -> usize {
        self.shard_idx
    }

    /// Get a reference to the connection info needed to reach the zdb namespace where this shard
    /// is stored.
    pub fn zdb(&self) -> &ZdbConnectionInfo {
        &self.ci
    }

    /// Get the key used to store the shard
    pub fn key(&self) -> &[Key] {
        &self.keys
    }

    /// Get the checksum of this shard
    pub fn checksum(&self) -> &Checksum {
        &self.checksum
    }
}

#[async_trait]
/// MetaStore defines `something` which can store metadata. The encoding of the metadata is an
/// internal detail of the metadata storage.
pub trait MetaStore {
    /// Save the metadata for the file identified by `path` with a given prefix
    async fn save_meta(&self, path: &Path, meta: &MetaData) -> Result<(), MetaStoreError>;

    /// Save the metadata for a given key
    async fn save_meta_by_key(&self, key: &str, meta: &MetaData) -> Result<(), MetaStoreError>;

    /// loads the metadata for a given path and prefix
    async fn load_meta(&self, path: &Path) -> Result<Option<MetaData>, MetaStoreError>;

    /// loads the metadata for a given path and prefix
    async fn load_meta_by_key(&self, key: &str) -> Result<Option<MetaData>, MetaStoreError>;

    /// Mark a Zdb backend as replaced based on its connection info
    async fn set_replaced(&self, ci: &ZdbConnectionInfo) -> Result<(), MetaStoreError>;

    /// Check to see if a Zdb backend has been marked as replaced based on its connection info
    async fn is_replaced(&self, ci: &ZdbConnectionInfo) -> Result<bool, MetaStoreError>;

    /// Get the (key, metadata) for all stored objects
    async fn object_metas(&self) -> Result<Vec<(String, MetaData)>, MetaStoreError>;

    /// Save info about a failed upload under the failures key
    async fn save_failure(
        &self,
        data_path: &Path,
        key_dir_path: &Option<PathBuf>,
        should_delete: bool,
    ) -> Result<(), MetaStoreError>;

    /// Delete info about a failed upload from the failure key
    async fn delete_failure(&self, fm: &FailureMeta) -> Result<(), MetaStoreError>;

    /// Get all the paths of files which failed to upload
    async fn get_failures(&self) -> Result<Vec<FailureMeta>, MetaStoreError>;
}

/// A high lvl error returned by the metadata store
#[derive(Debug)]
pub struct MetaStoreError {
    error: Box<dyn std::error::Error + Send>,
}

impl MetaStoreError {
    /// Create a new metastore error which wraps an existing error
    pub fn new(error: Box<dyn std::error::Error + Send>) -> Self {
        Self { error }
    }
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for MetaStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
}

/// The result type for the metastore interface
pub type MetaStoreResult<T> = Result<T, MetaStoreError>;

/// Create a new metastore for the provided config
pub async fn new_metastore(
    cfg: &config::Config,
) -> MetaStoreResult<Box<dyn MetaStore + Send + Sync>> {
    match cfg.meta() {
        config::Meta::Zdb(zdb_cfg) => {
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
            let store = ZdbMetaStore::new(
                backends,
                encoder,
                encryptor,
                zdb_cfg.prefix().to_string(),
                cfg.virtual_root().clone(),
            );
            Ok(Box::new(store))
        }
    }
}

/// Information about a failed invocation of zstor
#[derive(Debug, Deserialize, Serialize)]
pub struct FailureMeta {
    data_path: PathBuf,
    key_dir_path: Option<PathBuf>,
    should_delete: bool,
}

impl FailureMeta {
    /// Create a new instance of failure metadata
    pub fn new(data_path: PathBuf, key_dir_path: Option<PathBuf>, should_delete: bool) -> Self {
        Self {
            data_path,
            key_dir_path,
            should_delete,
        }
    }
    /// Returns the path to the data file used for uploading
    pub fn data_path(&self) -> &PathBuf {
        &self.data_path
    }

    /// Returns the path to the key dir, it is was set
    pub fn key_dir_path(&self) -> &Option<PathBuf> {
        &self.key_dir_path
    }

    /// Returns if the should-delete flag was set
    pub fn should_delete(&self) -> bool {
        self.should_delete
    }
}

/// Canonicalizes a path, even if it does not exist
pub(crate) fn canonicalize(path: &Path) -> io::Result<PathBuf> {
    // annoyingly, the path needs to exist for this to work. So here's the plan:
    // first we verify that it is actualy there
    // if it is, no problem
    // else, create a temp file, canonicalize that path, and remove the temp file again
    match fs::metadata(path) {
        Ok(_) => Ok(path.canonicalize()?),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => {
                fs::File::create(path)?;
                let cp = path.canonicalize()?;
                fs::remove_file(path)?;
                Ok(cp)
            }
            _ => Err(e),
        },
    }
}
