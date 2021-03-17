use crate::config::{Compression, Encryption};
use crate::zdb::{Key, ZdbConnectionInfo};
use serde::{Deserialize, Serialize};

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
    #[serde(flatten)]
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
