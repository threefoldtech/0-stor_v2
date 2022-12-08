use crate::{encryption::SymmetricKey, zdb::ZdbConnectionInfo};
use gray_codes::{InclusionExclusion, SetMutation};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The full configuration for the data encoding and decoding. This included the metastore to save the
/// data to, as well as all backends which may or may not be used when data is written.
///
/// Backends are separated into groups. A single group _should_ represent physically close nodes,
/// which are e.g. in the same data center. A single group can have multiple storage nodes.
/// Redundancy is specified both on group level, and on nodes in a single group level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The minimum amount of shards which are needed to recover the original data.
    pub minimal_shards: usize,
    /// The amount of redundant data shards which are generated when the data is encoded. Essentially,
    /// this many shards can be lost while still being able to recover the original data.
    pub expected_shards: usize,
    /// The amount of groups which one should be able to loose while still being able to recover
    /// the original data.
    pub redundant_groups: usize,
    /// The amount of nodes that can be lost in every group while still being able to recover the
    /// original data.
    pub redundant_nodes: usize,
    /// virtual root on the filesystem to use, this path will be removed from all files saved. If
    /// a file path is loaded, the path will be interpreted as relative to this directory
    pub root: Option<PathBuf>,
    /// Optional path to a unix socket. This socket is required in case zstor needs to run in
    /// daemon mode. If this is present, zstor invocations will first try to connect to the
    /// socket. If it is not found, the command is run in-process, else it is encoded and send to
    /// the socket so the daemon can process it.
    pub socket: Option<PathBuf>,
    /// Optional path to file to store the daemon pid. If another daemon already running
    /// the process won't start another.
    pub pid_file: Option<PathBuf>,
    /// Optional path to the local 0-db data file directory. If set, it will be monitored and kept
    /// within the size limits.
    pub zdb_data_dir_path: Option<PathBuf>,
    /// Maximum size of the data dir in MiB, if this is set and the sum of the file sizes in the
    /// data dir gets higher than this value, the least used, already encoded file will be removed.
    pub max_zdb_data_dir_size: Option<u64>,
    /// The mount point of an optional 0-db-fs. If present, stats will be collected from the
    /// 0-db-fs process.
    pub zdbfs_mountpoint: Option<PathBuf>,
    /// An optional port on which prometheus metrics will be exposed. If this is not set, the
    /// metrics will not get exposed.
    pub prometheus_port: Option<u16>,
    /// configuration to use for the encryption stage.
    pub encryption: Encryption,
    /// configuration to use for the compression stage.
    pub compression: Compression,
    /// configuration for the metadata store to use.
    pub meta: Meta,
    /// The backend groups to write the data to.
    pub groups: Vec<Group>,
}

/// A collection of backends to write to, which _should_ be geographically close to each other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    /// The individual backends in the group.
    pub backends: Vec<ZdbConnectionInfo>,
}

impl Group {
    /// Returns a list of all [`ZdbConnectionInfo`] objects in this [`Group`].
    pub fn backends(&self) -> &[ZdbConnectionInfo] {
        &self.backends
    }
}

/// Configuration for the used encryption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "algorithm", content = "key")]
#[serde(rename_all = "UPPERCASE")]
pub enum Encryption {
    /// Aes-Gcm authenticated encryption scheme using the AES cipher in GCM mode. The 256 bit
    /// variant is used, which requires a 32 byte key
    Aes(SymmetricKey),
}

/// Configuration for the used compression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "algorithm")]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// The snappy encryption algorithm
    Snappy,
}

/// Configuration for the metadata store to use
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", content = "config")]
#[serde(rename_all = "lowercase")]
pub enum Meta {
    /// Metadata storage on top of user-key zdbs, with client side redundancy and encryption
    Zdb(crate::zdb_meta::ZdbMetaStoreConfig),
}

impl Config {
    /// validate the config. This also makes sure that there is at least 1 valid configuration for
    /// the backends regarding groups and the required redundancy.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let backend_len = self
            .groups
            .iter()
            .fold(0, |total, group| total + group.backends.len());
        if backend_len < self.expected_shards {
            return Err(format!(
                "insufficient data backends, require at least {}, only found {}",
                self.expected_shards, backend_len
            )
            .into());
        };

        Ok(())
    }

    /// Get the amount of data shards to use for the encoding
    pub fn data_shards(&self) -> usize {
        self.minimal_shards
    }

    /// Get the amount of disposable_shards shards to use for the encoding
    pub fn disposable_shards(&self) -> usize {
        self.expected_shards - self.minimal_shards
    }

    /// Return the virtual root set in the config, if any
    pub fn virtual_root(&self) -> &Option<std::path::PathBuf> {
        &self.root
    }

    /// Return the prometheus port on which prometheus formatted metrics will be served, if one is set.
    pub fn prometheus_port(&self) -> Option<u16> {
        self.prometheus_port
    }

    /// Return the encryption config to use for encoding this object.
    pub fn encryption(&self) -> &Encryption {
        &self.encryption
    }

    /// Return the compression config to use for encoding this object.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Return the metastore configuration from the config.
    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Return the socket path if it is set.
    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_ref().map(|x| x as _)
    }

    /// Return the pid file path if it is set.
    pub fn pid_file(&self) -> Option<&Path> {
        self.pid_file.as_ref().map(|x| x as _)
    }

    /// Return a list of all available backends in the config.
    pub fn backends(&self) -> Vec<&ZdbConnectionInfo> {
        self.groups
            .iter()
            .flat_map(|group| &group.backends)
            .collect()
    }

    /// Returns all backend groups in the config.
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }

    /// Returns the local 0-db data file directory path.
    pub fn zdb_data_dir_path(&self) -> Option<&Path> {
        self.zdb_data_dir_path.as_ref().map(|x| x as _)
    }

    /// Returns the local 0-db maximum data file directory size that is requested.
    pub fn max_zdb_data_dir_size(&self) -> Option<u64> {
        self.max_zdb_data_dir_size
    }

    /// Returns the mountpoint of 0-db-fs, if present.
    pub fn zdbfs_mountpoint(&self) -> Option<&Path> {
        self.zdbfs_mountpoint.as_ref().map(|x| x as _)
    }

    /// Remove a shard from the config. If the shard is present multiple times, all instances will
    /// be removed.
    /// TODO: remove this.
    pub fn remove_shard(&mut self, address: &ZdbConnectionInfo) {
        for mut group in &mut self.groups {
            group.backends = group
                .backends
                .drain(..)
                .filter(|backend| backend != address)
                .collect();
        }
    }

    /// Remove a backend from the config. If it should be present multiple times, all instances
    /// will be removed.
    pub fn remove_backend(&mut self, backend: &ZdbConnectionInfo) {
        for mut group in &mut self.groups {
            group.backends = group.backends.drain(..).filter(|b| b != backend).collect();
        }
    }

    /// Add a backend to the config in the specified group. If the group does not exist yet, it
    /// will be created.
    pub fn add_backend(&mut self, group_idx: usize, backend: ZdbConnectionInfo) {
        if group_idx >= self.groups.len() {
            self.groups.push(Group {
                backends: vec![backend],
            });
        } else {
            self.groups[group_idx].backends.push(backend);
        }
    }

    /// Sets the [`Meta`] metastore info of the config to the specified [`Meta`] info.
    pub fn set_meta(&mut self, meta: Meta) {
        self.meta = meta;
    }

    /// Returns a list of 0-db's to use for storage of the data shards, in accordance to the
    /// encoding profile and redundancy policies. If no valid configuration can be found, an error
    /// is returned. If multiple valid configurations are found, one is selected at random.
    pub fn shard_stores(&self) -> Result<Vec<ZdbConnectionInfo>, ConfigError> {
        // The challenge here is to find a valid list of shards. We need exactly `data_shards +
        // disposable_shards` shards in total. We assume every shard in every group is valid.
        // Furthermore, we need to make sure that if any `redundant_groups` groups are lost, we
        // still have sufficient shards left to recover the data. Also, for every group we should
        // be able to loose `redundant_nodes` nodes, and still be able to recover the data. It is
        // acceptable to not find any good setup.

        // used groups must be <= disposable_shards/redundant_nodes, otherwise losing the max amount
        // of nodes per group will lose too many shards
        let max_groups = if self.redundant_nodes == 0 {
            self.groups.len()
        } else {
            // add the redundant groups to the max groups, if we lose the entire group we no longer
            // care about the individual nodes in the group after all
            self.disposable_shards() / self.redundant_nodes + self.redundant_groups
        };
        // Get the index of every group for later lookup, eliminate groups which are statically too
        // small
        let groups: Vec<_> = self
            .groups
            .iter()
            .filter(|group| group.backends.len() >= self.redundant_nodes)
            .collect();

        // InclusionExclusion::of_len panics when groups is empty
        if groups.is_empty() {
            return Err("no available groups".to_string().into());
        }

        let mut candidates = Vec::new();
        // high enough capacity so we don't reallocate
        let mut candidate = Vec::with_capacity(groups.len());
        // generate possible group configs
        for mutation in InclusionExclusion::of_len(groups.len()) {
            match mutation {
                SetMutation::Insert(i) => candidate.push((i, groups[i])),
                SetMutation::Remove(ref i) => candidate.retain(|(j, _)| i != j),
            }

            if candidate.len() <= max_groups
                && candidate.len() > self.redundant_groups
                && candidate
                    .iter()
                    .map(|(_, group)| group.backends.len() - self.redundant_nodes)
                    .sum::<usize>()
                    >= self.minimal_shards
            {
                candidates.push(candidate.clone());
            }
        }

        // so now we have all configurations which have sufficient capacity to hold all shards,
        // while still within the bouns of the redundant_nodes option
        if candidates.is_empty() {
            return Err(
                "could not find any viable backend distribution to statisfy redundancy requirement"
                    .to_string()
                    .into(),
            );
        }

        // for every possible solution, generate an equal distribution over all nodes, then verify
        // that we still have sufficient data shards left if we lose the redundant_groups largest
        // groups and redundant_nodes shards from the other groups (must still be larger than data
        // shars)
        let mut possible_configs = Vec::new();
        for candidate in candidates {
            let mut buckets: Vec<_> = candidate
                .iter()
                .map(|(_, group)| group.backends.len())
                .collect();
            self.build_configs(
                self.expected_shards,
                &mut buckets,
                &mut possible_configs,
                &candidate,
            );
        }

        // at this point we should have a list of _all_ possible configs
        if possible_configs.is_empty() {
            return Err(
                "unable to find a valid configuration due to redundancy settings"
                    .to_string()
                    .into(),
            );
        }

        // randomly pick a solution
        // unwrap is safe as we already established that we have at least 1 solution
        let shard_distribution = possible_configs.choose(&mut rand::thread_rng()).unwrap();

        let mut backends = Vec::with_capacity(self.expected_shards);
        for (group_idx, amount) in shard_distribution {
            backends.extend(
                self.groups[*group_idx]
                    .backends
                    .choose_multiple(&mut rand::thread_rng(), *amount)
                    .into_iter()
                    .cloned(),
            );
        }

        Ok(backends)
    }

    // backtrack algorithm to find all ways to distribute n tokens in m buckets. If the last token is
    // passed, finalizer is called
    // A closure would be so clean here but it needs to be FnMut and then we can't both call it and
    // pass it down it seems ffs
    fn build_configs(
        &self,
        tokens_left: usize,
        buckets: &mut [usize],
        possible_configs: &mut Vec<Vec<(usize, usize)>>,
        candidate: &[(usize, &Group)],
    ) {
        for i in 0..buckets.len() {
            if buckets[i] > 0 {
                buckets[i] -= 1;
                if tokens_left - 1 == 0 {
                    self.add_valid_config(possible_configs, candidate, buckets);
                } else {
                    self.build_configs(tokens_left - 1, buckets, possible_configs, candidate);
                }
                buckets[i] += 1;
            }
        }
    }

    fn add_valid_config(
        &self,
        possible_configs: &mut Vec<Vec<(usize, usize)>>,
        candidate: &[(usize, &Group)],
        used_buckets: &[usize],
    ) {
        // flip the remaining slots in the buckets to actually used slots
        // since we are pessimistic remove the largest `redundant_groups` groups
        let mut buckets_used: Vec<_> = used_buckets
            .iter()
            .enumerate()
            .map(|(idx, remainder)| {
                let (orig, group) = candidate[idx];
                (orig, group.backends.len() - remainder)
            })
            .collect();
        // cmp second to first so we sort large -> small
        buckets_used.sort_by(|(_, used_1), (_, used_2)| used_2.cmp(used_1));
        // verify that we still have sufficient data shards left if: we lose all
        // redundant_groups nodes AND we lose redundant_nodes nodes in the remaining
        // groups
        if buckets_used
            .iter()
            .skip(self.redundant_groups)
            .map(|(_, shard_count)| shard_count - self.redundant_nodes)
            .sum::<usize>()
            >= self.minimal_shards
        {
            possible_configs.push(buckets_used);
        }
    }
}

/// An error in the configuration
#[derive(Debug)]
pub struct ConfigError {
    msg: String,
}

use std::fmt;
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

// default impls are fine here
impl std::error::Error for ConfigError {}

impl From<String> for ConfigError {
    fn from(s: String) -> Self {
        ConfigError { msg: s }
    }
}

#[cfg(test)]
mod tests {
    use crate::{encryption::SymmetricKey, zdb::ZdbConnectionInfo};
    use std::net::ToSocketAddrs;

    #[test]
    fn encoding() {
        let saddr = ZdbConnectionInfo::new(
            "[fe80::1]:9900".to_socket_addrs().unwrap().next().unwrap(),
            None,
            None,
        );
        let saddr2 = ZdbConnectionInfo::new(
            "[fe80::1]:9900".to_socket_addrs().unwrap().next().unwrap(),
            Some("test".to_string()),
            None,
        );
        let saddr3 = ZdbConnectionInfo::new(
            "[2a02:1802:5e::dead:babe]:9900"
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap(),
            None,
            None,
        );
        let saddr4 = ZdbConnectionInfo::new(
            "[2a02:1802:5e::dead:beef]:9900"
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap(),
            Some("test2".to_string()),
            Some("supersecretpass".to_string()),
        );
        let cfg = super::Config {
            minimal_shards: 10,
            expected_shards: 15,
            redundant_groups: 1,
            redundant_nodes: 1,
            socket: Some("/tmp/zstor.sock".into()),
            pid_file: Some("/tmp/zstor.pid".into()),
            zdb_data_dir_path: None,
            zdbfs_mountpoint: Some("/tmp/test".into()),
            prometheus_port: None,
            max_zdb_data_dir_size: None,
            groups: vec![
                super::Group {
                    backends: vec![saddr, saddr2],
                },
                super::Group {
                    backends: vec![saddr3, saddr4],
                },
            ],
            encryption: super::Encryption::Aes(SymmetricKey::new([0u8; 32])),
            compression: super::Compression::Snappy,
            root: Some(std::path::PathBuf::from("/virtualroot")),
            meta: super::Meta::Zdb(crate::zdb_meta::ZdbMetaStoreConfig::new(
                "someprefix".to_string(),
                super::Encryption::Aes(SymmetricKey::new([1u8; 32])),
                [
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9900"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9901"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9902"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9903"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                ],
            )),
        };

        let expected = r#"minimal_shards = 10
expected_shards = 15
redundant_groups = 1
redundant_nodes = 1
root = "/virtualroot"
socket = "/tmp/zstor.sock"
pid_file = "/tmp/zstor.pid"
zdbfs_mountpoint = "/tmp/test"

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

[meta]
type = "zdb"

[meta.config]
prefix = "someprefix"

[meta.config.encryption]
algorithm = "AES"
key = "0101010101010101010101010101010101010101010101010101010101010101"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9901"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9902"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9903"
namespace = "test2"
password = "supersecretpass"

[[groups]]
[[groups.backends]]
address = "[fe80::1]:9900"

[[groups.backends]]
address = "[fe80::1]:9900"
namespace = "test"

[[groups]]
[[groups.backends]]
address = "[2a02:1802:5e::dead:babe]:9900"

[[groups.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"
"#;
        assert_eq!(toml::to_string(&cfg).unwrap(), expected);
    }

    #[test]
    fn decoding() {
        let saddr = ZdbConnectionInfo::new(
            "[fe80::1]:9900".to_socket_addrs().unwrap().next().unwrap(),
            None,
            None,
        );
        let saddr2 = ZdbConnectionInfo::new(
            "[fe80::1]:9900".to_socket_addrs().unwrap().next().unwrap(),
            Some("test".to_string()),
            None,
        );
        let saddr3 = ZdbConnectionInfo::new(
            "[2a02:1802:5e::dead:babe]:9900"
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap(),
            None,
            None,
        );
        let saddr4 = ZdbConnectionInfo::new(
            "[2a02:1802:5e::dead:beef]:9900"
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap(),
            Some("test2".to_string()),
            Some("supersecretpass".to_string()),
        );
        let expected_cfg = super::Config {
            minimal_shards: 10,
            expected_shards: 15,
            redundant_groups: 1,
            redundant_nodes: 1,
            socket: Some("/tmp/zstor.sock".into()),
            pid_file: Some("/tmp/zstor.pid".into()),
            zdb_data_dir_path: None,
            zdbfs_mountpoint: Some("/tmp/test".into()),
            prometheus_port: None,
            max_zdb_data_dir_size: None,
            groups: vec![
                super::Group {
                    backends: vec![saddr, saddr2],
                },
                super::Group {
                    backends: vec![saddr3, saddr4],
                },
            ],
            encryption: super::Encryption::Aes(SymmetricKey::new([0; 32])),
            compression: super::Compression::Snappy,
            root: Some(std::path::PathBuf::from("/virtualroot")),
            meta: super::Meta::Zdb(crate::zdb_meta::ZdbMetaStoreConfig::new(
                "someprefix".to_string(),
                super::Encryption::Aes(SymmetricKey::new([1u8; 32])),
                [
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9900"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9901"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9902"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                    ZdbConnectionInfo::new(
                        "[2a02:1802:5e::dead:beef]:9903"
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                        Some("test2".to_string()),
                        Some("supersecretpass".to_string()),
                    ),
                ],
            )),
        };

        let input = r#"minimal_shards = 10
expected_shards = 15
redundant_groups = 1
redundant_nodes = 1
root = "/virtualroot"
socket = "/tmp/zstor.sock"
pid_file = "/tmp/zstor.pid"
zdbfs_mountpoint = "/tmp/test"

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

[meta]
type = "zdb"

[meta.config]
prefix = "someprefix"

[meta.config.encryption]
algorithm = "AES"
key = "0101010101010101010101010101010101010101010101010101010101010101"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9901"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9902"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9903"
namespace = "test2"
password = "supersecretpass"

[[groups]]
[[groups.backends]]
address = "[fe80::1]:9900"

[[groups.backends]]
address = "[fe80::1]:9900"
namespace = "test"

[[groups]]
[[groups.backends]]
address = "[2a02:1802:5e::dead:babe]:9900"

[[groups.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"
"#;
        assert_eq!(
            toml::from_str::<super::Config>(input).unwrap(),
            expected_cfg
        );
    }
}
