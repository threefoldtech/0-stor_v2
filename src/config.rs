use crate::{encryption::SymmetricKey, zdb::ZdbConnectionInfo};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

/// The full configuration for the data encoding and decoding. This included the etcd to save the
/// data to, as well as all backends which may or may not be used when data is written.
///
/// Backends are separated into groups. A single group _should_ represent physically close nodes,
/// which are e.g. in the same data center. A single group can have multiple storage nodes.
/// Redundancy is specified both on group level, and on nodes in a single group level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The minimum amount of shards which are needed to recover the original data.
    data_shards: usize,
    /// The amount of redundant data shards which are generated when the data is encoded. Essentially,
    /// this many shards can be lost while still being able to recover the original data.
    parity_shards: usize,
    /// The amount of groups which one should be able to loose while still being able to recover
    /// the original data.
    redundant_groups: usize,
    /// The amount of nodes that can be lost in every group while still being able to recover the
    /// original data.
    redundant_nodes: usize,
    /// virtual root on the filesystem to use, this path will be removed from all files saved. If
    /// a file path is loaded, the path will be interpreted as relative to this directory
    root: Option<std::path::PathBuf>,
    /// configuration to use for the encryption stage
    encryption: Encryption,
    /// configuration to use for the compression stage
    compression: Compression,
    /// configuration for the metadata store to use
    meta: Meta,
    /// The backend groups to write the data to.
    groups: Vec<Group>,
}

/// A collection of backends to write to, which _should_ be geographically close to each other.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    /// The individual backends in the group.
    backends: Vec<ZdbConnectionInfo>,
}

/// Configuration for the used encryption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Encryption {
    /// Algorithm to use.
    algorithm: String, // TODO: make enum
    /// encryption key to use in hex form. The key must be 64 characters long (32 bytes).
    key: SymmetricKey,
}

/// Configuration for the used compression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compression {
    /// Algorithm to use.
    algorithm: String, // TODO: make enum
}

/// Configuration for the metadata store to use
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", content = "config")]
#[serde(rename_all = "lowercase")]
pub enum Meta {
    /// Write metadata to an etc cluster
    ETCD(crate::etcd::EtcdConfig),
}

impl Config {
    /// validate the config. This also makes sure that there is at least 1 valid configuration for
    /// the backends regarding groups and the required redundancy.
    pub fn validate(&self) -> Result<(), String> {
        let backend_len = self
            .groups
            .iter()
            .fold(0, |total, group| total + group.backends.len());
        if backend_len < self.parity_shards + self.data_shards {
            return Err(format!(
                "insufficient data backends, require at least {}, only found {}",
                self.data_shards + self.parity_shards,
                backend_len
            ));
        };
        if self.encryption.algorithm != "AES" {
            return Err(format!(
                "unknown encryption algorithm {}",
                self.encryption.algorithm
            ));
        }
        if self.compression.algorithm != "snappy" {
            return Err(format!(
                "unknown compression algorithm {}",
                self.compression.algorithm
            ));
        }

        Ok(())
    }

    /// Get the amount of data shards to use for the encoding
    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// Get the amount of parity shards to use for the encoding
    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    /// Return the virtual root set in the config, if any
    pub fn virtual_root(&self) -> &Option<std::path::PathBuf> {
        &self.root
    }

    /// Return the encryption config to use for encoding this object.
    pub fn encryption(&self) -> &Encryption {
        &self.encryption
    }

    /// Return the compression config to use for encoding this object.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Return the metastore configuration from the config
    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Returns a list of 0-db's to use for storage of the data shards, in accordance to the
    /// encoding profile and redundancy policies. If no valid configuration can be found, an error
    /// is returned. If multiple valid configurations are found, one is selected at random.
    pub fn shard_stores(&self) -> Result<Vec<ZdbConnectionInfo>, String> {
        // The challange here is to find a valid list of shards. We need exactly `data_shards +
        // parity_shards` shards in total. We assume every shard in every group is valid.
        // Furthermore, we need to make sure that if any `redundant_groups` groups are lost, we
        // still have sufficient shards left to recover the data. Also, for every group we should
        // be able to loose `redundant_nodes` nodes, and still be able to recover the data. It is
        // acceptable to not find any good setup.

        // used groups must be <= parity_shards/redundant_nodes, otherwise losing the max amount of
        // nodes per group will lose too many shards
        let max_groups = if self.redundant_nodes == 0 {
            self.groups.len()
        } else {
            // add the redundant groups to the max groups, if we lose the entire group we no longer
            // care about the individual nodes in the group after all
            self.parity_shards / self.redundant_nodes + self.redundant_groups
        };

        // Get the index of every group for later lookup, eliminate groups which are statically too
        // small
        let groups: Vec<_> = self
            .groups
            .iter()
            .filter(|group| group.backends.len() >= self.redundant_nodes)
            .collect();

        let mut candidates = Vec::new();
        // high enough capacity so we don't reallocate
        let mut candidate = Vec::with_capacity(groups.len());
        // generate possible group configs
        use gray_codes::{InclusionExclusion, SetMutation};
        for mutation in InclusionExclusion::of_len(groups.len()) {
            match mutation {
                SetMutation::Insert(i) => candidate.push((i, groups[i])),
                SetMutation::Remove(ref i) => {
                    candidate = candidate.into_iter().filter(|(j, _)| i != j).collect()
                }
            }

            if candidate.len() <= max_groups
                && candidate.len() > self.redundant_groups
                && candidate
                    .iter()
                    .map(|(_, group)| group.backends.len() - self.redundant_nodes)
                    .sum::<usize>()
                    >= self.data_shards
            {
                candidates.push(candidate.clone());
            }
        }

        // so now we have all configurations which have sufficient capacity to hold all shards,
        // while still within the bouns of the redundant_nodes option
        if candidates.len() == 0 {
            return Err(
                "could not find any viable backend distribution to statisfy redundancy requirement"
                    .to_string(),
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
                self.data_shards + self.parity_shards,
                &mut buckets,
                &mut possible_configs,
                &candidate,
            );
        }

        // at this point we should have a list of _all_ possible configs
        if possible_configs.len() == 0 {
            return Err(
                "unable to find a valid configuration due to redundancy settings".to_string(),
            );
        }

        // randomly pick a solution
        // unwrap is safe as we already established that we have at least 1 solution
        let shard_distribution = possible_configs.choose(&mut rand::thread_rng()).unwrap();

        let mut backends = Vec::with_capacity(self.data_shards + self.parity_shards);
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
        // cmp second to first so we sort large -> small TODO: verify
        buckets_used.sort_by(|(_, used_1), (_, used_2)| used_2.cmp(used_1));
        // verify that we still have sufficient data shards left if: we lose all
        // redundant_groups nodes AND we lose redundant_nodes nodes in the remaining
        // groups
        if buckets_used
            .iter()
            .skip(self.redundant_groups)
            .map(|(_, shard_count)| shard_count - self.redundant_nodes)
            .sum::<usize>()
            >= self.data_shards
        {
            possible_configs.push(buckets_used);
        }
    }
}

impl Encryption {
    /// Create a new encryption instance
    pub fn new(algorithm: &str, key: &SymmetricKey) -> Self {
        Self {
            algorithm: algorithm.to_owned(),
            key: key.clone(),
        }
    }

    /// Get the name of the encryption algorithm to use
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// Get the key to use for the encryption algorithm
    pub fn key(&self) -> &SymmetricKey {
        &self.key
    }
}

impl Compression {
    /// Create a new compression instance
    pub fn new(algorithm: &str) -> Self {
        Self {
            algorithm: algorithm.to_owned(),
        }
    }

    /// Get the name of the compression algorithm to use
    pub fn algorithm(&self) -> &str {
        &self.algorithm
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
            data_shards: 10,
            parity_shards: 5,
            redundant_groups: 1,
            redundant_nodes: 1,
            groups: vec![
                super::Group {
                    backends: vec![saddr, saddr2],
                },
                super::Group {
                    backends: vec![saddr3, saddr4],
                },
            ],
            encryption: super::Encryption {
                key: SymmetricKey::new([0; 32]),
                algorithm: "AES".into(),
            },
            compression: super::Compression {
                algorithm: "snappy".into(),
            },
            root: Some(std::path::PathBuf::from("/virtualroot")),
            meta: super::Meta::ETCD(crate::etcd::EtcdConfig::new(
                vec![
                    "http://127.0.0.1:2379".to_string(),
                    "http://127.0.0.1:22379".to_string(),
                    "http://127.0.0.1:32379".to_string(),
                ],
                "someprefix".to_string(),
                None,
                None,
            )),
        };

        let expected = r#"data_shards = 10
parity_shards = 5
redundant_groups = 1
redundant_nodes = 1
root = "/virtualroot"

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

[meta]
type = "etcd"

[meta.config]
endpoints = ["http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379"]
prefix = "someprefix"

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
            data_shards: 10,
            parity_shards: 5,
            redundant_groups: 1,
            redundant_nodes: 1,
            groups: vec![
                super::Group {
                    backends: vec![saddr, saddr2],
                },
                super::Group {
                    backends: vec![saddr3, saddr4],
                },
            ],
            encryption: super::Encryption {
                key: SymmetricKey::new([0; 32]),
                algorithm: "AES".into(),
            },
            compression: super::Compression {
                algorithm: "snappy".into(),
            },
            root: Some(std::path::PathBuf::from("/virtualroot")),
            meta: super::Meta::ETCD(crate::etcd::EtcdConfig::new(
                vec![
                    "http://127.0.0.1:2379".to_string(),
                    "http://127.0.0.1:22379".to_string(),
                    "http://127.0.0.1:32379".to_string(),
                ],
                "someprefix".to_string(),
                None,
                None,
            )),
        };

        let input = r#"data_shards = 10
parity_shards = 5
redundant_groups = 1
redundant_nodes = 1
root = "/virtualroot"

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

[meta]
type = "etcd"

[meta.config]
endpoints = ["http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379"]
prefix = "someprefix"

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
