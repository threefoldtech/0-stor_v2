use crate::{encryption::SymmetricKey, zdb::ZdbConnectionInfo};
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
    /// configuration to use for the encryption stage
    encryption: Encryption,
    /// configuration to use for the compression stage
    compression: Compression,
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
        // TODO
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

    /// Return the encryption config to use for encoding this object.
    pub fn encryption(&self) -> &Encryption {
        &self.encryption
    }

    /// Return the compression config to use for encoding this object.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Returns a list of 0-db's to use for storage of the data shards, in accordance to the
    /// encoding profile and redundancy policies. If no valid configuration can be found, an error
    /// is returned. If multiple valid configurations are found, one is selected at random.
    pub fn shard_stores(&self) -> Result<Vec<ZdbConnectionInfo>, String> {
        // TODO: temp
        Ok(self
            .groups
            .iter()
            .map(|group| group.backends.clone())
            .flatten()
            .collect())
        // unimplemented!();
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
        };

        let expected = r#"data_shards = 10
parity_shards = 5
redundant_groups = 1
redundant_nodes = 1

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

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
        };

        let input = r#"data_shards = 10
parity_shards = 5
redundant_groups = 1
redundant_nodes = 1

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

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
