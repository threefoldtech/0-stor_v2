use log::{debug, trace};
use redis::{aio::Connection, ConnectionAddr, ConnectionInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
// use sha1::{Digest, Sha1};
use std::fmt;

use std::convert::TryInto;
use std::net::SocketAddr;

/// The type of key's used in zdb in sequential mode.
pub type Key = u32;

/// The result type as used by this module.
pub type ZdbResult<T> = Result<T, ZdbError>;

// Max size of a single entry in zdb
const MAX_ZDB_CHUNK_SIZE: usize = 2 * 1024 * 1024;
// Max allowed duration for an operation
// Since max chunk size is 2MiB, 30 seconds would mean a throughput of ~69.9 KB/s
const ZDB_TIMEOUT: Duration = Duration::from_secs(30);

// TODO impl debug
/// An open connection to a 0-db instance. The connection might not be valid after opening (e.g. if
/// the remote closed). No reconnection is attempted.
pub struct Zdb {
    conn: Connection,
    // connection info tracked to conveniently inspect the remote address and namespace.
    ci: ZdbConnectionInfo,
}

impl fmt::Debug for Zdb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZDB at {}", self.ci.address)
    }
}

/// Connection info for a 0-db (namespace).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZdbConnectionInfo {
    address: SocketAddr,
    namespace: Option<String>,
    password: Option<String>,
}

impl ZdbConnectionInfo {
    /// Create a new connection info object from a socket address, and an optional namespace and
    /// namespace password.
    pub fn new(address: SocketAddr, namespace: Option<String>, password: Option<String>) -> Self {
        Self {
            address,
            namespace,
            password,
        }
    }

    /// Get the address of the 0-db.
    pub fn address(&self) -> &SocketAddr {
        &self.address
    }
}

impl Zdb {
    /// Create a new connection to a zdb instance. The connection is opened and verified by means
    /// of the PING command. If provided, a namespace is also opened. SECURE AUTH is used to
    /// authenticate.
    pub async fn new(info: ZdbConnectionInfo) -> ZdbResult<Self> {
        // It appears there is a small bug in the library when specifying an ipv6 connection
        // String. Although there is some similar behavior to the `redis-cli` tool, there are also
        // valid strings which are outright failing. To work around this, manually construct the
        // address information
        // NOTE: this behavior appears to be consistent with the `redis-cli` interface
        debug!("connecting to zdb at {}", info.address);
        let ci = ConnectionInfo {
            addr: Box::new(ConnectionAddr::Tcp(
                info.address.ip().to_string(),
                info.address.port(),
            )),
            db: 0,
            username: None,
            passwd: None,
        };

        let client = redis::Client::open(ci.clone()).map_err(|e| ZdbError {
            kind: ZdbErrorKind::Connect,
            remote: info.address,
            internal: ErrorCause::Redis(e),
        })?;
        let mut conn = timeout(ZDB_TIMEOUT, client.get_async_connection())
            .await
            .map_err(|_| ZdbError {
                kind: ZdbErrorKind::Connect,
                remote: info.address,
                internal: ErrorCause::Timeout,
            })?
            .map_err(|e| ZdbError {
                kind: ZdbErrorKind::Connect,
                remote: info.address,
                internal: ErrorCause::Redis(e),
            })?;
        trace!("opened connection to db");
        trace!("pinging db");
        timeout(ZDB_TIMEOUT, redis::cmd("PING").query_async(&mut conn))
            .await
            .map_err(|_| ZdbError {
                kind: ZdbErrorKind::Connect,
                remote: info.address,
                internal: ErrorCause::Timeout,
            })?
            .map_err(|e| ZdbError {
                kind: ZdbErrorKind::Connect,
                remote: info.address,
                internal: ErrorCause::Redis(e),
            })?;
        trace!("db connection established");
        // open the correct namespace, with or without password
        if let Some(ns) = &info.namespace {
            let mut ns_select = redis::cmd("SELECT");
            ns_select.arg(ns);
            if let Some(pass) = &info.password {
                trace!("password authenticating to namespace");
                ns_select.arg(pass);
                // trace!("requesting AUTH challange");
                // // request AUTH challenge
                // let challenge: String = redis::cmd("AUTH")
                //     .arg("SECURE")
                //     .arg("CHALLENGE")
                //     .query_async(&mut conn)
                //     .await
                //     .map_err(|e| e.to_string())?;
                // trace!("got challange {}", challenge);
                // let mut hasher = Sha1::new();
                // hasher.update(format!("{}:{}", challenge, pass).as_bytes());
                // let result = hex::encode(hasher.finalize());
                // trace!("auth result {}", result);
                // ns_select.arg("SECURE").arg(result);
            }
            trace!("opening namespace {}", ns);
            timeout(ZDB_TIMEOUT, ns_select.query_async(&mut conn))
                .await
                .map_err(|_| ZdbError {
                    // try to guess the right kind of failure based on wether we are authenticating
                    // or not
                    kind: if let Some(_) = &info.password {
                        ZdbErrorKind::Auth
                    } else {
                        ZdbErrorKind::Ns
                    },
                    remote: info.address,
                    internal: ErrorCause::Timeout,
                })?
                .map_err(|e| ZdbError {
                    kind: if let redis::ErrorKind::AuthenticationFailed = e.kind() {
                        ZdbErrorKind::Auth
                    } else {
                        ZdbErrorKind::Ns
                    },
                    remote: info.address,
                    internal: ErrorCause::Redis(e),
                })?;
            trace!("opened namespace");
        }

        Ok(Self { conn, ci: info })
    }

    /// Store some data in the zdb. The generated keys are returned for later retrieval.
    /// Multiple keys might be returned since zdb only allows for up to 8MB of data per request,
    /// so we internally chunk the data.
    pub async fn set(&mut self, data: &[u8]) -> ZdbResult<Vec<Key>> {
        trace!("storing data in zdb (length: {})", data.len());

        let mut keys =
            Vec::with_capacity((data.len() as f64 / MAX_ZDB_CHUNK_SIZE as f64).ceil() as usize);

        for chunk in data.chunks(MAX_ZDB_CHUNK_SIZE) {
            trace!("writing chunk of size {}", chunk.len());
            let raw_key: Vec<u8> = timeout(
                ZDB_TIMEOUT,
                redis::cmd("SET")
                    .arg::<&[u8]>(&[])
                    .arg(chunk)
                    .query_async(&mut self.conn),
            )
            .await
            .map_err(|_| ZdbError {
                kind: ZdbErrorKind::Write,
                remote: self.ci.address,
                internal: ErrorCause::Timeout,
            })?
            .map_err(|e| ZdbError {
                kind: ZdbErrorKind::Write,
                remote: self.ci.address,
                internal: ErrorCause::Redis(e),
            })?;

            // if a key is given, we just return that. Otherwise we interpret the returned byteslice as
            // a key
            debug_assert!(raw_key.len() == std::mem::size_of::<Key>());
            keys.push(read_le_key(&raw_key))
        }
        Ok(keys)
    }

    /// Retrieve some previously stored data with its keys
    pub async fn get(&mut self, keys: &[Key]) -> ZdbResult<Option<Vec<u8>>> {
        let mut data: Vec<u8> = Vec::new();
        for key in keys {
            trace!("loading data at key {}", key);
            data.extend_from_slice(
                timeout(
                    ZDB_TIMEOUT,
                    redis::cmd("GET")
                        .arg(&key.to_le_bytes())
                        .query_async::<_, Option<Vec<u8>>>(&mut self.conn),
                )
                .await
                .map_err(|_| ZdbError {
                    kind: ZdbErrorKind::Read,
                    remote: self.ci.address,
                    internal: ErrorCause::Timeout,
                })?
                .map_err(|e| ZdbError {
                    kind: ZdbErrorKind::Read,
                    remote: self.ci.address,
                    internal: ErrorCause::Redis(e),
                })?
                .ok_or(ZdbError {
                    kind: ZdbErrorKind::Read,
                    remote: self.ci.address,
                    internal: ErrorCause::Other(format!("missing key {}", key)),
                })?
                .as_ref(),
            );
        }
        Ok(Some(data))
    }

    /// Query info about the namespace.
    pub async fn ns_info(&mut self) -> ZdbResult<NsInfo> {
        let list: String = timeout(
            ZDB_TIMEOUT,
            redis::cmd("NSINFO")
                .arg(if let Some(ref ns) = self.ci.namespace {
                    ns
                } else {
                    "default"
                })
                .query_async(&mut self.conn),
        )
        .await
        .map_err(|_| ZdbError {
            kind: ZdbErrorKind::Read,
            remote: self.ci.address,
            internal: ErrorCause::Timeout,
        })?
        .map_err(|e| ZdbError {
            kind: ZdbErrorKind::Read,
            remote: self.ci.address,
            internal: ErrorCause::Redis(e),
        })?;

        let kvs: HashMap<_, _> = list
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.starts_with('#'))
            .map(|line| {
                let mut split = line.split(": ");
                // unwraps are safe because fixed
                (
                    split.next().or(Some("")).unwrap(),
                    split.next().or(Some("")).unwrap(),
                )
            })
            .collect();

        Ok(NsInfo {
            name: kvs["name"].to_string(),
            entries: kvs["entries"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!("expected entries to be an integer ({})", e)),
            })?,
            public: match kvs["public"] {
                "yes" => true,
                "no" => false,
                _ => {
                    return Err(ZdbError {
                        kind: ZdbErrorKind::Format,
                        remote: self.ci.address,
                        internal: ErrorCause::Other("expected public to be yes/no".to_string()),
                    })
                }
            },
            password: match kvs["password"] {
                "yes" => true,
                "no" => false,
                _ => {
                    return Err(ZdbError {
                        kind: ZdbErrorKind::Format,
                        remote: self.ci.address,
                        internal: ErrorCause::Other("expected password to be yes/no".to_string()),
                    })
                }
            },
            data_size_bytes: kvs["data_size_bytes"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!(
                    "expected data_size_bytes to be an integer ({})",
                    e
                )),
            })?,
            data_limit_bytes: match kvs["data_limits_bytes"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!(
                    "expected data_limit_bytes to be an integer ({})",
                    e
                )),
            })? {
                0 => None,
                limit => Some(limit),
            },
            index_size_bytes: kvs["index_size_bytes"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!(
                    "expected index_size_bytes to be an integer ({})",
                    e
                )),
            })?,
            mode: match kvs["mode"] {
                "userkey" => ZdbRunMode::User,
                "sequential" => ZdbRunMode::Seq,
                _ => {
                    return Err(ZdbError {
                        kind: ZdbErrorKind::Format,
                        remote: self.ci.address,
                        internal: ErrorCause::Other(
                            "expected mode to be usermode/sequential".to_string(),
                        ),
                    })
                }
            },
            index_disk_freespace_bytes: kvs["index_disk_freespace_bytes"].parse().map_err(|e| {
                ZdbError {
                    kind: ZdbErrorKind::Format,
                    remote: self.ci.address,
                    internal: ErrorCause::Other(format!(
                        "expected index_disk_freespace_bytes to be an integer ({})",
                        e
                    )),
                }
            })?,
            data_disk_freespace_bytes: kvs["data_disk_freespace_bytes"].parse().map_err(|e| {
                ZdbError {
                    kind: ZdbErrorKind::Format,
                    remote: self.ci.address,
                    internal: ErrorCause::Other(format!(
                        "expected index_disk_freespace_bytes to be an integer ({})",
                        e
                    )),
                }
            })?,
        })
    }

    /// Returns the [`zstor_v2::zdb::ZdbConnectionInfo`] object used to connect to this db.
    pub fn connection_info(&self) -> &ZdbConnectionInfo {
        &self.ci
    }
}

/// Information about a 0-db namespace, as reported by the db itself.
// TODO: not complete
#[derive(Debug)]
pub struct NsInfo {
    name: String,
    entries: usize,
    public: bool,
    password: bool,
    data_size_bytes: usize,
    data_limit_bytes: Option<usize>,
    index_size_bytes: usize,
    mode: ZdbRunMode,
    index_disk_freespace_bytes: usize,
    data_disk_freespace_bytes: usize,
}

impl NsInfo {
    /// Get the amount of free space in the namespace. If there is no limit, or the free
    /// space according to the limit is higher than the remaining free disk size, the remainder of
    /// the free disk size is returned.
    pub fn free_space(&self) -> usize {
        if let Some(limit) = self.data_limit_bytes {
            let free_limit = limit - self.data_size_bytes;
            if free_limit < self.data_disk_freespace_bytes {
                return free_limit;
            }
        }

        return self.data_disk_freespace_bytes;
    }
}

/// The different running modes for a zdb instance
#[derive(Debug)]
pub enum ZdbRunMode {
    /// Userkey run mode
    User,
    /// Sequential run mode
    Seq,
}

/// Interpret a byteslice as a [`Key`] type. This is a helper function to easily interpret the
/// byteslices returned when setting a new value.
fn read_le_key(input: &[u8]) -> Key {
    let (int_bytes, _) = input.split_at(std::mem::size_of::<Key>());
    Key::from_le_bytes(
        int_bytes
            .try_into()
            .expect("could not convert bytes to key"),
    )
}

/// A `ZdbError` holding details about failed zdb operations.
#[derive(Debug)]
pub struct ZdbError {
    kind: ZdbErrorKind,
    remote: SocketAddr,
    internal: ErrorCause,
}

impl fmt::Display for ZdbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZDB at {}, error {} caused by {}",
            self.remote, self.kind, self.internal
        )
    }
}

impl std::error::Error for ZdbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let ErrorCause::Redis(ref re) = self.internal {
            Some(re)
        } else {
            None
        }
    }
}

impl ZdbError {
    /// Create a new ZstorError indicating the namespace does not have sufficient storage space
    pub fn new_storage_size(remote: SocketAddr, required: usize, limit: usize) -> Self {
        ZdbError {
            kind: ZdbErrorKind::NsSize,
            remote,
            internal: ErrorCause::Other(format!(
                "Namespace only has {} bytes of space left, but {} bytes are needed",
                limit, required
            )),
        }
    }

    /// The address of the 0-db which caused this error.
    pub fn address(&self) -> &SocketAddr {
        &self.remote
    }
}

/// The cause of a zero db error.
#[derive(Debug)]
enum ErrorCause {
    Redis(redis::RedisError),
    Other(String),
    Timeout,
}

impl fmt::Display for ErrorCause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorCause::Redis(e) => e.to_string(),
                ErrorCause::Other(e) => e.clone(),
                ErrorCause::Timeout => "timeout".to_string(),
            }
        )
    }
}

/// Some information about the exact operation which failed
#[derive(Debug)]
pub enum ZdbErrorKind {
    /// Error in the connection information or while connecting to the remote
    Connect,
    /// Error while setting the namespace
    Ns,
    /// The namespace does not have sufficient capacity left
    NsSize,
    /// Error while authenticating to the namespace
    Auth,
    /// Error while writing data
    Write,
    /// Error while reading data
    Read,
    /// Data returned is not in the expected format
    Format,
    /// The operation timed out
    Timeout,
}

impl fmt::Display for ZdbErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "operation {}",
            match self {
                ZdbErrorKind::Connect => "CONNECT",
                ZdbErrorKind::Ns => "NS",
                ZdbErrorKind::NsSize => "NS SIZE",
                ZdbErrorKind::Auth => "AUTH",
                ZdbErrorKind::Write => "WRITE",
                ZdbErrorKind::Read => "READ",
                ZdbErrorKind::Format => "WRONG FORMAT",
                ZdbErrorKind::Timeout => "OPERATION TIMEOUT",
            }
        )
    }
}
