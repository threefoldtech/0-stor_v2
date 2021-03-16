use log::{debug, trace};
use redis::{aio::Connection, ConnectionAddr, ConnectionInfo};
use serde::{Deserialize, Serialize};
// use sha1::{Digest, Sha1};
use std::fmt;

use std::convert::TryInto;
use std::net::SocketAddr;

/// The type of key's used in zdb in sequential mode.
pub type Key = u32;

/// The result type as used by this module.
pub type ZdbResult<T> = Result<T, ZdbError>;

const MAX_ZDB_CHUNK_SIZE: usize = 2 * 1024 * 1024;

// TODO impl debug
/// An open connection to a 0-db instance. The connection might not be valid after opening (e.g. if
/// the remote closed). No reconnection is attempted.
pub struct Zdb {
    conn: Connection,
    // connection info tracked to conveniently inspect the remote address.
    ci: ConnectionInfo,
    ns: Option<String>,
}

impl fmt::Debug for Zdb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZDB at {}", self.ci.addr)
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
            remote: ci.addr.to_string(),
            internal: ErrorCause::Redis(e),
        })?;
        let mut conn = client.get_async_connection().await.map_err(|e| ZdbError {
            kind: ZdbErrorKind::Connect,
            remote: ci.addr.to_string(),
            internal: ErrorCause::Redis(e),
        })?;
        trace!("opened connection to db");
        trace!("pinging db");
        redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| ZdbError {
                kind: ZdbErrorKind::Connect,
                remote: ci.addr.to_string(),
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
            ns_select
                .query_async(&mut conn)
                .await
                .map_err(|e| ZdbError {
                    kind: if let redis::ErrorKind::AuthenticationFailed = e.kind() {
                        ZdbErrorKind::Auth
                    } else {
                        ZdbErrorKind::Ns
                    },
                    remote: ci.addr.to_string(),
                    internal: ErrorCause::Redis(e),
                })?;
            trace!("opened namespace");
        }

        Ok(Self {
            conn,
            ci,
            ns: info.namespace,
        })
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
            let raw_key: Vec<u8> = redis::cmd("SET")
                .arg::<&[u8]>(&[])
                .arg(chunk)
                .query_async(&mut self.conn)
                .await
                .map_err(|e| ZdbError {
                    kind: ZdbErrorKind::Write,
                    remote: self.ci.addr.to_string(),
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
                &redis::cmd("GET")
                    .arg(&key.to_le_bytes())
                    .query_async::<_, Option<Vec<u8>>>(&mut self.conn)
                    .await
                    .map_err(|e| ZdbError {
                        kind: ZdbErrorKind::Read,
                        remote: self.ci.addr.to_string(),
                        internal: ErrorCause::Redis(e),
                    })?
                    .ok_or(ZdbError {
                        kind: ZdbErrorKind::Read,
                        remote: self.ci.addr.to_string(),
                        internal: ErrorCause::Other(format!("missing key {}", key)),
                    })?,
            );
        }
        Ok(Some(data))
    }

    /// Get the amount of free space in the connected namespace. If there is no limit, or the free
    /// space according to the limit is higher than the remaining free disk size, the remainder of
    /// the free disk size is returned.
    pub async fn free_space(&mut self) -> ZdbResult<usize> {
        let ns_info = self.ns_info().await?;
        if let Some(limit) = ns_info.data_limit_bytes {
            let free_limit = limit - ns_info.data_size_bytes;
            if free_limit < ns_info.data_disk_freespace_bytes {
                return Ok(free_limit);
            }
        }

        return Ok(ns_info.data_disk_freespace_bytes);
    }

    /// Query info about the namespace.
    pub async fn ns_info(&mut self) -> ZdbResult<NsInfo> {
        let list: String = redis::cmd("NSINFO")
            .arg(if let Some(ref ns) = self.ns {
                ns
            } else {
                "default"
            })
            .query_async(&mut self.conn)
            .await
            .map_err(|e| ZdbError {
                kind: ZdbErrorKind::Read,
                remote: self.ci.addr.to_string(),
                internal: ErrorCause::Redis(e),
            })?;

        use std::collections::HashMap;

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

        println!("{:?}", kvs);

        Ok(NsInfo {
            name: kvs["name"].to_string(),
            entries: kvs["entries"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.addr.to_string(),
                internal: ErrorCause::Other(format!("expected entries to be an integer ({})", e)),
            })?,
            public: match kvs["public"] {
                "yes" => true,
                "no" => false,
                _ => {
                    return Err(ZdbError {
                        kind: ZdbErrorKind::Format,
                        remote: self.ci.addr.to_string(),
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
                        remote: self.ci.addr.to_string(),
                        internal: ErrorCause::Other("expected password to be yes/no".to_string()),
                    })
                }
            },
            data_size_bytes: kvs["data_size_bytes"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.addr.to_string(),
                internal: ErrorCause::Other(format!(
                    "expected data_size_bytes to be an integer ({})",
                    e
                )),
            })?,
            data_limit_bytes: match kvs["data_limits_bytes"].parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.addr.to_string(),
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
                remote: self.ci.addr.to_string(),
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
                        remote: self.ci.addr.to_string(),
                        internal: ErrorCause::Other(
                            "expected mode to be usermode/sequential".to_string(),
                        ),
                    })
                }
            },
            index_disk_freespace_bytes: kvs["index_disk_freespace_bytes"].parse().map_err(|e| {
                ZdbError {
                    kind: ZdbErrorKind::Format,
                    remote: self.ci.addr.to_string(),
                    internal: ErrorCause::Other(format!(
                        "expected index_disk_freespace_bytes to be an integer ({})",
                        e
                    )),
                }
            })?,
            data_disk_freespace_bytes: kvs["data_disk_freespace_bytes"].parse().map_err(|e| {
                ZdbError {
                    kind: ZdbErrorKind::Format,
                    remote: self.ci.addr.to_string(),
                    internal: ErrorCause::Other(format!(
                        "expected index_disk_freespace_bytes to be an integer ({})",
                        e
                    )),
                }
            })?,
        })
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
    remote: String,
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

/// The cause of a zero db error.
#[derive(Debug)]
enum ErrorCause {
    Redis(redis::RedisError),
    Other(String),
}

impl fmt::Display for ErrorCause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorCause::Redis(e) => e.to_string(),
                ErrorCause::Other(e) => e.clone(),
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
    /// Error while authenticating to the namespace
    Auth,
    /// Error while writing data
    Write,
    /// Error while reading data
    Read,
    /// Data returned is not in the expected format
    Format,
}

impl fmt::Display for ZdbErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "operation {}",
            match self {
                ZdbErrorKind::Connect => "CONNECT",
                ZdbErrorKind::Ns => "NS",
                ZdbErrorKind::Auth => "AUTH",
                ZdbErrorKind::Write => "WRITE",
                ZdbErrorKind::Read => "READ",
                ZdbErrorKind::Format => "WRONG FORMAT",
            }
        )
    }
}
