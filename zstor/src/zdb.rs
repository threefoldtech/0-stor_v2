use crate::meta::MetaStoreError;
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use futures::stream::{Stream, StreamExt};
use log::{debug, trace};
use redis::{aio::MultiplexedConnection, ConnectionAddr, ConnectionInfo};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
};
use tokio::time::timeout;
// use sha1::{Digest, Sha1};

/// The type of key's used in zdb in sequential mode.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Key {
    /// The key type returned by V1 0-db in sequential mode.
    V1(u32),
    /// The key type returned by V2 0-db in sequential mode.
    V2(u64),
}

impl Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Key::V1(ref key) => key as &dyn Display,
                Key::V2(ref key) => key,
            }
        )
    }
}

impl Key {
    fn to_le_bytes(self) -> Vec<u8> {
        match self {
            Key::V1(key) => key.to_le_bytes().into(),
            Key::V2(key) => key.to_le_bytes().into(),
        }
    }
}

/// The result type as used by this module.
pub type ZdbResult<T> = Result<T, ZdbError>;

// Max size of a single entry in zdb;
const MAX_ZDB_CHUNK_SIZE: usize = 2 * 1024 * 1024;
// Max size of a single entry in zdb;
const MAX_ZDB_DATA_SIZE: usize = 8 * 1024 * 1024;
// Max allowed duration for an operation
// Since max chunk size is 2MiB, 30 seconds would mean a throughput of ~69.9 KB/s
const ZDB_TIMEOUT: Duration = Duration::from_secs(30);

/// A connection to a 0-db namespace running in sequential mode
#[derive(Debug, Clone)]
pub struct SequentialZdb {
    internal: InternalZdb,
}

/// A connection to a 0-db namespace running in user-key mode
#[derive(Debug, Clone)]
pub struct UserKeyZdb {
    internal: InternalZdb,
}

/// An open connection to a 0-db instance. The connection might not be valid after opening (e.g. if
/// the remote closed). No reconnection is attempted.
#[derive(Clone)]
struct InternalZdb {
    conn: MultiplexedConnection,
    // connection info tracked to conveniently inspect the remote address and namespace.
    ci: ZdbConnectionInfo,
}

impl fmt::Debug for InternalZdb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZDB at {}", self.ci.address)
    }
}

/// The type returned by the SCAN command in 0-db. In practice, the outer vec only contains a
/// single element.
type ScanEntry = Vec<(Vec<u8>, u32, u32)>;
/// The outpout of a `SCAN` Cmd (future).
type ScanResult = redis::RedisResult<(Vec<u8>, Vec<ScanEntry>)>;

/// A stream over all the keys in a namespace.
struct CollectionKeys {
    conn: MultiplexedConnection,
    /// Cursor to advance the scan, only set after the first call.
    cursor: Option<Vec<u8>>,
    /// The SCAN commands returns multiple entries per call, yet does not specify how many, so we
    /// keep past results in an internal array for later consumption.
    buffer: VecDeque<ScanEntry>,
    /// The next entry in the buffer we will return.
    buffer_idx: usize,
    // active scan command, this is the actual command.
    active_scan: Option<&'static redis::Cmd>,
    // address of the active connection used by the running_scan. This is a pointer to a
    // ['MultiplexedConnection'].
    active_con: Option<*const usize>,
    // An in flight scan request issued by the stream.
    running_scan: Option<Pin<Box<dyn Future<Output = ScanResult> + Send>>>,
}

struct ScanRecord {
    /// The bytes making up the actual key
    raw_key: Vec<u8>,
}

impl CollectionKeys {
    fn dealloc_future(&mut self) {
        if let Some(cmd) = self.active_scan.take() {
            // SAFETY: the some variant here is only set through methods, and is only filled in
            // through a leak of the boxed variable. Therefore we can safely recreate the box and
            // let it go out of scope to deallocate it.
            unsafe { Box::from_raw(cmd as *const redis::Cmd as *mut redis::Cmd) };
        }
        if let Some(ca) = self.active_con.take() {
            // SAFETY: convert a raw pointer to the MultiplexedConnection. The conversion to a box,
            // which is then dropped to deallocate the MultiplexedConnection is safe since the field
            // can only contain a value to such a type through the leak of the boxed value,
            // similarly to the above. The validity of the raw pointer is reliant on the fact that
            // the MultiplexedConnection is not moved. This should ideally be a Pin<_>, but the redis
            // lib does not seem to like that.
            unsafe { Box::from_raw(ca as *mut MultiplexedConnection) };
        }
        // Clear the running future, this is needed in case this method gets called while the
        // object is still alive, i.e. as part of the [`Stream`] impl. It is however not strictly
        // needed as it will be dropped regardless in the `Drop` function.
        self.running_scan = None;
    }
}

// SAFETY: This is allowed since nothing should be able to change the address of the raw pointer to
// the `MultiplexedConnection`.
unsafe impl Send for CollectionKeys {}

impl Stream for CollectionKeys {
    type Item = ScanRecord;

    // TODO: consider if key order matters here. For now keys are returned in the order 0-db
    // returns them. However if ordering does not matter in the first place, this method can be
    // optimized to avoid reallocs, and possibly become zero copy.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        trace!("Starting CollectionKeys stream poll");
        if self.running_scan.is_none() {
            // if we are not in the process of running a scan, we check the local buffer first. The
            // buffer is here since 0-db returns an unspecified amount of keys per call.
            if let Some(mut rec) = self.buffer.pop_front() {
                trace!(
                    "Existing cached key found, returning ({} left in cache)",
                    self.buffer.len()
                );
                // if we have an item we are guarnateed to have a 1 element vec, as this is the item
                // type as returned by 0-db. No clue why it is wrapped in a vec though (perhaps the redis
                // lib doesn't handle a list with different type element perfectly.
                // We use remove since: it takes ownership, does not reallocate, and since there is
                // only 1 element, it actually also doesn't do any memcopies.
                return Poll::Ready(Some(ScanRecord {
                    raw_key: rec.remove(0).0,
                }));
            }

            // At this point there are no more entries in the buffer we haven't returned yet, fetch new
            // ones.
            let mut scan_cmd = redis::cmd("SCAN");
            // Set the cursor if there is one
            if let Some(ref cur) = self.cursor {
                trace!(
                    "Adding existing cursor {} to scan command",
                    hex::encode(cur)
                );
                // compiler isn't smart enough (yet) to cast &Vec<u8> to &[u8].
                scan_cmd.arg(cur as &[u8]);
            }

            // Since the redis lib uses await, our future expects a 'static lifetime. To do that,
            // we temporarily leak a heap allocated value and save the 'static pointer we get from
            // that on the struct in an option. We later reclaim this, by calling
            // `self.dealloc_future`. Important here is that the leaked info is always reclaimed,
            // either here when more data is returned or in the drop implementation.
            // Specifically, we leak 2 things:
            // - A clone of the connection manager, which is the connection used to talk with 0-db.
            // - The command we send. It needs to live for 'static due to the future lifetime, but
            //      we can't trivially guarantee that since it's a local variable, and even saving
            //      on the struct won't help as the struct itself is not 'static.
            //  Leaking these 2 things means all items in the future have the 'static lifetime,
            //  satisfying it's requirement.
            //  Because we can't save the leaked `MultiplexedConnection` on the struct, as that would
            //  require a 'static lifetime on the struct, we actually take a raw pointer to it, and
            //  save this. We can use this raw pointer to later drop the leaked `MultiplexedConnection`.
            //  NOTE: This relies on the address of the MultiplexedConnection not changing, i.e. it
            //  is not moved.
            //  NOTE: all of this would not be needed if `Cmd::query_async` would not require
            //  'static lifetimes.
            self.active_scan = Some(Box::leak(Box::new(scan_cmd)));
            let c = Box::leak(Box::new(self.conn.clone()));
            self.active_con = Some(c as *mut MultiplexedConnection as *const usize);
            if let Some(scan) = self.active_scan {
                // The future needs to be pin in order to call `poll`, since I don't want to deal with
                // figuring out if it's safe to pin it on the stack, pin it to the heap for now. The
                // performance penalty of a heap allocation will be marginal since this does a network
                // operation anyhow.
                self.running_scan = Some(Box::pin(scan.query_async(c)));
            }
        }

        let res: (Vec<u8>, Vec<ScanEntry>) = match self.running_scan {
            Some(ref mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(res) => {
                    self.running_scan = None;
                    match res {
                        Ok(r) => {
                            self.dealloc_future();
                            r
                        }
                        Err(e) => {
                            debug!("Terminating scan: {}", e);
                            return Poll::Ready(None);
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            },
            // This code can't be reached, since, if we skip the first code block, it means there
            // is already a future running. If the first block runs, it always creates and sets the
            // future to poll on.
            None => unreachable!(),
        };

        // Set new cursor
        self.cursor = Some(res.0);
        // New buffer - Converting the Vec into a VecDeque will, sadly, realloc the vec (under the
        // assumption that the vec has cap == len).
        self.buffer = res.1.into();
        // Reset buffer pointer, and set it to 1, as we will already return the first element.
        self.buffer_idx = 1;

        // Unwrapping is safe since this code is only executed when a new buffer is fetched, which
        // only returns successful if there are other elements.
        // Similarly to above, use `remove` to get the element as owner, and since its a 1 element
        // vec there are no other side effects.
        let rec = self.buffer.pop_front().unwrap().remove(0);
        Poll::Ready(Some(ScanRecord { raw_key: rec.0 }))
    }
}

/// Connection info for a 0-db (namespace).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ZdbConnectionInfo {
    address: SocketAddr,
    namespace: Option<String>,
    password: Option<String>,
}

impl fmt::Display for ZdbConnectionInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}",
            self.address,
            self.namespace.as_deref().unwrap_or("[default namespace]")
        )
    }
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

    /// Get the namespace to connect to, if one is set.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_ref().map(|s| s as _)
    }

    /// Get a possible reservation id from the [`ZdbConnectionInfo`].
    pub fn reservation_id(&self) -> Option<i64> {
        // A namespace usually takes the form of {wid}-{index}, so split on '-', and check if we
        // have 2 numbers.
        if let Some(ns) = &self.namespace {
            let mut parts = ns.split('-');
            let id = if let Some(head) = parts.next() {
                match head.parse::<i64>() {
                    Err(_) => return None,
                    Ok(id) => id,
                }
            } else {
                return None;
            };
            if let Some(trail) = parts.next() {
                if trail.parse::<u64>().is_err() {
                    return None;
                }
            }
            if parts.next().is_some() {
                return None;
            }
            return Some(id);
        }
        None
    }

    /// Get a hash of the connection info using the blake2b hash algorithm. The output size is 16
    /// bytes.
    pub fn blake2_hash(&self) -> [u8; 16] {
        let mut hasher = VarBlake2b::new(16).unwrap();
        hasher.update(self.address.to_string().as_bytes());
        if let Some(ref ns) = self.namespace {
            hasher.update(ns.as_bytes());
        }
        if let Some(ref password) = self.password {
            hasher.update(password.as_bytes());
        }
        let mut r = Vec::with_capacity(0); // no allocation
        hasher.finalize_variable(|res| r = res.to_vec());
        r.try_into().unwrap()
    }
}

impl InternalZdb {
    /// Create a new connection to a zdb instance. The connection is opened and verified by means
    /// of the PING command. If provided, a namespace is also opened. SECURE AUTH is used to
    /// authenticate.
    async fn new(info: ZdbConnectionInfo) -> ZdbResult<Self> {
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
        let mut conn = timeout(ZDB_TIMEOUT, client.get_multiplexed_tokio_connection())
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
                    kind: if info.password.is_some() {
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
    ///
    /// # Panics
    ///
    /// panics if the data len is larger than 8MiB
    async fn set(&self, key: Option<&[u8]>, data: &[u8]) -> ZdbResult<Vec<u8>> {
        trace!(
            "storing data in zdb (key: {} length: {} remote: {})",
            if let Some(key) = key {
                hex::encode(key)
            } else {
                "None".to_string()
            },
            data.len(),
            self.connection_info().address(),
        );

        assert!(data.len() < MAX_ZDB_DATA_SIZE);

        let returned_key: Vec<u8> = timeout(
            ZDB_TIMEOUT,
            redis::cmd("SET")
                .arg(key.unwrap_or(&[]))
                .arg(data)
                .query_async(&mut self.conn.clone()),
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

        Ok(returned_key)
    }

    /// Retrieve some previously stored data with its key
    async fn get(&self, key: &[u8]) -> ZdbResult<Option<Vec<u8>>> {
        trace!(
            "loading data at key {} from {}",
            hex::encode(key),
            self.connection_info().address()
        );
        let data = timeout(
            ZDB_TIMEOUT,
            redis::cmd("GET")
                .arg(key)
                .query_async::<_, Option<Vec<u8>>>(&mut self.conn.clone()),
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
            internal: ErrorCause::Other(format!("missing key {}", hex::encode(key))),
        })?;
        Ok(Some(data))
    }

    /// Delete some previously stored data by its key
    async fn delete(&self, key: &[u8]) -> ZdbResult<()> {
        trace!(
            "deleting data at key {} from {}",
            hex::encode(key),
            self.connection_info().address()
        );

        timeout(
            ZDB_TIMEOUT,
            redis::cmd("DEL")
                .arg(key)
                .query_async(&mut self.conn.clone()),
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

        Ok(())
    }

    /// Get a stream of all the keys in the namespace
    fn keys(&self) -> CollectionKeys {
        CollectionKeys {
            conn: self.conn.clone(),
            cursor: None,
            buffer: VecDeque::new(),
            buffer_idx: 0,
            active_scan: None,
            active_con: None,
            running_scan: None,
        }
    }

    /// Query info about the namespace.
    async fn ns_info(&self) -> ZdbResult<NsInfo> {
        let list: String = timeout(
            ZDB_TIMEOUT,
            redis::cmd("NSINFO")
                .arg(if let Some(ref ns) = self.ci.namespace {
                    ns
                } else {
                    "default"
                })
                .query_async(&mut self.conn.clone()),
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
            entries: self.parse_zdb_string(&kvs, "entries")?,
            public: self.parse_zdb_bool_string(kvs["public"])?,
            password: self.parse_zdb_bool_string(kvs["password"])?,
            data_size_bytes: self.parse_zdb_string(&kvs, "data_size_bytes")?,
            data_limit_bytes: match self.parse_zdb_string(&kvs, "data_limits_bytes")? {
                0 => None,
                limit => Some(limit),
            },
            index_size_bytes: self.parse_zdb_string(&kvs, "index_size_bytes")?,
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
            worm: self.parse_zdb_bool_string(kvs["worm"])?,
            locked: self.parse_zdb_bool_string(kvs["locked"])?,
            index_io_errors: self.parse_zdb_string(&kvs, "stats_index_io_errors")?,
            index_io_error_last: self.parse_zdb_string(&kvs, "stats_index_io_error_last")?,
            index_faults: self.parse_zdb_string(&kvs, "stats_index_faults")?,
            data_io_errors: self.parse_zdb_string(&kvs, "stats_data_io_errors")?,
            data_io_error_last: self.parse_zdb_string(&kvs, "stats_data_io_error_last")?,
            data_faults: self.parse_zdb_string(&kvs, "stats_data_faults")?,

            index_disk_freespace_bytes: self
                .parse_zdb_string(&kvs, "index_disk_freespace_bytes")?,
            data_disk_freespace_bytes: self.parse_zdb_string(&kvs, "data_disk_freespace_bytes")?,
        })
    }

    /// Returns the [`ZdbConnectionInfo`] object used to connect to this db.
    fn connection_info(&self) -> &ZdbConnectionInfo {
        &self.ci
    }

    /// parse a boolean from the string returned by 0-db
    fn parse_zdb_bool_string(&self, input: &str) -> ZdbResult<bool> {
        Ok(match input {
            "yes" => true,
            "no" => false,
            _ => {
                return Err(ZdbError {
                    kind: ZdbErrorKind::Format,
                    remote: self.ci.address,
                    internal: ErrorCause::Other("expected password to be yes/no".to_string()),
                })
            }
        })
    }

    fn parse_zdb_string<T>(&self, data_map: &HashMap<&str, &str>, field: &str) -> ZdbResult<T>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        Ok(if let Some(value) = data_map.get(field) {
            value.parse().map_err(|e| ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!("Couldn't parse field {}: {}", field, e)),
            })
        } else {
            Err(ZdbError {
                kind: ZdbErrorKind::Format,
                remote: self.ci.address,
                internal: ErrorCause::Other(format!("missing field {}", field)),
            })
        }?)
    }
}

impl SequentialZdb {
    /// Create a new connection to a 0-db namespace running in sequential mode. After the
    /// connection is established, the namespace is checked to make sure it is indeed running in
    /// sequential mode.
    pub async fn new(ci: ZdbConnectionInfo) -> ZdbResult<Self> {
        let internal = InternalZdb::new(ci).await?;
        let ns_info = internal.ns_info().await?;
        match ns_info.mode() {
            ZdbRunMode::Seq => Ok(Self { internal }),
            mode => Err(ZdbError {
                kind: ZdbErrorKind::Mode,
                remote: internal.connection_info().address,
                internal: ErrorCause::Other(format!(
                    "expected 0-db namespace to be in sequential mode, but is in {}",
                    mode
                )),
            }),
        }
    }

    /// Store some data in the zdb. The generated keys are returned for later retrieval.
    /// Multiple keys might be returned since zdb only allows for up to 8MB of data per request,
    /// so we internally chunk the data.
    pub async fn set(&self, data: &[u8]) -> ZdbResult<Vec<Key>> {
        let mut keys =
            Vec::with_capacity((data.len() as f64 / MAX_ZDB_CHUNK_SIZE as f64).ceil() as usize);

        for chunk in data.chunks(MAX_ZDB_CHUNK_SIZE) {
            trace!("writing chunk of size {}", chunk.len());
            let raw_key = self.internal.set(None, chunk).await?;

            // if a key is given, we just return that. Otherwise we interpret the returned byteslice as
            // a key
            debug_assert!(raw_key.len() == std::mem::size_of::<Key>());
            keys.push(read_le_key(&raw_key))
        }
        Ok(keys)
    }

    /// Retrieve some previously stored data with its keys
    pub async fn get(&self, keys: &[Key]) -> ZdbResult<Option<Vec<u8>>> {
        let mut data: Vec<u8> = Vec::new();
        for key in keys {
            trace!("loading data at key {}", key);
            data.extend_from_slice(&self.internal.get(&key.to_le_bytes()).await?.ok_or(
                ZdbError {
                    kind: ZdbErrorKind::Read,
                    remote: self.internal.ci.address,
                    internal: ErrorCause::Other(format!("missing key {}", key)),
                },
            )?);
        }
        Ok(Some(data))
    }

    /// Returns the [`ZdbConnectionInfo`] object used to connect to this db.
    #[inline]
    pub fn connection_info(&self) -> &ZdbConnectionInfo {
        self.internal.connection_info()
    }

    /// Query info about the namespace.
    #[inline]
    pub async fn ns_info(&self) -> ZdbResult<NsInfo> {
        self.internal.ns_info().await
    }
}

impl UserKeyZdb {
    /// Create a new connection to a 0-db namespace running in userkey mode. After the
    /// connection is established, the namespace is checked to make sure it is indeed running in
    /// userkey mode.
    pub async fn new(ci: ZdbConnectionInfo) -> ZdbResult<Self> {
        let internal = InternalZdb::new(ci).await?;
        let ns_info = internal.ns_info().await?;
        match ns_info.mode() {
            ZdbRunMode::User => Ok(Self { internal }),
            mode => Err(ZdbError {
                kind: ZdbErrorKind::Mode,
                remote: internal.connection_info().address,
                internal: ErrorCause::Other(format!(
                    "expected 0-db namespace to be in userkey mode, but is in {}",
                    mode
                )),
            }),
        }
    }

    /// Store some data in the zdb under the provided key. Data size is limited to 8MiB, anything
    /// larger will result in an error.
    pub async fn set<K: AsRef<[u8]>>(&self, key: K, data: &[u8]) -> ZdbResult<()> {
        if data.len() > MAX_ZDB_DATA_SIZE {
            return Err(ZdbError {
                kind: ZdbErrorKind::Write,
                remote: self.connection_info().address,
                internal: ErrorCause::Other(format!(
                    "Data size limit is 8MiB, data has length {}",
                    data.len()
                )),
            });
        }
        self.internal.set(Some(key.as_ref()), data).await?;

        Ok(())
    }

    /// Retrieve some previously stored data from it's key.
    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> ZdbResult<Option<Vec<u8>>> {
        Ok(self.internal.get(key.as_ref()).await?)
    }

    /// Delete some previously stored data from it's key.
    pub async fn delete<K: AsRef<[u8]>>(&self, key: K) -> ZdbResult<()> {
        Ok(self.internal.delete(key.as_ref()).await?)
    }

    /// Get a stream which yields all the keys in the namespace.
    pub fn keys(&self) -> impl Stream<Item = Vec<u8>> + '_ {
        trace!("key iteration on {}", self.connection_info().address());
        self.internal.keys().map(|st| st.raw_key)
    }

    /// Returns the [`ZdbConnectionInfo`] object used to connect to this db.
    #[inline]
    pub fn connection_info(&self) -> &ZdbConnectionInfo {
        self.internal.connection_info()
    }

    /// Query info about the namespace.
    #[inline]
    pub async fn ns_info(&self) -> ZdbResult<NsInfo> {
        self.internal.ns_info().await
    }
}

/// Information about a 0-db namespace, as reported by the db itself.
#[derive(Debug)]
pub struct NsInfo {
    /// The name of the namespace.
    pub name: String,
    /// The amount of entries in the namespace.
    pub entries: usize,
    /// Wether the namespace is publicly accessible.
    pub public: bool,
    /// Whether the namespace is password protected.
    pub password: bool,
    /// The current size of the data in the namespace, in bytes.
    pub data_size_bytes: u64,
    /// The maximum size of the data in the namespace, in bytes.
    pub data_limit_bytes: Option<u64>,
    /// The current size of the index in the namespace, in bytes.
    pub index_size_bytes: u64,
    /// The [`run mode`](ZdbRunMode) of the namespace.
    pub mode: ZdbRunMode,
    /// Wether worm mode is enabled for this namespace.
    pub worm: bool,
    /// Wether the namespace is currently locked or not.
    pub locked: bool,
    /// The amount of io errors which happened in the namespace index.
    pub index_io_errors: u32,
    /// The timestamp of the last io error in the namespace index..
    pub index_io_error_last: i64, // TODO: timestamp
    /// The amount of faults in the namespace index.
    pub index_faults: u32, // currently unused
    /// The amount of io errors which happened in the namespace data.
    pub data_io_errors: u32,
    /// The timestamp of the last io error in the namespace data..
    pub data_io_error_last: i64, // TODO: timestamp
    /// The amount of faults in the namespace data.
    pub data_faults: u32, // currently unused
    /// The amount of bytes free on the disk containing the namespace index.
    pub index_disk_freespace_bytes: u64,
    /// The amount of bytes free on the disk containing the namespace data.
    pub data_disk_freespace_bytes: u64,
}

impl NsInfo {
    /// Get the amount of entries, or keys, in the namespace.
    #[inline]
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Get the amount of free space in the namespace. If there is no limit, or the free
    /// space according to the limit is higher than the remaining free disk size, the remainder of
    /// the free disk size is returned.
    pub fn free_space(&self) -> u64 {
        // TODO re enable this check, see https://github.com/threefoldtech/0-stor_v2/issues/38
        // if let Some(limit) = self.data_limit_bytes {
        //     let free_limit = limit - self.data_size_bytes;
        //     if free_limit < self.data_disk_freespace_bytes {
        //         return free_limit;
        //     }
        // }

        // self.data_disk_freespace_bytes
        self.data_limit_bytes.unwrap_or(u64::MAX)
    }

    /// Returns the percentage of used data space in this namespace. If no limit is set, the
    /// percentage is calclated based on the used space of the disk compared to the total space of
    /// the disk.
    pub fn data_usage_percentage(&self) -> u8 {
        if let Some(limit) = self.data_limit_bytes {
            return (100 * self.data_size_bytes / limit) as u8;
        }
        // TODO: this is wrong
        (100 * self.data_size_bytes / (self.data_size_bytes + self.data_disk_freespace_bytes)) as u8
    }

    /// Get the [`mode`](ZdbRunMode) the 0-db is running in
    #[inline]
    pub fn mode(&self) -> ZdbRunMode {
        self.mode
    }
}

/// The different running modes for a zdb instance
#[derive(Debug, Clone, Copy)]
pub enum ZdbRunMode {
    /// Userkey run mode
    User,
    /// Sequential run mode
    Seq,
}

impl fmt::Display for ZdbRunMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ZdbRunMode::Seq => write!(f, "sequential"),
            ZdbRunMode::User => write!(f, "user-key"),
        }
    }
}

/// Interpret a byteslice as a [`Key`] type. This is a helper function to easily interpret the
/// byteslices returned when setting a new value.
fn read_le_key(input: &[u8]) -> Key {
    match input.len() {
        4 => {
            Key::V1(u32::from_le_bytes(input.try_into().expect(
                "Conversion of 4 byte key to u32 should always succeed",
            )))
        }
        8 => {
            Key::V2(u64::from_le_bytes(input.try_into().expect(
                "Conversion of 8 byte key to u64 should always succeed",
            )))
        }
        size => panic!("Unsupported key length {}", size),
    }
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
    /// The namespace is in an unexpected mode
    Mode,
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
                ZdbErrorKind::Mode => "UNEXPECTED MODE",
            }
        )
    }
}

impl From<ZdbError> for MetaStoreError {
    fn from(e: ZdbError) -> Self {
        Self::new(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::{NsInfo, ZdbRunMode};

    #[test]
    fn test_unused_ns_usage_percentage() {
        let ns_info = prep_limited_ns_info(0, true);

        assert_eq!(ns_info.data_usage_percentage(), 0);
    }

    #[test]
    fn test_filled_ns_usage_percentage() {
        let ns_info = prep_limited_ns_info(10 * 1 << 30, true);

        assert_eq!(ns_info.data_usage_percentage(), 100);
    }

    #[test]
    fn test_half_filled_ns_usage_percentage() {
        let ns_info = prep_limited_ns_info(5 * 1 << 30, true);

        assert_eq!(ns_info.data_usage_percentage(), 50);
    }

    #[test]
    fn test_ns_filled_one_third_usage_percentage() {
        let ns_info = prep_limited_ns_info(3_579_139_413, true);

        assert_eq!(ns_info.data_usage_percentage(), 33);
    }

    #[test]
    fn test_ns_filled_two_third_usage_percentage() {
        let ns_info = prep_limited_ns_info(7_158_278_827, true);

        assert_eq!(ns_info.data_usage_percentage(), 66);
    }

    #[test]
    fn test_unlimited_ns_usage_percentage() {
        let ns_info = prep_limited_ns_info(512 * 1 << 30, false);

        // 512 GiB used + 1TiB free => 1.5 TiB disk of which 512 GiB is used => expected 33% usage
        assert_eq!(ns_info.data_usage_percentage(), 33);
    }

    fn prep_limited_ns_info(size: u64, limit: bool) -> NsInfo {
        NsInfo {
            name: "".to_string(),
            entries: 0,
            public: false,
            password: false,
            data_size_bytes: size,
            data_limit_bytes: if limit {
                Some(10 * 1 << 30) // 10 GiB
            } else {
                None
            },
            worm: false,
            locked: false,
            data_faults: 0,
            data_io_errors: 0,
            data_io_error_last: 0,
            index_faults: 0,
            index_io_errors: 0,
            index_io_error_last: 0,
            index_size_bytes: 0,
            mode: ZdbRunMode::Seq,
            index_disk_freespace_bytes: 1 << 40, // 1 TiB still free
            data_disk_freespace_bytes: 1 << 40,  // 1 Tib still free
        }
    }
}
