use log::{debug, trace};
use redis::{aio::Connection, ConnectionAddr, ConnectionInfo};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use std::convert::TryInto;
use std::net::SocketAddr;

/// The type of key's used in zdb in sequential mode
pub type Key = u32;

// TODO impl debug
/// An open connection to a 0-db instance. The connection might not be valid after opening (e.g. if
/// the remote closed). No reconnection is attempted.
pub struct Zdb {
    conn: Connection,
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
}

impl Zdb {
    /// Create a new connection to a zdb instance. The connection is opened and verified by means
    /// of the PING command. If provided, a namespace is also opened. SECURE AUTH is used to
    /// authenticate.
    pub async fn new(info: ZdbConnectionInfo) -> Result<Self, String> {
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

        trace!("{:#?}", ci);
        let client = redis::Client::open(ci).map_err(|e| e.to_string())?;
        let mut conn = client
            .get_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        trace!("opened connection to db");
        //  conn.set_read_timeout(Some(std::time::Duration::from_secs(5)))
        //      .map_err(|e| e.to_string())?;
        trace!("pinging db");
        redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        // open the correct namespace, with or without password
        if let Some(ns) = &info.namespace {
            let mut ns_select = redis::cmd("SELECT");
            ns_select.arg(ns);
            if let Some(pass) = &info.password {
                trace!("requesting AUTH challange");
                // request AUTH challenge
                let challenge: String = redis::cmd("AUTH")
                    .arg("SECURE")
                    .arg("CHALLENGE")
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| e.to_string())?;
                trace!("got challange {}", challenge);
                let mut hasher = Sha1::new();
                hasher.update(format!("{}:{}", challenge, pass).as_bytes());
                let result = hex::encode(hasher.finalize());
                trace!("auth result {}", result);
                ns_select.arg("SECURE").arg(result);
            }
            trace!("opening namespace {}", ns);
            ns_select
                .query_async(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
            trace!("opened namespace");
        }

        Ok(Self { conn })
    }

    /// Store some data in the zdb. The generated key is returned for later retrieval.
    pub async fn set(&mut self, key: Option<Key>, data: &[u8]) -> Result<Key, String> {
        trace!("storing data in zdb (length: {})", data.len());
        let raw_key: Vec<u8> = redis::cmd("SET")
            .arg(if let Some(key) = key {
                trace!("overwriting existing key {}", key);
                Vec::from(&key.to_le_bytes()[..])
            } else {
                Vec::new()
            })
            .arg(data)
            .query_async(&mut self.conn)
            .await
            .map_err(|e| e.to_string())?;

        // if a key is given, we just return that. Otherwise we interpret the returned byteslice as
        // a key
        match key {
            Some(key) => Ok(key),
            None => {
                debug_assert!(raw_key.len() == std::mem::size_of::<Key>());
                Ok(read_le_key(&raw_key))
            }
        }
    }

    /// Retrieve some previously stored data with its key
    pub async fn get(&mut self, key: Key) -> Result<Option<Vec<u8>>, String> {
        trace!("loading data at key {}", key);
        Ok(redis::cmd("GET")
            .arg(&key.to_le_bytes())
            .query_async(&mut self.conn)
            .await
            .map_err(|e| e.to_string())?)
    }
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
