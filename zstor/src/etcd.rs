use crate::meta::MetaData;
use crate::zdb::ZdbConnectionInfo;
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use etcd_client::{Client, ConnectOptions, GetOptions};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::{fmt, fs, io};

/// Result type of all etcd operations
pub type EtcdResult<T> = Result<T, EtcdError>;

/// A basic etcd cluster client
// TODO: debug
pub struct Etcd {
    client: Client,
    prefix: String,
    virtual_root: Option<PathBuf>,
}

/// Configuration options for an etcd cluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdConfig {
    endpoints: Vec<String>,
    prefix: String,
    username: Option<String>,
    password: Option<String>,
}

impl Etcd {
    /// Create a new client connecting to the cluster with the given endpoints
    pub async fn new(cfg: &EtcdConfig, virtual_root: Option<PathBuf>) -> EtcdResult<Self> {
        // Don't set TLS client options, etcd client lib can figure this out for us.
        let mut co = ConnectOptions::new();
        if let EtcdConfig {
            username: Some(username),
            password: Some(password),
            ..
        } = cfg
        {
            co = co.with_user(username, password)
        };
        let client = Client::connect(&cfg.endpoints, Some(co))
            .await
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Connect,
                internal: InternalError::Etcd(e),
            })?;
        Ok(Etcd {
            client,
            prefix: cfg.prefix.clone(),
            virtual_root,
        })
    }

    /// Save the metadata for the file identified by `path` with a given prefix
    pub async fn save_meta(&mut self, path: &PathBuf, meta: &MetaData) -> EtcdResult<()> {
        self.save_meta_by_key(&self.build_key(path)?, meta).await
    }

    /// Save the metadata for a given key
    pub async fn save_meta_by_key(&mut self, key: &str, meta: &MetaData) -> EtcdResult<()> {
        // for now save metadata human readable
        trace!("encoding metadata");
        let enc_meta = toml::to_vec(meta).map_err(|e| EtcdError {
            kind: EtcdErrorKind::Write,
            internal: InternalError::Meta(Box::new(e)),
        })?;
        // hash
        self.write_value(key, &enc_meta).await
    }

    /// loads the metadata for a given path and prefix
    pub async fn load_meta(&mut self, path: &PathBuf) -> EtcdResult<Option<MetaData>> {
        self.load_meta_by_key(&self.build_key(path)?).await
    }

    /// loads the metadata for a given path and prefix
    pub async fn load_meta_by_key(&mut self, key: &str) -> EtcdResult<Option<MetaData>> {
        Ok(if let Some(value) = self.read_value(key).await? {
            Some(toml::from_slice(&value).map_err(|e| EtcdError {
                kind: EtcdErrorKind::Read,
                internal: InternalError::Meta(Box::new(e)),
            })?)
        } else {
            None
        })
    }

    /// Mark a Zdb backend as replaced based on its connection info
    pub async fn set_replaced(&mut self, ci: &ZdbConnectionInfo) -> EtcdResult<()> {
        let hash = hex::encode(ci.blake2_hash());
        let key = format!("/{}/replaced_backends/{}", self.prefix, hash);
        Ok(self.write_value(&key, &[]).await?)
    }

    /// Check to see if a Zdb backend has been marked as replaced based on its conenction info
    pub async fn is_replaced(&mut self, ci: &ZdbConnectionInfo) -> EtcdResult<bool> {
        let hash = hex::encode(ci.blake2_hash());
        let key = format!("/{}/replaced_backends/{}", self.prefix, hash);
        Ok(self.read_value(&key).await?.is_some())
    }

    /// Get the (key, metadata) for all stored objects
    pub async fn object_metas(&mut self) -> EtcdResult<Vec<(String, MetaData)>> {
        let mut data = Vec::new();
        let end = format!("/{}/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", self.prefix);
        let opts = GetOptions::new().with_range(end);
        for kv in self
            .client
            .get(
                &*format!("/{}/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", self.prefix),
                Some(opts),
            )
            .await
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Read,
                internal: InternalError::Etcd(e),
            })?
            .kvs()
        {
            // if the key is not a str it is a rogue key
            if let Ok(ks) = kv.key_str() {
                // if metdata decoding fails is is a rogue entry
                if let Ok(value) = toml::from_slice(kv.value()) {
                    data.push((ks.to_string(), value));
                }
            }
        }

        Ok(data)
    }

    /// Save info about a failed upload under the failures key
    pub async fn save_failure(&mut self, path: &PathBuf) -> EtcdResult<()> {
        let abs_path = canonicalize(path)?;
        let key = format!(
            "/{}/upload_failures/{}",
            self.prefix,
            self.build_failure_key(&abs_path)?
        );

        // unwrap here is safe as we already validated that to_str() returns some when building the
        // failure key
        Ok(self
            .write_value(&key, abs_path.to_str().unwrap().as_bytes())
            .await?)
    }

    /// Delete info about a failed upload from the failure key
    pub async fn delete_failure(&mut self, path: &PathBuf) -> EtcdResult<()> {
        let abs_path = canonicalize(path)?;
        let key = format!(
            "/{}/upload_failures/{}",
            self.prefix,
            self.build_failure_key(&abs_path)?
        );

        Ok(self.delete_value(&key).await?)
    }

    /// Get all the paths of files which failed to upload
    pub async fn get_failures(&mut self) -> EtcdResult<Vec<PathBuf>> {
        let mut data = Vec::new();
        let end = format!(
            "/{}/upload_failure/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            self.prefix
        );
        let opts = GetOptions::new().with_range(end);
        for kv in self
            .client
            .get(
                &*format!(
                    "/{}/upload_failure/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    self.prefix
                ),
                Some(opts),
            )
            .await
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Read,
                internal: InternalError::Etcd(e),
            })?
            .kvs()
        {
            // if the value is not a str it is a rogue key
            if let Ok(ks) = kv.value_str() {
                data.push(ks.into());
            }
        }

        Ok(data)
    }

    // This does not take into account the virtual root
    fn build_failure_key(&self, path: &PathBuf) -> EtcdResult<String> {
        let mut hasher = VarBlake2b::new(16).unwrap();
        hasher.update(
            path.as_os_str()
                .to_str()
                .ok_or(EtcdError {
                    kind: EtcdErrorKind::Key,
                    internal: InternalError::Other(
                        "could not interpret path as utf-8 str".to_string(),
                    ),
                })?
                .as_bytes(),
        );

        let mut out = Vec::new();
        hasher.finalize_variable(|res| out = res.to_vec());

        Ok(hex::encode(out))
    }

    // helper functions to read and write a value
    async fn write_value(&mut self, key: &str, value: &[u8]) -> EtcdResult<()> {
        self.client
            .put(key, value, None)
            .await
            .map(|_| ()) // ignore result
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Write,
                internal: InternalError::Etcd(e),
            })
    }

    async fn read_value(&mut self, key: &str) -> EtcdResult<Option<Vec<u8>>> {
        self.client
            .get(key, None)
            .await
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Read,
                internal: InternalError::Etcd(e),
            })
            .and_then(|resp| {
                let kvs = resp.kvs();
                match kvs.len() {
                    0 => Ok(None),
                    1 => Ok(Some(kvs[0].value().to_vec())),
                    keys => Err(EtcdError {
                        kind: EtcdErrorKind::Meta,
                        internal: InternalError::Other(format!(
                            "expected to find single key, found {}",
                            keys
                        )),
                    }),
                }
            })
    }

    async fn delete_value(&mut self, key: &str) -> EtcdResult<()> {
        Ok(self
            .client
            .delete(key, None)
            .await
            .map(|_| ())
            .map_err(|e| EtcdError {
                kind: EtcdErrorKind::Delete,
                internal: InternalError::Etcd(e),
            })?)
    }

    // hash a path using blake2b with 16 bytes of output, and hex encode the result
    // the path is canonicalized before encoding so the full path is used
    fn build_key(&self, path: &PathBuf) -> EtcdResult<String> {
        let canonical_path = canonicalize(path)?;

        // now strip the virtual_root, if one is set
        let actual_path = if let Some(ref virtual_root) = self.virtual_root {
            trace!("stripping path prefix {:?}", virtual_root);
            canonical_path
                .strip_prefix(virtual_root)
                .map_err(|e| EtcdError {
                    kind: EtcdErrorKind::Key,
                    internal: InternalError::Other(format!("could not strip path prefix: {}", e)),
                })?
        } else {
            trace!("maintaining path");
            canonical_path.as_path()
        };

        trace!("hashing path {:?}", actual_path);
        // The unwrap here is safe since we know that 16 is a valid output size
        let mut hasher = VarBlake2b::new(16).unwrap();
        // TODO: might not need the move to a regular &str
        hasher.update(
            actual_path
                .as_os_str()
                .to_str()
                .ok_or(EtcdError {
                    kind: EtcdErrorKind::Key,
                    internal: InternalError::Other(
                        "could not interpret path as utf-8 str".to_string(),
                    ),
                })?
                .as_bytes(),
        );

        // TODO: is there a better way to do this?
        let mut r = String::new();
        hasher.finalize_variable(|resp| r = hex::encode(resp));
        trace!("hashed path: {}", r);
        let fp = format!("/{}/{}", self.prefix, r);
        debug!("full path: {}", fp);
        Ok(fp)
    }
}

/// Canonicalizes a path, even if it does not exist
fn canonicalize(path: &PathBuf) -> EtcdResult<PathBuf> {
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
            _ => {
                return Err(EtcdError {
                    kind: EtcdErrorKind::Key,
                    internal: InternalError::IO(e),
                })
            }
        },
    }
}

impl EtcdConfig {
    /// Create a new config for an etcd cluster.
    pub fn new(
        endpoints: Vec<String>,
        prefix: String,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            endpoints,
            prefix,
            username,
            password,
        }
    }
}

/// An error related to etcd (or its configuration).
#[derive(Debug)]
pub struct EtcdError {
    kind: EtcdErrorKind,
    internal: InternalError,
}

impl fmt::Display for EtcdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EtcdError: {}: {}", self.kind, self.internal)
    }
}

impl std::error::Error for EtcdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.internal {
            InternalError::Etcd(ref e) => Some(e),
            InternalError::Meta(ref e) => Some(&**e),
            InternalError::IO(ref e) => Some(e),
            InternalError::Other(_) => None,
        }
    }
}

#[derive(Debug)]
enum InternalError {
    Etcd(etcd_client::Error),
    Meta(Box<dyn std::error::Error + Send>),
    IO(io::Error),
    Other(String),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::Etcd(e) => e.to_string(),
                InternalError::Meta(e) => e.to_string(),
                InternalError::IO(e) => e.to_string(),
                InternalError::Other(e) => e.to_string(),
            }
        )
    }
}

/// Specific type of error for etcd
#[derive(Debug)]
pub enum EtcdErrorKind {
    /// Error in the connection to etcd or the connection configuration
    Connect,
    /// Error while writing data to etcd
    Write,
    /// Error while reading data from etcd
    Read,
    /// Error while deleting data in etcd
    Delete,
    /// Error while building data key
    Key,
    /// Error while encoding or decoding
    Meta,
}

impl fmt::Display for EtcdErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Operation: {}",
            match self {
                EtcdErrorKind::Connect => "CONNECT",
                EtcdErrorKind::Write => "WRITE",
                EtcdErrorKind::Read => "READ",
                EtcdErrorKind::Delete => "DELETE",
                EtcdErrorKind::Key => "KEY",
                EtcdErrorKind::Meta => "META",
            }
        )
    }
}

// In practice io errors are only returned when building the storage key
impl From<io::Error> for EtcdError {
    fn from(e: io::Error) -> Self {
        EtcdError {
            kind: EtcdErrorKind::Key,
            internal: InternalError::IO(e),
        }
    }
}
