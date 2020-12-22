use crate::meta::MetaData;
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use etcd_rs::{Client, ClientConfig, KeyRange, PutRequest, RangeRequest};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;

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
    pub async fn new(cfg: &EtcdConfig, virtual_root: Option<PathBuf>) -> Result<Self, String> {
        let client = Client::connect(ClientConfig {
            endpoints: cfg.endpoints.clone(),
            auth: match cfg {
                EtcdConfig {
                    username: Some(username),
                    password: Some(password),
                    ..
                } => Some((username.clone(), password.clone())),
                _ => None,
            },
            tls: None,
        })
        .await
        .map_err(|e| format!("client connect failed: {}", e))?;
        Ok(Etcd {
            client,
            prefix: cfg.prefix.clone(),
            virtual_root,
        })
    }

    /// Save the metadata for the file identified by `path` with a given prefix
    pub async fn save_meta(&self, path: &PathBuf, meta: &MetaData) -> Result<(), String> {
        // for now save metadata human readable
        trace!("encoding metadata");
        let enc_meta =
            toml::to_vec(meta).map_err(|e| format!("could not encode metadata: {}", e))?;
        // hash
        let key = self.build_key(path)?;
        self.write_value(&key, &enc_meta).await
    }

    /// loads the metadata for a given path and prefix
    pub async fn load_meta(&self, path: &PathBuf) -> Result<Option<MetaData>, String> {
        let key = self.build_key(path)?;
        Ok(if let Some(value) = self.read_value(&key).await? {
            Some(
                toml::from_slice(&value)
                    .map_err(|e| format!("could not decode metadata: {}", e))?,
            )
        } else {
            None
        })
    }

    // helper functions to read and write a value
    async fn write_value(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.client
            .kv()
            .put(PutRequest::new(key, value))
            .await
            .map(|_| ()) // ignore result
            .map_err(|e| format!("could not save value: {}", e))
    }

    async fn read_value(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.client
            .kv()
            .range(RangeRequest::new(KeyRange::key(key)))
            .await
            .map_err(|e| format!("could not load value: {}", e))
            .and_then(|mut resp| {
                let mut kvs = resp.take_kvs();
                match kvs.len() {
                    0 => Ok(None),
                    1 => Ok(Some(kvs[0].take_value())),
                    keys => Err(format!("expected to find single key, found {}", keys)),
                }
            })
    }

    // hash a path using blake2b with 16 bytes of output, and hex encode the result
    // the path is canonicalized before encoding so the full path is used
    fn build_key(&self, path: &PathBuf) -> Result<String, String> {
        // annoyingly, the path needs to exist for this to work. So here's the plan:
        // first we verify that it is actualy there
        // if it is, no problem
        // else, create a temp file, canonicalize that path, and remove the temp file again
        let canonical_path = match fs::metadata(path) {
            Ok(_) => path.canonicalize().map_err(|e| e.to_string())?,
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    fs::File::create(path)
                        .map_err(|e| format!("could not create temp file: {}", e))?;
                    let cp = path.canonicalize().map_err(|e| e.to_string())?;
                    fs::remove_file(path).map_err(|e| e.to_string())?;
                    cp
                }
                _ => return Err(e.to_string()),
            },
        };

        // now strip the virtual_root, if one is set
        let actual_path = if let Some(ref virtual_root) = self.virtual_root {
            trace!("stripping path prefix {:?}", virtual_root);
            canonical_path
                .strip_prefix(virtual_root)
                .map_err(|e| format!("could not strip path prefix: {}", e))?
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
                .ok_or("could not interpret path as utf-8 str")?
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
