use crate::meta::MetaData;
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use etcd_rs::{Client, ClientConfig, KeyRange, PutRequest, RangeRequest};
use log::{info, trace};
use std::fs;
use std::io;
use std::path::PathBuf;

/// A basic etcd cluster client
// TODO: debug
pub struct Etcd {
    client: Client,
}

impl Etcd {
    /// Create a new client connecting to the cluster with the given endpoints
    pub async fn new(endpoints: Vec<String>) -> Result<Self, String> {
        let client = Client::connect(ClientConfig {
            endpoints,
            auth: None,
            tls: None,
        })
        .await
        .map_err(|e| format!("client connect failed: {}", e))?;
        Ok(Etcd { client })
    }

    /// Save the metadata for the file identified by `path` with a given prefix
    pub async fn save_meta(
        &self,
        prefix: &str,
        path: &PathBuf,
        meta: &MetaData,
    ) -> Result<(), String> {
        // for now save metadata human readable
        trace!("encoding metadata");
        let enc_meta =
            toml::to_vec(meta).map_err(|e| format!("could not encode metadata: {}", e))?;
        // hash
        let key = build_key(prefix, path)?;
        self.write_value(&key, &enc_meta).await
    }

    /// loads the metadata for a given path and prefix
    pub async fn load_meta(&self, prefix: &str, path: &PathBuf) -> Result<MetaData, String> {
        let key = build_key(prefix, path)?;
        Ok(toml::from_slice(
            &self
                .read_value(&key)
                .await?
                .ok_or("no meta found for path".to_string())?,
        )
        .map_err(|e| e.to_string())?)
    }

    // TODO: save and load config
    //
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
}

// hash a path using blake2b with 16 bytes of output, and hex encode the result
// the path is canonicalized before encoding so the full path is used
fn build_key(prefix: &str, path: &PathBuf) -> Result<String, String> {
    // annoyingly, the path needs to exist for this to work. So here's the plan:
    // first we verify that it is actualy there
    // if it is, no problem
    // else, create a temp file, canonicalize that path, and remove the temp file again
    let canonical_path = match fs::metadata(path) {
        Ok(_) => path.canonicalize().map_err(|e| e.to_string())?,
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => {
                fs::File::create(path).map_err(|e| format!("could not create temp file: {}", e))?;
                let cp = path.canonicalize().map_err(|e| e.to_string())?;
                fs::remove_file(path).map_err(|e| e.to_string())?;
                cp
            }
            _ => return Err(e.to_string()),
        },
    };
    trace!("hashing path {:?}", canonical_path);
    // The unwrap here is safe since we know that 16 is a valid output size
    let mut hasher = VarBlake2b::new(16).unwrap();
    // TODO: might not need the move to a regular &str
    hasher.update(
        canonical_path
            .as_os_str()
            .to_str()
            .ok_or("could not interpret path as utf-8 str")?
            .as_bytes(),
    );

    // TODO: is there a better way to do this?
    let mut r = String::new();
    hasher.finalize_variable(|resp| r = hex::encode(resp));
    trace!("hashed path: {}", r);
    let fp = format!("/{}/{}", prefix, r);
    info!("full path: {}", fp);
    Ok(fp)
}
