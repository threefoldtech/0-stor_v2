use etcd_rs::{Client, ClientConfig};

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

    // TODO: save and load meta
    // TODO: save and load config
}
