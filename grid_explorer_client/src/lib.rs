mod types;
mod workload;
mod reservation;
mod identity;
mod stellar;
use stellar_base::crypto::{KeyPair};

#[derive(Debug)]
pub enum ExplorerError {
    ExplorerClientError(String),
    Reqwest(reqwest::Error),
    StellarError(stellar_base::error::Error),
    HorizonError(stellar_horizon::error::Error)
}

#[derive(Debug)]
pub struct ExplorerClientError {
    msg: String,
}

use std::fmt;
impl fmt::Display for ExplorerClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

// default impls are fine here
impl std::error::Error for ExplorerClientError {}

impl From<String> for ExplorerError {
    fn from(s: String) -> Self {
        ExplorerError::ExplorerClientError(s)
    }
}

impl From<reqwest::Error> for ExplorerError {
    fn from(err: reqwest::Error) -> ExplorerError {
        ExplorerError::Reqwest(err)
    }
}

pub struct ExplorerClient {
    pub network: &'static str,
    pub user: identity::Identity,
    pub stellar_client: stellar::StellarClient
}

pub fn new_explorer_client(network: &'static str, secret: &str, user_id: i64) -> ExplorerClient {
    let keypair = KeyPair::from_secret_seed(&secret).unwrap();

    let user = identity::Identity {
        user_id,
        email: String::from(""),
        name: String::from(""),
    };
    
    let stellar_client = stellar::StellarClient {
        network,
        keypair
    };

    ExplorerClient{
        network,
        user,
        stellar_client
    }
}

impl ExplorerClient {
    pub async fn nodes_get(&self) -> Result<Vec<types::Node>, ExplorerError> {
        let url = format!("{url}/api/v1/nodes", url=self.get_url()); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<Vec<types::Node>>()
            .await?)
    }

    pub async fn node_get_by_id(&self, id: String) -> Result<types::Node, ExplorerError> {
        let url = format!("{url}/api/v1/nodes/{id}", url=self.get_url(), id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<types::Node>()
            .await?)
    }

    pub async fn farms_get(&self) -> Result<Vec<types::Farm>, ExplorerError> {
        let url = format!("{url}/api/v1/farms", url=self.get_url()); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<Vec<types::Farm>>()
            .await?)
    }

    pub async fn farm_get_by_id(&self, id: i64) -> Result<types::Farm, ExplorerError> {
        let url = format!("{url}/api/v1/farms/{id}", url=self.get_url(), id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<types::Farm>()
            .await?)
    }

    pub async fn workload_get_by_id(&self, id: i64) -> Result<workload::Workload, ExplorerError> {
        let url = format!("{url}/api/v1/reservations/workloads/{id}", url=self.get_url(), id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<workload::Workload>()
            .await?)
    }

    pub async fn create_capacity_pool(&self, data_reservation: reservation::ReservationData) -> Result<bool, ExplorerError> {
        let json = serde_json::to_string(&data_reservation).unwrap();
        
        let customer_signature = self.user.sign_hex(json.clone());

        let reservation = reservation::Reservation{
            id: 0,
            data_reservation,
            customer_tid: self.user.get_id(),
            json,
            customer_signature,
            sponsor_signature: String::from(""),
            sponsor_tid: 0
        };

        let data = serde_json::to_string(&reservation).unwrap();

        let url = format!("{url}/api/v1/reservations/pools", url=self.get_url()); 
        let resp = reqwest::Client::new()
            .post(url)
            .json(&data)
            .send()
            .await?
            .json::<reservation::CapacityPoolCreateResponse>()
            .await?;

        self.stellar_client.pay_capacity_pool(resp).await
    }

    pub async fn pool_get_by_id(&self, id: i64) -> Result<reservation::PoolData, ExplorerError> {
        let url = format!("{url}/api/v1/reservations/pools/{id}", url=self.get_url(), id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<reservation::PoolData>()
            .await?)
    }

    pub async fn pools_by_owner(&self) -> Result<reservation::PoolData, ExplorerError> {
        let url = format!("{url}/api/v1/reservations/pools/owner/{id}", url=self.get_url(), id=self.user.get_id()); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<reservation::PoolData>()
            .await?)
    }

    fn get_url(&self) -> &str {
        match self.network {
            "mainnet" => "https://explorer.grid.tf",
            "testnet" => "https://explorer.testnet.grid.tf",
            "devnet" => "https://explorer.devnet.grid.tf",
            _ => "https://explorer.grid.tf",
        }
    }
}