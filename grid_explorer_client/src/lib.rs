mod encryption;
pub mod identity;
pub mod reservation;
mod stellar;
mod types;
pub mod workload;
use reqwest::header::{HeaderMap, HeaderValue};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use stellar_base::crypto::KeyPair;
mod auth;
use chrono::Utc;
use std::fmt;
use tokio::time;
pub use types::GridNetwork;

#[derive(Debug)]
pub enum ExplorerError {
    ExplorerClientError(String),
    WorkloadTimeoutError(String),
    WorkloadFailedError(String),
    WorkloadCreationError(workload::BuilderError),
    Reqwest(reqwest::Error),
    StellarError(stellar_base::error::Error),
    HorizonError(stellar_horizon::error::Error),
}

#[derive(Debug)]
pub struct ExplorerClientError {
    msg: String,
}

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

impl From<workload::BuilderError> for ExplorerError {
    fn from(err: workload::BuilderError) -> ExplorerError {
        ExplorerError::WorkloadCreationError(err)
    }
}

pub struct ExplorerClient {
    pub network: GridNetwork,
    pub user_identity: identity::Identity,
    pub stellar_client: stellar::StellarClient,
    client: reqwest::Client,
}

impl ExplorerClient {
    pub fn new(
        network: GridNetwork,
        secret: &str,
        user_identity: identity::Identity,
    ) -> ExplorerClient {
        let keypair = KeyPair::from_secret_seed(&secret).unwrap();

        let stellar_client = stellar::StellarClient { network, keypair };
        let client = reqwest::Client::new();
        ExplorerClient {
            network,
            user_identity,
            stellar_client,
            client,
        }
    }
    pub async fn nodes_get(&self) -> Result<Vec<types::Node>, ExplorerError> {
        let url = format!("{url}/api/v1/nodes", url = self.get_url());
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<Vec<types::Node>>()
            .await?)
    }

    pub async fn nodes_filter(
        &self,
        farm_id: Option<i32>,
        cru: i32,
        mru: i32,
        sru: i32,
        hru: i32,
        up: Option<bool>,
    ) -> Result<Vec<types::Node>, ExplorerError> {
        let url = format!("{url}/api/v1/nodes", url = self.get_url());
        let params = &[("cru", cru), ("mru", mru), ("sru", sru), ("hru", hru)];
        let mut nodes = self
            .client
            .get(url.as_str())
            .query(params)
            .query(&[("farm", farm_id)])
            .send()
            .await?
            .json::<Vec<types::Node>>()
            .await?;

        if let Some(v) = up {
            let timestamp = self.epoch();
            nodes = nodes
                .into_iter()
                .filter(|node| (timestamp - node.updated <= 10 * 60) == v)
                .collect();
        }
        Ok(nodes)
    }

    pub async fn node_get_by_id(&self, id: &str) -> Result<types::Node, ExplorerError> {
        let url = format!("{url}/api/v1/nodes/{id}", url = self.get_url(), id = id);
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<types::Node>()
            .await?)
    }

    pub async fn farms_get(&self) -> Result<Vec<types::Farm>, ExplorerError> {
        let url = format!("{url}/api/v1/farms", url = self.get_url());
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<Vec<types::Farm>>()
            .await?)
    }

    pub async fn farm_get_by_id(&self, id: i64) -> Result<types::Farm, ExplorerError> {
        let url = format!("{url}/api/v1/farms/{id}", url = self.get_url(), id = id);
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<types::Farm>()
            .await?)
    }

    pub async fn workload_get_by_id(&self, id: i64) -> Result<workload::Workload, ExplorerError> {
        let url = format!(
            "{url}/api/v1/reservations/workloads/{id}",
            url = self.get_url(),
            id = id
        );
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<workload::Workload>()
            .await?)
    }

    pub async fn workload_poll(
        &self,
        id: i64,
        timeout: u64,
    ) -> Result<workload::Workload, ExplorerError> {
        let start = Instant::now();
        while start.elapsed().as_secs() < timeout {
            let w = self.workload_get_by_id(id).await?;
            if let Some(v) = w.result.as_ref() {
                match v.workload_state() {
                    workload::ResultState::Pending => time::sleep(Duration::from_secs(1)).await,
                    workload::ResultState::Ok => return Ok(w),
                    workload::ResultState::Err => {
                        return Err(ExplorerError::WorkloadFailedError(String::from(&v.message)))
                    }
                    workload::ResultState::Deleted => {
                        return Err(ExplorerError::ExplorerClientError(String::from(
                            "workload deleted while waiting",
                        )))
                    }
                }
            } else {
                panic!("result should always exist")
            }
        }
        Err(ExplorerError::WorkloadTimeoutError(String::from(
            "waited a long time for the workload to deploy",
        )))
    }

    pub async fn create_capacity_pool(
        &self,
        data_reservation: reservation::ReservationData,
    ) -> Result<bool, ExplorerError> {
        let mut reservation = reservation::Reservation {
            id: 0,
            data_reservation,
            customer_tid: self.user_identity.get_id(),
            json: String::from(""),
            customer_signature: String::from(""),
            sponsor_signature: String::from(""),
            sponsor_tid: 0,
        };

        reservation.json = serde_json::to_string(&reservation.data_reservation).unwrap();

        let customer_signature_bytes = self
            .user_identity
            .hash_and_sign(reservation.json.as_bytes());

        // hex encode the customer signature
        reservation.customer_signature = hex::encode(customer_signature_bytes.to_vec());

        let url = format!("{url}/api/v1/reservations/pools", url = self.get_url());
        let resp = self
            .client
            .post(url)
            .json(&reservation)
            .send()
            .await?
            .json::<reservation::CapacityPoolCreateResponse>()
            .await?;

        self.stellar_client.pay_capacity_pool(resp).await
    }

    pub async fn pool_get_by_id(&self, id: i64) -> Result<reservation::PoolData, ExplorerError> {
        let url = format!(
            "{url}/api/v1/reservations/pools/{id}",
            url = self.get_url(),
            id = id
        );
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<reservation::PoolData>()
            .await?)
    }

    pub async fn pools_by_owner(&self) -> Result<Vec<reservation::PoolData>, ExplorerError> {
        let url = format!(
            "{url}/api/v1/reservations/pools/owner/{id}",
            url = self.get_url(),
            id = self.user_identity.get_id()
        );
        Ok(self
            .client
            .get(url.as_str())
            .send()
            .await?
            .json::<Vec<reservation::PoolData>>()
            .await?)
    }

    pub async fn workload_decommission(&self, wid: i64) -> Result<bool, ExplorerError> {
        let mut w = self.workload_get_by_id(wid).await?;
        if w.next_action as u8 > 3 {
            return Ok(false);
        }
        w.sign(&self.user_identity);
        let data = workload::WorkloadDelete {
            signature: w.customer_signature,
            tid: self.user_identity.user_id,
            epoch: self.epoch(),
        };
        let url = format!(
            "{url}/api/v1/reservations/workloads/{wid}/sign/delete",
            url = self.get_url(),
            wid = wid
        );

        let date = Utc::now();
        let headers = self.construct_headers(date);
        self.client
            .post(url)
            .headers(headers)
            .json(&data)
            .send()
            .await?
            .text()
            .await?;

        Ok(true)
    }

    pub async fn workload_deploy(&self, w: &workload::Workload) -> Result<i64, ExplorerError> {
        let url = format!("{url}/api/v1/reservations/workloads", url = self.get_url());

        let date = Utc::now();
        let headers = self.construct_headers(date);
        let resp = self
            .client
            .post(url)
            .headers(headers)
            .json(&w)
            .send()
            .await?
            .json::<reservation::ReservationCreateResponse>()
            .await?;
        if let Some(e) = resp.error {
            Err(ExplorerError::ExplorerClientError(e))
        } else if let Some(id) = resp.reservation_id {
            Ok(id)
        } else {
            Err(ExplorerError::ExplorerClientError(String::from(
                "client didn't respond with error or id",
            )))
        }
    }

    pub async fn create_zdb_reservation(
        &self,
        node_id: String,
        pool_id: i64,
        zdb: workload::ZdbInformation,
    ) -> Result<i64, ExplorerError> {
        let w = workload::WorkloadBuilder::new()
            .customer_tid(self.user_identity.user_id)
            .node_id(node_id.as_str())
            .pool_id(pool_id)
            .data(workload::WorkloadData::Zdb(zdb))
            .build(&self.user_identity)?;

        Ok(self.workload_deploy(&w).await?)
    }

    fn construct_headers(&self, date: chrono::DateTime<chrono::Utc>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        let date_str = format!("{}", date.format("%a, %d %b %Y %H:%M:%S GMT"));
        let header = auth::create_header(&self.user_identity, &date, &date_str);

        let sig = HeaderValue::from_str(&header).unwrap();
        headers.insert("Authorization", sig);

        let id = HeaderValue::from_str(self.user_identity.get_id().to_string().as_str()).unwrap();
        headers.insert("Threebot-Id", id);

        let date = HeaderValue::from_str(date_str.as_str()).unwrap();
        headers.insert("date", date);

        headers
    }

    fn get_url(&self) -> &str {
        match self.network {
            GridNetwork::Main => "https://explorer.grid.tf",
            GridNetwork::Test => "https://explorer.testnet.grid.tf",
            GridNetwork::Dev => "https://explorer.devnet.grid.tf",
        }
    }

    fn epoch(&self) -> u64 {
        let start = SystemTime::now();
        start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }
}
