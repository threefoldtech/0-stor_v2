use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Reservation {
    pub id: i64,
    pub json: String,
    pub data_reservation: ReservationData,
    pub customer_tid: i64,
    pub customer_signature: String,
    pub sponsor_tid: i64,
    pub sponsor_signature: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReservationData {
    pub pool_id: i64,
    pub cus: u64,
    pub sus: u64,
    pub ipv4us: u64,
    pub node_ids: Vec<String>,
    pub currencies: Vec<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CapacityPoolCreateResponse {
    pub reservation_id: i64,
    pub escrow_information: EscrowInformation
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EscrowInformation {
    pub address: String,
    pub asset: String,
    pub amount: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PoolData {
    pub pool_id: i64,
    pub cus: f64,
    pub sus: f64,
    pub ipv4us: f64,
    pub last_updated: i64,
    pub active_cu: f64,
    pub active_su: f64,
    pub active_ipv4: f64,
    pub empty_at: i64,
    pub node_ids: Vec<String>,
    pub active_workload_ids: Option<Vec<i64>>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReservationCreateResponse {
    pub reservation_id: Option<i64>,
    pub error: Option<String>
}