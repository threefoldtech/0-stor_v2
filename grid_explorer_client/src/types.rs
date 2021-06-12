use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub id: i64,
    pub node_id: String,
    pub hostname: String,
    pub node_id_v1: String,
    pub farm_id: i64,
    pub os_version: String,
    pub created: u64,
    pub uptime: u64,
    pub updated: u64,
    pub address: String,
    pub location: Location,
    pub total_resources: ResourceAmount,
    pub used_resources: ResourceAmount,
    pub reserved_resources: ResourceAmount,
    pub workloads: WorkloadAmount,
    pub free_to_use: bool,
    pub approved: bool,
    pub public_key_hex: String,
    pub wg_ports: Option<Vec<i64>>,
    pub deleted: bool,
    pub reserved: bool,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Farm {
    id: i64,
    threebot_id: i64,
    iyo_organization: String,
    name: String,
    wallet_addresses: Option<Vec<WalletAddress>>,
    location: Location,
    email: String,
    resource_prices: Option<Vec<NodeResourcePrice>>,
    prefixzero: Option<IpAddr>,
    ipaddresses: Option<Vec<IpAddress>>,
    enable_custom_pricing: bool,
    farm_cloudunits_price: NodeCloudUnitPrice,
}

#[derive(Serialize, Deserialize, Debug)]
struct NodeCloudUnitPrice {
    cu: f64,
    su: f64,
    nu: f64,
    ipv4u: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct IpAddress {
    address: String,
    gateway: String,
    reservation_id: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct WalletAddress {
    asset: String,
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct NodeResourcePrice {
    cru: f64,
    mru: f64,
    hru: f64,
    sru: f64,
    nru: f64,
}

#[derive(Serialize, Deserialize, Debug)]
enum PriceCurrency {
    Eur,
    Usd,
    Tft,
    Aed,
    Gbp,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Location {
    pub city: String,
    pub country: String,
    pub continent: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResourceAmount {
    pub cru: u64,
    pub mru: f64,
    pub hru: f64,
    pub sru: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkloadAmount {
    pub network: u16,
    pub network_resource: u16,
    pub volume: u16,
    pub zdb_namespace: u16,
    pub container: u16,
    pub k8s_vm: u16,
    pub proxy: u16,
    pub reverse_proxy: u16,
    pub subdomain: u16,
    pub delegate_domain: u16,
}

#[derive(Serialize, Deserialize, Debug)]
struct Proof {
    created: u64,
    hardware_hash: String,
    disk_hash: String,
    hypervisor: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum GridNetwork {
    Dev,
    Test,
    Main,
}
