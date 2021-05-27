use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    id: i64,
    node_id: String,
    hostname: String,
    node_id_v1: String,
    farm_id: i64,
    os_version: String,
    created: u64,
    uptime: u64,
    pub updated: u64,
    address: String,
    location: Location,
    total_resources: ResourceAmount,
    used_resources: ResourceAmount,
    reserved_resources: ResourceAmount,
    workloads: WorkloadAmount,
    free_to_use: bool,
    approved: bool,
    pub public_key_hex: String,
    wg_ports: Option<Vec<i64>>,
    deleted: bool,
    reserved: bool,
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
struct Location {
    city: String,
    country: String,
    continent: String,
    latitude: f64,
    longitude: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResourceAmount {
    cru: u64,
    mru: f64,
    hru: f64,
    sru: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct WorkloadAmount {
    network: u16,
    network_resource: u16,
    volume: u16,
    zdb_namespace: u16,
    container: u16,
    k8s_vm: u16,
    proxy: u16,
    reverse_proxy: u16,
    subdomain: u16,
    delegate_domain: u16,
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
