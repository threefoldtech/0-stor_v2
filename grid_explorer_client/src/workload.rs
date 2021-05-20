use std::collections::HashMap;
use serde_repr::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Workload {
    pub workload_id: i64,
    pub node_id: String,
    pub pool_id: i64,
    
    pub reference: String,
    pub description: String,

    pub signing_request_provision: SigningRequest,
    pub signing_request_delete: SigningRequest,

    pub id: i64,
    pub json: Option<String>,
    pub customer_tid: i64,
    pub customer_signature: String,
    
    pub next_action: NextAction,
    // signatures_provision

    pub version: i64,
    pub epoch: u64,
    pub metadata: String,

    pub result: Option<WorkloadResult>,

    pub workload_type: WorkloadType,

    #[serde(flatten)]
    pub data: WorkloadData
}

impl Workload {
    pub fn signature_challenge(&self) -> String {
        let mut concat_string = format!("{}", self.workload_id);

        concat_string.push_str(&format!("{}", self.node_id));
        concat_string.push_str(&format!("{}", self.pool_id));
        concat_string.push_str(&format!("{}", self.reference));
        concat_string.push_str(&format!("{}", self.customer_tid));
        concat_string.push_str(&format!("{}", self.workload_type.workload_type_as_string()));
        concat_string.push_str(&format!("{}", self.epoch));
        concat_string.push_str(&format!("{}", self.description));
        concat_string.push_str(&format!("{}", self.metadata));
        concat_string
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SigningRequest {
    pub signers: Option<Vec<i64>>,
    pub quorum_min: i64
}

#[repr(i64)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum WorkloadType {
    WorkloadTypeZDB,
	WorkloadTypeContainer,
	WorkloadTypeVolume,
	WorkloadTypeNetwork,
	WorkloadTypeKubernetes,
	WorkloadTypeProxy,
	WorkloadTypeReverseProxy,
	WorkloadTypeSubDomain,
	WorkloadTypeDomainDelegate,
	WorkloadTypeGateway4To6,
	WorkloadTypeNetworkResource,
	WorkloadTypePublicIP
}

impl WorkloadType {
    pub fn workload_type_as_string(&self) -> String {
        match self {
            WorkloadType::WorkloadTypeZDB => String::from("ZDB"),
            _ => String::from("NOPE")
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum WorkloadData {
    Volume(VolumeInformation),
    ZDB(ZDBInformation),
    Container(ContainerInformation),
    K8S(K8SInformation),
    PublicIP(PublicIPInformation),
    Network(NetworkInformation),
    GatewayProxy(GatewayProxyInformation),
    GatewayReverseProxy(GatewayReverseProxyInformation),
    GatewaySubdomain(GatewaySubdomainInformation),
    GatewayDelegate(GatewayDelegateInformation),
    Gateway4To6(Gateway4To6Information)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VolumeInformation {
    pub size: i64,
    pub kind: DiskType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ZDBInformation {
    pub size: i64,
    pub mode: ZdbMode,
    pub password: String,
    pub disk_type: DiskType,
    pub public: bool,
}

impl ZDBInformation {
    pub fn signature_challenge(&self) -> String {
        let mut concat_string = format!("{}", self.size);

        concat_string.push_str(&format!("{}", self.mode.to_string().to_lowercase()));
        concat_string.push_str(&format!("{}", self.password));
        concat_string.push_str(&format!("{}", self.disk_type.to_string().to_lowercase()));
        concat_string.push_str(&format!("{}", self.public));

        concat_string
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerInformation {
    pub flist: String,
    pub hub_url: String,
    pub environment: HashMap<String, String>,
    pub secret_environment: HashMap<String, String>,
    pub entrypoint: String,
    pub interactive: bool,
    pub volumes: Vec<ContainerMount>,
    pub network_connections: Vec<NetworkConnection>,
    pub stats: Vec<Stats>,
    pub logs: Vec<Logs>,
    pub capacity: ContainerCapacity
}

#[derive(Serialize, Deserialize, Debug)]
pub struct K8SInformation {
    pub size: i64,
    pub cluster_secret: String,
    pub network_id: String,
    pub ipaddress: std::net::IpAddr,
    pub master_ips: Vec<std::net::IpAddr>,
    pub ssh_keys: Vec<String>,
    pub public_ip_wid: i64
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublicIPInformation {
    pub ipaddress: IPNet
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkInformation {
    pub name: String,
    pub workload_id: i64,
    pub iprange: IPNet,
    pub network_resources: Vec<NetworkResources>,
    pub farmer_tid: i64
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayProxyInformation {
    pub domain: String,
    pub addr: String,
    pub port: u32,
    pub port_tls: u32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayReverseProxyInformation {
    pub domain: String,
    pub secret: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewaySubdomainInformation {
    pub domain: String,
    pub ips: Vec<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayDelegateInformation {
    pub domain: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Gateway4To6Information {
    pub public_key: String
}

#[repr(i64)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Clone, Copy)]
pub enum NextAction {
    Create,
	Sign,
	Pay,
	Deploy,
	Delete,
	Invalid,
	Deleted,
	Migrated,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkloadResult {
    pub category: i64,
    pub workload_id: String,
    pub data_json: serde_json::Value,
    pub signature: String,
    pub state: ResultState,
    pub message: String,
    pub epoch: i64,
    pub node_id: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkloadDelete {
    pub signature: String,
    pub tid: i64,
    pub epoch: u64,
}

#[repr(i64)]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
pub enum ResultState {
    Err,
    Ok,
    Deleted,
    Pending // when Err and workload_id is empty
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkResources {
    pub node_id: String,
    pub wireguard_private_key_encrypted: String,
    pub wireguard_public_key: String,
    pub wireguard_listen_port: i64,
    pub iprange: IPNet,
    pub peers: Vec<WireguardPeer>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WireguardPeer {
    pub public_key: String,
    pub endpoint: String,
    pub iprange: IPNet,
    pub allowed_ip_range: Vec<IPNet>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IPNet {
    pub ip: std::net::IpAddr,
    pub mask: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerMount {
    pub volume_id: String,
    pub mount_point: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkConnection {
    pub network_id: String,
    pub ipaddress: std::net::IpAddr,
    pub public_ip6: bool,
    pub yggdrasil_ip: bool
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Stats {
    pub stats_type: String,
    pub data: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Logs {
    pub logs_type: String,
    pub data: LogRedis
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogRedis {
    pub stdout: String,
    pub stderr: String,

    pub secret_stdout: String,
    pub secret_stderr: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerCapacity {
    pub cpu: i64,
    pub memory: i64,
    pub disk_type: DiskType,
    pub disk_size: i64
}

#[repr(u8)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum ZdbMode {
    ZDBModeSeq,
	ZDBModeUser
}

impl ZdbMode {
    pub fn to_string(&self) -> String {
        match &self {
            ZdbMode::ZDBModeUser => String::from("User"),
            ZdbMode::ZDBModeSeq => String::from("Seq")
        }
    }
}

#[repr(u8)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum DiskType {
    HDD,
    SSD
}

impl DiskType {
    pub fn to_string(&self) -> String {
        match &self {
            DiskType::SSD => String::from("SSD"),
            DiskType::HDD => String::from("HDD")
        }
    }
}