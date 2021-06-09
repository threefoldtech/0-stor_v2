use crate::encryption;
use crate::identity::Identity;
use serde::{Deserialize, Serialize};
use serde_repr::*;
use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
/// Specific error type related to workload creation errors
#[derive(Debug)]
pub enum BuilderError {
    MissingField(String),
    EncryptionError(String),
}

impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "builder error: {}",
            match self {
                BuilderError::MissingField(msg) => msg,
                BuilderError::EncryptionError(msg) => msg,
            }
        )
    }
}

impl From<encryption::EncryptionError> for BuilderError {
    fn from(e: encryption::EncryptionError) -> Self {
        match e {
            encryption::EncryptionError::EncryptionError(s) => BuilderError::EncryptionError(s),
        }
    }
}

pub trait SignatureChallenge {
    fn challenge(&self) -> String;
}

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
    pub data: WorkloadData,
}

impl Workload {
    pub fn sign(&mut self, user_identity: &Identity) {
        let mut workload_signature_challenge = self.challenge();
        workload_signature_challenge.push_str(self.data.challenge().as_str());
        let customer_signature_bytes =
            user_identity.hash_and_sign(workload_signature_challenge.as_bytes());
        let encoded_signature = hex::encode(customer_signature_bytes.to_vec());
        self.customer_signature = encoded_signature;
    }
}

#[derive(Default)]
pub struct WorkloadBuilder {
    pub customer_tid: Option<i64>,
    pub node_id: Option<String>,
    pub pool_id: Option<i64>,

    pub reference: Option<String>,
    pub description: Option<String>,
    pub metadata: Option<String>,

    pub version: Option<i64>,

    pub data: Option<WorkloadData>,
}

impl WorkloadBuilder {
    pub fn new() -> WorkloadBuilder {
        WorkloadBuilder::default()
    }
    pub fn customer_tid(mut self, customer_tid: i64) -> WorkloadBuilder {
        self.customer_tid = Some(customer_tid);
        self
    }
    pub fn node_id(mut self, node_id: &str) -> WorkloadBuilder {
        self.node_id = Some(String::from(node_id));
        self
    }
    pub fn pool_id(mut self, pool_id: i64) -> WorkloadBuilder {
        self.pool_id = Some(pool_id);
        self
    }
    pub fn reference(mut self, reference: &str) -> WorkloadBuilder {
        self.reference = Some(String::from(reference));
        self
    }
    pub fn description(mut self, description: &str) -> WorkloadBuilder {
        self.description = Some(String::from(description));
        self
    }
    pub fn metadata(mut self, metadata: &str) -> WorkloadBuilder {
        self.metadata = Some(String::from(metadata));
        self
    }
    pub fn version(mut self, version: i64) -> WorkloadBuilder {
        self.version = Some(version);
        self
    }
    pub fn data(mut self, data: WorkloadData) -> WorkloadBuilder {
        self.data = Some(data);
        self
    }

    fn validate(&self) -> Option<BuilderError> {
        if self.customer_tid.is_none() {
            return Some(BuilderError::MissingField(String::from("customer_tid")));
        } else if self.node_id.is_none() {
            return Some(BuilderError::MissingField(String::from("node_id")));
        } else if self.pool_id.is_none() {
            return Some(BuilderError::MissingField(String::from("pool_id")));
        } else if self.data.is_none() {
            return Some(BuilderError::MissingField(String::from("data")));
        }
        None
    }

    fn workload_type(&self) -> WorkloadType {
        match self.data.as_ref().unwrap() {
            WorkloadData::Zdb(_) => WorkloadType::WorkloadTypeZdb,
            WorkloadData::Container(_) => WorkloadType::WorkloadTypeContainer,
            WorkloadData::Volume(_) => WorkloadType::WorkloadTypeVolume,
            WorkloadData::Network(_) => WorkloadType::WorkloadTypeNetwork,
            WorkloadData::K8S(_) => WorkloadType::WorkloadTypeKubernetes,
            WorkloadData::GatewayProxy(_) => WorkloadType::WorkloadTypeProxy,
            WorkloadData::GatewayReverseProxy(_) => WorkloadType::WorkloadTypeReverseProxy,
            WorkloadData::GatewaySubdomain(_) => WorkloadType::WorkloadTypeSubDomain,
            WorkloadData::GatewayDelegate(_) => WorkloadType::WorkloadTypeDomainDelegate,
            WorkloadData::Gateway4To6(_) => WorkloadType::WorkloadTypeGateway4To6,
            WorkloadData::PublicIp(_) => WorkloadType::WorkloadTypePublicIp,
            _ => WorkloadType::WorkloadTypeNetworkResource,
        }
    }
    pub fn build(self, user_identity: &Identity) -> Result<Workload, BuilderError> {
        if let Some(e) = self.validate() {
            return Err(e);
        }
        let version = default(self.version, 1);
        let metadata = default(self.metadata.clone(), String::from(""));
        let reference = default(self.reference.clone(), String::from(""));
        let description = default(self.description.clone(), String::from(""));
        let epoch = epoch();
        let node_id = self.node_id.as_ref().unwrap();
        let pool_id = self.pool_id.unwrap();
        let customer_tid = self.customer_tid.as_ref().unwrap();
        let signing_request_delete = SigningRequest {
            signers: Some(vec![*customer_tid]),
            quorum_min: 1,
        };
        let workload_type = self.workload_type();

        let mut w = Workload {
            workload_id: 1,
            node_id: node_id.clone(),
            pool_id,
            reference,
            description,

            signing_request_provision: SigningRequest::default(),
            signing_request_delete,

            id: 1,
            json: Some(String::from("")),
            customer_tid: *customer_tid,
            customer_signature: String::from(""),

            next_action: NextAction::Create,

            version,
            metadata,
            epoch,

            result: None,

            workload_type,

            data: self.data.unwrap(),
        };
        w.sign(user_identity);
        let json = serde_json::to_string(&w).unwrap();
        w.json = Some(json);
        Ok(w)
    }
}

fn epoch() -> u64 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
fn default<T>(v: Option<T>, d: T) -> T {
    if let Some(x) = v {
        x
    } else {
        d
    }
}
impl SignatureChallenge for Workload {
    fn challenge(&self) -> String {
        let mut concat_string = format!("{}", self.workload_id);

        concat_string.push_str(&self.node_id.to_string());
        concat_string.push_str(&format!("{}", self.pool_id));
        concat_string.push_str(&self.reference.to_string());
        concat_string.push_str(&format!("{}", self.customer_tid));
        concat_string.push_str(&format!("{}", self.workload_type));
        concat_string.push_str(&format!("{}", self.epoch));
        concat_string.push_str(&self.description.to_string());
        concat_string.push_str(&self.metadata.to_string());
        concat_string
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SigningRequest {
    pub signers: Option<Vec<i64>>,
    pub quorum_min: i64,
}

#[repr(i64)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum WorkloadType {
    WorkloadTypeZdb,
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
    WorkloadTypePublicIp,
    WorkloadTypeVirtualMachine,
}

impl fmt::Display for WorkloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadType::WorkloadTypeZdb => write!(f, "ZDB"),
            WorkloadType::WorkloadTypeContainer => write!(f, "CONTAINER"),
            WorkloadType::WorkloadTypeVolume => write!(f, "VOLUME"),
            WorkloadType::WorkloadTypeNetwork => write!(f, "NETWORK"),
            WorkloadType::WorkloadTypeKubernetes => write!(f, "KUBERNETES"),
            WorkloadType::WorkloadTypeProxy => write!(f, "PROXY"),
            WorkloadType::WorkloadTypeReverseProxy => write!(f, "REVERSE_PROXY"),
            WorkloadType::WorkloadTypeSubDomain => write!(f, "SUBDOMAIN"),
            WorkloadType::WorkloadTypeDomainDelegate => write!(f, "DOMAIN_DELEGATE"),
            WorkloadType::WorkloadTypeGateway4To6 => write!(f, "GATEWAY4TO6"),
            WorkloadType::WorkloadTypeNetworkResource => write!(f, "NETWORK_RESOURCE"),
            WorkloadType::WorkloadTypePublicIp => write!(f, "PUBLIC_IP"),
            WorkloadType::WorkloadTypeVirtualMachine => write!(f, "VIRTUAL_MACHINE"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum WorkloadData {
    Volume(VolumeInformation),
    Zdb(ZdbInformation),
    Container(Box<ContainerInformation>),
    K8S(K8SInformation),
    PublicIp(PublicIpInformation),
    Network(NetworkInformation),
    GatewayProxy(GatewayProxyInformation),
    GatewayReverseProxy(GatewayReverseProxyInformation),
    GatewaySubdomain(GatewaySubdomainInformation),
    GatewayDelegate(GatewayDelegateInformation),
    Gateway4To6(Gateway4To6Information),
    VirtualMachine(VirtualMachineInformation),
}

impl SignatureChallenge for WorkloadData {
    fn challenge(&self) -> String {
        match self {
            WorkloadData::Zdb(v) => v.challenge(),
            _ => String::from("Not implemented"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VolumeInformation {
    pub size: i64,
    pub kind: DiskType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VirtualMachineInformation {
    pub size: i64,
    pub network_id: String,
    pub name: String,
    pub ipaddress: std::net::IpAddr,
    pub ssh_keys: Vec<String>,
    pub public_ip: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ZdbInformation {
    pub size: i64,
    pub mode: ZdbMode,
    pub password: String,
    pub disk_type: DiskType,
    pub public: bool,
}

impl SignatureChallenge for ZdbInformation {
    fn challenge(&self) -> String {
        let mut concat_string = format!("{}", self.size);

        concat_string.push_str(&self.mode.to_string().to_lowercase());
        concat_string.push_str(&self.password);
        concat_string.push_str(&self.disk_type.to_string().to_lowercase());
        concat_string.push_str(&format!("{}", self.public));

        concat_string
    }
}

#[derive(Default)]
pub struct ZdbInformationBuilder {
    pub size: Option<i64>,
    pub mode: Option<ZdbMode>,
    pub password: Option<String>,
    pub disk_type: Option<DiskType>,
    pub public: Option<bool>,
}

impl ZdbInformationBuilder {
    pub fn new() -> ZdbInformationBuilder {
        ZdbInformationBuilder::default()
    }
    pub fn size(mut self, size: i64) -> ZdbInformationBuilder {
        self.size = Some(size);
        self
    }
    pub fn mode(mut self, mode: ZdbMode) -> ZdbInformationBuilder {
        self.mode = Some(mode);
        self
    }
    pub fn password(mut self, password: String) -> ZdbInformationBuilder {
        self.password = Some(password);
        self
    }
    pub fn disk_type(mut self, disk_type: DiskType) -> ZdbInformationBuilder {
        self.disk_type = Some(disk_type);
        self
    }
    pub fn public(mut self, public: bool) -> ZdbInformationBuilder {
        self.public = Some(public);
        self
    }
    fn validate(&self) -> Option<BuilderError> {
        if self.size.is_none() {
            return Some(BuilderError::MissingField(String::from("size")));
        } else if self.mode.is_none() {
            return Some(BuilderError::MissingField(String::from("mode")));
        } else if self.password.is_none() {
            return Some(BuilderError::MissingField(String::from("password")));
        } else if self.disk_type.is_none() {
            return Some(BuilderError::MissingField(String::from("disk_type")));
        } else if self.public.is_none() {
            return Some(BuilderError::MissingField(String::from("public")));
        }
        None
    }
    pub fn build(
        self,
        identity: &Identity,
        node_public_key: &str,
    ) -> Result<ZdbInformation, BuilderError> {
        if let Some(e) = self.validate() {
            return Err(e);
        }
        let encrypted = encryption::encrypt(
            self.password.unwrap().as_str(),
            &identity.mnemonic,
            node_public_key,
        )?;
        Ok(ZdbInformation {
            size: self.size.unwrap(),
            mode: self.mode.unwrap(),
            password: encrypted,
            disk_type: self.disk_type.unwrap(),
            public: self.public.unwrap(),
        })
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
    pub capacity: ContainerCapacity,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct K8SInformation {
    pub size: i64,
    pub cluster_secret: String,
    pub network_id: String,
    pub ipaddress: std::net::IpAddr,
    pub master_ips: Vec<std::net::IpAddr>,
    pub ssh_keys: Vec<String>,
    pub public_ip_wid: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublicIpInformation {
    pub ipaddress: IpNet,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkInformation {
    pub name: String,
    pub workload_id: i64,
    pub iprange: IpNet,
    pub network_resources: Vec<NetworkResources>,
    pub farmer_tid: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayProxyInformation {
    pub domain: String,
    pub addr: String,
    pub port: u32,
    pub port_tls: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayReverseProxyInformation {
    pub domain: String,
    pub secret: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewaySubdomainInformation {
    pub domain: String,
    pub ips: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GatewayDelegateInformation {
    pub domain: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Gateway4To6Information {
    pub public_key: String,
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
    pub node_id: String,
}

impl WorkloadResult {
    pub fn workload_state(&self) -> ResultState {
        if self.workload_id.is_empty() {
            ResultState::Pending
        } else if self.state == ResultState::Ok {
            ResultState::Ok
        } else {
            ResultState::Err
        }
    }
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
    Pending, // when Err and workload_id is empty
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkResources {
    pub node_id: String,
    pub wireguard_private_key_encrypted: String,
    pub wireguard_public_key: String,
    pub wireguard_listen_port: i64,
    pub iprange: IpNet,
    pub peers: Vec<WireguardPeer>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WireguardPeer {
    pub public_key: String,
    pub endpoint: String,
    pub iprange: IpNet,
    pub allowed_ip_range: Vec<IpNet>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IpNet {
    pub ip: std::net::IpAddr,
    pub mask: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerMount {
    pub volume_id: String,
    pub mount_point: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkConnection {
    pub network_id: String,
    pub ipaddress: std::net::IpAddr,
    pub public_ip6: bool,
    pub yggdrasil_ip: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Stats {
    pub stats_type: String,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Logs {
    pub logs_type: String,
    pub data: LogRedis,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogRedis {
    pub stdout: String,
    pub stderr: String,

    pub secret_stdout: String,
    pub secret_stderr: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerCapacity {
    pub cpu: i64,
    pub memory: i64,
    pub disk_type: DiskType,
    pub disk_size: i64,
}

#[repr(u8)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum ZdbMode {
    ZdbModeSeq,
    ZdbModeUser,
}

impl fmt::Display for ZdbMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ZdbMode::ZdbModeUser => write!(f, "User"),
            ZdbMode::ZdbModeSeq => write!(f, "Seq"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
pub enum DiskType {
    Hdd,
    Ssd,
}

impl fmt::Display for DiskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            DiskType::Ssd => write!(f, "SSD"),
            DiskType::Hdd => write!(f, "HDD"),
        }
    }
}
