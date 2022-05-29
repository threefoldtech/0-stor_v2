use crate::utils::exec;
use crate::utils::sed;
use crate::utils::wait_zstor;
use anyhow::Result;
use std::fs;
use std::fs::File;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::time;
use zstor_v2::config::Encryption;
use zstor_v2::config::Group;
use zstor_v2::config::Meta;
use zstor_v2::config::{Compression, Config};
use zstor_v2::encryption::SymmetricKey;
use zstor_v2::zdb::ZdbConnectionInfo;
use zstor_v2::zdb_meta::ZdbMetaStoreConfig;

const LOG_FILE: &str = "zstor.log";
const CONFIG_FILE: &str = "zstor.conf";
const SOCKET_FILE: &str = "zstor.sock";
const PID_FILE: &str = "zstor.pid";

pub struct Zstor {
    zdb_addr: String,
    workdir: PathBuf,
    fs_path: PathBuf,
    zdb_data_dir_path: PathBuf,
    zdb_index_dir_path: PathBuf,
    max_zdb_data_dir_size: Option<u64>,
}

impl Zstor {
    pub fn new(
        zdb_addr: &str,
        workdir: &Path,
        fs_path: &Path,
        zdb_data_dir_path: &Path,
        zdb_index_dir_path: &Path,
        max_zdb_data_dir_size: Option<u64>,
    ) -> Self {
        Self {
            zdb_addr: zdb_addr.to_string(),
            workdir: workdir.into(),
            fs_path: fs_path.into(),
            zdb_data_dir_path: zdb_data_dir_path.into(),
            zdb_index_dir_path: zdb_index_dir_path.into(),
            max_zdb_data_dir_size: max_zdb_data_dir_size,
        }
    }

    pub fn start(&self) -> Result<Child> {
        let cfg_path = self.config_path();
        let cfg_str = toml::to_string(&self.config())?;
        File::create(cfg_path.to_str().unwrap())?;
        fs::write(cfg_path.clone(), cfg_str)?;
        let mut zstor = Command::new("zstor");
        zstor
            .arg("--log_file")
            .arg(self.log_path().to_str().unwrap())
            .arg("-c")
            .arg(cfg_path.to_str().unwrap())
            .arg("monitor");
        let child = zstor.spawn()?;
        wait_zstor(
            self.socket_path().to_str().unwrap().into(),
            None,
            time::Duration::from_secs(60),
        )?;
        Ok(child)
    }

    pub fn start_blocking(&self) -> Result<()> {
        let cfg_path = self.config_path();
        let cfg_str = toml::to_string(&self.config())?;
        File::create(cfg_path.to_str().unwrap())?;
        fs::write(cfg_path.clone(), cfg_str)?;
        let mut zstor = Command::new("zstor");
        zstor
            .arg("--log_file")
            .arg(self.log_path().to_str().unwrap())
            .arg("-c")
            .arg(cfg_path.to_str().unwrap())
            .arg("monitor");
        exec(zstor)?;
        Ok(())
    }

    pub fn fix_hook(&self, hook_path: &Path) -> Result<()> {
        sed(
            hook_path,
            "zstorconf=\".*",
            format!("zstorconf=\"{}\"", self.config_path().to_str().unwrap())
                .replace('/', "\\/")
                .as_ref(),
        );
        sed(
            hook_path,
            "zstorindex=\".*",
            format!(
                "zstorindex=\"{}\"",
                self.zdb_index_dir_path.to_str().unwrap()
            )
            .replace('/', "\\/")
            .as_ref(),
        );
        sed(
            hook_path,
            "zstordata=\".*",
            format!("zstordata=\"{}\"", self.zdb_data_dir_path.to_str().unwrap())
                .replace('/', "\\/")
                .as_ref(),
        );
        Ok(())
    }

    fn log_path(&self) -> PathBuf {
        self.workdir.clone().join(LOG_FILE)
    }
    fn config_path(&self) -> PathBuf {
        self.workdir.clone().join(CONFIG_FILE)
    }
    fn pid_path(&self) -> PathBuf {
        self.workdir.clone().join(PID_FILE)
    }
    pub fn socket_path(&self) -> PathBuf {
        self.workdir.clone().join(SOCKET_FILE)
    }
    pub fn copy_log(&self) -> Result<()> {
        fs::copy(self.log_path(), PathBuf::from("/tmp/zstor.log"))?;
        Ok(())
    }
    fn config(&self) -> Config {
        let socket_path = self.socket_path();
        let pid_path = self.pid_path();
        let mut meta_backends = vec![];
        let mut data_backends = vec![];
        for i in 0..4 {
            meta_backends.push(ZdbConnectionInfo::new(
                self.zdb_addr.to_socket_addrs().unwrap().next().unwrap(),
                Some(format!("meta-{}", i).to_string()),
                None,
            ));
            data_backends.push(ZdbConnectionInfo::new(
                self.zdb_addr.to_socket_addrs().unwrap().next().unwrap(),
                Some(format!("data-{}", i).to_string()),
                None,
            ));
        }
        Config {
            minimal_shards: 1,
            expected_shards: 2,
            redundant_groups: 0,
            redundant_nodes: 0,
            socket: Some(socket_path),
            pid_file: Some(pid_path),
            zdb_data_dir_path: Some(self.zdb_data_dir_path.clone()),
            zdbfs_mountpoint: Some(self.fs_path.clone()),
            explorer: None,
            prometheus_port: None,
            max_zdb_data_dir_size: self.max_zdb_data_dir_size,
            groups: vec![Group {
                backends: data_backends,
            }],
            encryption: Encryption::Aes(SymmetricKey::new([0u8; 32])),
            compression: Compression::Snappy,
            root: None,
            meta: Meta::Zdb(ZdbMetaStoreConfig::new(
                "someprefix".to_string(),
                Encryption::Aes(SymmetricKey::new([1u8; 32])),
                meta_backends.try_into().unwrap(),
            )),
        }
    }
}
