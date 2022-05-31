use crate::disk::Disk;
use crate::netns::Netns;
use crate::utils::checksum_file;
use crate::utils::checksum_file_with_timeout;
use crate::utils::graceful_shutdown;
use crate::utils::tempdir;
use crate::utils::write_random_file;
use crate::veth::Veth;
use crate::zdb::Zdb;
use crate::zdb::ZdbArgs;
use crate::zdbfs;
use crate::zstor::Zstor;
use anyhow::Result;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::str;
use std::time::Duration;

pub const HOOK_PATH: &str = "/usr/local/bin/hook.sh";

pub struct TestParams {
    pub id: String,
    pub network_speed: Option<u32>,
    pub max_zdb_data_dir_size: Option<u64>,
    pub data_disk_size: String,
    pub fs_disk_size: String,
    pub zdb_fs_port: u16,
    // add more when we need to
}

impl TestParams {
    pub fn with_id(mut self, id: &str) -> Self {
        self.id = id.to_string();
        self
    }
    pub fn with_port(mut self, zdb_fs_port: u16) -> Self {
        self.zdb_fs_port = zdb_fs_port;
        self
    }
}

impl Default for TestParams {
    fn default() -> Self {
        Self {
            id: "id".into(),
            network_speed: None,
            max_zdb_data_dir_size: None,
            data_disk_size: "500M".into(),
            fs_disk_size: "500M".into(),
            zdb_fs_port: 9900,
        }
    }
}

#[derive(Default)]
pub struct TestManager {
    params: TestParams,

    zstor: Option<Zstor>,

    pub zstor_proc: Option<Child>,
    pub data_zdb: Option<Child>,
    pub fs_zdb: Option<Child>,
    pub zdbfs: Option<Child>,
    pub data_disk: Option<Disk>,
    pub fs_disk: Option<Disk>,
    pub namespace: Option<Netns>,
    pub veth_host: Option<Veth>,
    pub veth_data: Option<Veth>,
    pub workdir: Option<PathBuf>,
}

impl Drop for TestManager {
    fn drop(&mut self) {
        if self.zdbfs.is_some() {
            let _ = self.stop_zdbfs().map_err(|e| println!("{}", e));
        }
        if self.fs_zdb.is_some() {
            let _ = self.stop_fs_zdb().map_err(|e| println!("{}", e));
        }
        if self.zstor_proc.is_some() {
            let _ = self.stop_zstor().map_err(|e| println!("{}", e));
        }
        if self.data_zdb.is_some() {
            let _ = self.stop_data_zdb().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut veth_data) = self.veth_data {
            let _ = veth_data.delete().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut namespace) = self.namespace {
            let _ = namespace.delete().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut veth_host) = self.veth_host {
            let _ = veth_host.clear_limits().map_err(|e| println!("{}", e));
            let _ = veth_host.delete().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut fs_disk) = self.fs_disk {
            let _ = fs_disk.delete().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut data_disk) = self.data_disk {
            let _ = data_disk.delete().map_err(|e| println!("{}", e));
        }
        if let Some(ref mut workdir) = self.workdir {
            let _ = fs::remove_dir_all(workdir).map_err(|e| println!("{}", e));
        }
    }
}
impl TestManager {
    pub fn new(params: TestParams) -> Self {
        let mut res = Self::default();
        res.params = params;
        res
    }
    pub fn init_netns(&mut self) -> Result<()> {
        let netns = Netns::new(&self.netns_name());
        netns.create()?;
        self.namespace = Some(netns);
        Ok(())
    }
    pub fn init_veths(&mut self) -> Result<()> {
        let (veth1, veth2) = Veth::create_new_pair(
            &self.host_veth_name(),
            &self.remote_veth_name(),
            &self.network_ip(1),
            &self.network_ip(2),
            Some(self.namespace.as_ref().unwrap()),
            self.params.network_speed,
        )?;
        self.veth_host = Some(veth1);
        self.veth_data = Some(veth2);

        Ok(())
    }
    pub fn init_data_disk(&mut self) -> Result<()> {
        let mut data_disk = Disk::new(&self.params.data_disk_size.clone());
        data_disk.create();
        self.data_disk = Some(data_disk);

        Ok(())
    }
    pub fn start_data_zdb(&mut self) -> Result<()> {
        let data_zdb = Zdb::new(
            &self.data_zdb_ip(),
            8800,
            Some(self.namespace.as_ref().unwrap().clone()),
        );
        let data_zdb_prc =
            data_zdb.start(&ZdbArgs::data(&(&self.data_disk).as_ref().unwrap().path))?;
        self.data_zdb = Some(data_zdb_prc);
        data_zdb.init_data_namespaces(4, 4)?;

        Ok(())
    }
    pub fn init_workdir(&mut self) -> Result<()> {
        self.workdir = Some(tempdir().into());
        Ok(())
    }
    pub fn init_fs_disk(&mut self) -> Result<()> {
        let mut fs_disk = Disk::new(&self.params.fs_disk_size.clone());
        fs_disk.create();
        self.fs_disk = Some(fs_disk);

        Ok(())
    }
    pub fn start_zstor(&mut self) -> Result<()> {
        let hook_path = self.hook_path();
        fs::copy(HOOK_PATH, hook_path.clone())?;
        let fs_zdb_args = ZdbArgs::fs(&(&self.fs_disk).as_ref().unwrap().path, &hook_path);
        let workdir = (&self.workdir).as_ref().unwrap();
        let mountpoint = workdir.clone().join("mnt");
        let zstor = Zstor::new(
            &self.data_zdb_address(),
            workdir,
            &mountpoint,
            Path::new(&fs_zdb_args.data.unwrap()),
            Path::new(&fs_zdb_args.index.unwrap()),
            self.params.max_zdb_data_dir_size,
        );
        let zstor_proc = zstor.start()?;
        self.zstor_proc = Some(zstor_proc);
        zstor.fix_hook(&hook_path)?;
        self.zstor = Some(zstor);

        Ok(())
    }

    pub fn start_zstor_blocking(&mut self) -> Result<()> {
        let fs_zdb_args = ZdbArgs::fs(&(&self.fs_disk).as_ref().unwrap().path, &self.hook_path());
        let workdir = (&self.workdir).as_ref().unwrap();
        let mountpoint = workdir.clone().join("mnt");
        let zstor = Zstor::new(
            &self.data_zdb_address(),
            workdir,
            &mountpoint,
            Path::new(&fs_zdb_args.data.unwrap()),
            Path::new(&fs_zdb_args.index.unwrap()),
            self.params.max_zdb_data_dir_size,
        );
        zstor.start_blocking()?;

        Ok(())
    }
    pub fn start_fs_zdb(&mut self) -> Result<()> {
        let fs_workdir = &(&self.fs_disk).as_ref().unwrap().path;
        let fs_zdb_args = ZdbArgs::fs(fs_workdir, &self.hook_path());
        let zdb = Zdb::new("127.0.0.1", self.params.zdb_fs_port, None);
        self.fs_zdb = Some(zdb.start(&fs_zdb_args)?);
        zdb.init_fs_namespaces()?;

        Ok(())
    }
    pub fn start_zdbfs(&mut self) -> Result<()> {
        let workdir = (&self.workdir).as_ref().unwrap();
        let mountpoint = workdir.clone().join("mnt");
        let fs = zdbfs::Fs::new(&mountpoint, self.params.zdb_fs_port);
        self.zdbfs = Some(fs.start()?);
        Ok(())
    }
    pub fn stop_zstor(&mut self) -> Result<()> {
        graceful_shutdown(self.zstor_proc.as_mut().unwrap(), Duration::from_secs(30))?;
        Ok(())
    }
    pub fn force_stop_zstor(&mut self) -> Result<()> {
        self.zstor_proc.as_mut().unwrap().kill()?;
        self.zstor_proc.as_mut().unwrap().wait()?;
        Ok(())
    }
    pub fn stop_zdbfs(&mut self) -> Result<()> {
        let workdir = (&self.workdir).as_ref().unwrap();
        let mountpoint = workdir.clone().join("mnt");

        zdbfs::Fs::stop(&mountpoint, self.zdbfs.as_mut().unwrap())?;
        Ok(())
    }
    pub fn stop_fs_zdb(&mut self) -> Result<()> {
        graceful_shutdown(self.fs_zdb.as_mut().unwrap(), Duration::from_secs(30))?;
        Ok(())
    }
    pub fn stop_data_zdb(&mut self) -> Result<()> {
        graceful_shutdown(self.data_zdb.as_mut().unwrap(), Duration::from_secs(30))?;
        Ok(())
    }
    pub fn cleanup_fs_disk(&mut self) -> Result<()> {
        self.fs_disk.as_mut().unwrap().cleanup()?;
        Ok(())
    }
    pub fn write_file(&self, name: String, size_in_mb: u32) -> Result<()> {
        let workdir = self.workdir.as_ref().unwrap();
        let file_path = workdir.clone().join("mnt").join(name);
        write_random_file(&file_path, size_in_mb)?;
        Ok(())
    }
    pub fn checksum_file(&self, name: String) -> Result<String> {
        let workdir = self.workdir.as_ref().unwrap();
        let file_path = workdir.clone().join("mnt").join(name);
        checksum_file(&file_path)
    }
    pub fn checksum_file_with_timwout(&self, name: String, timeout: Duration) -> Result<String> {
        let workdir = self.workdir.as_ref().unwrap();
        let file_path = workdir.clone().join("mnt").join(name);
        checksum_file_with_timeout(&file_path, timeout)
    }
    pub fn init(&mut self) -> Result<()> {
        self.init_netns()?;
        self.init_veths()?;
        self.init_data_disk()?;
        self.start_data_zdb()?;
        self.init_workdir()?;
        self.init_fs_disk()?;
        self.start_zstor()?;
        self.start_fs_zdb()?;
        self.start_zdbfs()?;
        Ok(())
    }

    pub fn copy_zstor_log(&self) -> Result<()> {
        (&self.zstor.as_ref()).unwrap().copy_log()?;
        Ok(())
    }

    fn hook_path(&self) -> PathBuf {
        PathBuf::from(self.workdir.as_ref().unwrap()).join("hook.sh")
    }
    fn netns_name(&self) -> String {
        format!("zr-{}", self.params.id)
    }
    fn host_veth_name(&self) -> String {
        format!("zh-{}", self.params.id)
    }
    fn remote_veth_name(&self) -> String {
        format!("zr-{}", self.params.id)
    }
    fn data_zdb_ip(&self) -> String {
        self.network_ip(2)
    }
    fn network_ip(&self, last_octet: u8) -> String {
        format!(
            "128.{}.{}.{}",
            self.params.zdb_fs_port / 256,
            self.params.zdb_fs_port % 256,
            last_octet
        )
    }
    fn data_zdb_address(&self) -> String {
        format!("{}:8800", self.data_zdb_ip())
    }
}
