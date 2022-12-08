use crate::namespace::Namespace;
use crate::netns::Netns;
use crate::utils::exec;
use crate::utils::wait_server;
use anyhow::Result;
use std::ffi::OsStr;
use std::fs;
use std::net::Ipv4Addr;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::str;
use std::str::FromStr;
use std::time;

#[derive(Default)]
pub struct ZdbArgs {
    pub dualnet: bool,
    pub verbose: bool,
    pub datasize: Option<u32>,
    pub mode: Option<String>,
    pub index: Option<String>,
    pub data: Option<String>,
    pub hook: Option<String>,
}

impl ZdbArgs {
    pub fn data(work_dir: &Path) -> ZdbArgs {
        ZdbArgs {
            dualnet: true,
            verbose: false,
            datasize: None,
            mode: None,
            index: Some(
                work_dir
                    .to_path_buf()
                    .join("index")
                    .to_str()
                    .unwrap()
                    .into(),
            ),
            data: Some(work_dir.to_path_buf().join("data").to_str().unwrap().into()),
            hook: None,
        }
    }
    pub fn fs(work_dir: &Path, hook_path: &Path) -> ZdbArgs {
        let index = work_dir.to_path_buf().join("index");
        let data = work_dir.to_path_buf().join("data");
        ZdbArgs {
            dualnet: false,
            verbose: false,
            datasize: Some(16777216),
            mode: Some("seq".into()),
            index: Some(index.to_str().unwrap().into()),
            data: Some(data.to_str().unwrap().into()),
            hook: Some(hook_path.to_str().unwrap().into()),
        }
    }
}

#[derive(Clone)]
pub struct Zdb {
    addr: SocketAddr,
    namespace: Option<Netns>,
}

impl Zdb {
    pub fn new(ipv4_addr: &str, port: u16, namespace: Option<Netns>) -> Self {
        println!("{}", ipv4_addr);
        Zdb {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::from_str(ipv4_addr).unwrap()), port),
            namespace,
        }
    }
    pub fn start(&self, args: &ZdbArgs) -> Result<Child> {
        let mut zdb_command = Command::new("zdb");
        if args.dualnet {
            zdb_command.arg("--dualnet");
        }
        if args.verbose {
            zdb_command.arg("--verbose");
        }
        if let Some(v) = args.datasize {
            zdb_command.arg("--datasize");
            zdb_command.arg(v.to_string());
        }
        if let Some(v) = &args.mode {
            zdb_command.arg("--mode");
            zdb_command.arg(v);
        }
        if let Some(v) = &args.index {
            fs::create_dir_all(v)?;

            zdb_command.arg("--index");
            zdb_command.arg(v);
        }
        if let Some(v) = &args.data {
            fs::create_dir_all(v)?;
            zdb_command.arg("--data");
            zdb_command.arg(v);
        }
        if let Some(v) = &args.hook {
            zdb_command.arg("--hook");
            zdb_command.arg(v);
        }
        zdb_command.arg("--port");
        zdb_command.arg(self.addr.port().to_string());
        if let Some(ns) = &self.namespace {
            zdb_command = ns.wrap(zdb_command);
        }
        println!("{:?}", zdb_command);
        let child = zdb_command.spawn().expect("failed to start zdb");
        wait_server(
            self.addr.ip().to_string(),
            self.addr.port(),
            self.namespace.clone(),
            time::Duration::from_secs(60),
        )?;

        Ok(child)
    }

    pub fn exec<I, S>(&self, args: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut cmd = Command::new("redis-cli");
        cmd.arg("-u")
            .arg(format!("redis://{}", self.addr))
            .args(args);
        if let Some(ns) = &self.namespace {
            cmd = ns.wrap(cmd);
        }
        exec(cmd).expect("failed to execute command");
        Ok(())
    }

    pub fn create_namespace(&self, name: String, mode: Option<String>) -> Result<Namespace> {
        self.exec(["NSNEW", &name])?;
        if let Some(mode) = mode {
            self.exec(["NSSET", &name, "mode", &mode])?;
        }
        Ok(Namespace::new(name, self.clone()))
    }

    pub fn delete_namespace(&self, name: String) {
        self.exec(["NSDEL", &name])
            .expect("failed to delete namespace");
    }

    pub fn init_fs_namespaces(&self) -> Result<()> {
        self.create_namespace("zdbfs-meta".into(), None)?;
        self.create_namespace("zdbfs-data".into(), None)?;
        self.create_namespace("zdbfs-temp".into(), None)?;
        Ok(())
    }

    pub fn init_data_namespaces(&self, data_count: u32, meta_count: u32) -> Result<()> {
        for i in 0..meta_count {
            self.create_namespace(format!("meta-{}", i), Some("user".into()))?;
        }
        for i in 0..data_count {
            self.create_namespace(format!("data-{}", i), Some("seq".into()))?;
        }

        Ok(())
    }
}
