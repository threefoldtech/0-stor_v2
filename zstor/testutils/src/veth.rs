use crate::netns::Netns;
use crate::utils::exec;
use anyhow::Result;
use std::process::Command;

pub struct Veth {
    name: String,
    namespace: Option<Netns>,
}

impl Veth {
    pub fn create_new_pair(
        name1: &str,
        name2: &str,
        ip1: &str,
        ip2: &str,
        netns2: Option<&Netns>,
        network_speed: Option<u32>,
    ) -> Result<(Veth, Veth)> {
        let mut cmd = Command::new("sudo");
        cmd.arg("ip")
            .arg("link")
            .arg("add")
            .arg(name1)
            .arg("type")
            .arg("veth")
            .arg("peer")
            .arg("name")
            .arg(name2);
        exec(cmd)?;

        let (mut veth1, mut veth2) = (
            Veth {
                name: name1.into(),
                namespace: None,
            },
            Veth {
                name: name2.into(),
                namespace: None,
            },
        );
        if let Some(ns) = netns2 {
            veth2.set_ns(ns)?;
        }
        veth1.set_addr(format!("{}/24", ip1))?;
        veth2.set_addr(format!("{}/24", ip2))?;
        veth1.set_up()?;
        veth2.set_up()?;
        if let Some(network_speed) = network_speed {
            veth1.limit_traffic(network_speed)?;
        }

        Ok((veth1, veth2))
    }

    pub fn set_ns(&mut self, ns: &Netns) -> Result<()> {
        ns.own(self.name.clone())?;
        self.namespace = Some(ns.clone());
        Ok(())
    }
    pub fn set_up(&mut self) -> Result<()> {
        let mut cmd = Command::new("sudo");
        cmd.arg("ip")
            .arg("link")
            .arg("set")
            .arg(&self.name)
            .arg("up");
        if let Some(ns) = &self.namespace {
            cmd = ns.wrap(cmd);
        }
        exec(cmd)?;
        Ok(())
    }
    pub fn set_addr(&mut self, addr: String) -> Result<()> {
        let mut cmd = Command::new("sudo");
        cmd.arg("ip")
            .arg("addr")
            .arg("add")
            .arg(&addr)
            .arg("dev")
            .arg(&self.name);
        if let Some(ns) = &self.namespace {
            cmd = ns.wrap(cmd);
        }
        exec(cmd)?;
        Ok(())
    }

    pub fn delete(&mut self) -> Result<()> {
        let mut cmd = Command::new("sudo");
        cmd.arg("ip").arg("link").arg("del").arg(&self.name);
        if let Some(ns) = &self.namespace {
            cmd = ns.wrap(cmd);
        }
        exec(cmd)?;
        Ok(())
    }

    pub fn limit_traffic(&self, network_speed: u32) -> Result<()> {
        let mut cmd = Command::new("wondershaper");
        cmd.arg("-a")
            .arg(&self.name)
            .arg("-d")
            .arg(format!("{}", network_speed))
            .arg("-u")
            .arg(format!("{}", network_speed));
        exec(cmd)?;
        Ok(())
    }

    pub fn clear_limits(&self) -> Result<()> {
        let mut cmd = Command::new("sudo");
        cmd.arg("wondershaper").arg("-a").arg(&self.name).arg("-c");
        exec(cmd)?;
        Ok(())
    }
}
