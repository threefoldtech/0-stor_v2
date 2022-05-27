use crate::utils::exec;
use anyhow::Result;
use std::process::Command;

#[derive(Clone)]
pub struct Netns {
    name: String,
}

impl Netns {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn create(&self) -> Result<()> {
        println!("creating {}", self.name);
        let mut cmd = Command::new("sudo");
        cmd.arg("ip").arg("netns").arg("add").arg(&self.name);
        exec(cmd)?;
        let mut cmd = Command::new("ip");
        cmd.arg("link").arg("set").arg("lo").arg("up");
        cmd = self.wrap(cmd);
        exec(cmd)?;
        Ok(())
    }

    pub fn delete(&self) -> Result<()> {
        println!("deleting {}", self.name);
        Command::new("sudo")
            .arg("ip")
            .arg("netns")
            .arg("del")
            .arg(&self.name)
            .output()?;
        Ok(())
    }

    pub fn own(&self, name: String) -> Result<()> {
        Command::new("sudo")
            .arg("ip")
            .arg("link")
            .arg("set")
            .arg(&name)
            .arg("netns")
            .arg(&self.name)
            .output()?;
        Ok(())
    }

    pub fn wrap(&self, cmd: Command) -> Command {
        let name = cmd.get_program().to_str().unwrap();
        let mut res = Command::new("ip");
        res.arg("netns")
            .arg("exec")
            .arg(&self.name)
            .arg(name)
            .args(cmd.get_args());
        res
    }
}
