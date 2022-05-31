use crate::utils::graceful_shutdown;
use crate::utils::wait_mountpoint;
use crate::utils::wait_mountpoint_released;
use anyhow::Result;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::time::Duration;

pub struct Fs {
    mountpoint: PathBuf,
    port: u16,
}

impl Fs {
    pub fn new(mountpoint: &Path, port: u16) -> Self {
        Fs {
            mountpoint: mountpoint.into(),
            port,
        }
    }
    pub fn start(&self) -> Result<Child> {
        fs::create_dir_all(&self.mountpoint)?;
        let child = Command::new("zdbfs")
            .arg("-o")
            .arg("allow_other")
            .arg("-o")
            .arg("auto_unmount")
            .arg("-o")
            .arg(format!(
                "mp={},dp={},tp={}",
                self.port, self.port, self.port
            ))
            .arg("-f")
            .arg(&self.mountpoint)
            .spawn()?;
        wait_mountpoint(&self.mountpoint);
        Ok(child)
    }
    pub fn stop(mountpoint: &Path, child: &mut Child) -> Result<()> {
        graceful_shutdown(child, Duration::from_secs(30))?;
        wait_mountpoint_released(mountpoint, Duration::from_secs(60))?;
        Ok(())
    }
}
