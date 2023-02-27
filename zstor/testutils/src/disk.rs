use crate::utils::exec;
use crate::utils::remove_dir_contents;
use crate::utils::tempdir;
use crate::utils::tempfile;
use crate::utils::write_random_file;
use anyhow::Result;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::path::PathBuf;
use std::process::Command;

pub struct Disk {
    pub path: PathBuf,
    disk_size: String,
    pub image: Option<PathBuf>,
}

impl Disk {
    pub fn new(size: &str) -> Self {
        let path = tempdir();
        Self {
            path: path.into(),
            disk_size: size.into(),
            image: None,
        }
    }
    pub fn create(&mut self) {
        let temp = tempfile();
        self.image = Some((&temp).into());
        let mut cmd = Command::new("truncate");
        cmd.arg("-s").arg(&self.disk_size).arg(&temp);
        exec(cmd).expect("failed to create image");
        let mut cmd = Command::new("mkfs");
        cmd.arg("-t").arg("btrfs").arg(&temp);
        exec(cmd).expect("failed to format image as btrfs");
        create_dir_all(self.path.as_path()).expect("failed to create disk mount point");
        let mut cmd = Command::new("mount");
        cmd.arg(&temp).arg(self.path.to_str().unwrap());
        exec(cmd).expect("failed to mount image");
        let mut cmd = Command::new("chmod");
        cmd.arg("777").arg(self.path.to_str().unwrap());
        exec(cmd).expect("failed to chmod image");
    }

    pub fn delete(&mut self) -> Result<()> {
        if let Some(ref image_path) = self.image {
            let mut cmd = Command::new("umount");
            cmd.arg(&self.path);
            exec(cmd)?;
            let mut cmd = Command::new("rm");
            cmd.arg("-rf").arg(&self.path);
            exec(cmd)?;
            let mut cmd = Command::new("rm");
            cmd.arg("-rf").arg(image_path);
            exec(cmd)?;
            self.image = None;
        }
        Ok(())
    }
    pub fn cleanup(&self) -> Result<()> {
        remove_dir_contents(&self.path)?;

        Ok(())
    }
    pub fn write_file(&self, name: &str, size_in_mb: u32) -> Result<()> {
        let file_path = self.path.join(name);
        write_random_file(&file_path, size_in_mb)?;
        Ok(())
    }
    pub fn remove_file(&self, name: &str) -> Result<()> {
        let file_path = self.path.join(name);
        remove_file(file_path)?;
        Ok(())
    }
}
