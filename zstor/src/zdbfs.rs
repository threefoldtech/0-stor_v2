use std::{
    fs::{File, OpenOptions},
    os::unix::prelude::{FromRawFd, IntoRawFd, RawFd},
    path::PathBuf,
};

pub use types::stats_t;

/// Tools to work with zdbfs. For now, this just allows to get zdbfs stats for a zdbfs which is
/// curentl mounted through means of a syscall.
use nix::ioctl_read;

const ZDBFS_IOC_MAGIC: u8 = b'E';
const ZDBFS_IOC_TYPE_MODE: u8 = 1;

ioctl_read! {
    /// Read the statistics of a 0-db-fs process which is currenlty mounted.
    zdbfs_read_stats,
    ZDBFS_IOC_MAGIC,
    ZDBFS_IOC_TYPE_MODE,
    types::fs_stats_t
}

/// An instance of a 0-db-fs mountpoint which supports fetching stats.
pub struct ZdbFsStats {
    /// A file descriptor for the mountpoint of 0-db-fs.
    fd: RawFd,
    /// Full path of the mountpoint.
    mountpoint: PathBuf,
}

impl Drop for ZdbFsStats {
    fn drop(&mut self) {
        // Don't assing the return value as that is pointless anyway, we just care about taking
        // ownership of the Fd again in a proper struct so it's drop can properly clean up
        // associated resources.
        // SAFETY: This is safe as the fd is private and was aquired by calling `into_raw_fd` in
        // the constructor.
        let _ = unsafe { File::from_raw_fd(self.fd) };
    }
}

impl ZdbFsStats {
    /// Attempt to create a new ZdbFsStats with the given possible mountpoint.
    pub fn try_new(mountpoint: PathBuf) -> Result<ZdbFsStats, std::io::Error> {
        let fd = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .truncate(false)
            .open(&mountpoint)?
            .into_raw_fd();
        Ok(ZdbFsStats { fd, mountpoint })
    }

    /// Get the current stats from the 0-db-fs.
    pub fn get_stats(&self) -> Result<stats_t, std::io::Error> {
        let mut stats = types::fs_stats_t::default();
        // SAFETY: This is safe as we properly return the error, and only return the struct if
        // there is no error.
        unsafe {
            zdbfs_read_stats(self.fd, &mut stats as *mut _)?;
        }
        Ok(stats)
    }

    /// Get a copy of the mountpoint.
    pub fn mountpoint(&self) -> PathBuf {
        // Clone here because we are not allowed to move out of self, as we have a drop
        // implementation.
        self.mountpoint.clone()
    }
}

mod types {
    #![allow(non_camel_case_types)]
    // TODO: remove this once bindgen stops using nullptr derefs for the layout tests.
    #![allow(deref_nullptr)]
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/zdbfs_stats.rs"));
}
