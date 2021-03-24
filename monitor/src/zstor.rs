use futures::lock::Mutex;
use std::error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, PoisonError};
use tokio::io;
use tokio::process::Command;

pub type ZstorBinResult<T> = Result<T, ZstorBinError>;

#[derive(Debug, Clone)]
pub struct SingleZstor {
    internal: Arc<Mutex<Zstor>>,
}

impl SingleZstor {
    pub fn new(internal: Zstor) -> Self {
        SingleZstor {
            internal: Arc::new(Mutex::new(internal)),
        }
    }

    pub async fn rebuild_key(&self, key: &str) -> ZstorBinResult<()> {
        self.internal.lock().await.rebuild_key(key).await
    }

    pub async fn upload_file(
        &self,
        data_path: &Path,
        key_path: Option<&Path>,
        should_delete: bool,
    ) -> ZstorBinResult<()> {
        self.internal
            .lock()
            .await
            .upload_file(data_path, key_path, should_delete)
            .await
    }

    pub async fn download_file(&self, path: &Path) -> ZstorBinResult<()> {
        self.internal.lock().await.download_file(path).await
    }

    pub async fn file_is_uploaded(&self, path: &Path) -> ZstorBinResult<bool> {
        self.internal.lock().await.file_is_uploaded(path).await
    }
}

#[derive(Debug)]
pub struct Zstor {
    bin_path: PathBuf,
    config_path: PathBuf,
}

impl Zstor {
    pub fn new(bin_path: PathBuf, config_path: PathBuf) -> Self {
        Zstor {
            bin_path,
            config_path,
        }
    }

    pub fn bin_path(&self) -> &Path {
        &self.bin_path
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Triggers the zstor binary to perform a rebuild command on the given key.
    pub async fn rebuild_key(&mut self, key: &str) -> ZstorBinResult<()> {
        if self
            .base_command()
            .arg("rebuild")
            .arg("-k")
            .arg(key)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()
            .await?
            .success()
        {
            Ok(())
        } else {
            Err(ZstorBinError {
                kind: ErrorKind::Runtime,
                internal: InternalError::Message(
                    "Program finished with none zero exit code".to_string(),
                ),
            })
        }
    }

    /// Trigger the zstor binary to try and upload a file
    pub async fn upload_file(
        &mut self,
        data_path: &Path,
        key_path: Option<&Path>,
        should_delete: bool,
    ) -> ZstorBinResult<()> {
        let mut cmd = self.base_command();
        cmd.arg("store");
        if should_delete {
            cmd.arg("--delete");
        }
        if let Some(kp) = key_path {
            cmd.arg("--key-path").arg(kp);
        };
        if cmd
            .arg("--file")
            .arg(data_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()
            .await?
            .success()
        {
            Ok(())
        } else {
            Err(ZstorBinError {
                kind: ErrorKind::Runtime,
                internal: InternalError::Message(
                    "Program finished with none zero exit code".to_string(),
                ),
            })
        }
    }

    /// Trigger the zstor binary to try and download a file
    pub async fn download_file(&mut self, path: &Path) -> ZstorBinResult<()> {
        if self
            .base_command()
            .arg("retrieve")
            .arg("-f")
            .arg(path.as_os_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()
            .await?
            .success()
        {
            Ok(())
        } else {
            Err(ZstorBinError {
                kind: ErrorKind::Runtime,
                internal: InternalError::Message(
                    "Program finished with none zero exit code".to_string(),
                ),
            })
        }
    }

    /// Trigger the zstor binary to perform a check on the file. If true, the file is uploaded
    pub async fn file_is_uploaded(&mut self, check_path: &Path) -> ZstorBinResult<bool> {
        Ok(self
            .base_command()
            .arg("check")
            .arg("-f")
            .arg(check_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()
            .await?
            .success())
    }

    fn base_command(&self) -> Command {
        let mut cmd = Command::new(&self.bin_path);
        cmd.arg("--config").arg(&self.config_path);
        cmd
    }
}

#[derive(Debug)]
pub struct ZstorBinError {
    kind: ErrorKind,
    internal: InternalError,
}

impl fmt::Display for ZstorBinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} error in zstor bin: {}", self.kind, self.internal)
    }
}

impl error::Error for ZstorBinError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self.internal {
            InternalError::IO(ref e) => Some(e),
            InternalError::Message(_) => None,
        }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    IO,
    Runtime,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorKind::IO => "I/O",
                ErrorKind::Runtime => "Runtime",
            }
        )
    }
}

#[derive(Debug)]
enum InternalError {
    IO(std::io::Error),
    Message(String),
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InternalError::IO(ref e) => e as &dyn fmt::Display,
                InternalError::Message(ref e) => e,
            }
        )
    }
}

impl From<io::Error> for ZstorBinError {
    fn from(e: io::Error) -> Self {
        ZstorBinError {
            kind: ErrorKind::IO,
            internal: InternalError::IO(e),
        }
    }
}

impl<T> From<PoisonError<T>> for ZstorBinError {
    fn from(_: PoisonError<T>) -> Self {
        ZstorBinError {
            kind: ErrorKind::Runtime,
            internal: InternalError::Message(
                "Previous execution caused unrecoverable runtime errors".to_string(),
            ),
        }
    }
}
