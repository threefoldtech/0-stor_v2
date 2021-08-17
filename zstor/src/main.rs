use actix::Addr;
use futures::future::join_all;
use log::LevelFilter;
use log::{debug, error, info, trace};
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::{Filter, Response};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;
use structopt::StructOpt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use zstor_v2::actors::zstor::{
    Check, Rebuild, Retrieve, Store, ZstorActor, ZstorCommand, ZstorResponse,
};
use zstor_v2::config::Config;
use zstor_v2::zdb::SequentialZdb;
use zstor_v2::{ZstorError, ZstorErrorKind, ZstorResult};

const MIB: u64 = 1 << 20;

#[derive(StructOpt, Debug)]
#[structopt(about = "zstor data encoder")]
/// Zstor data encoder
///
/// Compresses, encrypts, and erasure codes data according to the provided config file and options.
/// Data is send to a specified group of backends.
struct Opts {
    /// Path to the config file to use for this invocation.
    #[structopt(
        name = "config",
        default_value = "zstor_config.toml",
        long,
        short,
        parse(from_os_str)
    )]
    config: PathBuf,
    /// Path to the log file to use. The logfile will automatically roll over if the size
    /// increases beyond 10MiB.
    #[structopt(
        name = "log_file",
        default_value = "zstor.log",
        long,
        parse(from_os_str)
    )]
    log_file: PathBuf,
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(StructOpt, Debug)]
enum Cmd {
    /// Encode data and store it in the 0-db backends
    ///
    /// The data is compressed and encrypted according to the config before being encoded. Successful
    /// termination of this command means all shars have been written.
    Store {
        /// Path to the file or directory to store
        ///
        /// The path to the file or directory to store. In case it is a path:
        /// The path is used to create a metadata key (by hashing
        /// the full path). If a file is encoded at `path`, and then a new file is encoded for the
        /// same `path`. The old file metadata is overwritten and you will no longer be able to
        /// restore the file.
        ///
        /// In case path is a directory, an attempt will be made to store all files in the
        /// directory. Only files in the directory are stored, i.e. there is on descending into
        /// subdirectories. In this mode, errors encoding files are none fatal, and an effort is
        /// made to continue encoding. If any or all files fail to upload but the program, the exit
        /// code is still 0 (succes).
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: PathBuf,
        /// Upload the file as if it was located in the directory at the given path
        ///
        /// The metadata storage key is computed as if the file is part of the given directory. The
        /// file can later be recovered by retrieving it with this path + the file name. The
        /// directory passed to this flag MUST exist on the system.
        #[structopt(name = "key-path", long, short, parse(from_os_str))]
        key_path: Option<PathBuf>,
        /// Exits if a file can't be uploaded in directory upload mode
        ///
        /// The regular behavior when uploading a directory is to simply log errors and proceed
        /// with the next file. If this flag is set, an error when uploading will be fatal and
        /// cause the process to exit with a non-zero exit code.
        #[structopt(name = "error-on-failure", long, short)]
        error_on_failure: bool,
        /// Save data about upload failures in the metadata store
        ///
        /// Saves info about a failed upload in the metadata store. The exact data saved is an
        /// implementation detail. This allows related components to monitor the metadata store to
        /// detect upload failures and retry at a later time. If the metadata store itself errors,
        /// no data will be saved.
        #[structopt(name = "save-failure", long, short)]
        save_failure: bool,
        /// Deletes files after a successful upload
        ///
        /// Deletes the file after a successful upload. Deletion is done on a "best effort" basis.
        /// Failure to delete the file is not considered an error, and should this happen, the
        /// program will still return exitcode 0 (success).
        #[structopt(name = "delete", long, short)]
        delete: bool,
    },
    /// Rebuild already stored data
    ///
    /// The data itself is recreated from the (available) shards, and
    /// re-encoded and redistributed over the backends in the current config file. This operation
    /// will fail if insufficient backends are available.
    Rebuild {
        /// Path to the file to rebuild
        ///
        /// The path to the file to rebuild. The path is used to create a metadata key (by hashing
        /// the full path). The original data is decoded, and then reencoded as per the provided
        /// config. The new metadata is then used to replace the old metadata in the metadata
        /// store.
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: Option<PathBuf>,
        /// Raw key to reconstruct
        ///
        /// The raw key to reconstruct. If this argument is given, the metadata store is checked
        /// for this key. If it exists, the data will be reconstructed according to the new policy,
        /// and the old metadata is replaced with the new metadata.
        #[structopt(name = "key", long, short)]
        key: Option<String>,
    },
    /// Load encoded data
    ///
    /// Loads data from available shards, restores it, decrypts and decompresses it. This operation
    /// will fail if insufficient shards are available to retrieve the data.
    Retrieve {
        /// Path of the file to retrieve.
        ///
        /// The original path which was used to store the file.
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: PathBuf,
    },
    /// Check if a file exists in the backend
    ///
    /// Checks if a valid metadata entry is available for the path. If it is, the hex of the file
    /// checksum is printed, as well as the resolved path.
    Check {
        /// Path of the file to check.
        ///
        /// The original path which was used to store the file.
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: PathBuf,
    },
    /// Run zstor in monitor daemon mode.
    ///
    /// Runs the monitor. This keeps running untill an exit signal is send to the process (SIGINT).
    /// Additional features are available depending on the configuration. All communication happens
    /// over a unix socket, specified in the config. If such a socket path is present in the
    /// config, other commands will try to connect to the daemon, and run the commands through the
    /// daemon.
    Monitor,

    /// Test the configuration and backends
    ///
    /// Tests if the configuration is valid, and all backends are available. Also makes sure the
    /// metadata storage is reachable. Validation of the configuration also includes making sure
    /// at least 1 distribution can be generated for writing files.
    Test,
}

/// ModuleFilter is a naive log filter which only allows (child modules of) a given module.
#[derive(Debug)]
struct ModuleFilter {
    module: String,
}

impl Filter for ModuleFilter {
    fn filter(&self, record: &log::Record) -> Response {
        if let Some(mod_path) = record.module_path() {
            // this is technically not correct but sufficient for our purposes
            if mod_path.starts_with(self.module.as_str()) {
                return Response::Neutral;
            }
        }
        Response::Reject
    }
}

#[actix_rt::main]
async fn main() -> ZstorResult<()> {
    if let Err(e) = real_main().await {
        error!("{}", e);
        return Err(e);
    }

    Ok(())
}

async fn real_main() -> ZstorResult<()> {
    let opts = Opts::from_args();

    // TODO: add check for file name
    let mut rolled_log_file = opts.log_file.clone();
    let name = if let Some(ext) = rolled_log_file.extension() {
        format!(
            "{}.{{}}.{}",
            rolled_log_file.file_stem().unwrap().to_str().unwrap(),
            ext.to_str().unwrap(),
        )
    } else {
        format!(
            "{}.{{}}",
            rolled_log_file.file_stem().unwrap().to_str().unwrap(),
        )
    };
    rolled_log_file.set_file_name(name);

    // init logger
    let policy = CompoundPolicy::new(
        Box::new(SizeTrigger::new(10 * MIB)),
        Box::new(
            FixedWindowRoller::builder()
                .build(rolled_log_file.to_str().unwrap(), 5)
                .unwrap(),
        ),
    );
    let log_file = RollingFileAppender::builder()
        .append(true)
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S %Z)(local)}: {l} {m}{n}",
        )))
        .build(&opts.log_file, Box::new(policy))
        .unwrap();
    let log_config = LogConfig::builder()
        .appender(
            Appender::builder()
                .filter(Box::new(ModuleFilter {
                    module: "zstor_v2".to_string(),
                }))
                .build("logfile", Box::new(log_file)),
        )
        .logger(Logger::builder().build("filelogger", LevelFilter::Debug))
        .build(
            Root::builder()
                .appender("logfile")
                .build(log::LevelFilter::Debug),
        )
        .unwrap();
    log4rs::init_config(log_config).unwrap();

    let cfg = read_cfg(&opts.config)?;

    match opts.cmd {
        Cmd::Store {
            file,
            key_path,
            save_failure,
            error_on_failure: _error_on_failure,
            delete,
        } => {
            handle_command(
                ZstorCommand::Store(Store {
                    file,
                    key_path,
                    save_failure,
                    delete,
                }),
                opts.config,
            )
            .await?
        }
        Cmd::Retrieve { file } => {
            handle_command(ZstorCommand::Retrieve(Retrieve { file }), opts.config).await?
        }
        Cmd::Rebuild { file, key } => {
            handle_command(ZstorCommand::Rebuild(Rebuild { file, key }), opts.config).await?
        }
        Cmd::Check { file } => {
            handle_command(ZstorCommand::Check(Check { path: file }), opts.config).await?
        }
        Cmd::Test => {
            // load config => already done
            // connect to metastore => already done
            // connect to backends
            debug!("Testing 0-db reachability");
            for conn_result in join_all(
                cfg.backends()
                    .into_iter()
                    .cloned()
                    .map(|ci| tokio::spawn(async move { SequentialZdb::new(ci).await })),
            )
            .await
            {
                // type here is Resut<Result<Zdb, ZdbError>, JoinError>
                // so we need to try on the outer JoinError to get to the possible ZdbError
                conn_result??;
            }

            // retrieve a config
            cfg.shard_stores()?;
        }
        Cmd::Monitor => {
            let server = if let Some(socket) = cfg.socket() {
                UnixListener::bind(socket)
                    .map_err(|e| ZstorError::new_io("Failed to bind unix socket".into(), e))?
            } else {
                eprintln!("Missing \"socket\" argument in config");
                process::exit(1);
            };

            // unwrapping this is safe as we just checked it is Some. We clone the value here to
            // avoid having to clone the whole config.
            let socket_path = cfg.socket().unwrap().to_path_buf();
            let _f = DropFile::new(&socket_path);

            let zstor = zstor_v2::setup_system(opts.config, cfg).await?;

            loop {
                tokio::select! {
                    accepted = server.accept() => {
                        let (con, remote) = match accepted {
                            Err(e) => {
                                error!("Could not accept client connection: {}", e);
                                continue;
                            },
                            Ok((con, remote)) => (con, remote),
                        };
                        let zs = zstor.clone();
                        tokio::spawn(async move {
                            debug!("Handling new client connection from {:?}", remote);
                            if let Err(e) = handle_client(con, zs).await {
                                error!("Error while handeling client: {}", e);
                            }
                        });
                    }
                    _ = actix_rt::signal::ctrl_c() => break,
                }
            }
            actix_rt::signal::ctrl_c()
                .await
                .map_err(|e| ZstorError::new_io("Failed to wait for CTRL-C".to_string(), e))?;

            info!("Shutting down zstor daemon after receiving CTRL-C");
        }
    };

    Ok(())
}

async fn handle_client<C>(mut con: C, zstor: Addr<ZstorActor>) -> ZstorResult<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    // Read the command from the connection
    let size = con
        .read_u16()
        .await
        .map_err(|e| ZstorError::new_io("failed to read msg length prefix".into(), e))?;
    let mut cmd_buf = vec![0; size as usize]; // Fill buffer with 0 bytes, instead of reserving space.
    let n = con
        .read_exact(&mut cmd_buf)
        .await
        .map_err(|e| ZstorError::new_io("failed to read exact amount of bytes".into(), e))?;
    if n != size as usize {
        error!("Could not read exact amount of bytes from connection");
        return Ok(());
    }
    let cmd = bincode::deserialize(&cmd_buf)?;
    let res = match cmd {
        ZstorCommand::Store(store) => match zstor.send(store).await? {
            Err(e) => ZstorResponse::Err(e.to_string()),
            Ok(()) => ZstorResponse::Success,
        },
        ZstorCommand::Retrieve(retrieve) => match zstor.send(retrieve).await? {
            Err(e) => ZstorResponse::Err(e.to_string()),
            Ok(()) => ZstorResponse::Success,
        },
        ZstorCommand::Rebuild(rebuild) => match zstor.send(rebuild).await? {
            Err(e) => ZstorResponse::Err(e.to_string()),
            Ok(()) => ZstorResponse::Success,
        },
        ZstorCommand::Check(check) => match zstor.send(check).await? {
            Err(e) => ZstorResponse::Err(e.to_string()),
            Ok(Some(checksum)) => ZstorResponse::Checksum(checksum),
            Ok(None) => ZstorResponse::Success,
        },
    };

    let res_buf = bincode::serialize(&res)?;
    con.write_u16(res_buf.len() as u16)
        .await
        .map_err(|e| ZstorError::new_io("failed to write response length".into(), e))?;
    con.write_all(&res_buf)
        .await
        .map_err(|e| ZstorError::new_io("failed to write response buffer".into(), e))?;
    Ok(())
}

async fn handle_command(zc: ZstorCommand, cfg_path: PathBuf) -> Result<(), ZstorError> {
    let cfg = zstor_v2::load_config(&cfg_path).await?;
    // If a socket is set, try to send a command to a daemon.
    if let Some(socket) = cfg.socket() {
        debug!("Sending command to zstor daemon");
        let mut con = UnixStream::connect(socket)
            .await
            .map_err(|e| ZstorError::new_io("Could not connect to daemon socket".to_string(), e))?;
        // expect here is fine as its a programming error
        let buf = bincode::serialize(&zc).expect("Failed to encode command");
        con.write_u16(buf.len() as u16).await.map_err(|e| {
            ZstorError::new_io("Could not write command length to socket".into(), e)
        })?;
        con.write_all(&buf)
            .await
            .map_err(|e| ZstorError::new_io("Could not write command to socket".into(), e))?;
        // now wait for the response
        let res_len = con.read_u16().await.map_err(|e| {
            ZstorError::new_io("Could not read response length from socket".into(), e)
        })?;
        let mut res_buf = vec![0; res_len as usize];
        con.read_exact(&mut res_buf)
            .await
            .map_err(|e| ZstorError::new_io("Could not read response from socket".into(), e))?;
        let res = bincode::deserialize::<ZstorResponse>(&res_buf)
            .expect("Failed to decode reply from socket");

        match res {
            ZstorResponse::Checksum(checksum) => println!("{}", hex::encode(checksum)),
            ZstorResponse::Err(err) => {
                eprintln!("Zstor error: {}", err);
                process::exit(1);
            }
            _ => {}
        }
    } else {
        debug!("No zstor daemon socket found, running command in process");
        let zstor = zstor_v2::setup_system(cfg_path, cfg).await?;
        match zc {
            ZstorCommand::Store(store) => zstor.send(store).await??,
            ZstorCommand::Retrieve(retrieve) => zstor.send(retrieve).await??,
            ZstorCommand::Rebuild(rebuild) => zstor.send(rebuild).await??,
            ZstorCommand::Check(check) => {
                if let Some(checksum) = zstor.send(check).await?? {
                    println!("{}", hex::encode(checksum));
                }
            }
        };
    };
    Ok(())
}

fn read_cfg(config: &Path) -> ZstorResult<Config> {
    trace!("opening config file {:?}", config);
    let mut cfg_file = File::open(config)
        .map_err(|e| ZstorError::new_io("could not open config file".to_string(), e))?;
    let mut cfg_str = String::new();
    cfg_file
        .read_to_string(&mut cfg_str)
        .map_err(|e| ZstorError::new_io("could not read config file".to_string(), e))?;

    let cfg: Config = toml::from_str(&cfg_str)
        .map_err(|e| ZstorError::new(ZstorErrorKind::Config, Box::new(e)))?;
    trace!("config read");
    cfg.validate()?;
    trace!("config validated");
    Ok(cfg)
}

/// A struct which removes the file at the Path it wraps when it goes out of scope.
struct DropFile<'a> {
    path: &'a Path,
}

impl<'a> DropFile<'a> {
    /// Create a new DropFile with the given Path.
    fn new(path: &'a Path) -> Self {
        Self { path }
    }
}

impl<'a> Drop for DropFile<'a> {
    fn drop(&mut self) {
        // try to remove the file
        if let Err(e) = fs::remove_file(self.path) {
            error!("Could not remove file: {}", e);
        };
    }
}
