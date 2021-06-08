use blake2::{digest::VariableOutput, VarBlake2b};
use futures::future::{join_all, try_join_all};
use log::LevelFilter;
use log::{debug, error, info, trace, warn};
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::{Filter, Response};
use std::convert::TryInto;
use std::fs::{self, File};
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::process;
use structopt::StructOpt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use zstor_v2::actors::zstor::{Check, Rebuild, Retrieve, Store, ZstorCommand, ZstorResponse};
use zstor_v2::compression::{Compressor, Snappy};
use zstor_v2::config::Config;
use zstor_v2::encryption;
use zstor_v2::erasure::{Encoder, Shard};
use zstor_v2::meta::{Checksum, MetaData, MetaStore, ShardInfo, CHECKSUM_LENGTH};
use zstor_v2::zdb::{SequentialZdb, ZdbError, ZdbResult};
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
            // if file.is_file() {
            //     handle_file_upload(&mut *cluster, &file, &key_path, save_failure, delete, &cfg)
            //         .await?;
            // } else if file.is_dir() {
            //     for entry in get_dir_entries(&file).map_err(|e| {
            //         ZstorError::new_io(format!("Could not read dir entries in {:?}", file), e)
            //     })? {
            //         if let Err(e) = handle_file_upload(
            //             &mut *cluster,
            //             &entry,
            //             &key_path,
            //             save_failure,
            //             delete,
            //             &cfg,
            //         )
            //         .await
            //         {
            //             error!("Could not upload file {:?}: {}", entry, e);
            //             if error_on_failure {
            //                 return Err(e);
            //             } else {
            //                 continue;
            //             }
            //         }
            //     }
            // } else {
            //     return Err(ZstorError::new_io(
            //         "Unknown file type".to_string(),
            //         std::io::Error::from(std::io::ErrorKind::InvalidData),
            //     ));
            // }
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
            // let metadata = cluster.load_meta(file).await?.ok_or_else(|| {
            //     ZstorError::new_io(
            //         "no metadata found for file".to_string(),
            //         std::io::Error::from(std::io::ErrorKind::NotFound),
            //     )
            // })?;
            // let decoded = recover_data(&metadata).await?;

            // let encryptor = encryption::new(metadata.encryption().clone());
            // let decrypted = encryptor.decrypt(&decoded)?;

            // // create the file
            // let mut out = if let Some(ref root) = cfg.virtual_root() {
            //     File::create(root.join(&file))
            // } else {
            //     File::create(file)
            // }
            // .map_err(|e| ZstorError::new_io("could not create output file".to_string(), e))?;

            // let mut cursor = Cursor::new(decrypted);
            // Snappy.decompress(&mut cursor, &mut out)?;

            // // get file size
            // let file_size = if let Ok(meta) = out.metadata() {
            //     Some(meta.len())
            // } else {
            //     // TODO: is this possible?
            //     None
            // };

            // if !std::fs::metadata(&file)
            //     .map_err(|e| ZstorError::new_io("could not load file metadata".to_string(), e))?
            //     .is_file()
            // {
            //     return Err(ZstorError::new_io(
            //         "only files can be stored".to_string(),
            //         std::io::Error::from(std::io::ErrorKind::InvalidData),
            //     ));
            // }

            // info!(
            //     "Recovered file {:?} ({} bytes) from {}",
            //     if let Some(ref root) = cfg.virtual_root() {
            //         root.join(&file)
            //     } else {
            //         file.clone()
            //     },
            //     if let Some(size) = file_size {
            //         size.to_string()
            //     } else {
            //         "unknown".to_string()
            //     },
            //     metadata
            //         .shards()
            //         .iter()
            //         .map(|si| si.zdb().address().to_string())
            //         .collect::<Vec<_>>()
            //         .join(",")
            // );
            handle_command(ZstorCommand::Retrieve(Retrieve { file }), opts.config).await?
        }
        Cmd::Rebuild { file, key } => {
            // if file.is_none() && key.is_none() {
            //     panic!("Either `file` or `key` argument must be set");
            // }
            // if file.is_some() && key.is_some() {
            //     panic!("Only one of `file` or `key` argument must be set");
            // }
            // let old_metadata = if let Some(file) = file {
            //     cluster.load_meta(file).await?.ok_or_else(|| {
            //         ZstorError::new_io(
            //             "no metadata found for file".to_string(),
            //             std::io::Error::from(std::io::ErrorKind::NotFound),
            //         )
            //     })?
            // } else if let Some(key) = key {
            //     // key is set so the unwrap is safe
            //     cluster.load_meta_by_key(key).await?.ok_or_else(|| {
            //         ZstorError::new_io(
            //             "no metadata found for file".to_string(),
            //             std::io::Error::from(std::io::ErrorKind::NotFound),
            //         )
            //     })?
            // } else {
            //     unreachable!();
            // };
            // let decoded = recover_data(&old_metadata).await?;

            // let metadata = store_data(decoded, *old_metadata.checksum(), &cfg).await?;
            // if let Some(file) = file {
            //     cluster.save_meta(&file, &metadata).await?;
            // } else if let Some(key) = key {
            //     cluster.save_meta_by_key(key, &metadata).await?;
            // };
            // info!(
            //     "Rebuild file from {} to {}",
            //     old_metadata
            //         .shards()
            //         .iter()
            //         .map(|si| si.zdb().address().to_string())
            //         .collect::<Vec<_>>()
            //         .join(","),
            //     metadata
            //         .shards()
            //         .iter()
            //         .map(|si| si.zdb().address().to_string())
            //         .collect::<Vec<_>>()
            //         .join(",")
            // );
            handle_command(ZstorCommand::Rebuild(Rebuild { file, key }), opts.config).await?
        }
        Cmd::Check { file } => {
            // match cluster.load_meta(file).await? {
            //     Some(metadata) => {
            //         let file = canonicalize_path(&file)?;
            //         // strip the virtual_root, if one is set
            //         let actual_path = if let Some(ref virtual_root) = cfg.virtual_root() {
            //             file.strip_prefix(virtual_root).map_err(|_| {
            //                 ZstorError::new_io(
            //                     format!(
            //                         "path prefix {} not found",
            //                         virtual_root.as_path().to_string_lossy()
            //                     ),
            //                     std::io::Error::from(std::io::ErrorKind::NotFound),
            //                 )
            //             })?
            //         } else {
            //             file.as_path()
            //         };
            //         println!(
            //             "{}\t{}",
            //             hex::encode(metadata.checksum()),
            //             actual_path.to_string_lossy()
            //         );
            //     }
            //     None => std::process::exit(1),
            // };
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
            let zstor = zstor_v2::setup_system(opts.config).await?;

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

            // try to remove the socket file
            if let Err(e) = fs::remove_file(cfg.socket().unwrap()) {
                error!("Could not remove socket file: {}", e);
            };
        }
    };

    Ok(())
}

use actix::Addr;
use zstor_v2::actors::zstor::ZstorActor;
async fn handle_client<C>(
    mut con: C,
    zstor: Addr<ZstorActor<impl MetaStore + Unpin>>,
) -> ZstorResult<()>
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
            Ok(checksum) => ZstorResponse::Checksum(checksum),
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
        let zstor = zstor_v2::setup_system(cfg_path).await?;
        match zc {
            ZstorCommand::Store(store) => zstor.send(store).await??,
            ZstorCommand::Retrieve(retrieve) => zstor.send(retrieve).await??,
            ZstorCommand::Rebuild(rebuild) => zstor.send(rebuild).await??,
            ZstorCommand::Check(check) => {
                let checksum = zstor.send(check).await??;
                println!("{}", hex::encode(checksum));
            }
        }
    };
    Ok(())
}

// TODO: Async version
fn get_dir_entries(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut dir_entries = Vec::new();
    for dir_entry in fs::read_dir(&dir)? {
        let entry = dir_entry?;
        let ft = entry.file_type()?;

        if !ft.is_file() {
            debug!(
                "Skippin entry {:?} for upload as it is not a file",
                entry.path(),
            );
            continue;
        }

        dir_entries.push(entry.path());
    }

    Ok(dir_entries)
}

async fn handle_file_upload(
    cluster: &mut dyn MetaStore,
    data_file: &Path,
    key_path: &Option<PathBuf>,
    save_failure: bool,
    delete: bool,
    cfg: &Config,
) -> ZstorResult<()> {
    let (data_file_path, mut key_dir_path) = match key_path {
        Some(ref kp) => (data_file, kp.to_path_buf()),
        None => {
            let mut key_dir = data_file.to_path_buf();
            if !key_dir.pop() {
                return Err(ZstorError::new_io(
                    "Could not remove file name to get parent dir name".to_string(),
                    std::io::Error::from(std::io::ErrorKind::InvalidData),
                ));
            }
            (data_file, key_dir)
        }
    };

    if data_file_path.file_name().is_none() {
        return Err(ZstorError::new_io(
            "Could not get file name of data file".to_string(),
            std::io::Error::from(std::io::ErrorKind::NotFound),
        ));
    }

    // check above makes unwrap here safe
    key_dir_path.push(data_file_path.file_name().unwrap());

    // canonicalize the key path
    let key_file_path = canonicalize_path(&key_dir_path)?;
    debug!(
        "encoding file {:?} with key path {:?}",
        data_file_path, key_file_path
    );

    // make sure key path is in root dir
    if let Some(ref root) = cfg.virtual_root() {
        if !key_file_path.starts_with(root) {
            return Err(ZstorError::new_io(
                format!(
                    "attempting to store file which is not in the file tree rooted at {}",
                    root.to_string_lossy()
                ),
                std::io::Error::from(std::io::ErrorKind::InvalidData),
            ));
        }
    }

    if !std::fs::metadata(&data_file_path)
        .map_err(|e| ZstorError::new_io("could not load file metadata".to_string(), e))?
        .is_file()
    {
        return Err(ZstorError::new_io(
            "only files can be stored".to_string(),
            std::io::Error::from(std::io::ErrorKind::InvalidData),
        ));
    }

    if let Err(e) = save_file(cluster, &data_file_path, &key_file_path, &cfg).await {
        error!("Could not save file {:?}: {}", data_file_path, e);
        if save_failure {
            debug!("Attempt to save upload failure data");
            //if let Err(e2) = cluster.save_failure(&dat_file_path, &key_file_path).await {
            if let Err(e2) = cluster
                .save_failure(&data_file_path, key_path, delete)
                .await
            {
                error!("Could not save failure metadata: {}", e2);
            };
        }
        // return the original error
        return Err(e);
    } else if delete {
        debug!(
            "Attempting to delete file {:?} after successful upload",
            data_file_path
        );
        if let Err(e) = std::fs::remove_file(&data_file_path) {
            warn!("Could not delete uploaded file: {}", e);
        }
    };

    Ok(())
}

async fn save_file(
    cluster: &mut dyn MetaStore,
    data_file: &Path,
    key_file: &Path,
    cfg: &Config,
) -> ZstorResult<()> {
    let file_checksum = checksum(data_file)?;
    debug!(
        "file checksum: {} ({:?})",
        hex::encode(file_checksum),
        data_file
    );

    // start reading file to encrypt
    trace!("loading file data");
    let mut encoding_file = File::open(&data_file)
        .map_err(|e| ZstorError::new_io("could not open file to encode".to_string(), e))?;

    let compressed = Vec::new();
    let mut cursor = Cursor::new(compressed);
    let original_size = Snappy.compress(&mut encoding_file, &mut cursor)?;
    let compressed = cursor.into_inner();
    trace!("compressed size: {} bytes", original_size);

    let encryptor = encryption::new(cfg.encryption().clone());
    let encrypted = encryptor.encrypt(&compressed)?;
    trace!("encrypted size: {} bytes", encrypted.len());

    let metadata = store_data(encrypted, file_checksum, &cfg).await?;

    cluster.save_meta(key_file, &metadata).await?;
    info!(
        "Stored file {:?} as file {:?} ({} bytes) to {}",
        data_file,
        key_file,
        original_size,
        metadata
            .shards()
            .iter()
            .map(|si| si.zdb().address().to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    Ok(())
}

async fn recover_data(metadata: &MetaData) -> ZstorResult<Vec<u8>> {
    // attempt to retrieve all shards
    let mut shard_loads: Vec<JoinHandle<(usize, Result<(_, _), ZstorError>)>> =
        Vec::with_capacity(metadata.shards().len());
    for si in metadata.shards().iter().cloned() {
        shard_loads.push(tokio::spawn(async move {
            let db = match SequentialZdb::new(si.zdb().clone()).await {
                Ok(ok) => ok,
                Err(e) => return (si.index(), Err(e.into())),
            };
            match db.get(si.key()).await {
                Ok(potential_shard) => match potential_shard {
                    Some(shard) => (si.index(), Ok((shard, *si.checksum()))),
                    None => (
                        si.index(),
                        // TODO: Proper error here?
                        Err(ZstorError::new_io(
                            "shard not found".to_string(),
                            std::io::Error::from(std::io::ErrorKind::NotFound),
                        )),
                    ),
                },
                Err(e) => (si.index(), Err(e.into())),
            }
        }));
    }

    // Since this is the amount of actual shards needed to pass to the encoder, we calculate the
    // amount we will have from the amount of parity and data shards. Reason is that the `shards()`
    // might not have all data shards, due to a bug on our end, or later in case we allow for
    // degraded writes.
    let mut shards: Vec<Option<Vec<u8>>> =
        vec![None; metadata.data_shards() + metadata.parity_shards()];
    for shard_info in join_all(shard_loads).await {
        let (idx, shard) = shard_info?;
        match shard {
            Err(e) => warn!("could not download shard {}: {}", idx, e),
            Ok((raw_shard, saved_checksum)) => {
                let shard = Shard::from(raw_shard);
                let checksum = shard.checksum();
                if saved_checksum != checksum {
                    warn!("shard {} checksum verification failed", idx);
                    continue;
                }
                shards[idx] = Some(shard.into_inner());
            }
        }
    }

    let encoder = Encoder::new(metadata.data_shards(), metadata.parity_shards());
    let decoded = encoder.decode(shards)?;

    info!("rebuild data from shards");

    Ok(decoded)
}

async fn store_data(data: Vec<u8>, checksum: Checksum, cfg: &Config) -> ZstorResult<MetaData> {
    let encoder = Encoder::new(cfg.data_shards(), cfg.parity_shards());
    let shards = encoder.encode(data);
    trace!("data encoded");

    // craate a local copy of the config which we can modify to remove dead nodes
    let mut cfg = cfg.clone();

    let shard_len = shards[0].len(); // Safe as a valid encoder needs at least 1 shard

    trace!("verifiying backends");

    let dbs = loop {
        debug!("Finding backend config");
        let backends = cfg.shard_stores()?;

        let mut failed_shards: usize = 0;
        let mut handles: Vec<JoinHandle<ZdbResult<_>>> = Vec::with_capacity(shards.len());

        for backend in backends {
            handles.push(tokio::spawn(async move {
                let db = SequentialZdb::new(backend.clone()).await?;
                // check space in backend
                let ns_info = db.ns_info().await?;
                match ns_info.free_space() {
                    insufficient if (insufficient as usize) < shard_len => {
                        Err(ZdbError::new_storage_size(
                            *db.connection_info().address(),
                            shard_len,
                            ns_info.free_space() as usize,
                        ))
                    }
                    _ => Ok(db),
                }
            }));
        }

        let mut dbs = Vec::new();
        for db in join_all(handles).await {
            match db? {
                Err(zdbe) => {
                    debug!("could not connect to 0-db: {}", zdbe);
                    cfg.remove_shard(zdbe.address());
                    failed_shards += 1;
                }
                Ok(db) => dbs.push(db), // no error so healthy db backend
            }
        }

        // if we find one we are good
        if failed_shards == 0 {
            debug!("found valid backend configuration");
            break dbs;
        }

        debug!("Backend config failed");
    };

    trace!("store shards in backends");

    let mut metadata = MetaData::new(
        cfg.data_shards(),
        cfg.parity_shards(),
        checksum,
        cfg.encryption().clone().into(),
        cfg.compression().clone().into(),
    );

    let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(shards.len());
    for (db, (shard_idx, shard)) in dbs.into_iter().zip(shards.into_iter().enumerate()) {
        handles.push(tokio::spawn(async move {
            let keys = db.set(&shard).await?;
            Ok(ShardInfo::new(
                shard_idx,
                shard.checksum(),
                keys,
                db.connection_info().clone(),
            ))
        }));
    }

    for shard_info in try_join_all(handles).await? {
        metadata.add_shard(shard_info?);
    }

    Ok(metadata)
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

/// wrapper around the standard library method [`std::fs::canonicalize_path`]. This method will
/// work on files which don't exist by creating a dummy file if the file does not exist,
/// canonicalizing the path, and removing the dummy file.
fn canonicalize_path(path: &Path) -> ZstorResult<PathBuf> {
    // annoyingly, the path needs to exist for this to work. So here's the plan:
    // first we verify that it is actualy there
    // if it is, no problem
    // else, create a temp file, canonicalize that path, and remove the temp file again
    Ok(match std::fs::metadata(path) {
        Ok(_) => path
            .canonicalize()
            .map_err(|e| ZstorError::new_io("could not canonicalize path".to_string(), e))?,
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                File::create(path)
                    .map_err(|e| ZstorError::new_io("could not create temp file".to_string(), e))?;
                let cp = path.canonicalize().map_err(|e| {
                    ZstorError::new_io("could not canonicalize path".to_string(), e)
                })?;
                std::fs::remove_file(path)
                    .map_err(|e| ZstorError::new_io("could not remove temp file".to_string(), e))?;
                cp
            }
            _ => {
                return Err(ZstorError::new_io(
                    "could not load file metadata".to_string(),
                    e,
                ))
            }
        },
    })
}

/// Get a 16 byte blake2b checksum of a file
fn checksum(file: &Path) -> ZstorResult<Checksum> {
    trace!("getting file checksum");
    let mut file =
        File::open(file).map_err(|e| ZstorError::new_io("could not open file".to_string(), e))?;
    // The unwrap here is safe since we know that 16 is a valid output size
    let mut hasher = VarBlake2b::new(CHECKSUM_LENGTH).unwrap();
    std::io::copy(&mut file, &mut hasher)
        .map_err(|e| ZstorError::new_io("could not get file hash".to_string(), e))?;

    // expect is safe due to the static size, which is known to be valid
    Ok(hasher
        .finalize_boxed()
        .as_ref()
        .try_into()
        .expect("Invalid hash size returned"))
}
