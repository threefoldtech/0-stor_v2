use blake2::{digest::VariableOutput, VarBlake2b};
use futures::future::{join_all, try_join_all};
use log::{debug, error, info, trace, warn};
use std::convert::TryInto;
use std::fs::File;
use std::io::{Cursor, Read};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use zstor_v2::compression::{Compressor, Snappy};
use zstor_v2::config::{Config, Meta};
use zstor_v2::encryption::{Encryptor, AESGCM};
use zstor_v2::erasure::{Encoder, Shard};
use zstor_v2::etcd::Etcd;
use zstor_v2::meta::{Checksum, MetaData, ShardInfo, CHECKSUM_LENGTH};
use zstor_v2::zdb::{Zdb, ZdbError, ZdbResult};
use zstor_v2::{ZstorError, ZstorErrorKind, ZstorResult};

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
        default_value = "config.toml",
        long,
        short,
        parse(from_os_str)
    )]
    config: std::path::PathBuf,
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
        /// Path to the file to store
        ///
        /// The path to the file to store. The path is used to create a metadata key (by hashing
        /// the full path). If a file is encoded at `path`, and then a new file is encoded for the
        /// same `path`. The old file metadata is overwritten and you will no longer be able to
        /// restore the file.
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: std::path::PathBuf,
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
        file: std::path::PathBuf,
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
        file: std::path::PathBuf,
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
        file: std::path::PathBuf,
    },
    /// Test the configuration and backends
    ///
    /// Tests if the configuration is valid, and all backends are available. Also makes sure the
    /// metadata storage is reachable. Validation of the configuration also includes making sure
    /// at least 1 distribution can be generated for writing files.
    Test,
}

use log::LevelFilter;
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::{Filter, Response};
const MIB: u64 = 1 << 20;

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

fn main() -> ZstorResult<()> {
    if let Err(e) = real_main() {
        error!("{}", e);
        return Err(e);
    }

    Ok(())
}

fn real_main() -> ZstorResult<()> {
    // init logger
    let policy = CompoundPolicy::new(
        Box::new(SizeTrigger::new(10 * MIB)),
        Box::new(
            FixedWindowRoller::builder()
                .build("./zstor.{}.log", 5)
                .unwrap(),
        ),
    );
    let log_file = RollingFileAppender::builder()
        .append(true)
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S %Z)(local)}: {l} {m}{n}",
        )))
        .build("./zstor.log", Box::new(policy))
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

    // construct an async runtime, do this manually so we can select the single threaded runtime.
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        // Realistically this should never happen. If it does happen, its fatal anyway.
        .expect("Could not build configure program runtime");

    rt.block_on(async {
        let opts = Opts::from_args();

        let cfg = read_cfg(&opts.config)?;

        // Get from config if not present
        let mut cluster = match cfg.meta() {
            Meta::ETCD(etcdconf) => Etcd::new(etcdconf, cfg.virtual_root().clone()).await?,
        };

        match opts.cmd {
            Cmd::Store { ref file } => {
                // start by canonicalizing the path
                let file = canonicalize_path(&file)?;
                trace!("encoding file {:?}", file);
                // make sure file is in root dir
                if let Some(ref root) = cfg.virtual_root() {
                    if !file.starts_with(root) {
                        return Err(ZstorError::new_io(
                            format!(
                            "attempting to store file which is not in the file tree rooted at {}",
                            root.to_string_lossy()),
                            std::io::Error::from(std::io::ErrorKind::InvalidData),
                        ));
                    }
                }

                if !std::fs::metadata(&file)
                    .map_err(|e| ZstorError::new_io("could not load file metadata".to_string(), e))?
                    .is_file()
                {
                    return Err(ZstorError::new_io(
                        "only files can be stored".to_string(),
                        std::io::Error::from(std::io::ErrorKind::InvalidData),
                    ));
                }

                let file_checksum = checksum(&file)?;
                debug!("file checksum: {}", hex::encode(file_checksum));

                // start reading file to encrypt
                trace!("loading file data");
                let mut encoding_file = File::open(&file).map_err(|e| {
                    ZstorError::new_io("could not open file to encode".to_string(), e)
                })?;

                let compressed = Vec::new();
                let mut cursor = Cursor::new(compressed);
                let original_size = Snappy.compress(&mut encoding_file, &mut cursor)?;
                let compressed = cursor.into_inner();
                trace!("compressed size: {} bytes", original_size);

                let encryptor = AESGCM::new(cfg.encryption().key().clone());
                let encrypted = encryptor.encrypt(&compressed)?;
                trace!("encrypted size: {} bytes", encrypted.len());

                let metadata = store_data(encrypted, file_checksum, &cfg).await?;
                cluster.save_meta(&file, &metadata).await?;
                info!(
                    "Stored file {:?} ({} bytes) to {}",
                    file.as_path(),
                    original_size,
                    metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
            Cmd::Retrieve { ref file } => {
                let metadata = cluster.load_meta(file).await?.ok_or_else(|| {
                    ZstorError::new_io(
                        "no metadata found for file".to_string(),
                        std::io::Error::from(std::io::ErrorKind::NotFound),
                    )
                })?;
                let decoded = recover_data(&metadata).await?;

                let encryptor = AESGCM::new(metadata.encryption().key().clone());
                let decrypted = encryptor.decrypt(&decoded)?;

                // create the file
                let mut out = if let Some(ref root) = cfg.virtual_root() {
                    File::create(root.join(&file))
                } else {
                    File::create(file)
                }
                .map_err(|e| ZstorError::new_io("could not create output file".to_string(), e))?;

                let mut cursor = Cursor::new(decrypted);
                Snappy.decompress(&mut cursor, &mut out)?;

                // get file size
                let file_size = if let Ok(meta) = out.metadata() {
                    Some(meta.len())
                } else {
                    // TODO: is this possible?
                    None
                };

                if !std::fs::metadata(&file)
                    .map_err(|e| ZstorError::new_io("could not load file metadata".to_string(), e))?
                    .is_file()
                {
                    return Err(ZstorError::new_io(
                        "only files can be stored".to_string(),
                        std::io::Error::from(std::io::ErrorKind::InvalidData),
                    ));
                }

                info!(
                    "Recovered file {:?} ({} bytes) from {}",
                    if let Some(ref root) = cfg.virtual_root() {
                        root.join(&file)
                    } else {
                        file.clone()
                    },
                    if let Some(size) = file_size {
                        size.to_string()
                    } else {
                        "unknown".to_string()
                    },
                    metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
            Cmd::Rebuild { ref file } => {
                let old_metadata = cluster.load_meta(file).await?.ok_or_else(|| {
                    ZstorError::new_io(
                        "no metadata found for file".to_string(),
                        std::io::Error::from(std::io::ErrorKind::NotFound),
                    )
                })?;
                let decoded = recover_data(&old_metadata).await?;

                let metadata = store_data(decoded, *old_metadata.checksum(), &cfg).await?;
                cluster.save_meta(&file, &metadata).await?;
                info!(
                    "Rebuild file from {} to {}",
                    old_metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    metadata
                        .shards()
                        .iter()
                        .map(|si| si.zdb().address().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
            Cmd::Check { ref file } => {
                match cluster.load_meta(file).await? {
                    Some(metadata) => {
                        let file = canonicalize_path(&file)?;
                        // strip the virtual_root, if one is set
                        let actual_path = if let Some(ref virtual_root) = cfg.virtual_root() {
                            file.strip_prefix(virtual_root).map_err(|_| {
                                ZstorError::new_io(
                                    format!(
                                        "path prefix {} not found",
                                        virtual_root.as_path().to_string_lossy()
                                    ),
                                    std::io::Error::from(std::io::ErrorKind::NotFound),
                                )
                            })?
                        } else {
                            file.as_path()
                        };
                        println!(
                            "{}\t{}",
                            hex::encode(metadata.checksum()),
                            actual_path.to_string_lossy()
                        );
                    }
                    None => std::process::exit(1),
                };
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
                        .map(|ci| tokio::spawn(async move { Zdb::new(ci).await })),
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
        };

        Ok(())
    })
}

async fn recover_data(metadata: &MetaData) -> ZstorResult<Vec<u8>> {
    // attempt to retrieve all shards
    let mut shard_loads: Vec<JoinHandle<(usize, Result<(_, _), ZstorError>)>> =
        Vec::with_capacity(metadata.shards().len());
    for si in metadata.shards().iter().cloned() {
        shard_loads.push(tokio::spawn(async move {
            let mut db = match Zdb::new(si.zdb().clone()).await {
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
                let mut db = Zdb::new(backend.clone()).await?;
                // check space in backend
                let ns_info = db.ns_info().await?;
                match ns_info.free_space() {
                    insufficient if insufficient < shard_len => Err(ZdbError::new_storage_size(
                        *db.connection_info().address(),
                        shard_len,
                        ns_info.free_space(),
                    )),
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
        cfg.encryption().clone(),
        cfg.compression().clone(),
    );

    let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(shards.len());
    for (mut db, (shard_idx, shard)) in dbs.into_iter().zip(shards.into_iter().enumerate()) {
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

fn read_cfg(config: &std::path::PathBuf) -> ZstorResult<Config> {
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
fn canonicalize_path(path: &std::path::PathBuf) -> ZstorResult<std::path::PathBuf> {
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
                std::fs::File::create(path)
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
fn checksum(file: &std::path::PathBuf) -> ZstorResult<Checksum> {
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
