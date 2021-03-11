use blake2::{digest::VariableOutput, VarBlake2b};
use futures::future::{join_all, try_join_all};
use log::{debug, info, trace};
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Write};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use zstor_v2::compression::{Compressor, Snappy};
use zstor_v2::config::{Config, Meta};
use zstor_v2::encryption::{Encryptor, AESGCM};
use zstor_v2::erasure::Encoder;
use zstor_v2::etcd::Etcd;
use zstor_v2::meta::{Checksum, MetaData, ShardInfo, CHECKSUM_LENGTH};
use zstor_v2::zdb::Zdb;
use zstor_v2::{ZstorError, ZstorErrorKind, ZstorResult};

#[derive(StructOpt, Debug)]
#[structopt(about = "xstor data encoder")]
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
    /// checksum is printed, as well as the resolved path
    Check {
        /// Path of the file to check.
        ///
        /// The original path which was used to store the file.
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: std::path::PathBuf,
    },
}

fn main() -> ZstorResult<()> {
    // construct an async runtime, do this manually so we can select the single threaded runtime.
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        // Realistically this should never happen. If it does happen, its fatal anyway.
        .expect("Could not build configure program runtime");

    rt.block_on(async {
        let opts = Opts::from_args();
        pretty_env_logger::init();

        let cfg = read_cfg(&opts.config)?;

        // Get from config if not present
        let cluster = match cfg.meta() {
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
                let mut buffer = Vec::new();
                encoding_file
                    .read_to_end(&mut buffer)
                    .map_err(|e| ZstorError::new_io("".to_string(), e))?;
                trace!("loaded {} bytes of data", buffer.len());

                let compressor = Snappy;
                let compressed = compressor.compress(&buffer)?;
                trace!("compressed size: {} bytes", compressed.len());

                let encryptor = AESGCM::new(cfg.encryption().key().clone());
                let encrypted = encryptor.encrypt(&compressed)?;
                trace!("encrypted size: {} bytes", encrypted.len());

                let metadata = store_data(encrypted, file_checksum, &cfg).await?;
                cluster.save_meta(&file, &metadata).await?;
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

                let original = Snappy.decompress(&decrypted)?;

                // create the file
                // Ideally we would do an if let on just the argument to create, but due to
                // lifetimes that is not possible.
                let mut out = if let Some(ref root) = cfg.virtual_root() {
                    File::create(root.join(&file))
                } else {
                    File::create(file)
                }
                .map_err(|e| ZstorError::new_io("could not create output file".to_string(), e))?;

                out.write_all(&original).map_err(|e| {
                    ZstorError::new_io("could not write data to output file".to_string(), e)
                })?;
            }
            Cmd::Rebuild { ref file } => {
                let metadata = cluster.load_meta(file).await?.ok_or_else(|| {
                    ZstorError::new_io(
                        "no metadata found for file".to_string(),
                        std::io::Error::from(std::io::ErrorKind::NotFound),
                    )
                })?;
                let decoded = recover_data(&metadata).await?;

                let metadata = store_data(decoded, *metadata.checksum(), &cfg).await?;
                cluster.save_meta(&file, &metadata).await?;
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
        };

        Ok(())
    })
}

async fn recover_data(metadata: &MetaData) -> ZstorResult<Vec<u8>> {
    // attempt to retrieve all shards
    let mut shard_loads: Vec<JoinHandle<(usize, Result<_, ZstorError>)>> =
        Vec::with_capacity(metadata.shards().len());
    for si in metadata.shards().iter().cloned() {
        shard_loads.push(tokio::spawn(async move {
            let mut db = match Zdb::new(si.zdb().clone()).await {
                Ok(ok) => ok,
                Err(e) => return (si.index(), Err(e.into())),
            };
            match db.get(si.key()).await {
                Ok(potential_shard) => match potential_shard {
                    Some(shard) => (si.index(), Ok(shard)),
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

    let mut indexed_shards: Vec<(usize, Option<Vec<u8>>)> = Vec::with_capacity(shard_loads.len());
    for shard_info in join_all(shard_loads).await {
        let (idx, shard) = shard_info?;
        indexed_shards.push((idx, shard.ok())); // don't really care about errors here
    }

    // sort the shards
    indexed_shards.sort_by(|(a, _), (b, _)| a.cmp(b));

    let shards = indexed_shards.into_iter().map(|(_, shard)| shard).collect();

    let encoder = Encoder::new(metadata.data_shards(), metadata.parity_shards());
    let decoded = encoder.decode(shards)?;

    info!("rebuild data from shards");

    Ok(decoded)
}

async fn store_data(data: Vec<u8>, checksum: Checksum, cfg: &Config) -> ZstorResult<MetaData> {
    let encoder = Encoder::new(cfg.data_shards(), cfg.parity_shards());
    let shards = encoder.encode(data);
    debug!("data encoded");

    let backends = cfg.shard_stores()?;

    trace!("store shards in backends");
    let mut handles: Vec<JoinHandle<ZstorResult<_>>> = Vec::with_capacity(shards.len());

    for (backend, (shard_idx, shard)) in backends.into_iter().zip(shards.into_iter().enumerate()) {
        handles.push(tokio::spawn(async move {
            let mut db = Zdb::new(backend.clone()).await?;
            let keys = db.set(&shard).await?;
            Ok(ShardInfo::new(
                shard_idx,
                shard.checksum(),
                keys,
                backend.clone(),
            ))
        }));
    }

    let mut metadata = MetaData::new(
        cfg.data_shards(),
        cfg.parity_shards(),
        checksum,
        cfg.encryption().clone(),
        cfg.compression().clone(),
    );

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
