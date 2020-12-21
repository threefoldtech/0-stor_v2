use futures::future::{join_all, try_join_all};
use log::{debug, info, trace};
use std::fs::File;
use std::io::{Read, Write};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio_compat_02::FutureExt;
use zstor_v2::compression::{Compressor, Snappy};
use zstor_v2::config::{Config, Meta};
use zstor_v2::encryption::{Encryptor, AESGCM};
use zstor_v2::erasure::Encoder;
use zstor_v2::etcd::Etcd;
use zstor_v2::meta::{MetaData, ShardInfo};
use zstor_v2::zdb::Zdb;

#[derive(StructOpt, Debug)]
#[structopt(about = "rstor data encoder")]
/// Rstor data encoder
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
}

fn main() -> Result<(), String> {
    // construct an async runtime, do this manually so we can select the single threaded runtime.
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;

    rt.block_on(async {
        let opts = Opts::from_args();
        pretty_env_logger::init();

        let cfg = read_cfg(&opts.config)?;

        // Get from config if not present
        let cluster = match cfg.meta() {
            Meta::ETCD(etcdconf) => {
                Etcd::new(etcdconf, cfg.virtual_root().clone())
                    .compat()
                    .await?
            }
        };

        match opts.cmd {
            Cmd::Store { ref file } => {
                // start by canonicalizing the path
                let file = canonicalize_path(&file)?;
                // make sure file is in root dir
                if let Some(ref root) = cfg.virtual_root() {
                    if !file.starts_with(root) {
                        return Err(format!(
                            "attempting to store file which is not in the file tree rooted at {}",
                            root.to_string_lossy()
                        ));
                    }
                }

                if !std::fs::metadata(&file)
                    .map_err(|e| e.to_string())?
                    .is_file()
                {
                    return Err("only files can be stored".to_string());
                }
                trace!("encoding file {:?}", file);

                // start reading file to encrypt
                trace!("loading file data");
                let mut encoding_file = File::open(&file).map_err(|e| e.to_string())?;
                let mut buffer = Vec::new();
                encoding_file
                    .read_to_end(&mut buffer)
                    .map_err(|e| e.to_string())?;
                trace!("loaded {} bytes of data", buffer.len());

                let compressor = Snappy;
                let compressed = compressor.compress(&buffer)?;
                trace!("compressed size: {} bytes", compressed.len());

                let encryptor = AESGCM::new(cfg.encryption().key().clone());
                let encrypted = encryptor.encrypt(&compressed)?;
                trace!("encrypted size: {} bytes", encrypted.len());

                let metadata = store_data(encrypted, &cfg).await?;
                cluster.save_meta(&file, &metadata).compat().await?;
            }
            Cmd::Retrieve { ref file } => {
                let metadata = cluster.load_meta(file).compat().await?;
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
                .map_err(|e| e.to_string())?;

                out.write_all(&original).map_err(|e| e.to_string())?;
            }
            Cmd::Rebuild { ref file } => {
                let metadata = cluster.load_meta(file).compat().await?;
                let decoded = recover_data(&metadata).await?;

                let metadata = store_data(decoded, &cfg).await?;
                cluster.save_meta(&file, &metadata).compat().await?;
            }
        };

        Ok(())
    })
}

async fn recover_data(metadata: &MetaData) -> Result<Vec<u8>, String> {
    // attempt to retrieve al shards
    let mut shard_loads: Vec<JoinHandle<(usize, Result<_, String>)>> =
        Vec::with_capacity(metadata.shards().len());
    for si in metadata.shards().iter().cloned() {
        shard_loads.push(tokio::spawn(async move {
            let mut db = match Zdb::new(si.zdb().clone()).await {
                Ok(ok) => ok,
                Err(e) => return (si.index(), Err(e)),
            };
            match db.get(si.key()).await {
                Ok(potential_shard) => match potential_shard {
                    Some(shard) => (si.index(), Ok(shard)),
                    None => (si.index(), Err("shard not found".to_string())),
                },
                Err(e) => (si.index(), Err(e)),
            }
        }));
    }

    let mut indexed_shards: Vec<(usize, Option<Vec<u8>>)> = Vec::with_capacity(shard_loads.len());
    for shard_info in join_all(shard_loads).await {
        let (idx, shard) = shard_info.map_err(|e| e.to_string())?;
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

async fn store_data(data: Vec<u8>, cfg: &Config) -> Result<MetaData, String> {
    let encoder = Encoder::new(cfg.data_shards(), cfg.parity_shards());
    let shards = encoder.encode(data);
    debug!("data encoded");

    let backends = cfg.shard_stores()?;

    trace!("store shards in backends");
    let mut handles: Vec<JoinHandle<Result<_, String>>> = Vec::with_capacity(shards.len());

    for (backend, (shard_idx, shard)) in backends.into_iter().zip(shards.into_iter().enumerate()) {
        handles.push(tokio::spawn(async move {
            let mut db = Zdb::new(backend.clone()).await?;
            let keys = db.set(&shard).await?;
            Ok(ShardInfo::new(shard_idx, keys, backend.clone()))
        }));
    }

    let mut metadata = MetaData::new(
        cfg.data_shards(),
        cfg.parity_shards(),
        cfg.encryption().clone(),
        cfg.compression().clone(),
    );

    for shard_info in try_join_all(handles).await.map_err(|e| e.to_string())? {
        metadata.add_shard(shard_info?);
    }

    Ok(metadata)
}

fn read_cfg(config: &std::path::PathBuf) -> Result<Config, String> {
    trace!("opening config file {:?}", config);
    let mut cfg_file = File::open(config).map_err(|e| e.to_string())?;
    let mut cfg_str = String::new();
    cfg_file
        .read_to_string(&mut cfg_str)
        .map_err(|e| e.to_string())?;

    let cfg: Config = toml::from_str(&cfg_str).map_err(|e| e.to_string())?;
    trace!("config read");
    cfg.validate()?;
    trace!("config validated");
    Ok(cfg)
}

/// wrapper around the standard library method [`std::fs::canonicalize_path`]. This method will
/// work on files which don't exist by creating a dummy file if the file does not exist,
/// canonicalizing the path, and removing the dummy file.
fn canonicalize_path(path: &std::path::PathBuf) -> Result<std::path::PathBuf, String> {
    // annoyingly, the path needs to exist for this to work. So here's the plan:
    // first we verify that it is actualy there
    // if it is, no problem
    // else, create a temp file, canonicalize that path, and remove the temp file again
    Ok(match std::fs::metadata(path) {
        Ok(_) => path.canonicalize().map_err(|e| e.to_string())?,
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                std::fs::File::create(path)
                    .map_err(|e| format!("could not create temp file: {}", e))?;
                let cp = path.canonicalize().map_err(|e| e.to_string())?;
                std::fs::remove_file(path).map_err(|e| e.to_string())?;
                cp
            }
            _ => return Err(e.to_string()),
        },
    })
}
