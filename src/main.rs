use futures::future::try_join_all;
use log::{debug, trace};
use rand::{prelude::SliceRandom, Rng};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use zstor_v2::compression::{Compressor, Snappy};
use zstor_v2::config::Config;
use zstor_v2::encryption::{Encryptor, AESGCM};
use zstor_v2::erasure::Encoder;
use zstor_v2::meta::{MetaData, ShardInfo};
use zstor_v2::zdb::Zdb;

#[derive(StructOpt, Debug)]
#[structopt(about = "rstor data encoder")]
/// Rstor data encoder
///
/// Compresses, encrypts, and erasure codes data according to the provided config file and options.
/// Data is send to a specified group of backends.
struct Rstor {
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
        /// Path to the config file to use for this invocation.
        #[structopt(
            name = "config",
            default_value = "config.toml",
            long,
            short,
            parse(from_os_str)
        )]
        config: std::path::PathBuf,
        #[structopt(name = "file", long, short, parse(from_os_str))]
        file: std::path::PathBuf,
    },
    /// Rebuild already stored data
    ///
    /// The data itself is recreated from the (available) shards, and
    /// re-encoded and redistributed over the backends in the current config file. This operation
    /// will fail if insufficient backends are available.
    Rebuild {
        /// Path to the config file to use for this invocation.
        #[structopt(
            name = "config",
            default_value = "config.toml",
            long,
            short,
            parse(from_os_str)
        )]
        config: std::path::PathBuf,
    },
    /// Load encoded data
    ///
    /// Loads data from available shards, restores it, decrypts and decompresses it. This operation
    /// will fail if insufficient shards are available to retrieve the data.
    Retrieve {},
}

fn main() -> Result<(), String> {
    // construct an async runtime, do this manually so we can select the single threaded runtime.
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;

    rt.block_on(async {
        let mut opts = Rstor::from_args();
        simple_logger::SimpleLogger::new().init().unwrap();

        match opts.cmd {
            Cmd::Store {
                ref config,
                ref mut file,
            } => {
                // TODO: check that `file` points to file and not a dir
                trace!("encoding file {:?}", file);
                trace!("opening config file {:?}", config);
                let mut cfg_file = File::open(config).map_err(|e| e.to_string())?;
                let mut cfg_str = String::new();
                cfg_file
                    .read_to_string(&mut cfg_str)
                    .map_err(|e| e.to_string())?;

                let cfg: Config = toml::from_str(&cfg_str).map_err(|e| e.to_string())?;
                trace!("config read");
                if let Err(e) = cfg.validate() {
                    return Err(e);
                }
                trace!("config validated");

                // start reading file to encrypt
                // construct full path for meta storeage later
                let path = fs::canonicalize(&file).map_err(|e| e.to_string())?;
                trace!("full path {:?}", path);

                trace!("loading file data");
                let mut encoding_file = File::open(path).map_err(|e| e.to_string())?;
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

                let encoder = Encoder::new(cfg.data_shards(), cfg.parity_shards());
                let shards = encoder.encode(encrypted);
                debug!("data encoded");

                let backends = cfg.shard_stores()?;

                trace!("store shards in backends");
                let mut handles: Vec<JoinHandle<Result<_, String>>> =
                    Vec::with_capacity(shards.len());

                for (backend, (shard_idx, shard)) in
                    backends.into_iter().zip(shards.into_iter().enumerate())
                {
                    handles.push(tokio::spawn(async move {
                        let mut db = Zdb::new(backend.clone()).await?;
                        let key = db.set(None, &shard).await?;
                        Ok(ShardInfo::new(shard_idx, key, backend.clone()))
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

                let filename = file
                    .file_name()
                    .ok_or("could not load file name".to_string())?
                    .to_str()
                    .ok_or("could not convert filename to standard string".to_string())?;
                let metaname = format!("{}.meta", filename);
                file.set_file_name(metaname);

                let mut metafile = File::create(&file).map_err(|e| e.to_string())?;
                metafile
                    .write_all(&toml::to_vec(&metadata).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;

                debug!("{}", toml::to_string(&metadata).unwrap());
            }
            _ => unimplemented!(),
        };

        println!("{:#?}", opts);
        const DATA_SHARDS: usize = 20;
        const PARITY_SHARDS: usize = 15;
        let mut data: Vec<u8> = vec![0; 3_000_000_000];
        for d in data.iter_mut() {
            *d = rand::thread_rng().gen();
        }

        let enc = Encoder::new(DATA_SHARDS, PARITY_SHARDS);

        let packets = enc.encode(data.clone());

        let mut shard_to_erase = Vec::with_capacity(DATA_SHARDS + PARITY_SHARDS);
        for i in 0..shard_to_erase.capacity() {
            shard_to_erase.push(i);
        }
        shard_to_erase.shuffle(&mut rand::thread_rng());

        let mut recovered: Vec<Option<Vec<u8>>> = packets.into_iter().map(Some).collect();

        for idx in &shard_to_erase[..PARITY_SHARDS] {
            recovered[*idx] = None;
        }

        let orig = enc.decode(recovered).unwrap();

        assert_eq!(orig, data);

        Ok(())
    })
}
