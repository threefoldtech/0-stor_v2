use crate::{
    compression::{Compressor, Snappy},
    config::Config,
    encryption,
    erasure::{Encoder, Shard},
    meta::{Checksum, MetaData, CHECKSUM_LENGTH},
    ZstorError, ZstorResult,
};
use actix::prelude::*;
use blake2::{digest::VariableOutput, VarBlake2b};
use log::{debug, info, trace};
use std::{
    convert::TryInto,
    fs::File,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Message)]
#[rtype(result = "Result<(MetaData, PathBuf, Vec<Shard>), ZstorError>")]
/// Required info to process a file for storage.
pub struct StoreFile {
    /// Path to the file to store.
    pub file: PathBuf,
    /// Optional different path to use when computing the key. If set, the key is generated as if
    /// the file is saved in this path.
    pub key_path: Option<PathBuf>,
    /// Config to use.
    pub cfg: Arc<Config>,
}

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Required info to recover a file from its raw parts as they are stored.
pub struct RecoverFile {
    /// The path to place the recovered file.
    pub path: PathBuf,
    /// The data shards retrieved from the backend
    pub shards: Vec<Option<Vec<u8>>>,
    /// Config to use.
    pub cfg: Arc<Config>,
    /// Metadata associated with the file, generated during the upload.
    pub meta: MetaData,
}

#[derive(Message)]
#[rtype(result = "Result<(MetaData, Vec<Shard>), ZstorError>")]
/// Required info to rebuild file data from its raw parts as they are stored, to store them again so
/// all parts are available again
pub struct RebuildData {
    /// The original data which was recovered.
    pub input: Vec<Option<Vec<u8>>>,
    /// Config to use.
    pub cfg: Arc<Config>,
    /// Metadata associated with the file, generated during the upload.
    pub input_meta: MetaData,
}

/// The actor implementation of the pipeline
pub struct PipelineActor;

impl Actor for PipelineActor {
    type Context = SyncContext<Self>;
}

impl Handler<StoreFile> for PipelineActor {
    type Result = Result<(MetaData, PathBuf, Vec<Shard>), ZstorError>;

    fn handle(&mut self, msg: StoreFile, _: &mut Self::Context) -> Self::Result {
        let (data_file_path, mut key_dir_path) = match msg.key_path {
            Some(kp) => (&msg.file, kp),
            None => {
                let mut key_dir = msg.file.clone();
                if !key_dir.pop() {
                    return Err(ZstorError::new_io(
                        "Could not remove file name to get parent dir name".to_string(),
                        std::io::Error::from(std::io::ErrorKind::InvalidData),
                    ));
                }
                (&msg.file, key_dir)
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
        if let Some(ref root) = msg.cfg.virtual_root() {
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

        let (meta, shards) = process_file(&data_file_path, &msg.cfg)?;

        Ok((meta, key_file_path, shards))
    }
}

impl Handler<RecoverFile> for PipelineActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: RecoverFile, _: &mut Self::Context) -> Self::Result {
        let encoder = Encoder::new(msg.meta.data_shards(), msg.meta.parity_shards());
        let decoded = encoder.decode(msg.shards)?;

        info!("rebuild data from shards");

        let encryptor = encryption::new(msg.meta.encryption().clone().into());
        let decrypted = encryptor.decrypt(&decoded)?;

        // create the file
        let mut out = if let Some(ref root) = msg.cfg.virtual_root() {
            File::create(root.join(&msg.path))
        } else {
            File::create(&msg.path)
        }
        .map_err(|e| ZstorError::new_io("could not create output file".to_string(), e))?;

        let mut cursor = Cursor::new(decrypted);
        Snappy.decompress(&mut cursor, &mut out)?;

        // TODO: whats this supposed to do???
        // if !std::fs::metadata(&msg.path)
        //     .map_err(|e| ZstorError::new_io("could not load file metadata".to_string(), e))?
        //     .is_file()
        // {
        //     return Err(ZstorError::new_io(
        //         "only files can be stored".to_string(),
        //         std::io::Error::from(std::io::ErrorKind::InvalidData),
        //     ));
        // }

        Ok(())
    }
}

impl Handler<RebuildData> for PipelineActor {
    type Result = Result<(MetaData, Vec<Shard>), ZstorError>;

    fn handle(&mut self, msg: RebuildData, _: &mut Self::Context) -> Self::Result {
        let encoder = Encoder::new(msg.input_meta.data_shards(), msg.input_meta.parity_shards());
        let decoded = encoder.decode(msg.input)?;

        let encoder = Encoder::new(msg.cfg.data_shards(), msg.cfg.parity_shards());
        Ok((
            MetaData::new(
                msg.input_meta.data_shards(),
                msg.input_meta.parity_shards(),
                *msg.input_meta.checksum(),
                msg.input_meta.encryption().clone(),
                msg.input_meta.compression().clone(),
            ),
            encoder.encode(decoded),
        ))
    }
}

/// Actual processing of the file
fn process_file(data_file: &Path, cfg: &Config) -> ZstorResult<(MetaData, Vec<Shard>)> {
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

    let encoder = Encoder::new(cfg.data_shards(), cfg.parity_shards());
    let shards = encoder.encode(encrypted);

    let metadata = MetaData::new(
        cfg.data_shards(),
        cfg.parity_shards(),
        file_checksum,
        cfg.encryption().clone().into(),
        cfg.compression().clone().into(),
    );

    Ok((metadata, shards))
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
