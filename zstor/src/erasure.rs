use crate::meta::{Checksum, CHECKSUM_LENGTH};
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use log::trace;
use reed_solomon_erasure::{galois_8::ReedSolomon, Error as RsError};
use std::convert::TryInto;
use std::fmt;
use std::ops::{Deref, DerefMut};

/// Result type for erasur operations.
pub type ErasureResult<T> = Result<T, EncodingError>;

/// A data encoder is responsible for encoding original data into multiple shards, and decoding
/// multiple shards back to the original data, if sufficient shards are available.
#[derive(Debug)]
pub struct Encoder {
    data_shards: usize,
    parity_shards: usize,
}

/// A data shard resulting from encoding data
#[derive(Debug, Clone)]
pub struct Shard(Vec<u8>);

impl Encoder {
    /// Create a new encoder. There can be at most 255 data shards.
    ///
    /// # Panics
    ///
    /// Panics when attempting to create an encoder with more than 255 data shards.
    ///
    /// Panics when attempting to create an encoder with 0 data shards.
    pub fn new(data_shards: usize, parity_shards: usize) -> Self {
        trace!(
            "creating new encoder, data shards {}, parity shards {}",
            data_shards,
            parity_shards
        );
        assert!(data_shards < 256);
        Encoder {
            data_shards,
            parity_shards,
        }
    }

    /// Erasure encode data using ReedSolomon encoding over the galois 8 field. This returns the
    /// shards created by the encoding. The order of the shards is important to later retrieve
    /// the values
    // TODO: we can improve this by making a special type which has an ARC to a single vec holding
    // all the data from all the shards, and start and end indexes per struct. This would also
    // allow to efficiently set the shard_idx in the shard without a realloc later
    pub fn encode(&self, mut data: Vec<u8>) -> Vec<Shard> {
        trace!("encoding data ({} bytes)", data.len());
        // pkcs7 padding
        let padding_len = self.data_shards - data.len() % self.data_shards;
        trace!("adding {} bytes of padding", padding_len);
        // Padding len is necessarily smaller than 256 here (and thus fits in a single byte) due to
        // the constraints on the amount of data shards when a new encoder is created.
        data.extend_from_slice(&vec![padding_len as u8; padding_len]);
        // data shards
        let mut shards: Vec<Shard> = data
            .chunks_exact(data.len() / self.data_shards) // we already padded so this is always exact
            .map(|chunk| Shard::from(Vec::from(chunk)))
            .collect();
        // add parity shards
        // NOTE: we don't need to do a float division with ceiling, since integer division
        // essentially always rounds down, and we always add padding
        trace!("preparing parity shards");
        shards.extend(vec![
            Shard::from(vec![0; data.len() / self.data_shards]); // data is padded so this is a perfect division
            self.parity_shards
        ]);

        // encode
        trace!("start encoding shards");
        let enc = ReedSolomon::new(self.data_shards, self.parity_shards).unwrap();
        enc.encode(&mut shards).unwrap();

        shards
    }

    /// Decode shards previously obtained by encoding data. The exact output configuration of the
    /// encode stage is required. That is, the same amount of shards must be provided, and ordering
    /// is important. If a shard is not available or otherwise corrupted, it can be marked as
    /// missing. As long as sufficient shards (at least the amount of specified data shards) are
    /// available, the input can be recovered.
    pub fn decode(&self, mut shards: Vec<Option<Vec<u8>>>) -> ErasureResult<Vec<u8>> {
        trace!("decoding data");
        // reconstruct all shards
        let dec = ReedSolomon::new(self.data_shards, self.parity_shards).unwrap();
        dec.reconstruct(&mut shards).map_err(|e| EncodingError {
            kind: EncodingErrorKind::Reconstruct,
            internal: e,
        })?;

        // rebuild data
        let shard_len = if let Some(ref shard) = shards[0] {
            shard.len()
        } else {
            // encoding succeeded so this is impossible
            unreachable!()
        };

        trace!("data reconstructed, shard length {} bytes", shard_len);

        // rebuild original data
        let mut data = Vec::with_capacity(shard_len * self.data_shards);
        for shard in shards.into_iter().take(self.data_shards) {
            // unwrap here is safe since encoding succeeded.
            data.extend_from_slice(&shard.unwrap());
        }

        trace!("strip padding");
        // pkcs7 padding
        let padding_len = data[data.len() - 1];
        data.resize(data.len() - padding_len as usize, 0);

        trace!("original data length: {} bytes", data.len());

        Ok(data)
    }
}

impl From<Vec<u8>> for Shard {
    fn from(data: Vec<u8>) -> Self {
        Shard(data)
    }
}

impl Deref for Shard {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Shard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<[u8]> for Shard {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for Shard {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Shard {
    /// Create a new shard from some data
    pub fn new(data: Vec<u8>) -> Self {
        Shard(data)
    }

    /// Generate a checksum for the data in the shard
    pub fn checksum(&self) -> Checksum {
        let mut hasher = VarBlake2b::new(CHECKSUM_LENGTH).unwrap();
        hasher.update(&self.0);

        // expect is safe due to the static size, which is known to be valid
        hasher
            .finalize_boxed()
            .as_ref()
            .try_into()
            .expect("Invalid hash size returned")
    }

    /// Cosume the shard, returning the actual data
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

/// An error related to encoding or decoding data
#[derive(Debug)]
pub struct EncodingError {
    kind: EncodingErrorKind,
    internal: RsError,
}

impl fmt::Display for EncodingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error in encoder/decoder: {}, {}",
            self.kind, self.internal
        )
    }
}

impl std::error::Error for EncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.internal)
    }
}

/// Specific error type for the encoding.
#[derive(Debug)]
pub enum EncodingErrorKind {
    /// Error while reconstructing data.
    Reconstruct,
}

impl fmt::Display for EncodingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Operation: {}",
            match self {
                EncodingErrorKind::Reconstruct => "RECONSTRUCT",
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Encoder;
    use rand::{prelude::SliceRandom, Rng};
    use simple_logger::SimpleLogger;

    #[test]
    fn roundtrip() {
        SimpleLogger::new().init().unwrap();

        const DATA_SHARDS: usize = 11;
        const PARITY_SHARDS: usize = 7;

        let mut data: Vec<u8> = vec![3; 3_333_333];
        for d in data.iter_mut() {
            *d = rand::thread_rng().gen();
        }

        let encoder = Encoder::new(DATA_SHARDS, PARITY_SHARDS);
        let shards = encoder.encode(data.clone()); // clone data so we can compare later

        assert_eq!(shards.len(), DATA_SHARDS + PARITY_SHARDS);

        let mut shard_to_erase = Vec::with_capacity(DATA_SHARDS + PARITY_SHARDS);
        for i in 0..shard_to_erase.capacity() {
            shard_to_erase.push(i);
        }
        shard_to_erase.shuffle(&mut rand::thread_rng());

        let recovered: Vec<Option<Vec<u8>>> = shards
            .into_iter()
            .map(|shard| Some(shard.into_inner()))
            .collect();

        let orig_res = encoder.decode(recovered);

        assert!(orig_res.is_ok());

        let orig = orig_res.unwrap();

        assert_eq!(orig.len(), data.len());
        assert_eq!(orig, data);
    }
}
