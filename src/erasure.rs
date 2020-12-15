use log::trace;
use reed_solomon_erasure::galois_8::ReedSolomon;

/// A data encoder is responsible for encoding original data into multiple shards, and decoding
/// multiple shards back to the original data, if sufficient shards are available.
#[derive(Debug)]
pub struct Encoder {
    data_shards: usize,
    parity_shards: usize,
}

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
    pub fn encode(&self, mut data: Vec<u8>) -> Vec<Vec<u8>> {
        trace!("encoding data ({} bytes)", data.len());
        // pkcs7 padding
        let padding_len = self.data_shards - data.len() % self.data_shards;
        trace!("adding {} bytes of padding", padding_len);
        // Padding len is necessarily smaller than 256 here (and thus fits in a single byte) due to
        // the constraints on the amount of data shards when a new encoder is created.
        data.extend_from_slice(&vec![padding_len as u8; padding_len]);
        // data shards
        let mut shards: Vec<Vec<u8>> = data
            .chunks_exact(data.len() / self.data_shards) // we already padded so this is always exact
            .map(Vec::from)
            .collect();
        // add parity shards
        // NOTE: we don't need to do a float division with ceiling, since integer division
        // essentially always rounds down, and we always add padding
        trace!("preparing parity shards");
        shards.extend(vec![
            vec![0; data.len() / self.data_shards]; // data is padded so this is a perfect division
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
    pub fn decode(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>, String> {
        trace!("decoding data");
        // reconstruct all shards
        let dec = ReedSolomon::new(self.data_shards, self.parity_shards).unwrap();
        dec.reconstruct(&mut shards)
            .map_err(|e| format!("could not reconstruct shards: {}", e))?;

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

        let recovered: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();

        let orig_res = encoder.decode(recovered);

        assert!(orig_res.is_ok());

        let orig = orig_res.unwrap();

        assert_eq!(orig.len(), data.len());
        assert_eq!(orig, data);
    }
}
