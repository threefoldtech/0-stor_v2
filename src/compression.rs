use std::io::{copy, Cursor};

/// A compression unit allows compressing and later decompressing data.
pub trait Compressor {
    /// Compress the given input buffer, returning the compressed data.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String>;
    /// Decompress the given input buffer, returning the decompressed data.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String>;
}

/// A compressor implementing the snappy algorithm
#[derive(Debug, Clone)]
pub struct Snappy;

impl Compressor for Snappy {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut input = Cursor::new(data);
        let out = Cursor::new(Vec::new());

        let mut wtr = snap::write::FrameEncoder::new(out);

        copy(&mut input, &mut wtr).map_err(|e| format!("could not compress data: {}", e))?;

        Ok(wtr
            .into_inner()
            .map_err(|e| format!("could not recover output buffer: {}", e))?
            .into_inner())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let input = Cursor::new(data);
        let mut out = Cursor::new(Vec::new());

        let mut rdr = snap::read::FrameDecoder::new(input);

        copy(&mut rdr, &mut out).map_err(|e| format!("could not compress data: {}", e))?;

        Ok(out.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::{Compressor, Snappy};
    use rand::Rng;

    #[test]
    fn snappy_roundtrip() {
        let data = rand::thread_rng().gen::<[u8; 16]>();

        let comp = Snappy;
        let comp_res = comp.compress(&data);
        assert!(comp_res.is_ok());

        let res = comp_res.unwrap();

        let orig_res = comp.decompress(&res);
        assert!(orig_res.is_ok());

        let orig = orig_res.unwrap();

        assert_eq!(&orig, &data);
    }
}
