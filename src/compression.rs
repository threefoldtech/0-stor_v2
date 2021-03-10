use std::fmt;
use std::io;
use std::io::{copy, Cursor};

/// Result type for compressor operations.
pub type CompressorResult<T> = Result<T, CompressorError>;

/// A compression unit allows compressing and later decompressing data.
pub trait Compressor {
    /// Compress the given input buffer, returning the compressed data.
    fn compress(&self, data: &[u8]) -> CompressorResult<Vec<u8>>;
    /// Decompress the given input buffer, returning the decompressed data.
    fn decompress(&self, data: &[u8]) -> CompressorResult<Vec<u8>>;
}

/// A compressor implementing the snappy algorithm
#[derive(Debug, Clone)]
pub struct Snappy;

impl Compressor for Snappy {
    fn compress(&self, data: &[u8]) -> CompressorResult<Vec<u8>> {
        let mut input = Cursor::new(data);
        let out = Cursor::new(Vec::new());

        let mut wtr = snap::write::FrameEncoder::new(out);

        copy(&mut input, &mut wtr).map_err(|e| CompressorError {
            kind: CompressorErrorKind::Compress,
            internal: e,
        })?;

        Ok(wtr
            .into_inner()
            .map_err(|e| CompressorError {
                kind: CompressorErrorKind::Compress,
                internal: io::Error::from(e.error().kind()), // TODO: this could be better
            })?
            .into_inner())
    }

    fn decompress(&self, data: &[u8]) -> CompressorResult<Vec<u8>> {
        let input = Cursor::new(data);
        let mut out = Cursor::new(Vec::new());

        let mut rdr = snap::read::FrameDecoder::new(input);

        copy(&mut rdr, &mut out).map_err(|e| CompressorError {
            kind: CompressorErrorKind::Decompress,
            internal: e,
        })?;

        Ok(out.into_inner())
    }
}

/// An error holding details about compression failure
#[derive(Debug)]
pub struct CompressorError {
    kind: CompressorErrorKind,
    internal: io::Error,
}

impl fmt::Display for CompressorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error in {}: {}", self.kind, self.internal)
    }
}

impl std::error::Error for CompressorError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        Some(&self.internal)
    }
}

/// The cause of a compression error
#[derive(Debug)]
pub enum CompressorErrorKind {
    /// Error while compressing
    Compress,
    /// Error while decompressing
    Decompress,
}

impl fmt::Display for CompressorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "operation {}",
            match self {
                CompressorErrorKind::Compress => "COMPRESS",
                CompressorErrorKind::Decompress => "DECOMPRESS",
            }
        )
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
