use std::fmt;
use std::io;
use std::io::copy;

/// Result type for compressor operations.
pub type CompressorResult<T> = Result<T, CompressorError>;

/// A compression unit allows compressing and later decompressing data.
pub trait Compressor {
    /// Compress the given input buffer, returning the amount of bytes compressed from the input.
    fn compress(
        &self,
        input: &mut dyn io::Read,
        output: &mut dyn io::Write,
    ) -> CompressorResult<u64>;
    /// Decompress the given input buffer, returning the amount of decompressed bytes.
    fn decompress(
        &self,
        input: &mut dyn io::Read,
        output: &mut dyn io::Write,
    ) -> CompressorResult<u64>;
}

/// A compressor implementing the snappy algorithm
#[derive(Debug, Clone)]
pub struct Snappy;

impl Compressor for Snappy {
    fn compress(
        &self,
        mut input: &mut dyn io::Read,
        output: &mut dyn io::Write,
    ) -> CompressorResult<u64> {
        let mut wtr = snap::write::FrameEncoder::new(output);

        let total = copy(&mut input, &mut wtr).map_err(|e| CompressorError {
            kind: CompressorErrorKind::Compress,
            internal: e,
        })?;

        Ok(total)
    }

    fn decompress(
        &self,
        input: &mut dyn io::Read,
        mut output: &mut dyn io::Write,
    ) -> CompressorResult<u64> {
        let mut rdr = snap::read::FrameDecoder::new(input);

        let total = copy(&mut rdr, &mut output).map_err(|e| CompressorError {
            kind: CompressorErrorKind::Decompress,
            internal: e,
        })?;

        Ok(total)
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
    use std::io::Cursor;

    #[test]
    fn snappy_roundtrip() {
        let data = rand::thread_rng().gen::<[u8; 16]>();
        let mut src = Cursor::new(data);
        let mut dst = Cursor::new(Vec::new());

        let comp = Snappy;
        let res = comp.compress(&mut src, &mut dst);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 16);

        let compressed_data = dst.into_inner();

        let mut src = Cursor::new(compressed_data);
        let mut dst = Cursor::new(Vec::new());

        let res = comp.decompress(&mut src, &mut dst);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 16);

        let orig = dst.into_inner();

        assert_eq!(orig.len(), 16);

        assert_eq!(&orig, &data);
    }
}
