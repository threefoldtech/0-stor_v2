use crate::{compression::Compressor, encryption::Encryptor, erasure::Encoder};
use std::fmt::Debug;

/// The main data pipeline, this allows to go from raw data to the processed data ready to be
/// stored in the backend.
pub struct PipeLine<C, E> {
    compressor: C,
    encryptor: E,
    encoder: Encoder,
}

impl<C, E> PipeLine<C, E>
where
    C: Compressor + Send + Sync + Debug,
    E: Encryptor + Send + Sync + Debug,
{
    /// Create a new pipeline from the given components
    pub fn new(compressor: C, encryptor: E, encoder: Encoder) -> PipeLine<C, E> {
        Self {
            compressor,
            encryptor,
            encoder,
        }
    }

    /// Consumes the pipeline and returns the components it is made of
    pub fn into_parts(self) -> (C, E, Encoder) {
        (self.compressor, self.encryptor, self.encoder)
    }
}
