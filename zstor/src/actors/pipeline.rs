use crate::ZstorError;
use actix::prelude::*;
use std::path::PathBuf;

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Required info to process a file for storage.
pub struct StoreFile {
    /// The path of the file to store
    pub path: PathBuf,
}

/// The actor implementation of the pipeline
pub struct PipelineActor;

impl Actor for PipelineActor {
    type Context = SyncContext<Self>;
}

impl Handler<StoreFile> for PipelineActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: StoreFile, _: &mut Self::Context) -> Self::Result {
        println!("{:?}", msg.path);
        Ok(())
    }
}
