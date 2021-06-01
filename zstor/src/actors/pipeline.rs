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

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Required info to recover a file from its raw parts as they are stored.
pub struct RecoverFile {
    /// The path to place the recovered file
    pub path: PathBuf,
}

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Required info to rebuild a file from its raw parts as they are stored, to store them again so
/// all parts are available again
pub struct Rebuild {}

/// The actor implementation of the pipeline
pub struct PipelineActor;

impl Actor for PipelineActor {
    type Context = SyncContext<Self>;
}

impl Handler<StoreFile> for PipelineActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: StoreFile, _: &mut Self::Context) -> Self::Result {
        todo!();
    }
}

impl Handler<RecoverFile> for PipelineActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: RecoverFile, ctx: &mut Self::Context) -> Self::Result {
        todo!();
    }
}

impl Handler<Rebuild> for PipelineActor {
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: Rebuild, ctx: &mut Self::Context) -> Self::Result {
        todo!();
    }
}
