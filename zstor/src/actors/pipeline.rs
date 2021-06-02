use crate::actors::{
    config::{ConfigActor, GetConfig},
    meta::MetaStoreActor,
};
use crate::{meta::MetaStore, ZstorError};
use actix::dev::channel::channel;
use actix::prelude::*;
use std::path::PathBuf;

#[derive(Message)]
#[rtype(result = "Result<Vec<Vec<u8>>, ZstorError>")]
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
pub struct PipelineActor<T: Unpin + 'static> {
    cfg: Addr<ConfigActor>,
    meta: Addr<MetaStoreActor<T>>,
}

impl<T> Actor for PipelineActor<T>
where
    T: Unpin + 'static,
{
    type Context = SyncContext<Self>;
}

impl<T> Handler<StoreFile> for PipelineActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = Result<Vec<Vec<u8>>, ZstorError>;

    fn handle(&mut self, msg: StoreFile, ctx: &mut Self::Context) -> Self::Result {
        let cfg = self.cfg.clone();
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        ctx.wait(async {
            let config = cfg.send(GetConfig).await;
            tx.send(config);
            ()
        });
        // TODO
        let config = rx.recv().expect("Failed to receive value on channel")?;
        todo!();
    }
}

impl<T> Handler<RecoverFile> for PipelineActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: RecoverFile, ctx: &mut Self::Context) -> Self::Result {
        todo!();
    }
}

impl<T> Handler<Rebuild> for PipelineActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = Result<(), ZstorError>;

    fn handle(&mut self, msg: Rebuild, ctx: &mut Self::Context) -> Self::Result {
        todo!();
    }
}
