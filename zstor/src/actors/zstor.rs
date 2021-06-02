use crate::actors::{
    config::ConfigActor,
    meta::MetaStoreActor,
    pipeline::{PipelineActor, StoreFile},
};
use crate::{meta::MetaStore, ZstorError};
use actix::prelude::*;
use std::path::PathBuf;

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message for the store command of zstor.
pub struct Store {
    /// Path to the file to store.
    pub file: PathBuf,
    /// Optional different path to use when computing the key. If set, the key is generated as if
    /// the file is saved in this path.
    pub key_path: Option<PathBuf>,
    /// Remember failure metadata to later retry the upload.
    pub save_failure: bool,
    /// Attempt to delete the file after a successful upload.
    pub delete: bool,
}

/// Actor for the main zstor object encoding and decoding.
pub struct ZstorActor<T: Unpin + 'static> {
    cfg: Addr<ConfigActor>,
    // pipeline: Addr<PipelineActor>,
    meta: Addr<MetaStoreActor<T>>,
}

impl<T> ZstorActor<T>
where
    T: Unpin + 'static,
{
    /// new
    pub fn new(
        cfg: Addr<ConfigActor>,
        // pipeline: Addr<PipelineActor>,
        meta: Addr<MetaStoreActor<T>>,
    ) -> ZstorActor<T> {
        Self {
            cfg,
            //  pipeline,
            meta,
        }
    }
}

impl<T> Actor for ZstorActor<T>
where
    T: Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<T> Handler<Store> for ZstorActor<T>
where
    T: MetaStore + Unpin + 'static,
{
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, msg: Store, _: &mut Self::Context) -> Self::Result {
        // let pipeline = self.pipeline.clone();
        let config = self.cfg.clone();
        let meta = self.meta.clone();
        AtomicResponse::new(Box::pin(
            async move {
                // let shards = pipeline.send(StoreFile { path: msg.file }).await??;

                Ok(())
            }
            .into_actor(self),
        ))
    }
}
