use crate::actors::pipeline::{PipelineActor, StoreFile};
use actix::prelude::*;

/// Start the 0-stor monitor daemon
pub fn start() {
    System::new().block_on(async {
        let addr = SyncArbiter::start(1, || PipelineActor);

        addr.send(StoreFile {
            path: std::path::PathBuf::from("/home/lee/rust/0-stor_v2"),
        })
        .await
        .unwrap()
        .unwrap();
    });
}
