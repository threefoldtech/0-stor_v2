//! Integration test for data dir cache eviction through a symlinked data dir path, run
//! against real 0-db processes. Regression test: the metastore keys files by the path they
//! were stored with, without resolving symlinks. Older versions resolved the path on the
//! filesystem before the eviction lookup, so a data dir reached through a symlink (a mounted
//! or relocated volume) never matched its own metadata and nothing was ever evicted.
//!
//! See `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is
//! found (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::setup_cache_rig;
use std::time::{Duration, Instant};
use zstor_v2::actors::zstor::Retrieve;

#[actix_rt::test]
async fn eviction_works_through_symlinked_data_dir() {
    // A single dispersed 1.3 MiB file in a symlinked data dir capped at 1 MiB: the dir is
    // over its cap from the start, so the monitor must evict the file.
    let Some((rig, _data_dir)) = setup_cache_rig("symlink", 1, 1, 1300 * 1024, true).await else {
        return;
    };

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && rig.data_file.exists() {
        actix_rt::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        !rig.data_file.exists(),
        "the dispersed file in the symlinked data dir was never evicted"
    );

    // The evicted file must still be retrievable through the same (symlinked) path.
    rig.system
        .zstor
        .send(Retrieve {
            file: rig.data_file.clone(),
        })
        .await
        .expect("can deliver retrieve command")
        .expect("retrieve succeeds");
    let restored = std::fs::read(&rig.data_file).expect("can read restored file");
    assert_eq!(
        restored, rig.payload,
        "restored object content differs from the original"
    );
}
