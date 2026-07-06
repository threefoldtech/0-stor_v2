//! Integration test for the data dir cache eviction, run against real 0-db processes. See
//! `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is found
//! (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_cache_rig, write_test_file};
use std::time::{Duration, Instant};
use zstor_v2::actors::zstor::Retrieve;

#[actix_rt::test]
async fn eviction_removes_dispersed_files_only() {
    // A 1 MiB cap checked every second, holding one dispersed 700 KiB file.
    let Some((rig, data_dir)) = setup_cache_rig("evict", 1, 1, 700 * 1024, false).await else {
        return;
    };

    // A second file which is never dispersed pushes the dir over its cap.
    let undispersed = data_dir.join("d1");
    write_test_file(&undispersed, 7, 700 * 1024);

    // The monitor must evict the dispersed file (bringing the dir back under the cap) and
    // must leave the undispersed file alone: deleting it would lose data.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && rig.data_file.exists() {
        actix_rt::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        !rig.data_file.exists(),
        "the dispersed file was never evicted from the over-cap data dir"
    );
    assert!(
        undispersed.exists(),
        "a file which was never dispersed must not be evicted"
    );

    // An eviction is only acceptable if the evicted file can come back: retrieve it and
    // verify the content survived the round trip.
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
