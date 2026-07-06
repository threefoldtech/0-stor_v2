//! Integration test for degraded writes, run against real 0-db processes. See
//! `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is found
//! (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_rig, shard_ports, verify_retrievable};
use std::time::{Duration, Instant};
use zstor_v2::actors::repairer::SweepNow;
use zstor_v2::actors::zstor::Store;

/// With one backend dead and no spare (4 configured for 4 shards), a write must degrade to 3
/// placed shards instead of stalling; once the backend returns, a repair sweep must backfill
/// the object to full redundancy. With two backends dead, a write must be refused: 2 healthy
/// equals the minimal shards, which is below minimal + margin.
#[actix_rt::test]
async fn writes_degrade_survive_and_backfill() {
    // The rig starts 5 data backends but only the first 4 form the write set of the initial
    // object; the point here is a config with NO spare, so build the degraded config over
    // the 4 backends the object landed on and kill within that set. Simplest equivalent: use
    // the rig as-is (5 configured backends), kill TWO backends holding shards - leaving 3
    // healthy of the 5 - so a full 4-shard placement is impossible but a degraded 3-shard
    // write (minimal 2 + margin 1) is allowed.
    let Some(mut rig) = setup_rig("degraded", false, 3600).await else {
        return;
    };

    let used = shard_ports(&rig.cfg, &rig.data_file).await;
    assert_eq!(used.len(), 4);
    let (victim_a, victim_b) = (used[0], used[1]);
    rig.fleet.kill_port(victim_a);
    rig.fleet.kill_port(victim_b);

    // Write a NEW object while 2 of 5 backends are dead. Full placement (4 shards on 4
    // distinct healthy backends) is impossible with 3 healthy; the write must degrade to 3
    // shards instead of failing.
    let payload: Vec<u8> = (0..64 * 1024u32)
        .map(|i| (i.wrapping_mul(40503) >> 7) as u8)
        .collect();
    let degraded_file = rig.fleet_root().join("degraded.bin");
    std::fs::write(&degraded_file, &payload).expect("can write second data file");

    rig.system
        .zstor
        .send(Store {
            file: degraded_file.clone(),
            key_path: None,
            save_failure: false,
            delete: false,
            blocking: true,
        })
        .await
        .expect("can deliver store command")
        .expect("store must succeed degraded instead of stalling");

    let degraded_shards = shard_ports(&rig.cfg, &degraded_file).await;
    assert_eq!(
        degraded_shards.len(),
        3,
        "degraded write should place exactly as many shards as healthy backends"
    );
    assert!(!degraded_shards.contains(&victim_a));
    assert!(!degraded_shards.contains(&victim_b));

    // The degraded object must be readable.
    std::fs::remove_file(&degraded_file).expect("can delete local copy");
    rig.system
        .zstor
        .send(zstor_v2::actors::zstor::Retrieve {
            file: degraded_file.clone(),
        })
        .await
        .expect("can deliver retrieve command")
        .expect("degraded object must be retrievable");
    assert_eq!(
        std::fs::read(&degraded_file).expect("can read restored file"),
        payload
    );

    // Bring one dead backend back (its data dirs survived the kill), then sweep: the
    // degraded object must be backfilled to full redundancy, and the object from setup
    // (which lost 2 of its 4 shards) must be repaired too.
    rig.fleet.respawn_port(victim_a);

    // Sweep until both objects are whole again; a single sweep may only manage one of the
    // two repairs depending on how quickly the backend manager notices the comeback.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut backfilled = false;
    while Instant::now() < deadline {
        let report = rig
            .system
            .repairer
            .send(SweepNow)
            .await
            .expect("can deliver sweep command")
            .expect("sweep completes");
        assert_eq!(report.objects, 2, "sweep should inspect both objects");
        let degraded_now = shard_ports(&rig.cfg, &degraded_file).await;
        let original_now = shard_ports(&rig.cfg, &rig.data_file).await;
        if degraded_now.len() == 4
            && !degraded_now.contains(&victim_b)
            && !original_now.contains(&victim_b)
        {
            backfilled = true;
            break;
        }
        actix_rt::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(
        backfilled,
        "sweep never backfilled the degraded object and repaired the original"
    );

    verify_retrievable(&rig).await;

    // Finally: kill backends down to exactly the minimal shard count (2 healthy). A write
    // must now be refused - minimal + margin(1) = 3 healthy required.
    let remaining = shard_ports(&rig.cfg, &degraded_file).await;
    let extra_victim = *remaining
        .iter()
        .find(|p| **p != victim_a)
        .expect("a third backend is in use");
    rig.fleet.kill_port(extra_victim);
    rig.fleet.kill_port(victim_a);

    let refused_file = rig.fleet_root().join("refused.bin");
    std::fs::write(&refused_file, &payload).expect("can write third data file");
    let result = rig
        .system
        .zstor
        .send(Store {
            file: refused_file,
            key_path: None,
            save_failure: false,
            delete: false,
            blocking: true,
        })
        .await
        .expect("can deliver store command");
    assert!(
        result.is_err(),
        "a write below minimal shards + margin must be refused"
    );
}
