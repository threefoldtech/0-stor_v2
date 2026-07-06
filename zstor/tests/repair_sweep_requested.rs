//! Integration test for the repair sweep, run against real 0-db processes. See
//! `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is found
//! (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_rig, shard_ports, verify_retrievable};
use std::time::{Duration, Instant};
use zstor_v2::actors::repairer::SweepNow;

#[actix_rt::test]
async fn requested_sweep_repairs_lost_backend() {
    let Some(mut rig) = setup_rig("sweepnow", false, 3600).await else {
        return;
    };

    let used = shard_ports(&rig.cfg, &rig.data_file).await;
    assert_eq!(used.len(), 4, "object should hold 4 shards");
    let victim = used[0];
    rig.fleet.kill_port(victim);

    // The backend manager needs a few of its 3 second check ticks to see the death and move
    // the backend past the (1 second) grace period. Sweep until the object is repaired.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut repaired = false;
    while Instant::now() < deadline {
        let report = rig
            .system
            .repairer
            .send(SweepNow)
            .await
            .expect("can deliver sweep command")
            .expect("sweep completes");
        assert_eq!(report.objects, 1, "sweep should inspect the stored object");
        if report.rebuilt == 1 {
            assert_eq!(report.degraded, 1);
            assert_eq!(report.failed, 0);
            repaired = true;
            break;
        }
        assert_eq!(
            report.failed, 0,
            "sweep must not fail rebuilds while a spare backend is available"
        );
        actix_rt::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(repaired, "sweep never repaired the object");

    let after = shard_ports(&rig.cfg, &rig.data_file).await;
    assert!(
        !after.contains(&victim),
        "metadata still references the dead backend after repair"
    );
    assert_eq!(after.len(), 4, "object should again hold 4 shards");

    verify_retrievable(&rig).await;
}
