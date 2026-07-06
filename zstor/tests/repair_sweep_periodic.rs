//! Integration test for the repair sweep, run against real 0-db processes. See
//! `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is found
//! (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_rig, shard_ports, verify_retrievable};
use std::time::{Duration, Instant};

#[actix_rt::test]
async fn periodic_sweep_repairs_lost_backend_unattended() {
    let Some(mut rig) = setup_rig("periodic", true, 2).await else {
        return;
    };

    let used = shard_ports(&rig.cfg, &rig.data_file).await;
    let victim = used[0];
    rig.fleet.kill_port(victim);

    // No sweep is requested: the periodic sweep alone must notice the dead backend and
    // rebuild the object.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut repaired = false;
    while Instant::now() < deadline {
        let ports = shard_ports(&rig.cfg, &rig.data_file).await;
        if !ports.contains(&victim) {
            repaired = true;
            break;
        }
        actix_rt::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(
        repaired,
        "unattended repair never rebuilt the object after backend loss"
    );

    verify_retrievable(&rig).await;
}
