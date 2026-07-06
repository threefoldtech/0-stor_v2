//! Integration test for the repair sweep, run against real 0-db processes. See
//! `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is found
//! (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_rig, shard_ports, verify_retrievable};
use std::time::Duration;
use zstor_v2::actors::repairer::SweepNow;

#[actix_rt::test]
async fn disabled_unattended_repair_keeps_hands_off() {
    let Some(mut rig) = setup_rig("disabled", false, 2).await else {
        return;
    };

    let used = shard_ports(&rig.cfg, &rig.data_file).await;
    let victim = used[0];
    rig.fleet.kill_port(victim);

    // Give the (2 second) sweep interval ample ticks; with unattended repair disabled the
    // metadata must remain untouched.
    actix_rt::time::sleep(Duration::from_secs(15)).await;
    let ports = shard_ports(&rig.cfg, &rig.data_file).await;
    assert!(
        ports.contains(&victim),
        "repair ran although unattended repair is disabled"
    );

    // An explicitly requested sweep still repairs.
    let report = rig
        .system
        .repairer
        .send(SweepNow)
        .await
        .expect("can deliver sweep command")
        .expect("sweep completes");
    assert_eq!(report.degraded, 1);
    assert_eq!(report.rebuilt, 1);
    assert_eq!(report.failed, 0);

    let after = shard_ports(&rig.cfg, &rig.data_file).await;
    assert!(!after.contains(&victim));

    verify_retrievable(&rig).await;
}
