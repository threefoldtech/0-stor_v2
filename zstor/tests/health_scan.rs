//! Integration test for the daemon health scan, run against real 0-db processes. The scan
//! reports degraded objects and the remaining redundancy margin without repairing anything,
//! probing reachability against the backends recorded in the object metadata — so backends
//! swapped out of the config stay visible as long as objects reference them.
//!
//! See `common/mod.rs` for the shared harness; the test is skipped when no 0-db binary is
//! found (set ZDB_BINARY or have `zdb` on the PATH).

mod common;

use common::{setup_rig, shard_ports};
use std::time::{Duration, Instant};
use zstor_v2::actors::config::ReloadConfig;
use zstor_v2::actors::repairer::ScanNow;

#[actix_rt::test]
async fn health_scan_reports_margin_and_swapped_out_backends() {
    // Unattended repair off and a huge sweep interval: any rebuild observed below could
    // only come from the scan itself, which must never rebuild.
    let Some(mut rig) = setup_rig("scan", false, 3600).await else {
        return;
    };

    // Phase 1: healthy fabric. One object with 4 reachable shards, 2 needed to read.
    let report = rig
        .system
        .repairer
        .send(ScanNow)
        .await
        .expect("can deliver scan command")
        .expect("scan succeeds");
    assert_eq!(report.objects, 1);
    assert_eq!(report.degraded, 0);
    assert_eq!(
        report.margin,
        Some(2),
        "4 reachable shards, 2 needed to read"
    );
    assert_eq!(report.unconfigured_backends, 0);
    assert!(!report.sweep_active);

    // Phase 2: swap a shard-holding backend out of the config (hot reload) but keep it
    // alive. The object is still fully readable, so neither degraded nor margin may move —
    // only the hygiene counter. This is the backend the config-based status tables lose
    // track of entirely.
    let victim = shard_ports(&rig.cfg, &rig.data_file).await[0];
    let mut swapped_cfg = rig.cfg.clone();
    swapped_cfg
        .groups
        .retain(|group| group.backends.iter().all(|b| b.address().port() != victim));
    std::fs::write(
        rig.fleet_root().join("zstor.toml"),
        toml::to_string(&swapped_cfg).expect("can encode config"),
    )
    .expect("can write swapped config");
    rig.system
        .zstor
        .send(ReloadConfig)
        .await
        .expect("can deliver reload command")
        .expect("reload succeeds");

    let report = rig
        .system
        .repairer
        .send(ScanNow)
        .await
        .expect("can deliver scan command")
        .expect("scan succeeds");
    assert_eq!(
        report.degraded, 0,
        "a live backend swapped out of the config must not count as degraded"
    );
    assert_eq!(
        report.margin,
        Some(2),
        "reachability is probed against metadata, not config membership"
    );
    assert_eq!(report.unconfigured_backends, 1);

    // Phase 3: kill the swapped-out backend. Its shards are now really unreachable, and the
    // scan must say so.
    rig.fleet.kill_port(victim);
    let deadline = Instant::now() + Duration::from_secs(30);
    let report = loop {
        let report = rig
            .system
            .repairer
            .send(ScanNow)
            .await
            .expect("can deliver scan command")
            .expect("scan succeeds");
        if report.degraded == 1 || Instant::now() >= deadline {
            break report;
        }
        actix_rt::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(report.degraded, 1, "the dead backend's object is degraded");
    assert_eq!(
        report.margin,
        Some(1),
        "3 reachable shards, 2 needed to read"
    );
    assert_eq!(report.unconfigured_backends, 1);

    // The scan is dry: the object must still reference the dead backend, because only a
    // repair (sweep) may rebuild.
    assert!(
        shard_ports(&rig.cfg, &rig.data_file)
            .await
            .contains(&victim),
        "a health scan must never rebuild an object"
    );
}
