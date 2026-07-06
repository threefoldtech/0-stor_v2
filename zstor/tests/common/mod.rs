//! Shared harness for the integration tests which run against real 0-db processes.
//!
//! These tests need a 0-db binary: set the ZDB_BINARY environment variable to its path, or
//! have `zdb` on the PATH. When no binary is found the tests print a notice and pass without
//! testing anything, so the regular test suite stays independent of external tools.

// This module is compiled once per test binary, and not every binary uses every helper.
#![allow(dead_code)]

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use zstor_v2::actors::zstor::{Retrieve, Store};
use zstor_v2::config::{Compression, Config, Encryption, Group, Meta};
use zstor_v2::encryption::SymmetricKey;
use zstor_v2::meta::new_metastore;
use zstor_v2::zdb::ZdbConnectionInfo;
use zstor_v2::zdb_meta::ZdbMetaStoreConfig;
use zstor_v2::ZstorSystem;

/// Locate the 0-db binary to run the tests against.
fn zdb_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("ZDB_BINARY") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    for dir in std::env::split_paths(&std::env::var_os("PATH")?) {
        for name in ["zdb", "zdbd"] {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// A running fleet of 0-db processes, killed and cleaned up on drop.
pub struct ZdbFleet {
    bin: PathBuf,
    root: PathBuf,
    children: Vec<(u16, String, Option<Child>)>,
}

fn spawn_one(bin: &Path, root: &Path, port: u16, mode: &str) -> Child {
    let dir = root.join(format!("zdb-{}", port));
    std::fs::create_dir_all(&dir).expect("can create zdb dir");
    Command::new(bin)
        .arg("--port")
        .arg(port.to_string())
        .arg("--listen")
        .arg("127.0.0.1")
        .arg("--data")
        .arg(dir.join("data"))
        .arg("--index")
        .arg(dir.join("index"))
        .arg("--mode")
        .arg(mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("can spawn zdb")
}

impl ZdbFleet {
    fn spawn(bin: &Path, root: PathBuf, seq_ports: &[u16], user_ports: &[u16]) -> ZdbFleet {
        let mut children = Vec::new();
        for (mode, ports) in [("seq", seq_ports), ("user", user_ports)] {
            for &port in ports {
                let child = spawn_one(bin, &root, port, mode);
                children.push((port, mode.to_string(), Some(child)));
            }
        }
        let fleet = ZdbFleet {
            bin: bin.to_path_buf(),
            root,
            children,
        };
        for (port, _, _) in &fleet.children {
            wait_reachable(*port);
        }
        fleet
    }

    /// Kill the 0-db listening on the given port.
    pub fn kill_port(&mut self, port: u16) {
        for (p, _, child) in self.children.iter_mut() {
            if *p == port {
                if let Some(mut child) = child.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                return;
            }
        }
        panic!("no zdb running on port {}", port);
    }

    /// Restart a previously killed 0-db on its old port, over its surviving data dirs.
    pub fn respawn_port(&mut self, port: u16) {
        for (p, mode, child) in self.children.iter_mut() {
            if *p == port {
                assert!(child.is_none(), "zdb on port {} is still running", port);
                *child = Some(spawn_one(&self.bin, &self.root, port, mode));
                wait_reachable(port);
                return;
            }
        }
        panic!("no zdb known on port {}", port);
    }
}

impl Drop for ZdbFleet {
    fn drop(&mut self) {
        for (_, _, child) in self.children.iter_mut() {
            if let Some(mut child) = child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn wait_reachable(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("zdb on port {} did not come up", port);
}

/// Reserve a set of distinct free localhost ports.
fn free_ports(amount: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..amount)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("can bind localhost"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("bound socket has an address").port())
        .collect()
}

fn local_ci(port: u16) -> ZdbConnectionInfo {
    let addr: SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .expect("valid localhost address");
    ZdbConnectionInfo::new(addr, None, None)
}

/// Build a config over the given fleet: every data backend forms its own group, so repair
/// target selection can be observed keeping the group spread. When `data_dir` is given, the
/// data dir monitor is configured with that (path, size cap in MiB, check interval in
/// seconds).
fn make_config(
    root: &Path,
    data_ports: &[u16],
    meta_ports: &[u16],
    unattended_repair: bool,
    repair_interval_secs: u64,
    data_dir: Option<(&Path, u64, u64)>,
) -> Config {
    let meta_backends: [ZdbConnectionInfo; 4] = [
        local_ci(meta_ports[0]),
        local_ci(meta_ports[1]),
        local_ci(meta_ports[2]),
        local_ci(meta_ports[3]),
    ];
    Config {
        minimal_shards: 2,
        expected_shards: 4,
        redundant_groups: 0,
        redundant_nodes: 0,
        root: Some(root.to_path_buf()),
        socket: None,
        pid_file: None,
        zdb_data_dir_path: data_dir.map(|(path, _, _)| path.to_path_buf()),
        max_zdb_data_dir_size: data_dir.map(|(_, cap_mib, _)| cap_mib),
        zdb_data_dir_check_interval_secs: data_dir.map(|(_, _, interval)| interval),
        zdbfs_mountpoint: None,
        prometheus_port: None,
        unattended_repair: Some(unattended_repair),
        repair_interval_secs: Some(repair_interval_secs),
        missing_backend_grace_secs: Some(1),
        degraded_write_margin: None,
        encryption: Encryption::Aes(SymmetricKey::new([3u8; 32])),
        compression: Compression::Snappy,
        meta: Meta::Zdb(ZdbMetaStoreConfig::new(
            "repairtest".to_string(),
            Encryption::Aes(SymmetricKey::new([4u8; 32])),
            meta_backends,
        )),
        groups: data_ports
            .iter()
            .map(|&port| Group {
                backends: vec![local_ci(port)],
            })
            .collect(),
    }
}

pub struct TestRig {
    pub fleet: ZdbFleet,
    pub cfg: Config,
    pub system: ZstorSystem,
    pub data_file: PathBuf,
    pub payload: Vec<u8>,
}

impl TestRig {
    /// The root directory of the test fleet, usable for additional scratch files.
    pub fn fleet_root(&self) -> &Path {
        &self.fleet.root
    }
}

/// Stand up a fleet, a zstor system over it, and one stored object. Returns None when no zdb
/// binary is available.
pub async fn setup_rig(
    name: &str,
    unattended_repair: bool,
    repair_interval_secs: u64,
) -> Option<TestRig> {
    let Some(bin) = zdb_binary() else {
        eprintln!("skipping repair integration test: no zdb binary found (set ZDB_BINARY)");
        return None;
    };

    let root = std::env::temp_dir().join(format!("zstor-repair-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("can create test root");

    let ports = free_ports(9);
    let data_ports = ports[..5].to_vec();
    let meta_ports = ports[5..].to_vec();
    let fleet = ZdbFleet::spawn(&bin, root.clone(), &data_ports, &meta_ports);

    let cfg = make_config(
        &root,
        &data_ports,
        &meta_ports,
        unattended_repair,
        repair_interval_secs,
        None,
    );
    let system = zstor_v2::setup_system(root.join("zstor.toml"), &cfg)
        .await
        .expect("can set up zstor system");

    // A payload which spans several erasure blocks and doesn't compress away entirely.
    let payload: Vec<u8> = (0..200 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let data_file = root.join("data.bin");
    let mut file = std::fs::File::create(&data_file).expect("can create data file");
    file.write_all(&payload).expect("can write data file");
    drop(file);

    system
        .zstor
        .send(Store {
            file: data_file.clone(),
            key_path: None,
            save_failure: false,
            delete: false,
            blocking: true,
        })
        .await
        .expect("can deliver store command")
        .expect("store succeeds");

    Some(TestRig {
        fleet,
        cfg,
        system,
        data_file,
        payload,
    })
}

/// Write a deterministic test payload of the given length to the given path, and return it.
pub fn write_test_file(path: &Path, seed: u32, len: usize) -> Vec<u8> {
    let payload: Vec<u8> = (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761).wrapping_add(seed) >> 13) as u8)
        .collect();
    std::fs::write(path, &payload).expect("can write test file");
    payload
}

/// Stand up a fleet and a zstor system with the data dir monitor enabled: a data dir capped
/// at `cap_mib` MiB, checked every `interval_secs` seconds, holding one dispersed file of
/// `file_len` bytes (the rig's `data_file`). With `via_symlink` the configured data dir path
/// goes through a symlink, like a data dir on a mounted or relocated volume would. Returns
/// the rig and the data dir path as configured. Returns None when no zdb binary is available.
pub async fn setup_cache_rig(
    name: &str,
    cap_mib: u64,
    interval_secs: u64,
    file_len: usize,
    via_symlink: bool,
) -> Option<(TestRig, PathBuf)> {
    let Some(bin) = zdb_binary() else {
        eprintln!("skipping cache integration test: no zdb binary found (set ZDB_BINARY)");
        return None;
    };

    let root = std::env::temp_dir().join(format!("zstor-cache-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("can create test root");

    let ports = free_ports(9);
    let data_ports = ports[..5].to_vec();
    let meta_ports = ports[5..].to_vec();
    let fleet = ZdbFleet::spawn(&bin, root.clone(), &data_ports, &meta_ports);

    let data_dir = if via_symlink {
        let real = root.join("realdir");
        std::fs::create_dir_all(&real).expect("can create real data dir");
        let link = root.join("datadir");
        std::os::unix::fs::symlink(&real, &link).expect("can create data dir symlink");
        link
    } else {
        let dir = root.join("datadir");
        std::fs::create_dir_all(&dir).expect("can create data dir");
        dir
    };

    let cfg = make_config(
        &root,
        &data_ports,
        &meta_ports,
        false,
        600,
        Some((&data_dir, cap_mib, interval_secs)),
    );
    let system = zstor_v2::setup_system(root.join("zstor.toml"), &cfg)
        .await
        .expect("can set up zstor system");

    let data_file = data_dir.join("d0");
    let payload = write_test_file(&data_file, 1, file_len);

    system
        .zstor
        .send(Store {
            file: data_file.clone(),
            key_path: None,
            save_failure: false,
            delete: false,
            blocking: true,
        })
        .await
        .expect("can deliver store command")
        .expect("store succeeds");

    Some((
        TestRig {
            fleet,
            cfg,
            system,
            data_file,
            payload,
        },
        data_dir,
    ))
}

/// The ports of the data backends currently referenced by the stored object's metadata.
pub async fn shard_ports(cfg: &Config, path: &Path) -> Vec<u16> {
    let meta = new_metastore(cfg)
        .await
        .expect("can build metastore client");
    let metadata = meta
        .load_meta(path)
        .await
        .expect("can load metadata")
        .expect("metadata for stored object exists");
    metadata
        .shards()
        .iter()
        .map(|shard| shard.zdb().address().port())
        .collect()
}

/// Retrieve the object (after deleting the local copy) and verify its content is intact.
pub async fn verify_retrievable(rig: &TestRig) {
    std::fs::remove_file(&rig.data_file).expect("can delete local copy");
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
