extern crate testutils;
use std::thread::sleep;
use std::time::Duration;
use testutils::init::TestManager;
use testutils::init::TestParams;
use testutils::utils::count_procs;

fn stop_qsfs(manager: &mut TestManager) {
    manager.stop_zdbfs().expect("stop zdbfs");
    manager.stop_fs_zdb().expect("stop fs zdb");
    manager.stop_zstor().expect("stop zstor");
    manager.cleanup_fs_disk().expect("cleanup fs disk");
}

fn start_qsfs(manager: &mut TestManager) {
    manager.start_zstor().expect("start zstor again");
    manager.start_fs_zdb().expect("start fs zdb again");
    manager.start_zdbfs().expect("start zdbfs again");
}

fn restart_qsfs(manager: &mut TestManager) {
    stop_qsfs(manager);
    start_qsfs(manager);
}

#[test]
fn simple_recovery() {
    let mut manager = TestManager::new(TestParams::default().with_id("sr").with_port(9901));
    manager.init().expect("starting services");
    manager
        .write_file("test".into(), 100)
        .expect("write file to zdbfs");
    let checksum1 = manager.checksum_file("test".into()).expect("checksum file");
    restart_qsfs(&mut manager);
    let checksum2 = manager
        .checksum_file("test".into())
        .expect("checksum2 file");
    if checksum1 != checksum2 {
        panic!("mismatching checksums!");
    }
}

#[test]
fn single_zstor_instance() {
    // new instance of zstor doesn't start while another
    // one is running
    let mut manager = TestManager::new(TestParams::default().with_id("szi").with_port(9902));
    manager.init().expect("starting services");
    let result = manager.start_zstor_blocking();
    assert!(result.is_err());
}

#[test]
fn leaked_socket_handled() {
    // when zstor exits without cleaning up socket (i.e. due to a SIGKILL)
    // it starts again normally
    let mut manager = TestManager::new(TestParams::default().with_id("lsh").with_port(9903));
    manager.init().expect("starting services");
    manager
        .write_file("test".into(), 100)
        .expect("write file to zdbfs");
    let checksum1 = manager.checksum_file("test".into()).expect("checksum file");
    manager.stop_zdbfs().expect("stop zdbfs");
    // so that all data is flushed
    manager.stop_fs_zdb().expect("stop fs zdb");
    manager.copy_zstor_log().expect("copy zstor log");
    // wait for some time until zstor uploads everything
    sleep(Duration::from_secs(5));
    manager.force_stop_zstor().expect("force stop zstor");
    manager.cleanup_fs_disk().expect("cleanup fs disk");
    start_qsfs(&mut manager);
    let checksum2 = manager
        .checksum_file("test".into())
        .expect("checksum2 file");
    println!("checksum1: {}, checksum2: {}", checksum1, checksum2);
    if checksum1 != checksum2 {
        panic!("mismatching checksums!");
    }
}

#[test]
fn remote_zdb_outages_handled() {
    // - connections are reestablished
    // - stores are retried
    let mut manager = TestManager::new(TestParams::default().with_id("rzoh").with_port(9904));
    manager.init().expect("starting services");
    manager.stop_data_zdb().expect("stopping remote zdbs");
    manager
        .write_file("test".into(), 100)
        .expect("write file to zdbfs");
    let checksum1 = manager.checksum_file("test".into()).expect("checksum file");
    manager.stop_zdbfs().expect("stop zdbfs");
    manager.stop_fs_zdb().expect("stop fs zdb");
    manager.start_data_zdb().expect("start fs zdb again");
    manager.stop_zstor().expect("force stop zstor");
    manager.copy_zstor_log().expect("copy zstor log");
    manager.cleanup_fs_disk().expect("clean up fs disk");
    start_qsfs(&mut manager);
    let checksum2 = manager
        .checksum_file("test".into())
        .expect("checksum2 file");
    println!("checksum1: {}, checksum2: {}", checksum1, checksum2);
    if checksum1 != checksum2 {
        panic!("mismatching checksums!");
    }
}

#[test]
fn slow_connection_no_proc_bomb() {
    // when the connection is slow and a lot of data is written to disk
    // a lot of hooks get called.
    // they shouldn't block to prevent eating up much memory
    let mut manager = TestManager::new(TestParams {
        id: "scnpb".to_string(),
        network_speed: None,
        max_zdb_data_dir_size: None,
        data_disk_size: "20G".into(),
        fs_disk_size: "10G".into(),
        zdb_fs_port: 9905,
    });
    manager.init().expect("starting services");
    manager
        .veth_host
        .as_ref()
        .unwrap()
        .limit_traffic(1)
        .expect("limit traffic");
    // should generate ~128 store hook calls
    manager
        .write_file("test".into(), 1024)
        .expect("write file to zdbfs");
    let _ = manager.checksum_file("test".into()).expect("checksum file");
    // TODO: fix, this count hooks globally (bad for parallel tests)
    let proc_count = count_procs("hook.sh");
    if proc_count > 15 {
        // 60
        panic!("{} hook.sh procs running! should be < 10", proc_count)
    }
}

#[test]
fn retrieves_more_prior_than_stores() {
    // when the connection is slow and a lot of data is written to disk
    // a lot of store hooks get called.
    // when reading from the file system, if the file is not present
    // it issues a retrieve command. this command should be handled before
    // the rest of the stores. this is because the zdb file system blocks on it
    let mut manager = TestManager::new(TestParams {
        id: "zse".to_string(),
        network_speed: None,
        max_zdb_data_dir_size: None,
        data_disk_size: "20G".into(),
        fs_disk_size: "10G".into(),
        zdb_fs_port: 9906,
    });
    manager.init().expect("starting services");
    // should be stored remotely as 4 shards and a couple of small metadata shards(?)
    manager
        .write_file("test".into(), 2 * 16)
        .expect("write file to zdbfs");
    let checksum1 = manager.checksum_file("test".into()).expect("checksum file");
    let _ = manager.checksum_file("test".into()).expect("checksum file");
    restart_qsfs(&mut manager);
    // now data are stored remotely
    // set limit to a shard per second
    manager
        .veth_host
        .as_ref()
        .unwrap()
        .limit_traffic(1024 * 16 * 8)
        .expect("limit traffic");
    // generates ~128 shard
    manager
        .write_file("test2".into(), 1024)
        .expect("write file to zdbfs");
    // 4 shards to be downloaded + 1 pending shard being stored + 1 shard for the metadata + 4 seconds error margin
    let checksum2 = manager
        .checksum_file_with_timwout("test".into(), Duration::from_secs(10))
        .expect("checksum file");
    if checksum1 != checksum2 {
        panic!("mismatching checksums!");
    }
}

#[test]
fn zdbfs_storage_exceeded() {
    // if the disk on which zdbfs writes its data is full
    // - the old files (cached locally) should be normally readable
    // - write failures are non-blocking
    // - when the storage is extended, new writes works as expected
    let mut manager = TestManager::new(TestParams {
        id: "rmpts".to_string(),
        network_speed: None,
        max_zdb_data_dir_size: None,
        data_disk_size: "3G".into(),
        fs_disk_size: "600M".into(),
        zdb_fs_port: 9906,
    });
    manager.init().expect("starting services");
    (&manager.fs_disk)
        .as_ref()
        .unwrap()
        .write_file("filler", 300)
        .expect("failed to write filler file");
    manager
        .write_file("test1".into(), 2 * 16)
        .expect("write file to zdbfs");
    let checksum1 = manager
        .checksum_file("test1".into())
        .expect("checksum file");
    // fails because there is no space
    let _ = manager.write_file("test2".into(), 600);
    // might break under pressure
    let _ = manager.write_file("test2".into(), 600);

    let checksum2 = manager
        .checksum_file_with_timwout("test1".into(), Duration::from_secs(10))
        .expect("checksum file");
    if checksum1 != checksum2 {
        panic!("mismatching checksums!");
    }
    (&manager.fs_disk)
        .as_ref()
        .unwrap()
        .remove_file("filler")
        .expect("failed to write filler file");
    // read/write still works after extending the space
    manager
        .write_file("test3".into(), 2 * 16)
        .expect("write file to zdbfs");

    let _ = manager
        .checksum_file_with_timwout("test3".into(), Duration::from_secs(10))
        .expect("checksum file");
}
