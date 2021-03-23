use crate::config::Config;
use crate::read_zstor_config;
use crate::MonitorResult;
use log::{debug, error, info};
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time::interval;
use zstor_v2::config::Meta;
use zstor_v2::etcd::Etcd;

const REPAIR_BACKLOG_RETRY_INTERVAL_DURATION: u64 = 60 * 5; // 5 minutes
const REPAIR_BACKLOG_RETRY_INTERVAL: Duration =
    Duration::from_secs(REPAIR_BACKLOG_RETRY_INTERVAL_DURATION);

pub async fn monitor_failed_writes(
    mut rx: Receiver<()>,
    config: Config,
) -> JoinHandle<MonitorResult<()>> {
    tokio::spawn(async move {
        let mut ticker = interval(REPAIR_BACKLOG_RETRY_INTERVAL);

        loop {
            select! {
                _ = rx.recv() => {
                    info!("shutting down failed write monitor");
                    return Ok(())
                }
                _ = ticker.tick() => {
                    debug!("Checking for failed uploads");
                    // read zstor config
                    let zstor_config = match read_zstor_config(config.zstor_config_path()).await {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            error!("could not read zstor config: {}", e);
                            continue
                        }
                    };

                    // connect to meta store
                    let mut cluster = match zstor_config.meta() {
                        Meta::ETCD(etcdconf) => match Etcd::new(etcdconf, zstor_config.virtual_root().clone()).await {
                            Ok(cluster) => cluster,
                            Err(e) => {error!("could not create metadata cluster: {}", e); continue},
                        },
                    };

                    let failures = match cluster.get_failures().await {
                        Ok(failures) => failures,
                        Err(e) => {
                            error!("Could not get failed uploads from metastore: {}", e);
                            continue
                        }
                    };

                    for failure_data in failures {
                        debug!("Attempting to upload previously failed file {:?}", failure_data.data_path());
                        match crate::upload_file(&failure_data.data_path(), failure_data.key_dir_path(), failure_data.should_delete(), &config).await {
                            Ok(_) => {
                                info!("Successfully uploaded {:?} after previous failure", failure_data.data_path());
                                match cluster.delete_failure(&failure_data).await {
                                    Ok(_) => debug!("Removed failed upload of {:?} from metastore", failure_data.data_path()),
                                    Err(e) => {
                                        error!("Could not delete failed upload of {:?} from metastore: {}", failure_data.data_path(), e);
                                    },
                                }
                            },
                            Err(e) => {
                                error!("Could not upload {:?}: {}", failure_data.data_path(), e);
                                continue
                            }
                        }
                    }
                }
            }
        }
    })
}
