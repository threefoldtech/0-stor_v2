use crate::{
    zdb::{NsInfo, ZdbConnectionInfo},
    zdbfs::stats_t,
};
use actix::prelude::*;
use log::warn;
use prometheus::{
    register_int_gauge, register_int_gauge_vec, Encoder, IntGauge, IntGaugeVec, TextEncoder,
};
use std::mem;
use std::{collections::HashMap, fmt, string::FromUtf8Error};

const BACKEND_TYPE_DATA: &str = "data";
const BACKEND_TYPE_META: &str = "meta";

/// A metrics actor collecting metrics from the system.
pub struct MetricsActor {
    data_zdbs: HashMap<ZdbConnectionInfo, NsInfo>,
    meta_zdbs: HashMap<ZdbConnectionInfo, NsInfo>,
    removed_zdbs: Vec<ZdbConnectionInfo>,
    successful_zstor_commands: HashMap<ZstorCommandId, usize>,
    failed_zstor_commands: HashMap<ZstorCommandId, usize>,
    zdbfs_stats: stats_t,
    prom_metrics: PromMetrics,
}

struct PromMetrics {
    entries_gauges: IntGaugeVec,
    data_size_bytes_gauges: IntGaugeVec,
    data_limit_bytes_gauges: IntGaugeVec,
    index_size_bytes_gauges: IntGaugeVec,
    index_io_errors_gauges: IntGaugeVec,
    index_faults_gauges: IntGaugeVec,
    data_io_errors_gauges: IntGaugeVec,
    data_faults_gauges: IntGaugeVec,
    index_disk_freespace_bytes_gauges: IntGaugeVec,
    data_disk_freespace_bytes_gauges: IntGaugeVec,

    zstor_store_commands_finished_gauges: IntGaugeVec,
    zstor_retrieve_commands_finished_gauges: IntGaugeVec,
    zstor_rebuild_commands_finished_gauges: IntGaugeVec,
    zstor_check_commands_finished_gauges: IntGaugeVec,

    fs_fuse_reqs: IntGauge,
    fs_cache_hits: IntGauge,
    fs_cache_miss: IntGauge,
    fs_cache_full: IntGauge,
    fs_cache_linear_flush: IntGauge,
    fs_cache_random_flush: IntGauge,
    fs_cache_branches: IntGauge,
    fs_cache_branches_allocated: IntGauge,
    fs_cache_entries: IntGauge,
    fs_cache_blocks: IntGauge,
    fs_cache_blocksize: IntGauge,
    fs_syscalls: IntGaugeVec,
    fs_bytes_read: IntGauge,
    fs_bytes_written: IntGauge,
    fs_fuse_errors: IntGauge,
}

impl MetricsActor {
    /// Create a new [`MetricsActor`].
    pub fn new() -> MetricsActor {
        Self {
            data_zdbs: HashMap::new(),
            meta_zdbs: HashMap::new(),
            removed_zdbs: Vec::new(),
            successful_zstor_commands: HashMap::new(),
            failed_zstor_commands: HashMap::new(),
            zdbfs_stats: stats_t::default(),

            prom_metrics: Self::setup_prometheus(),
        }
    }

    fn setup_prometheus() -> PromMetrics {
        PromMetrics {
            entries_gauges: register_int_gauge_vec!(
                "entries",
                "entries in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),

            data_size_bytes_gauges: register_int_gauge_vec!(
                "data_size_bytes",
                "data_size_bytes in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            data_limit_bytes_gauges: register_int_gauge_vec!(
                "data_limit_bytes",
                "data_limit_bytes in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            index_size_bytes_gauges: register_int_gauge_vec!(
                "index_size_bytes",
                "index_size_bytes in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            index_io_errors_gauges: register_int_gauge_vec!(
                "index_io_errors",
                "index_io_errors in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            index_faults_gauges: register_int_gauge_vec!(
                "index_faults",
                "index_faults in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            data_io_errors_gauges: register_int_gauge_vec!(
                "data_io_errors",
                "data_io_errors in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            data_faults_gauges: register_int_gauge_vec!(
                "data_faults",
                "data_faults in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            index_disk_freespace_bytes_gauges: register_int_gauge_vec!(
                "index_disk_freespace_bytes",
                "index_disk_freespace_bytes in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            data_disk_freespace_bytes_gauges: register_int_gauge_vec!(
                "data_disk_freespace_bytes",
                "data_disk_freespace_bytes in namespace",
                &["address", "namespace", "backend_type"]
            )
            .unwrap(),
            zstor_store_commands_finished_gauges: register_int_gauge_vec!(
                "zstore_store_commands_finished",
                "zstor store commands that finished",
                &["success"]
            )
            .unwrap(),
            zstor_retrieve_commands_finished_gauges: register_int_gauge_vec!(
                "zstore_retrieve_commands_finished",
                "zstor retrieve commands that finished",
                &["success"]
            )
            .unwrap(),
            zstor_rebuild_commands_finished_gauges: register_int_gauge_vec!(
                "zstore_rebuild_commands_finished",
                "zstor rebuild commands that finished",
                &["success"]
            )
            .unwrap(),
            zstor_check_commands_finished_gauges: register_int_gauge_vec!(
                "zstore_check_commands_finished",
                "zstor check commands that finished",
                &["success"]
            )
            .unwrap(),
            fs_fuse_reqs: register_int_gauge!("fs_fuse_reqs", "Total amount of fuse requests")
                .unwrap(),
            fs_cache_hits: register_int_gauge!(
                "fs_cache_hits",
                "Total amount of cache hits in the filesystem"
            )
            .unwrap(),
            fs_cache_miss: register_int_gauge!(
                "fs_cache_miss",
                "Total amount of cache misses in the filesystem"
            )
            .unwrap(),
            fs_cache_full: register_int_gauge!(
                "fs_cache_full",
                "Total amount of times the cache was completely filled"
            )
            .unwrap(),
            fs_cache_linear_flush: register_int_gauge!(
                "fs_cache_linear_flush",
                "Total amount of linear flushes"
            )
            .unwrap(),
            fs_cache_random_flush: register_int_gauge!(
                "fs_cache_random_flush",
                "Total amount of random flushes"
            )
            .unwrap(),
            fs_cache_branches: register_int_gauge!(
                "fs_cache_branches",
                "Total amount of cache branches"
            )
            .unwrap(),
            fs_cache_branches_allocated: register_int_gauge!(
                "fs_cache_branches_allocated",
                "Amount of cache branches allocated"
            )
            .unwrap(),
            fs_cache_entries: register_int_gauge!(
                "fs_cache_entries",
                "Amount of memory cache entries"
            )
            .unwrap(),
            fs_cache_blocks: register_int_gauge!(
                "fs_cache_blocks",
                "Amount of blocks in the memory cache"
            )
            .unwrap(),
            fs_cache_blocksize: register_int_gauge!(
                "fs_cache_blocksize",
                "Amount of bytes used by cache blocks"
            )
            .unwrap(),
            fs_syscalls: register_int_gauge_vec!(
                "fs_syscalls",
                "Total amount of syscalls done on the filesystem",
                &["syscall"]
            )
            .unwrap(),
            fs_bytes_read: register_int_gauge!(
                "fs_bytes_read",
                "Total amount of bytes read from the filessytem"
            )
            .unwrap(),
            fs_bytes_written: register_int_gauge!(
                "fs_bytes_written",
                "Total amount of bytes written to the filessytem"
            )
            .unwrap(),
            fs_fuse_errors: register_int_gauge!(
                "fs_fuse_errors",
                "Total amount of errors returned by fuse calls"
            )
            .unwrap(),
        }
    }
}

impl Default for MetricsActor {
    fn default() -> Self {
        Self::new()
    }
}

/// Message updating the status of a data 0-db backend.
#[derive(Message)]
#[rtype(result = "()")]
pub struct SetDataBackendInfo {
    /// Info identifying the backend.
    pub ci: ZdbConnectionInfo,
    /// The backend stats. If this is None, the backend is removed.
    pub info: Option<NsInfo>,
}

/// Message updating the status of a meta 0-db backend.
#[derive(Message)]
#[rtype(result = "()")]
pub struct SetMetaBackendInfo {
    /// Info identifying the backend.
    pub ci: ZdbConnectionInfo,
    /// The backend stats. If this is None, the backend is removed.
    pub info: Option<NsInfo>,
}

/// Message requesting exported metrics.
#[derive(Message)]
#[rtype(result = "Result<String, MetricsError>")]
pub struct GetPrometheusMetrics;

/// Message updating the amount of finished zstor commands.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ZstorCommandFinsihed {
    /// The command which finished.
    pub id: ZstorCommandId,
    /// Whether the command finished successfully or not.
    pub success: bool,
}

/// Message updating the stats of a monitored 0-db-fs.
#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateZdbFsStats {
    /// The stats of the 0-db-fs.
    pub stats: stats_t,
}

impl Actor for MetricsActor {
    type Context = Context<Self>;
}

impl Handler<SetDataBackendInfo> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: SetDataBackendInfo, _: &mut Self::Context) -> Self::Result {
        if let Some(info) = msg.info {
            self.data_zdbs.insert(msg.ci, info);
        } else {
            self.data_zdbs.remove(&msg.ci);
            self.removed_zdbs.push(msg.ci);
        }
    }
}

impl Handler<SetMetaBackendInfo> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: SetMetaBackendInfo, _: &mut Self::Context) -> Self::Result {
        if let Some(info) = msg.info {
            self.meta_zdbs.insert(msg.ci, info);
        } else {
            self.meta_zdbs.remove(&msg.ci);
            self.removed_zdbs.push(msg.ci);
        }
    }
}

impl Handler<ZstorCommandFinsihed> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: ZstorCommandFinsihed, _: &mut Self::Context) -> Self::Result {
        if msg.success {
            *self.successful_zstor_commands.entry(msg.id).or_insert(0) += 1;
        } else {
            *self.failed_zstor_commands.entry(msg.id).or_insert(0) += 1;
        }
    }
}

impl Handler<UpdateZdbFsStats> for MetricsActor {
    type Result = ();

    fn handle(&mut self, msg: UpdateZdbFsStats, _: &mut Self::Context) -> Self::Result {
        self.zdbfs_stats = msg.stats;
    }
}

impl Handler<GetPrometheusMetrics> for MetricsActor {
    type Result = Result<String, MetricsError>;

    fn handle(&mut self, _: GetPrometheusMetrics, _: &mut Self::Context) -> Self::Result {
        // Update metrics.
        //
        // Remove outdated 0-db stats.
        if !self.removed_zdbs.is_empty() {
            // Take ownerhsip of the removed zdb list, and leave an empty (default) list in its
            // place.
            let removed_zdbs = mem::take(&mut self.removed_zdbs);
            for ci in removed_zdbs {
                let mut labels = HashMap::new();
                labels.insert("namespace", ci.namespace().unwrap_or(""));
                let address = ci.address().to_string();
                labels.insert("address", &address);
                if self.data_zdbs.contains_key(&ci) {
                    labels.insert("backend_type", BACKEND_TYPE_DATA);
                } else {
                    labels.insert("backend_type", BACKEND_TYPE_META);
                }

                if let Err(e) = self.prom_metrics.entries_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.data_size_bytes_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.data_limit_bytes_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.index_size_bytes_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.index_io_errors_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.index_faults_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.data_io_errors_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self.prom_metrics.data_faults_gauges.remove(&labels) {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self
                    .prom_metrics
                    .index_disk_freespace_bytes_gauges
                    .remove(&labels)
                {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
                if let Err(e) = self
                    .prom_metrics
                    .data_disk_freespace_bytes_gauges
                    .remove(&labels)
                {
                    warn!("Failed to delete removed metric by label: {}", e)
                };
            }
        }

        // Update backend 0-db stats.
        for (ci, (info, backend_type)) in self
            .data_zdbs
            .iter()
            .map(|(ci, info)| (ci, (info, BACKEND_TYPE_DATA)))
            .chain(
                self.meta_zdbs
                    .iter()
                    .map(|(ci, info)| (ci, (info, BACKEND_TYPE_META))),
            )
        {
            let mut labels = HashMap::new();
            labels.insert("namespace", ci.namespace().unwrap_or(""));
            let address = ci.address().to_string();
            labels.insert("address", &address);
            labels.insert("backend_type", backend_type);

            let entries_gauge = self.prom_metrics.entries_gauges.get_metric_with(&labels)?;
            let data_size_bytes_gauge = self
                .prom_metrics
                .data_size_bytes_gauges
                .get_metric_with(&labels)?;
            let data_limit_bytes_gauge = self
                .prom_metrics
                .data_limit_bytes_gauges
                .get_metric_with(&labels)?;
            let index_size_bytes_gauge = self
                .prom_metrics
                .index_size_bytes_gauges
                .get_metric_with(&labels)?;
            let index_io_errors_gauge = self
                .prom_metrics
                .index_io_errors_gauges
                .get_metric_with(&labels)?;
            let index_faults_gauge = self
                .prom_metrics
                .index_faults_gauges
                .get_metric_with(&labels)?;
            let data_io_errors_gauge = self
                .prom_metrics
                .data_io_errors_gauges
                .get_metric_with(&labels)?;
            let data_faults_gauge = self
                .prom_metrics
                .data_faults_gauges
                .get_metric_with(&labels)?;
            let index_disk_freespace_bytes_gauge = self
                .prom_metrics
                .index_disk_freespace_bytes_gauges
                .get_metric_with(&labels)?;
            let data_disk_freespace_bytes_gauge = self
                .prom_metrics
                .data_disk_freespace_bytes_gauges
                .get_metric_with(&labels)?;

            entries_gauge.set(info.entries as i64);
            data_size_bytes_gauge.set(info.data_size_bytes as i64);
            data_limit_bytes_gauge.set(
                info.data_limit_bytes
                    .unwrap_or(info.data_disk_freespace_bytes) as i64,
            );
            index_size_bytes_gauge.set(info.index_size_bytes as i64);
            index_io_errors_gauge.set(info.index_io_errors as i64);
            index_faults_gauge.set(info.index_faults as i64);
            data_io_errors_gauge.set(info.data_io_errors as i64);
            data_faults_gauge.set(info.data_faults as i64);
            index_disk_freespace_bytes_gauge.set(info.index_disk_freespace_bytes as i64);
            data_disk_freespace_bytes_gauge.set(info.data_disk_freespace_bytes as i64);
        }

        // Update zstor stats
        //
        // Successful calls
        let mut labels = HashMap::new();
        labels.insert("success", "true");

        self.prom_metrics
            .zstor_store_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .successful_zstor_commands
                    .get(&ZstorCommandId::Store)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_retrieve_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .successful_zstor_commands
                    .get(&ZstorCommandId::Retrieve)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_rebuild_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .successful_zstor_commands
                    .get(&ZstorCommandId::Rebuild)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_check_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .successful_zstor_commands
                    .get(&ZstorCommandId::Check)
                    .unwrap_or(&0) as i64,
            );

        // Failed calls
        let mut labels = HashMap::new();
        labels.insert("success", "false");
        self.prom_metrics
            .zstor_store_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .failed_zstor_commands
                    .get(&ZstorCommandId::Store)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_retrieve_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .failed_zstor_commands
                    .get(&ZstorCommandId::Retrieve)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_rebuild_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .failed_zstor_commands
                    .get(&ZstorCommandId::Rebuild)
                    .unwrap_or(&0) as i64,
            );
        self.prom_metrics
            .zstor_check_commands_finished_gauges
            .get_metric_with(&labels)?
            .set(
                *self
                    .failed_zstor_commands
                    .get(&ZstorCommandId::Check)
                    .unwrap_or(&0) as i64,
            );

        // 0-db-fs info
        self.prom_metrics
            .fs_fuse_reqs
            .set(self.zdbfs_stats.fuse_reqs as i64);
        self.prom_metrics
            .fs_cache_hits
            .set(self.zdbfs_stats.cache_hit as i64);
        self.prom_metrics
            .fs_cache_miss
            .set(self.zdbfs_stats.cache_miss as i64);
        self.prom_metrics
            .fs_cache_full
            .set(self.zdbfs_stats.cache_full as i64);
        self.prom_metrics
            .fs_cache_linear_flush
            .set(self.zdbfs_stats.cache_linear_flush as i64);
        self.prom_metrics
            .fs_cache_random_flush
            .set(self.zdbfs_stats.cache_random_flush as i64);
        self.prom_metrics
            .fs_bytes_read
            .set(self.zdbfs_stats.read_bytes as i64);
        self.prom_metrics
            .fs_bytes_written
            .set(self.zdbfs_stats.write_bytes as i64);
        self.prom_metrics
            .fs_fuse_errors
            .set(self.zdbfs_stats.errors as i64);
        self.prom_metrics
            .fs_cache_branches
            .set(self.zdbfs_stats.cache_branches as i64);
        self.prom_metrics
            .fs_cache_branches_allocated
            .set(self.zdbfs_stats.cache_branches_allocated as i64);
        self.prom_metrics
            .fs_cache_entries
            .set(self.zdbfs_stats.cache_entries as i64);
        self.prom_metrics
            .fs_cache_blocks
            .set(self.zdbfs_stats.cache_blocks as i64);
        self.prom_metrics
            .fs_cache_blocksize
            .set(self.zdbfs_stats.cache_blocksize as i64);
        // set syscall info
        let mut labels = HashMap::new();
        labels.insert("syscall", "getattr");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_getattr as i64);
        labels.insert("syscall", "setattr");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_setattr as i64);
        labels.insert("syscall", "create");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_create as i64);
        labels.insert("syscall", "readdir");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_readdir as i64);
        labels.insert("syscall", "open");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_open as i64);
        labels.insert("syscall", "read");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_read as i64);
        labels.insert("syscall", "write");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_write as i64);
        labels.insert("syscall", "mkdir");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_mkdir as i64);
        labels.insert("syscall", "unlink");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_unlink as i64);
        labels.insert("syscall", "rmdir");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_rmdir as i64);
        labels.insert("syscall", "rename");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_rename as i64);
        labels.insert("syscall", "link");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_link as i64);
        labels.insert("syscall", "symlink");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_symlink as i64);
        labels.insert("syscall", "statsfs");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_statsfs as i64);
        labels.insert("syscall", "ioctl");
        self.prom_metrics
            .fs_syscalls
            .get_metric_with(&labels)?
            .set(self.zdbfs_stats.syscall_ioctl as i64);

        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();

        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer)?)
    }
}

/// Possible zstor commands.
#[derive(Hash, PartialEq, Eq)]
pub enum ZstorCommandId {
    /// Store command.
    Store,
    /// Retrieve command.
    Retrieve,
    /// Rebuild command.
    Rebuild,
    /// Check command.
    Check,
}

impl fmt::Display for ZstorCommandId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ZstorCommandId::Store => "Store",
                ZstorCommandId::Retrieve => "Retrieve",
                ZstorCommandId::Rebuild => "Rebuild",
                ZstorCommandId::Check => "Check",
            }
        )
    }
}

/// An error that can be generated when collecting metrics.
#[derive(Debug)]
pub enum MetricsError {
    /// Could not expose metrics in text format.
    Utf8Error(FromUtf8Error),
    /// An error in the prometheus library.
    Prometheus(prometheus::Error),
}

impl From<FromUtf8Error> for MetricsError {
    fn from(e: FromUtf8Error) -> Self {
        MetricsError::Utf8Error(e)
    }
}

impl From<prometheus::Error> for MetricsError {
    fn from(e: prometheus::Error) -> Self {
        MetricsError::Prometheus(e)
    }
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MetricsError::Utf8Error(ref e) => e as &dyn fmt::Display,
                MetricsError::Prometheus(ref e) => e,
            }
        )
    }
}

impl std::error::Error for MetricsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MetricsError::Utf8Error(ref e) => Some(e),
            MetricsError::Prometheus(ref e) => Some(e),
        }
    }
}
