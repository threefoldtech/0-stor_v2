/// Actor to manage multiple 0-db backends. The actor periodically checks the state of the
/// connection, and capacity of the db. If a connection is down for some time, or the 0-db runs out
/// of space, there is an attempt to reserve a new one.
pub mod backends;
/// Actor implementation of the configuration manager
pub mod config;
/// Actor implementation of a monitor that keeps the total size of all files in a directory below a
/// certain threshold by deleting the least recently accessed one if the size exceeds the limit.
pub mod dir_monitor;
/// Actor implementation of an explorer client. The actor takes care of managing capacity pools
/// automatically.
pub mod explorer;
/// Actor implementation of the [`MetaStore`](crate::meta::MetaStore)
pub mod meta;
/// Actor implementation of a metric collector.
pub mod metrics;
/// Actor implementation of the pipeline
pub mod pipeline;
/// Priority queue implementation
pub mod priority_queue;
/// Actor implementation of a repair queue.
pub mod repairer;
/// Actor which periodically gets stats from a 0-db-fs.
pub mod zdbfs;
/// Actor implementation of the main zstor functionality. This actor takes care of the actual
/// object manipulation.
pub mod zstor;
/// Forwards the commands to the store actors after ordering them.
pub mod zstor_scheduler;
