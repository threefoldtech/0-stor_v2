/// Actor implementation of the configuration manager
pub mod config;
/// Actor implementation of the [`MetaStore`](crate::meta::MetaStore)
pub mod meta;
/// Actor implementation of the pipeline
pub mod pipeline;
/// Actor implementation of the main zstor functionality. This actor takes care of the actual
/// object manipulation.
pub mod zstor;
