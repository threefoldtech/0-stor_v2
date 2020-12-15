#![deny(missing_docs)]

//! This crate contains the main implementations for the rstor library. This includes utilities to
//! write to 0-dbs, compress, encrypt and erasure code data, and write to etc. There are also
//! config structs available.

/// Contains a general compression interface and implementations.
pub mod compression;
/// Contains global configuration details.
pub mod config;
/// Contains a general encryption interface and implementations.
pub mod encryption;
/// Contains a general erasure encoder.
pub mod erasure;
/// A small etcd cluster client to load and set metadata.
pub mod etcd;
/// Metadata for stored shards after encoding
pub mod meta;
/// Very basic 0-db client, allowing to connect to a given (password protected) namespace, and
/// write and read data from it.
pub mod zdb;
