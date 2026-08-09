//! Byte-exact codec for `__remote_log_metadata` topic records, from KIP-405.
//!
//! The codec is interoperable with the JVM `RemoteLogMetadataSerde`.

pub mod record;

pub use record::RemoteLogMetadataRecord;
