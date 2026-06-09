//! Byte-exact codec for `__remote_log_metadata` topic records (KIP-405),
//! interoperable with the JVM `RemoteLogMetadataSerde`.

pub mod record;

pub use record::RemoteLogMetadataRecord;
