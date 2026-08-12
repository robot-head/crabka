//! pgkv: ordered key-value storage with an order-preserving key encoding and
//! a versioned row-value encoding.
//!
//! It is the permanent storage seam for Crabka Gres.

#![doc(html_root_url = "https://docs.rs/crabka-pgkv/0.4.0")]

pub mod error;
pub mod fjall_store;
pub mod key;
pub mod keyenc;
pub mod notify_record;
pub mod rowenc;
pub mod store;

pub use error::KvError;
pub use fjall_store::{FjallKv, FjallOptions, KeyspaceKv, RotateAfterOps};
pub use notify_record::{NOTIFY_RECORD_VERSION, NotifyRecord, is_notify_op};
pub use store::{Kv, KvPair, KvScan, KvSnapshot, MemKv, RestoreKv, SnapshotKv, WriteOp};
