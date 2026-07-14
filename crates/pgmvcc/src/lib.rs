//! mvcc: PostgreSQL-faithful multiversion concurrency control for Crabka Gres —
//! xids, the clog (`pg_xact`), xid-keyed tuple (xmin/xmax) encoding, xid-list
//! snapshots, and `HeapTupleSatisfiesMVCC` visibility. Concurrent writers (row
//! locks, block-and-retry, `EvalPlanQual`) arrive in SP6; deadlock detection SP7.

#![doc(html_root_url = "https://docs.rs/crabka-pgmvcc/0.3.9")]

pub mod clog;
pub mod gc;
pub mod version;
pub mod visibility;
pub mod xid;

pub use visibility::{Snapshot, satisfies_mvcc};
pub use xid::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID, Xid};
