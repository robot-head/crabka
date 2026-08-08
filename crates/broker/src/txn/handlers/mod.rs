//! Wire handlers for the transaction protocol APIs.
//!
//! Each sub-module implements the `handle` function for one `api_key`.
//! `crate::handlers::build_table`, in the top-level handlers module,
//! registers them. This module only declares the sub-modules.

pub(crate) mod add_offset_commits_to_txn;
pub(crate) mod add_partitions_to_txn;
pub(crate) mod end_txn;
pub(crate) mod txn_offset_commit;
pub(crate) mod write_txn_markers;
