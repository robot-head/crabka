//! Formally verified pure kernels shared by Crabka's consensus and log crates.
//!
//! Every function here is a total, synchronous, allocation-light kernel whose
//! functional contract is proven with Creusot (see `docs/verification.md`).
//! Host crates call through - there are no duplicate bodies anywhere.
#![doc(html_root_url = "https://docs.rs/crabka-verified/0.3.8")]

pub mod compaction;
pub mod consensus;
pub mod log_index;

pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{election_jitter_ms, log_is_up_to_date, recompute_high_watermark};
pub use log_index::offset_index_lookup;
