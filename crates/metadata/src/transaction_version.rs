//! KIP-890 `transaction.version` feature-level constants. Plain integer
//! feature: 0 = classic (KIP-98) non-flexible txn-state records; 1 = flexible
//! (tagged) txn-state records; 2 = epoch-bump-on-completion + server-side
//! `AddPartitionsToTxn` verification. Range + the metadata.version bootstrap
//! threshold are pinned against the cp-kafka 4.0 `TransactionVersion` enum
//! (verified empirically 2026-05-30; both `TV_1` and `TV_2` bootstrap at
//! 4.0-IV2).

pub const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";
pub const TRANSACTION_VERSION_MIN: i16 = 0;
pub const TRANSACTION_VERSION_MAX: i16 = 2;

/// metadata.version at/above which transaction.version becomes a bootstrap
/// default. Both `TV_1` and `TV_2` bootstrap at 4.0-IV2 (level 24), so the
/// per-release default jumps 0 -> 2 at level 24. Bootstrap-default input only,
/// NOT a hard `UpdateFeatures` dependency.
pub const TV1_METADATA_LEVEL: i16 = 24; // 4.0-IV2
pub const TV2_METADATA_LEVEL: i16 = 24; // 4.0-IV2
