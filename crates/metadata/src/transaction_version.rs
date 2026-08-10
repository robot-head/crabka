//! KIP-890 `transaction.version` feature-level constants. It is a plain integer
//! feature: 0 means classic (KIP-98) non-flexible txn-state records, 1 means
//! flexible (tagged) txn-state records, 2 means an epoch bump on completion
//! plus server-side `AddPartitionsToTxn` verification, and 3 enables the
//! KIP-939 two-phase-commit extensions. `TV_3` is opt-in: no released
//! metadata.version selects it as a bootstrap default. Both `TV_1` and `TV_2`
//! bootstrap at 4.0-IV2.

pub const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";
pub const TRANSACTION_VERSION_MIN: i16 = 0;
pub const TRANSACTION_VERSION_MAX: i16 = 3;

/// metadata.version at or above which transaction.version becomes a bootstrap
/// default. Both `TV_1` and `TV_2` bootstrap at 4.0-IV2 (level 24), so the
/// per-release default jumps 0 -> 2 at level 24. This is a bootstrap-default
/// input only, NOT a hard `UpdateFeatures` dependency.
pub const TV1_METADATA_LEVEL: i16 = 24; // 4.0-IV2
pub const TV2_METADATA_LEVEL: i16 = 24; // 4.0-IV2
