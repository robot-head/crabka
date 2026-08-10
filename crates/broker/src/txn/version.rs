//! KIP-890 transaction.version resolution.
//!
//! This module reads the finalized `transaction.version` from the live image
//! and maps it to the behavior the coordinator runs. An unfinalized, that is
//! UNKNOWN, version resolves to `Classic`, the safest behavior for a
//! pre-bootstrap or legacy image. A 4.0-formatted cluster, or a standalone
//! self-bootstrapped one, finalizes `TV_2`, so the common path is `Verified`.
//! `TV_3` is an explicit opt-in for KIP-939 two-phase-commit participation.

use crabka_metadata::MetadataImage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnVersion {
    /// `TV_0`: classic (KIP-98), non-flexible `__transaction_state` records.
    Classic,
    /// `TV_1`: flexible (tagged) `__transaction_state` records.
    Flexible,
    /// `TV_2`: an epoch bump on completion and server-side
    /// `AddPartitionsToTxn` verification. It also uses flexible records.
    Verified,
    /// `TV_3`: KIP-939 two-phase-commit participation. It includes all `TV_2`
    /// behavior and enables prepared-transaction recovery.
    TwoPhase,
}

impl TxnVersion {
    /// Flexible `__transaction_state` record format applies at `TV >= 1`.
    pub(crate) fn flexible_records(self) -> bool {
        matches!(
            self,
            TxnVersion::Flexible | TxnVersion::Verified | TxnVersion::TwoPhase
        )
    }
    /// The epoch bump on completion and the verify-only `AddPartitionsToTxn`
    /// both apply at `TV >= 2`.
    pub(crate) fn verified(self) -> bool {
        matches!(self, TxnVersion::Verified | TxnVersion::TwoPhase)
    }
    /// KIP-939 request fields apply only at opt-in `TV_3`.
    pub(crate) fn two_phase(self) -> bool {
        matches!(self, TxnVersion::TwoPhase)
    }
}

pub(crate) fn resolve_txn_version(image: &MetadataImage) -> TxnVersion {
    match image.finalized_feature(crabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE)
    {
        Some(3) => TxnVersion::TwoPhase,
        Some(2) => TxnVersion::Verified,
        Some(1) => TxnVersion::Flexible,
        _ => TxnVersion::Classic,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};

    use super::*;

    fn image_with_tv(level: Option<i16>) -> MetadataImage {
        let mut m = MetadataImage::new(uuid::Uuid::nil());
        if let Some(l) = level {
            m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "transaction.version".into(),
                level: l,
            }));
        }
        m
    }

    #[test]
    fn resolves_levels() {
        for (level, want) in [
            (None, TxnVersion::Classic),
            (Some(0), TxnVersion::Classic),
            (Some(1), TxnVersion::Flexible),
            (Some(2), TxnVersion::Verified),
            (Some(3), TxnVersion::TwoPhase),
        ] {
            assert!(
                resolve_txn_version(&image_with_tv(level)) == want,
                "{level:?}"
            );
        }
    }

    #[test]
    fn behavior_predicates() {
        for (v, want_flexible, want_verified, want_two_phase) in [
            (TxnVersion::Classic, false, false, false),
            (TxnVersion::Flexible, true, false, false),
            (TxnVersion::Verified, true, true, false),
            (TxnVersion::TwoPhase, true, true, true),
        ] {
            assert!(
                v.flexible_records() == want_flexible,
                "{v:?} flexible_records"
            );
            assert!(v.verified() == want_verified, "{v:?} verified");
            assert!(v.two_phase() == want_two_phase, "{v:?} two_phase");
        }
    }
}
