//! KIP-890 transaction.version resolution. Reads the finalized
//! `transaction.version` from the live image and maps it to the behavior the
//! coordinator runs. Unfinalized (UNKNOWN) resolves to `Classic` — the safest
//! behavior for a pre-bootstrap / legacy image; a 4.0-formatted (or standalone
//! self-bootstrapped) cluster finalizes `TV_2`, so the common path is `Verified`.

use crabka_metadata::MetadataImage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnVersion {
    /// `TV_0`: classic (KIP-98), non-flexible `__transaction_state` records.
    Classic,
    /// `TV_1`: flexible (tagged) `__transaction_state` records.
    Flexible,
    /// `TV_2`: epoch bump on completion + server-side `AddPartitionsToTxn`
    /// verification (also flexible records).
    Verified,
}

impl TxnVersion {
    /// Flexible `__transaction_state` record format applies at `TV >= 1`.
    pub(crate) fn flexible_records(self) -> bool {
        matches!(self, TxnVersion::Flexible | TxnVersion::Verified)
    }
    /// Epoch-bump-on-completion + verify-only `AddPartitionsToTxn` apply at `TV_2`.
    pub(crate) fn verified(self) -> bool {
        matches!(self, TxnVersion::Verified)
    }
}

pub(crate) fn resolve_txn_version(image: &MetadataImage) -> TxnVersion {
    match image.finalized_feature(crabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE)
    {
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
        for (case, level, want) in [
            ("feature absent", None, TxnVersion::Classic),
            ("classic level", Some(0), TxnVersion::Classic),
            ("flexible level", Some(1), TxnVersion::Flexible),
            ("verified level", Some(2), TxnVersion::Verified),
        ] {
            assert!(
                resolve_txn_version(&image_with_tv(level)) == want,
                "case: {case}; level {level:?}"
            );
        }
    }

    #[test]
    fn behavior_predicates() {
        for (case, v, want_flexible, want_verified) in [
            ("classic behavior", TxnVersion::Classic, false, false),
            ("flexible behavior", TxnVersion::Flexible, true, false),
            ("verified behavior", TxnVersion::Verified, true, true),
        ] {
            assert!(
                v.flexible_records() == want_flexible,
                "case: {case}; {v:?} flexible_records"
            );
            assert!(
                v.verified() == want_verified,
                "case: {case}; {v:?} verified"
            );
        }
    }
}
