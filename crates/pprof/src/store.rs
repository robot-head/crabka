//! Data-access seam for the profiles engine.

use std::sync::Arc;

use crabka_blockstore::LabelMatcher;
use datafusion::prelude::SessionContext;

use crate::{error::ProfileError, frame::SymbolSource};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileStats {
    pub data_ingested: bool,
    pub oldest_profile_time: Option<i64>,
    pub newest_profile_time: Option<i64>,
}

/// A selected samples table plus the symbol source that resolves its raw ids.
pub struct ProfileScan {
    pub ctx: SessionContext,
    pub samples_table: String,
    pub symbols: Arc<dyn SymbolSource>,
}

/// Resolves profile matchers to a `DataFusion` samples table over a tenant's data.
#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError>;

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError>;

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_blockstore::LabelMatcher;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::SymbolDb;

    struct Empty;

    #[async_trait::async_trait]
    impl ProfileStore for Empty {
        async fn select(
            &self,
            _tenant: &str,
            _profile_type: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ProfileScan, crate::ProfileError> {
            Ok(ProfileScan {
                ctx: SessionContext::new(),
                samples_table: "samples".to_string(),
                symbols: Arc::new(SymbolDb::new()),
            })
        }

        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn profile_types(
            &self,
            _tenant: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _label_names: &[String],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Vec<(String, String)>>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn stats(
            &self,
            _tenant: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ProfileStats, crate::ProfileError> {
            Ok(ProfileStats::default())
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let store: Arc<dyn ProfileStore> = Arc::new(Empty);
        let scan = store
            .select(
                "t",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                &[],
                0,
                1,
            )
            .await
            .unwrap();
        assert2::assert!(scan.samples_table == "samples");
        assert2::assert!(store.profile_types("t", 0, 1).await.unwrap().is_empty());
    }
}
