//! Registry-backed range endpoint discovery.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use crabka_gres_control::{RangeLayoutEntry, TenantRecord, TenantRegistryStore};
use tokio::sync::RwLock;

use crate::{RangeId, TableId};

/// Parsed endpoint placement for one range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeEndpoint {
    /// Range identifier.
    pub range_id: RangeId,
    /// Exclusive key upper bound for this range.
    pub end_key: Option<crate::RangeKey>,
    /// Host:port used by range-forwarding RPC.
    pub endpoint: String,
    /// Per-range WAL generation from the control registry.
    pub wal_generation: u64,
}

/// Registry-discovered range handles keyed by range id.
#[derive(Clone, Default)]
pub struct RangeRegistry {
    ranges: Arc<RwLock<BTreeMap<RangeId, RangeEndpoint>>>,
    authoritative_source: Option<Arc<dyn RangeRegistrySource>>,
}

impl std::fmt::Debug for RangeRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RangeRegistry")
            .field("authoritative_source", &self.authoritative_source.is_some())
            .finish_non_exhaustive()
    }
}

/// Supplies the current authoritative tenant layout for retry re-resolution.
#[async_trait]
pub trait RangeRegistrySource: Send + Sync {
    /// Load the latest tenant record from the authoritative control plane.
    async fn load_current(&self) -> Result<TenantRecord, RegistryError>;
}

impl RangeRegistry {
    /// Build an empty range registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry snapshot from one control-plane tenant record.
    pub fn from_tenant_record(record: &TenantRecord) -> Result<Self, RegistryError> {
        let ranges = range_endpoints_from_layout(&record.ranges);
        Ok(Self {
            ranges: Arc::new(RwLock::new(ranges)),
            authoritative_source: None,
        })
    }

    /// Attach the authoritative source used before a stale-endpoint retry.
    #[must_use]
    pub fn with_authoritative_source(mut self, source: Arc<dyn RangeRegistrySource>) -> Self {
        self.authoritative_source = Some(source);
        self
    }

    /// Refresh this registry from its authoritative source.
    pub async fn refresh_authoritatively(&self) -> Result<(), RegistryError> {
        let Some(source) = &self.authoritative_source else {
            return Err(RegistryError::NoAuthoritativeSource);
        };
        let record = source.load_current().await?;
        self.refresh_from_tenant_record(&record).await
    }

    /// Refresh this registry from a control-plane tenant record.
    pub async fn refresh_from_tenant_record(
        &self,
        record: &TenantRecord,
    ) -> Result<(), RegistryError> {
        let ranges = range_endpoints_from_layout(&record.ranges);
        *self.ranges.write().await = ranges;
        Ok(())
    }

    /// Refresh this registry from a [`TenantRegistryStore`] lookup.
    pub async fn refresh_from_store<S>(
        &self,
        store: &S,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<(), RegistryError>
    where
        S: TenantRegistryStore + Sync,
    {
        let Some(record) = store.get(tenant) else {
            return Err(RegistryError::TenantMissing(tenant.as_str().to_string()));
        };
        self.refresh_from_tenant_record(&record).await
    }

    /// Resolve the current endpoint for one range.
    pub async fn resolve(&self, range_id: RangeId) -> Result<RangeEndpoint, RegistryError> {
        let Some(endpoint) = self.ranges.read().await.get(&range_id).cloned() else {
            return Err(RegistryError::RangeMissing(range_id));
        };
        Ok(endpoint)
    }

    /// Return the currently known range ids in deterministic order.
    pub async fn range_ids(&self) -> Vec<RangeId> {
        self.ranges.read().await.keys().copied().collect()
    }
}

/// Registry discovery failures.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A retry needed a fresh control-plane layout but this registry has no source.
    #[error("range registry has no authoritative refresh source")]
    NoAuthoritativeSource,
    /// The authoritative control-plane lookup failed.
    #[error("authoritative range registry refresh failed: {0}")]
    Authoritative(String),
    /// Tenant was absent from the control registry.
    #[error("tenant {0} is absent from the registry")]
    TenantMissing(String),
    /// Requested range had no endpoint in the layout.
    #[error("range r{0} is absent from the registry layout")]
    RangeMissing(RangeId),
    /// Range id cannot be represented locally.
    #[error("range id {0} is too large")]
    RangeIdTooLarge(u32),
    /// Table bound cannot be represented locally.
    #[error("table bound {0} is too large")]
    TableIdTooLarge(u64),
}

fn range_endpoints_from_layout(layout: &[RangeLayoutEntry]) -> BTreeMap<RangeId, RangeEndpoint> {
    let mut ranges = BTreeMap::new();
    for entry in layout {
        let range_id = RangeId::new(entry.range_id);
        let end_key = entry
            .end_key
            .map(|key| crate::RangeKey::new(TableId::new(key.table_id), key.rowid));
        ranges.insert(
            range_id,
            RangeEndpoint {
                range_id,
                end_key,
                endpoint: entry.endpoint.clone(),
                wal_generation: entry.wal_generation,
            },
        );
    }
    ranges
}

#[cfg(test)]
mod tests {
    use crabka_gres_control::{
        InMemoryRegistryStore, RangeLayoutEntry, SqlUser, TenantId, TenantName, TenantRecord,
        TenantRegistryStore, TenantState,
    };

    use super::*;

    fn tenant_record() -> TenantRecord {
        TenantRecord::new(
            1,
            TenantId::try_from("tenant-a").unwrap(),
            TenantName::try_from("tenant-a").unwrap(),
            TenantState::Active,
            SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .unwrap()
        .with_range_layout(vec![
            RangeLayoutEntry {
                range_id: 0,
                end_key: Some(crabka_gres_control::RangeBoundary::new(10, 25)),
                endpoint: "127.0.0.1:7000".to_string(),
                wal_generation: 1,
                lifecycle: Default::default(),
                retirement: None,
            },
            RangeLayoutEntry {
                range_id: 1,
                end_key: None,
                endpoint: "127.0.0.1:7001".to_string(),
                wal_generation: 4,
                lifecycle: Default::default(),
                retirement: None,
            },
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn discovery_resolves_range_endpoints_from_registry_layout() {
        let registry = RangeRegistry::from_tenant_record(&tenant_record()).unwrap();

        let endpoint = registry.resolve(RangeId::new(1)).await.unwrap();

        assert_eq!(endpoint.endpoint, "127.0.0.1:7001");
        assert_eq!(endpoint.wal_generation, 4);
        assert_eq!(endpoint.end_key, None);
    }

    #[tokio::test]
    async fn discovery_refreshes_from_registry_store() {
        let mut store = InMemoryRegistryStore::new();
        let record = tenant_record();
        let tenant = record.name.clone();
        store.upsert(record).unwrap();
        let registry = RangeRegistry::new();

        registry.refresh_from_store(&store, &tenant).await.unwrap();

        assert_eq!(
            registry
                .resolve(RangeId::COORDINATOR)
                .await
                .unwrap()
                .endpoint,
            "127.0.0.1:7000"
        );
    }
}
