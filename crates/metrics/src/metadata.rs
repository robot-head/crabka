//! Per-tenant metric metadata index built from compacted WAL rows.

use std::collections::{BTreeMap, BTreeSet};

use crate::TenantCompactionRows;

/// Metric metadata entry served by Prometheus-compatible metadata APIs.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricMetadata {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// Tenant-scoped metric metadata lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataIndex {
    by_tenant: BTreeMap<String, Vec<MetricMetadata>>,
}

impl MetadataIndex {
    /// Builds a deterministic metadata index from compacted tenant rows.
    #[must_use]
    pub fn from_compaction_rows(rows: &[TenantCompactionRows]) -> Self {
        let mut by_tenant = BTreeMap::<String, BTreeSet<MetricMetadata>>::new();
        for tenant_rows in rows {
            let tenant_metadata = by_tenant.entry(tenant_rows.tenant.clone()).or_default();
            for row in &tenant_rows.metadata_rows {
                tenant_metadata.insert(MetricMetadata {
                    metric_family_name: row.metric_family_name.clone(),
                    metric_type: row.metric_type.clone(),
                    help: row.help.clone(),
                    unit: row.unit.clone(),
                });
            }
        }

        Self {
            by_tenant: by_tenant
                .into_iter()
                .map(|(tenant, metadata)| (tenant, metadata.into_iter().collect()))
                .collect(),
        }
    }

    /// Returns metadata for `tenant`. An optional argument restricts it to one
    /// metric family.
    #[must_use]
    pub fn metadata(&self, tenant: &str, metric: Option<&str>) -> Vec<MetricMetadata> {
        self.by_tenant
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|record| metric.is_none_or(|metric| metric == record.metric_family_name))
            .cloned()
            .collect()
    }
}
