use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{RangeId, TenantName};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopicName(String);

impl TopicName {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransactionalId(String);

impl TransactionalId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TransactionalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointPrefix(String);

impl CheckpointPrefix {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for CheckpointPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[must_use]
pub fn wal_topic(tenant: &TenantName, range: RangeId) -> TopicName {
    TopicName(format!("__gres_wal.{tenant}.r{range}"))
}

#[must_use]
pub fn txn_id(tenant: &TenantName, range: RangeId) -> TransactionalId {
    TransactionalId(format!("__gres.{tenant}.r{range}"))
}

#[must_use]
pub fn checkpoint_prefix(tenant: &TenantName, range: RangeId) -> CheckpointPrefix {
    CheckpointPrefix(format!("gres/{tenant}/r{range}/ckpt/"))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn range_zero_names_are_stable() {
        let tenant = TenantName::parse("tenant_a").unwrap();

        assert!(wal_topic(&tenant, RangeId::COORDINATOR).as_str() == "__gres_wal.tenant_a.r0");
        assert!(txn_id(&tenant, RangeId::COORDINATOR).as_str() == "__gres.tenant_a.r0");
        assert!(
            checkpoint_prefix(&tenant, RangeId::COORDINATOR).as_str() == "gres/tenant_a/r0/ckpt/"
        );
    }

    #[test]
    fn data_range_names_are_stable() {
        let tenant = TenantName::parse("tenant_a").unwrap();

        assert!(wal_topic(&tenant, RangeId::new(7)).as_str() == "__gres_wal.tenant_a.r7");
        assert!(txn_id(&tenant, RangeId::new(7)).as_str() == "__gres.tenant_a.r7");
        assert!(checkpoint_prefix(&tenant, RangeId::new(7)).as_str() == "gres/tenant_a/r7/ckpt/");
    }
}
