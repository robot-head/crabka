//! WAL topic naming and ensure abstraction.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClientLike, CreateTopicSpec};
use crabka_gres_ranges::{
    RangeId, TenantName, txn_id as range_txn_id, wal_topic as range_wal_topic,
};

use crate::error::SubstrateError;

/// Partition count for each range WAL topic.
pub const WAL_TOPIC_PARTITIONS: i32 = 1;
/// Default replication factor requested for the WAL topic.
pub const WAL_TOPIC_REPLICAS: i32 = 1;
/// Timeout used by the admin ensure path.
pub const WAL_TOPIC_ENSURE_TIMEOUT_MS: i32 = 30_000;
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Return the legacy range-neutral WAL topic for a tenant.
#[must_use]
pub fn wal_topic(tenant: &str) -> String {
    format!("__gres_wal.{tenant}")
}

/// Return the G-7 range WAL topic for a tenant range.
#[must_use]
pub fn wal_topic_for_range(tenant: &TenantName, range: RangeId) -> String {
    range_wal_topic(tenant, range).to_string()
}

/// Return the immutable physical WAL topic for one lifecycle generation.
/// Generation zero preserves the original name; recreated generations get a
/// distinct topic so a zombie admin request can only prune its old log.
#[must_use]
pub fn wal_topic_for_generation(
    tenant: &TenantName,
    range: RangeId,
    wal_generation: u64,
) -> String {
    let base = wal_topic_for_range(tenant, range);
    if wal_generation == 0 {
        base
    } else {
        format!("{base}.g{wal_generation:010}")
    }
}

/// Return the G-7 transactional producer id for a tenant range.
#[must_use]
pub fn transactional_id_for_range(tenant: &TenantName, range: RangeId) -> String {
    range_txn_id(tenant, range).to_string()
}

/// Narrow topic-admin seam needed by the substrate WAL setup path.
#[async_trait::async_trait]
pub trait TopicAdmin: Send {
    /// Return true when `topic` already exists and is usable.
    async fn topic_exists(&mut self, topic: &str) -> Result<bool, SubstrateError>;

    /// Create `topic` if the broker does not already have it.
    async fn create_wal_topic(&mut self, topic: &str) -> Result<(), SubstrateError>;
}

#[async_trait::async_trait]
impl<T> TopicAdmin for T
where
    T: AdminClientLike + Send,
{
    async fn topic_exists(&mut self, topic: &str) -> Result<bool, SubstrateError> {
        let metadata = self
            .metadata(&[topic])
            .await
            .map_err(|error| SubstrateError::Topic(error.to_string()))?;
        Ok(metadata
            .topics
            .iter()
            .any(|entry| entry.name == topic && entry.error.is_none()))
    }

    async fn create_wal_topic(&mut self, topic: &str) -> Result<(), SubstrateError> {
        let specs = [CreateTopicSpec {
            name: topic.to_string(),
            partitions: WAL_TOPIC_PARTITIONS,
            replicas: WAL_TOPIC_REPLICAS,
            configs: BTreeMap::from([
                ("cleanup.policy".to_string(), "delete".to_string()),
                ("retention.ms".to_string(), "-1".to_string()),
            ]),
        }];
        let outcomes = self
            .create_topics(&specs, WAL_TOPIC_ENSURE_TIMEOUT_MS)
            .await
            .map_err(|error| SubstrateError::Topic(error.to_string()))?;
        let failed = outcomes.iter().find(|outcome| {
            outcome.name == topic
                && outcome
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code != TOPIC_ALREADY_EXISTS)
        });
        if let Some(outcome) = failed {
            return Err(SubstrateError::Topic(format!(
                "create topic {topic}: {:?}",
                outcome.error
            )));
        }
        Ok(())
    }
}

/// Ensure a tenant WAL topic exists.
pub async fn ensure_wal_topic(
    admin: &mut dyn TopicAdmin,
    tenant: &str,
) -> Result<String, SubstrateError> {
    let topic = wal_topic(tenant);
    if admin.topic_exists(&topic).await? {
        return Ok(topic);
    }
    admin.create_wal_topic(&topic).await?;
    Ok(topic)
}

/// Ensure a G-7 tenant-range WAL topic exists.
pub async fn ensure_wal_topic_for_range(
    admin: &mut dyn TopicAdmin,
    tenant: &TenantName,
    range: RangeId,
) -> Result<String, SubstrateError> {
    let topic = wal_topic_for_range(tenant, range);
    if admin.topic_exists(&topic).await? {
        return Ok(topic);
    }
    admin.create_wal_topic(&topic).await?;
    Ok(topic)
}

/// Ensure an already-derived immutable physical WAL topic exists.
pub async fn ensure_wal_topic_name(
    admin: &mut dyn TopicAdmin,
    topic: &str,
) -> Result<String, SubstrateError> {
    if !admin.topic_exists(topic).await? {
        admin.create_wal_topic(topic).await?;
    }
    Ok(topic.to_owned())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[derive(Default)]
    struct FakeTopicAdmin {
        existing: bool,
        creates: usize,
        checked: Vec<String>,
        created: Vec<String>,
    }

    #[async_trait::async_trait]
    impl TopicAdmin for FakeTopicAdmin {
        async fn topic_exists(&mut self, topic: &str) -> Result<bool, SubstrateError> {
            self.checked.push(topic.to_string());
            Ok(self.existing)
        }

        async fn create_wal_topic(&mut self, topic: &str) -> Result<(), SubstrateError> {
            self.creates += 1;
            self.existing = true;
            self.created.push(topic.to_string());
            Ok(())
        }
    }

    #[test]
    fn wal_topic_is_range_neutral() {
        assert!(wal_topic("tenant-a") == "__gres_wal.tenant-a");
    }

    #[test]
    fn range_wal_topic_and_transactional_id_use_g7_names() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");

        assert!(wal_topic_for_range(&tenant, RangeId::COORDINATOR) == "__gres_wal.tenant-a.r0");
        assert!(wal_topic_for_range(&tenant, RangeId::new(7)) == "__gres_wal.tenant-a.r7");
        assert!(transactional_id_for_range(&tenant, RangeId::COORDINATOR) == "__gres.tenant-a.r0");
        assert!(transactional_id_for_range(&tenant, RangeId::new(7)) == "__gres.tenant-a.r7");
        assert!(
            transactional_id_for_range(&tenant, RangeId::COORDINATOR)
                != transactional_id_for_range(&tenant, RangeId::new(7))
        );
        assert!(
            wal_topic_for_generation(&tenant, RangeId::COORDINATOR, 0) == "__gres_wal.tenant-a.r0"
        );
        assert!(
            wal_topic_for_generation(&tenant, RangeId::COORDINATOR, 7)
                == "__gres_wal.tenant-a.r0.g0000000007"
        );
    }

    #[tokio::test]
    async fn ensure_wal_topic_creates_missing_topic() {
        let mut admin = FakeTopicAdmin::default();

        let topic = ensure_wal_topic(&mut admin, "t1").await.expect("ensure");

        assert!(topic == "__gres_wal.t1");
        assert!(admin.creates == 1);
    }

    #[tokio::test]
    async fn ensure_wal_topic_for_range_creates_only_that_range_topic() {
        let mut admin = FakeTopicAdmin::default();
        let tenant = TenantName::parse("t1").expect("tenant");

        let topic = ensure_wal_topic_for_range(&mut admin, &tenant, RangeId::new(3))
            .await
            .expect("ensure");

        assert!(topic == "__gres_wal.t1.r3");
        assert!(admin.checked == vec!["__gres_wal.t1.r3".to_string()]);
        assert!(admin.created == vec!["__gres_wal.t1.r3".to_string()]);
    }
}
