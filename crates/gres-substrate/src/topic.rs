//! WAL topic naming and ensure abstraction.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClientLike, CreateTopicSpec};
use crabka_gres_ranges::{
    RangeId, TenantName, txn_id as range_txn_id, wal_topic as range_wal_topic,
};
use crabka_units::{Time, convert::TimeExt as _, fmt::Human as _, secs};
use refined_type::rule::{GreaterI32, GreaterU64, MinMaxI32};

use crate::error::SubstrateError;

/// Partition count for each range WAL topic.
pub const WAL_TOPIC_PARTITIONS: i32 = 1;
/// Default replication factor requested for the WAL topic.
pub const DEFAULT_WAL_TOPIC_REPLICATION_FACTOR: i32 = 1;
/// Compatibility alias for the default WAL topic replication factor.
pub const WAL_TOPIC_REPLICAS: i32 = DEFAULT_WAL_TOPIC_REPLICATION_FACTOR;
/// Timeout used by the admin ensure path.
pub const DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT: Time = secs(30);
/// Compatibility alias for the default WAL topic ensure timeout.
pub const WAL_TOPIC_ENSURE_TIMEOUT: Time = DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT;
/// Default timeout for establishing a WAL admin connection.
pub const DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT: Time = secs(5);
/// Default timeout for WAL admin requests.
pub const DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT: Time = secs(30);
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Validated topic creation and admin connection settings for WAL recovery.
///
/// Not `Eq`: the timeout fields are `f64`-backed quantities.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WalAdminPolicy {
    replication_factor: i32,
    topic_ensure_timeout: Time,
    connect_timeout: Time,
    request_timeout: Time,
}

impl WalAdminPolicy {
    /// Validate WAL admin settings.
    ///
    /// The parameters keep their unit suffixes: they are the raw integers a CLI
    /// flag or CRD field carries, and this constructor is the seam where they
    /// become quantities.
    ///
    /// # Errors
    ///
    /// Returns an error when the replication factor is not in `1..=i16::MAX`
    /// or any other value is not positive.
    pub fn new(
        replication_factor: i32,
        topic_ensure_timeout_ms: i32,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            replication_factor: MinMaxI32::<1, { i16::MAX as i32 }>::new(replication_factor)
                .map_err(|error| error.to_string())?
                .into_value(),
            topic_ensure_timeout: Time::from_millis(i64::from(
                GreaterI32::<0>::new(topic_ensure_timeout_ms)
                    .map_err(|error| error.to_string())?
                    .into_value(),
            )),
            connect_timeout: validated_timeout(connect_timeout_ms)?,
            request_timeout: validated_timeout(request_timeout_ms)?,
        })
    }

    /// Return the requested WAL topic replication factor.
    #[must_use]
    pub const fn replication_factor(self) -> i32 {
        self.replication_factor
    }

    /// Return the topic ensure timeout.
    #[must_use]
    pub const fn topic_ensure_timeout(self) -> Time {
        self.topic_ensure_timeout
    }

    /// Return the WAL admin connection timeout.
    #[must_use]
    pub const fn connect_timeout(self) -> Time {
        self.connect_timeout
    }

    /// Return the WAL admin request timeout.
    #[must_use]
    pub const fn request_timeout(self) -> Time {
        self.request_timeout
    }
}

/// Validate a raw millisecond timeout and lift it into a time extent.
///
/// [`TimeExt::from_millis`] takes an `i64`, so a value past `i64::MAX`
/// milliseconds saturates rather than wrapping negative.
fn validated_timeout(milliseconds: u64) -> Result<Time, String> {
    GreaterU64::<0>::new(milliseconds)
        .map(|value| Time::from_millis(i64::try_from(value.into_value()).unwrap_or(i64::MAX)))
        .map_err(|error| error.to_string())
}

impl Default for WalAdminPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_WAL_TOPIC_REPLICATION_FACTOR,
            DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT.millis_i32(),
            millis_u64(DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT),
            millis_u64(DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT),
        )
        .expect("default WAL admin policy is valid")
    }
}

/// The raw millisecond count [`WalAdminPolicy::new`] validates, for a default
/// that is already a quantity.
fn millis_u64(timeout: Time) -> u64 {
    u64::try_from(timeout.millis_i64()).unwrap_or_default()
}

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

    /// Create `topic` with explicit replication and timeout settings.
    async fn create_wal_topic_with_policy(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<(), SubstrateError> {
        if replication_factor != WAL_TOPIC_REPLICAS || timeout != WAL_TOPIC_ENSURE_TIMEOUT {
            return Err(SubstrateError::Topic(format!(
                "unsupported WAL topic policy for legacy admin: replicas={replication_factor}, timeout={}",
                timeout.human()
            )));
        }
        self.create_wal_topic(topic).await
    }
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
        let policy = WalAdminPolicy::default();
        self.create_wal_topic_with_policy(
            topic,
            policy.replication_factor(),
            policy.topic_ensure_timeout(),
        )
        .await
    }

    async fn create_wal_topic_with_policy(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<(), SubstrateError> {
        let specs = [CreateTopicSpec {
            name: topic.to_string(),
            partitions: WAL_TOPIC_PARTITIONS,
            replicas: replication_factor,
            configs: BTreeMap::from([
                ("cleanup.policy".to_string(), "delete".to_string()),
                ("retention.ms".to_string(), "-1".to_string()),
            ]),
        }];
        let outcomes = self
            .create_topics(&specs, timeout)
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn ensure_wal_topic(
    admin: &mut dyn TopicAdmin,
    tenant: &str,
) -> Result<String, SubstrateError> {
    let topic = wal_topic(tenant);
    ensure_wal_topic_name_with_policy(admin, &topic, WalAdminPolicy::default()).await
}

/// Ensure a G-7 tenant-range WAL topic exists.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn ensure_wal_topic_for_range(
    admin: &mut dyn TopicAdmin,
    tenant: &TenantName,
    range: RangeId,
) -> Result<String, SubstrateError> {
    let topic = wal_topic_for_range(tenant, range);
    ensure_wal_topic_name_with_policy(admin, &topic, WalAdminPolicy::default()).await
}

/// Ensure an already-derived immutable physical WAL topic exists.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn ensure_wal_topic_name(
    admin: &mut dyn TopicAdmin,
    topic: &str,
) -> Result<String, SubstrateError> {
    ensure_wal_topic_name_with_policy(admin, topic, WalAdminPolicy::default()).await
}

/// Ensure an already-derived immutable physical WAL topic exists using `policy`.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn ensure_wal_topic_name_with_policy(
    admin: &mut dyn TopicAdmin,
    topic: &str,
    policy: WalAdminPolicy,
) -> Result<String, SubstrateError> {
    if !admin.topic_exists(topic).await? {
        admin
            .create_wal_topic_with_policy(
                topic,
                policy.replication_factor(),
                policy.topic_ensure_timeout(),
            )
            .await?;
    }
    Ok(topic.to_owned())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::millis;

    use super::*;

    #[derive(Default)]
    struct FakeTopicAdmin {
        existing: bool,
        creates: usize,
        checked: Vec<String>,
        created: Vec<String>,
        replication_factor: Option<i32>,
        timeout: Option<Time>,
    }

    #[async_trait::async_trait]
    impl TopicAdmin for FakeTopicAdmin {
        async fn topic_exists(&mut self, topic: &str) -> Result<bool, SubstrateError> {
            self.checked.push(topic.to_string());
            Ok(self.existing)
        }

        async fn create_wal_topic(&mut self, topic: &str) -> Result<(), SubstrateError> {
            self.record_create(topic, WAL_TOPIC_REPLICAS, WAL_TOPIC_ENSURE_TIMEOUT);
            Ok(())
        }

        async fn create_wal_topic_with_policy(
            &mut self,
            topic: &str,
            replication_factor: i32,
            timeout: Time,
        ) -> Result<(), SubstrateError> {
            self.record_create(topic, replication_factor, timeout);
            Ok(())
        }
    }

    impl FakeTopicAdmin {
        fn record_create(&mut self, topic: &str, replication_factor: i32, timeout: Time) {
            self.creates += 1;
            self.existing = true;
            self.created.push(topic.to_string());
            self.replication_factor = Some(replication_factor);
            self.timeout = Some(timeout);
        }
    }

    #[derive(Default)]
    struct LegacyTopicAdmin {
        creates: usize,
    }

    #[async_trait::async_trait]
    impl TopicAdmin for LegacyTopicAdmin {
        async fn topic_exists(&mut self, _topic: &str) -> Result<bool, SubstrateError> {
            Ok(false)
        }

        async fn create_wal_topic(&mut self, _topic: &str) -> Result<(), SubstrateError> {
            self.creates += 1;
            Ok(())
        }
    }

    #[test]
    fn wal_admin_policy_owns_defaults() {
        let policy = WalAdminPolicy::default();

        assert!(policy.replication_factor() == DEFAULT_WAL_TOPIC_REPLICATION_FACTOR);
        assert!(policy.topic_ensure_timeout() == DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT);
        assert!(policy.connect_timeout() == DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT);
        assert!(policy.request_timeout() == DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT);
        assert!(WAL_TOPIC_REPLICAS == DEFAULT_WAL_TOPIC_REPLICATION_FACTOR);
        assert!(WAL_TOPIC_ENSURE_TIMEOUT == DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT);
    }

    #[test]
    fn wal_admin_policy_rejects_zero_values() {
        assert!(WalAdminPolicy::new(0, 2, 3, 4).is_err());
        assert!(WalAdminPolicy::new(1, 0, 3, 4).is_err());
        assert!(WalAdminPolicy::new(1, 2, 0, 4).is_err());
        assert!(WalAdminPolicy::new(1, 2, 3, 0).is_err());
    }

    #[test]
    fn wal_admin_policy_preserves_distinct_values() {
        let policy = WalAdminPolicy::new(11, 22, 33, 44).expect("valid policy");

        assert!(policy.replication_factor() == 11);
        assert!(policy.topic_ensure_timeout() == millis(22));
        assert!(policy.connect_timeout() == millis(33));
        assert!(policy.request_timeout() == millis(44));
    }

    #[test]
    fn wal_admin_policy_accepts_max_wire_replication_factor_and_rejects_overflow() {
        let policy = WalAdminPolicy::new(i32::from(i16::MAX), 2, 3, 4).expect("wire maximum");

        assert!(policy.replication_factor() == i32::from(i16::MAX));
        assert!(WalAdminPolicy::new(i32::from(i16::MAX) + 1, 2, 3, 4).is_err());
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
        assert!(admin.replication_factor == Some(DEFAULT_WAL_TOPIC_REPLICATION_FACTOR));
        assert!(admin.timeout == Some(DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT));
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

    #[tokio::test]
    async fn policy_aware_ensure_passes_replication_and_timeout() {
        let mut admin = FakeTopicAdmin::default();
        let policy = WalAdminPolicy::new(i32::from(i16::MAX), 8, 9, 10).expect("valid policy");

        ensure_wal_topic_name_with_policy(&mut admin, "__gres_wal.t1.r0", policy)
            .await
            .expect("ensure");

        assert!(admin.replication_factor == Some(i32::from(i16::MAX)));
        assert!(admin.timeout == Some(millis(8)));
    }

    #[tokio::test]
    async fn legacy_topic_admin_delegates_default_policy() {
        let mut admin = LegacyTopicAdmin::default();

        ensure_wal_topic_name_with_policy(
            &mut admin,
            "__gres_wal.t1.r0",
            WalAdminPolicy::default(),
        )
        .await
        .expect("default policy");

        assert!(admin.creates == 1);
    }

    #[tokio::test]
    async fn legacy_topic_admin_rejects_non_default_policy() {
        let mut admin = LegacyTopicAdmin::default();
        let policy =
            WalAdminPolicy::new(2, WAL_TOPIC_ENSURE_TIMEOUT.millis_i32(), 3, 4).expect("policy");

        let error = ensure_wal_topic_name_with_policy(&mut admin, "__gres_wal.t1.r0", policy)
            .await
            .expect_err("custom policy unsupported");

        assert!(error.to_string().contains("unsupported WAL topic policy"));
        assert!(admin.creates == 0);
    }
}
