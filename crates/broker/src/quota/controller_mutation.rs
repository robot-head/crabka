//! KIP-599 `controller_mutation_rate` helper. The `CreateTopics`,
//! `CreatePartitions`, and `DeleteTopics` handlers call it after they assemble
//! the response.

use crabka_metadata::MetadataImage;
use crabka_units::{Time, convert::TimeExt};

use super::{
    QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota, positive_f64_to_u64,
    u64_to_f64,
};

/// Consume `mutations` from the `controller_mutation_rate` bucket for
/// `(principal, client_id)`. This function returns the throttle delay to apply
/// before the handler sends the response. The delay is zero if no quota is
/// configured, if there is no overage, or if `mutations == 0`. The delay is
/// capped at `maximum_delay`.
#[must_use]
pub fn consume_controller_mutation_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    mutations: u64,
    maximum_delay: Time,
) -> Time {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "controller_mutation_rate",
            amount: mutations,
        },
        |_| {},
        |rate| Some(positive_f64_to_u64(rate)),
        |overage, rate, _| Time::from_secs_f64(u64_to_f64(overage) / rate),
        maximum_delay,
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{millis, secs};

    use super::*;
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "controller_mutation_rate", rate)
    }

    #[test]
    fn zero_mutations_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 0, secs(1));
        assert!(delay == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn under_rate_returns_zero_delay() {
        // rate=10/sec, burst capacity=10 (one second of capacity).
        // 5 mutations consumed → bucket has 5 left → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 5, secs(1));
        assert!(delay == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=1/sec, burst=1; 100 mutations → overage 99 → delay 99s
        // → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100, secs(1));
        assert!(delay == secs(1));
    }

    #[test]
    fn overage_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100, millis(25));

        assert!(delay == millis(25));
    }
}
