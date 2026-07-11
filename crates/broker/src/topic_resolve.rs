//! KIP-516 strict topic resolution. A single place that maps a request's
//! `(name, topic_id)` pair to a `TopicRecord`, applying the KIP-516
//! strictness rules. The `Err` carries the wire error code to surface for
//! the offending topic/partition.

use crabka_metadata::{MetadataImage, TopicRecord};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::codes;

/// Resolve a requested topic to its record.
///
/// - non-zero `id`, unknown ⇒ `UNKNOWN_TOPIC_ID`
/// - non-zero `id` + non-empty `name` whose stored name differs ⇒ `INCONSISTENT_TOPIC_ID`
/// - zero `id`, `name` resolves ⇒ name path; unknown name ⇒ `UNKNOWN_TOPIC_OR_PARTITION`
pub(crate) fn resolve<'a>(
    image: &'a MetadataImage,
    name: &str,
    id: WireUuid,
) -> Result<&'a TopicRecord, i16> {
    if id == WireUuid::ZERO {
        image.topic(name).ok_or(codes::UNKNOWN_TOPIC_OR_PARTITION)
    } else {
        let uuid = uuid::Uuid::from_bytes(id.0);
        match image.topic_by_id(&uuid) {
            None => Err(codes::UNKNOWN_TOPIC_ID),
            Some(t) if !name.is_empty() && t.name != name => Err(codes::INCONSISTENT_TOPIC_ID),
            Some(t) => Ok(t),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_metadata::{MetadataRecord, TopicRecord};

    use super::*;

    fn image_with(name: &str, id: uuid::Uuid) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: id,
            partitions: 1,
            replication_factor: 1,
        }));
        img
    }

    #[test]
    fn resolves_by_id() {
        let id = uuid::Uuid::from_u128(7);
        let img = image_with("t", id);
        let r = resolve(&img, "", WireUuid(id.into_bytes())).unwrap();
        assert!(r.name == "t");
    }

    #[test]
    fn unknown_id_is_unknown_topic_id() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let err = resolve(&img, "", WireUuid(uuid::Uuid::from_u128(99).into_bytes())).unwrap_err();
        assert!(err == codes::UNKNOWN_TOPIC_ID);
    }

    #[test]
    fn mismatched_name_and_id_is_inconsistent() {
        let id = uuid::Uuid::from_u128(7);
        let img = image_with("t", id);
        // id resolves to "t" but the request also names "other".
        let err = resolve(&img, "other", WireUuid(id.into_bytes())).unwrap_err();
        assert!(err == codes::INCONSISTENT_TOPIC_ID);
    }

    #[test]
    fn name_only_resolves() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let r = resolve(&img, "t", WireUuid::ZERO).unwrap();
        assert!(r.name == "t");
    }

    #[test]
    fn unknown_name_is_unknown_topic_or_partition() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let err = resolve(&img, "missing", WireUuid::ZERO).unwrap_err();
        assert!(err == codes::UNKNOWN_TOPIC_OR_PARTITION);
    }
}
