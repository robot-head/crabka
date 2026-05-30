//! Classic ↔ next-gen consumer-group conversion predicates (KIP-848 64d-C).
//!
//! This slice adds the *machinery* the conversion triggers in Slices 64d-D/E
//! consume — the [`super::config::ConsumerGroupMigrationPolicy`] and the
//! convertibility predicate — without performing any live conversion yet. The
//! predicates are unit-tested here and wired into the conversion triggers in
//! D/E, so they are dead in the lib build until then.
#![allow(dead_code)]

use crabka_protocol::Decode;
use crabka_protocol::owned::consumer_protocol_subscription::ConsumerProtocolSubscription;

use super::classic_state::Group as ClassicState;

/// Decode a classic member's `protocol_metadata` blob as a
/// `ConsumerProtocolSubscription`. The blob carries a leading `i16` version
/// (the "consumer" embedded-protocol version negotiation, separate from the
/// `ConsumerProtocolSubscription` schema's per-field version gates) followed by
/// the schema body. Returns `None` on any decode error or unknown version —
/// such a member's subscription cannot survive translation to the server-side
/// consumer model. Mirrors `offset_delete::decode_subscribed_topics`.
pub(crate) fn decode_consumer_subscription(
    metadata: &[u8],
) -> Option<ConsumerProtocolSubscription> {
    use bytes::Buf;
    if metadata.len() < 2 {
        return None;
    }
    let mut cur = metadata;
    let version = cur.get_i16();
    if !(0..=3).contains(&version) {
        return None;
    }
    ConsumerProtocolSubscription::decode(&mut cur, version).ok()
}

/// Can this classic group be upgraded to a next-gen consumer group?
///
/// Mirrors Apache Kafka's `ConsumerGroup.fromClassicGroup` admission rule: the
/// group must use the `"consumer"` protocol type and **every** current member's
/// selected `protocol_metadata` must decode as a valid
/// `ConsumerProtocolSubscription`, so each subscription survives translation. An
/// empty group is trivially convertible.
pub(crate) fn classic_is_convertible(state: &ClassicState) -> bool {
    if state.protocol_type.as_deref() != Some("consumer") {
        return false;
    }
    state
        .members
        .values()
        .all(|m| decode_consumer_subscription(&m.protocol_metadata).is_some())
}

/// Can this consumer group be downgraded to a classic group? Always `true` in
/// Kafka — a server-managed consumer group can always be re-expressed as a
/// classic group (members become classic members, the server target becomes the
/// seed assignment). Provided for symmetry; the real work is in Slice 64d-E.
#[allow(dead_code)] // consumed by Slice 64d-E (downgrade trigger)
pub(crate) fn consumer_is_convertible() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::{BufMut, Bytes, BytesMut};
    use crabka_protocol::Encode;
    use std::time::Duration;

    use super::super::classic_state::{Group, Member};

    /// Encode a `ConsumerProtocolSubscription` with the leading version prefix,
    /// as a real classic consumer client sends in its `JoinGroup` protocol
    /// metadata.
    fn subscription_blob(topics: &[&str]) -> Bytes {
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = BytesMut::new();
        out.put_i16(0); // protocol version-negotiation prefix
        sub.encode(&mut out, 0).unwrap();
        out.freeze()
    }

    fn consumer_member(id: &str, metadata: Bytes) -> Member {
        let mut m = Member::new(
            id,
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), metadata.clone())],
        );
        m.protocol_metadata = metadata;
        m
    }

    #[test]
    fn empty_consumer_group_is_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn non_consumer_protocol_type_is_not_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("connect".into());
        assert!(!classic_is_convertible(&g));
        // None protocol_type (never joined) is also not convertible.
        let g2 = Group::new("g2");
        assert!(!classic_is_convertible(&g2));
    }

    #[test]
    fn group_of_valid_consumer_members_is_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn member_with_undecodable_metadata_blocks_conversion() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("ok", subscription_blob(&["t1"])));
        // Garbage metadata that is not a ConsumerProtocolSubscription.
        g.add_member(consumer_member(
            "bad",
            Bytes::from_static(&[0xff, 0xff, 0x01]),
        ));
        assert!(!classic_is_convertible(&g));
    }

    #[test]
    fn decode_rejects_short_and_bad_version() {
        assert!(decode_consumer_subscription(&[]).is_none());
        assert!(decode_consumer_subscription(&[0]).is_none());
        // Version 99 is out of the supported 0..=3 range.
        assert!(decode_consumer_subscription(&[0, 99]).is_none());
    }

    #[test]
    fn consumer_group_always_downgradable() {
        assert!(consumer_is_convertible());
    }
}
