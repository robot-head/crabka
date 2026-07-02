//! Public value types for the share-group consumer.

use bytes::Bytes;

/// One record delivered by [`ShareConsumer::poll`](super::ShareConsumer).
///
/// Unlike a classic `ConsumerRecord`, a share record carries a
/// `delivery_count`: the broker increments it each time the record is
/// re-acquired after a prior delivery was released or its acquisition lock
/// expired (KIP-932). A first delivery has `delivery_count == 1`.
#[derive(Debug, Clone)]
pub struct ShareConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub delivery_count: i16,
}

/// How acquired records are acknowledged.
///
/// In [`Implicit`](ShareAckMode::Implicit) mode the next `poll()` (and
/// `close()`) implicitly `Accept`s every record returned by the previous
/// `poll()`. In [`Explicit`](ShareAckMode::Explicit) mode the application must
/// call `acknowledge()` for each record; un-acknowledged records are released
/// back to the queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShareAckMode {
    /// Auto-accept the previous batch on the next poll. The default, matching
    /// the JVM `share.acknowledgement.mode=implicit`.
    #[default]
    Implicit,
    /// The application acknowledges each record explicitly.
    Explicit,
}

/// The disposition of an acknowledged record (KIP-932 wire codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAckType {
    /// Successfully processed; advance the share-partition start offset past it.
    Accept,
    /// Return to the queue for re-delivery (increments `delivery_count`).
    Release,
    /// Permanently skip; do not re-deliver.
    Reject,
}

impl ShareAckType {
    /// The `i8` wire value carried in `ShareFetch` / `ShareAcknowledge`
    /// acknowledgement batches.
    pub(crate) fn wire(self) -> i8 {
        match self {
            ShareAckType::Accept => 1,
            ShareAckType::Release => 2,
            ShareAckType::Reject => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ack_wire_codes() {
        for (ack, expected) in [
            (ShareAckType::Accept, 1),
            (ShareAckType::Release, 2),
            (ShareAckType::Reject, 3),
        ] {
            assert!(ack.wire() == expected, "ack: {ack:?}");
        }
    }

    #[test]
    fn ack_mode_default_is_implicit() {
        assert!(ShareAckMode::default() == ShareAckMode::Implicit);
    }
}
