//! `AddOffsetsToTxn` (`api_key=25`). Registers a consumer-group's offset
//! commit topic with an ongoing transaction so the broker knows to write
//! `__consumer_offsets` markers when the transaction is finalised.
//!
//! Wire format: v0-2 non-flexible, v3-4 flexible (tagged fields).
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `group_id`.
//! Response fields: `throttle_time_ms`, `error_code`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use crabka_protocol::owned::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_NUM_PARTITIONS, OFFSETS_TOPIC};
use crate::error::BrokerError;
use crate::txn::partitioner::partition_for_tid;
use crate::txn::state::{TopicPartition, TxnState};
use crate::txn::util::now_millis;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AddOffsetsToTxnRequest::decode(&mut cur, version)?;

        // Refresh leader-partition view from the current metadata image
        // before checking coordinator-ness, to avoid a race. Resolve the
        // finalized transaction.version from the same image read.
        let image = controller.current_image();
        let txnv = crate::txn::version::resolve_txn_version(&image);
        coord.refresh_leader_partitions(&image).await;

        let tid = req.transactional_id.as_str();

        if !coord.is_coordinator_for(tid).await {
            return encode_err(version, codes::NOT_COORDINATOR);
        }

        let Some(entry_mutex) = coord.get(tid) else {
            return encode_err(version, codes::INVALID_PRODUCER_ID_MAPPING);
        };

        let mut entry = entry_mutex.lock().await;

        if entry.producer_id != req.producer_id || entry.producer_epoch != req.producer_epoch {
            return encode_err(version, codes::INVALID_PRODUCER_EPOCH);
        }

        // State machine: Empty/Ongoing → Ongoing.
        if !entry.state.can_transition_to(TxnState::Ongoing) {
            return encode_err(version, codes::INVALID_TXN_STATE);
        }
        entry.state = TxnState::Ongoing;

        // KIP-890 / Kafka model: a consumer-group offset commit is represented
        // as the group's __consumer_offsets partition in the txn partition set
        // (Kafka's TransactionLogValue has no group-name field). EndTxn fans a
        // marker to every partition in the set, including this one.
        //
        // NOTE: `partition_for_tid` is the murmur2 partitioner Kafka uses for
        // `transactional_id -> __transaction_state`. Kafka partitions
        // `__consumer_offsets` by group with `abs(groupId.hashCode()) % N`
        // (Java String.hashCode), NOT murmur2. This is only correct today
        // because `OFFSETS_NUM_PARTITIONS == 1` (every group maps to 0). A real
        // multi-partition `__consumer_offsets` will need a dedicated
        // `partition_for_group` using the Java-hashCode rule — see the const's
        // doc in `coordinator::bootstrap`.
        entry.partitions.insert(TopicPartition {
            topic: OFFSETS_TOPIC.to_string(),
            partition: partition_for_tid(&req.group_id, OFFSETS_NUM_PARTITIONS),
        });
        entry.last_update_ms = now_millis();

        let snap = entry.clone();
        // Drop lock before the async persist call.
        drop(entry);

        if let Err(e) = coord.put(snap, txnv).await {
            tracing::error!(
                tid,
                group_id = %req.group_id,
                error = %e,
                "AddOffsetsToTxn: failed to persist TxnEntry"
            );
            return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
        }

        encode_ok(version)
    })
}

// ── encoding helpers ──────────────────────────────────────────────────────────

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, error_code)
}

fn encode_ok(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, codes::NONE)
}

fn encode_response(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = AddOffsetsToTxnResponse {
        error_code,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn decode(bytes: &Bytes, version: i16) -> AddOffsetsToTxnResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = AddOffsetsToTxnResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    #[test]
    fn encode_err_preserves_error_code_on_the_wire() {
        let bytes = encode_err(4, codes::NOT_COORDINATOR).expect("encode error");
        assert!(!bytes.is_empty());
        let resp = decode(&bytes, 4);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NOT_COORDINATOR);
    }

    #[test]
    fn encode_ok_preserves_success_code_on_the_wire() {
        let bytes = encode_ok(4).expect("encode ok");
        assert!(!bytes.is_empty());
        let resp = decode(&bytes, 4);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
    }
}
