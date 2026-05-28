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
use crate::error::BrokerError;
use crate::txn::state::TxnState;
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
        // before checking coordinator-ness, to avoid a race.
        coord
            .refresh_leader_partitions(&controller.current_image())
            .await;

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

        // Register the consumer group as a participant in this transaction.
        entry.offset_commit_groups.insert(req.group_id.clone());
        entry.last_update_ms = now_millis();

        let snap = entry.clone();
        // Drop lock before the async persist call.
        drop(entry);

        if let Err(e) = coord.put(snap).await {
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
        throttle_time_ms: 0,
        error_code,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
