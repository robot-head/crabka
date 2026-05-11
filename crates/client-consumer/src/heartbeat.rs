//! Background `Heartbeat` loop. Spawned by `Consumer` after a successful
//! join+sync. Signals the foreground via an `mpsc::Sender<RebalanceNotice>`
//! whenever the broker tells us to rejoin.

#![allow(dead_code)] // Wired up by `ConsumerBuilder::build` in Task 17.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;

/// Reason the broker has asked us to rejoin.
#[derive(Debug, Clone, Copy)]
pub enum RebalanceNotice {
    /// `REBALANCE_IN_PROGRESS` (27): rejoin with the current `member_id`.
    NeedRejoin,
    /// `UNKNOWN_MEMBER_ID` (25): re-handshake from scratch (clear `member_id`).
    RejoinFromScratch,
}

/// Periodic heartbeat. Exits when `shutdown` is cancelled.
pub async fn run(
    client: Client,
    group_id: String,
    member_id: String,
    generation_id: i32,
    interval: Duration,
    notice_tx: mpsc::Sender<RebalanceNotice>,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let result = client
                    .send(HeartbeatRequest {
                        group_id: group_id.clone(),
                        generation_id,
                        member_id: member_id.clone(),
                        ..Default::default()
                    })
                    .await;
                match result {
                    Ok(r) if r.error_code == 0 => {}
                    Ok(r) if r.error_code == 27 => {
                        // REBALANCE_IN_PROGRESS
                        let _ = notice_tx.send(RebalanceNotice::NeedRejoin).await;
                    }
                    Ok(r) if r.error_code == 25 => {
                        // UNKNOWN_MEMBER_ID
                        let _ = notice_tx.send(RebalanceNotice::RejoinFromScratch).await;
                    }
                    Ok(r) => {
                        tracing::warn!(error_code = r.error_code, "unexpected heartbeat error");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "heartbeat send failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebalance_notice_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<RebalanceNotice>();
    }
}
