//! Per-phase action functions. The `ClientFacade` trait decouples them from
//! `crabka_client_core::Client`, so the state-machine tests can drive the
//! executor against a `MockClient`.

use std::collections::BTreeSet;

use async_trait::async_trait;
use crabka_units::{ByteRate, convert::ByteRateExt};

use crate::{executor::throttle::ThrottleTargets, model::Movement};

/// A typed wrapper over the small set of admin RPCs the executor needs.
///
/// The production impl forwards to `crabka_client_core::Client::send` with the
/// generated `crabka_protocol::owned::*` request types. Tests substitute a
/// `MockClient`.
#[async_trait]
pub trait ClientFacade: Send + Sync {
    /// `IncrementalAlterConfigs`: sets or deletes the four KIP-73 throttle
    /// keys derived from `targets` and `throttle`. `op` is `ConfigOp::Set` for
    /// `ApplyThrottle` and `ConfigOp::Delete` for `ClearThrottle`.
    async fn alter_throttle_configs(
        &self,
        op: ConfigOp,
        targets: &ThrottleTargets,
        throttle: ByteRate,
    ) -> Result<(), PhaseError>;

    /// `AlterPartitionReassignments`: submits the partition movements in one
    /// request. The caller passes the movements already chunked at
    /// `batch_size`.
    async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError>;

    /// `AlterPartitionReassignments` with `null` replicas: cancels the listed
    /// partition reassignments. `Cancel` and the deadline-exceeded path use
    /// it.
    async fn cancel_reassignments(&self, partitions: &[(String, i32)]) -> Result<(), PhaseError>;

    /// `ListPartitionReassignments`: returns the set of (topic, partition)
    /// keys that still have an in-flight reassignment, scoped to the caller's
    /// interest set.
    async fn list_in_flight(
        &self,
        of_interest: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>, PhaseError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOp {
    Set,
    Delete,
}

#[derive(Debug, thiserror::Error)]
pub enum PhaseError {
    #[error("broker rejected request: {0}")]
    Broker(String),
    #[error("client error: {0}")]
    Client(String),
}

/// Apply the throttle with one `IncrementalAlterConfigs` request that SETs all
/// four KIP-73 keys to the target values.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn apply_throttle(
    client: &(impl ClientFacade + ?Sized),
    targets: &ThrottleTargets,
    throttle: ByteRate,
) -> Result<(), PhaseError> {
    client
        .alter_throttle_configs(ConfigOp::Set, targets, throttle)
        .await
}

/// Clear the throttle with one `IncrementalAlterConfigs` request that DELETEs
/// all four KIP-73 keys on the same resources. The call is idempotent and safe
/// to re-run.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn clear_throttle(
    client: &(impl ClientFacade + ?Sized),
    targets: &ThrottleTargets,
) -> Result<(), PhaseError> {
    client
        .alter_throttle_configs(ConfigOp::Delete, targets, ByteRate::ZERO)
        .await
}

/// Submit a movement plan, chunked at `batch_size`.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn submit_movements(
    client: &(impl ClientFacade + ?Sized),
    movements: &[Movement],
    batch_size: usize,
) -> Result<(), PhaseError> {
    for chunk in movements.chunks(batch_size.max(1)) {
        client.submit_reassignments(chunk).await?;
    }
    Ok(())
}

/// Track per-partition keys derived from a proposal. They scope
/// `ListPartitionReassignments` and cancel calls to the proposal's surface.
#[must_use]
pub fn partition_keys(movements: &[Movement]) -> Vec<(String, i32)> {
    let mut s: BTreeSet<(String, i32)> = BTreeSet::new();
    for mv in movements {
        s.insert((mv.topic.clone(), mv.partition));
    }
    s.into_iter().collect()
}

#[cfg(test)]
pub mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use crabka_units::mebibytes_per_sec;

    use super::*;

    /// Mock that records every call. Tests read the recorded log to assert
    /// what the executor did.
    pub struct MockClient {
        pub calls: Mutex<Vec<MockCall>>,
        pub submit_remaining_failures: AtomicUsize,
        /// When >= 1, `list_in_flight` returns the proposal's full set for
        /// that many invocations, and an empty set after that. This simulates
        /// reassignment completion.
        pub list_in_flight_remaining: AtomicUsize,
        pub list_scope: Mutex<Vec<(String, i32)>>,
    }

    /// `PartialEq` but not `Eq`, because `AlterConfigs::rate` is an
    /// `f64`-backed quantity.
    #[derive(Debug, Clone, PartialEq)]
    pub enum MockCall {
        AlterConfigs {
            op: ConfigOp,
            targets: ThrottleTargets,
            rate: ByteRate,
        },
        Submit(Vec<Movement>),
        Cancel(Vec<(String, i32)>),
        ListInFlight(Vec<(String, i32)>),
    }

    impl Default for MockClient {
        fn default() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                submit_remaining_failures: AtomicUsize::new(0),
                list_in_flight_remaining: AtomicUsize::new(0),
                list_scope: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockClient {
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// # Panics
        /// Panics if the test call log mutex is poisoned.
        pub fn calls(&self) -> Vec<MockCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ClientFacade for MockClient {
        async fn alter_throttle_configs(
            &self,
            op: ConfigOp,
            targets: &ThrottleTargets,
            throttle: ByteRate,
        ) -> Result<(), PhaseError> {
            self.calls.lock().unwrap().push(MockCall::AlterConfigs {
                op,
                targets: targets.clone(),
                rate: throttle,
            });
            Ok(())
        }

        async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError> {
            if self.submit_remaining_failures.load(Ordering::SeqCst) > 0 {
                self.submit_remaining_failures
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(PhaseError::Broker("simulated".into()));
            }
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Submit(movements.to_vec()));
            Ok(())
        }

        async fn cancel_reassignments(
            &self,
            partitions: &[(String, i32)],
        ) -> Result<(), PhaseError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Cancel(partitions.to_vec()));
            Ok(())
        }

        async fn list_in_flight(
            &self,
            of_interest: &[(String, i32)],
        ) -> Result<Vec<(String, i32)>, PhaseError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::ListInFlight(of_interest.to_vec()));
            let remaining = self.list_in_flight_remaining.load(Ordering::SeqCst);
            if remaining > 0 {
                self.list_in_flight_remaining.fetch_sub(1, Ordering::SeqCst);
                Ok(self.list_scope.lock().unwrap().clone())
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    #[tokio::test]
    async fn submit_movements_chunks_at_batch_size() {
        let client = MockClient::new();
        let ms = vec![
            mv("t", 0, vec![1], vec![2]),
            mv("t", 1, vec![1], vec![2]),
            mv("t", 2, vec![1], vec![2]),
        ];
        submit_movements(&client, &ms, 2).await.unwrap();
        let calls = client.calls();
        let submits: Vec<_> = calls
            .iter()
            .filter_map(|c| {
                if let MockCall::Submit(m) = c {
                    Some(m.len())
                } else {
                    None
                }
            })
            .collect();
        assert2::assert!(submits == vec![2, 1]);
    }

    #[tokio::test]
    async fn apply_throttle_then_clear_records_two_alter_configs() {
        let client = MockClient::new();
        let targets =
            crate::executor::throttle::compute_throttle_targets(&[mv("t", 0, vec![1], vec![2])]);
        apply_throttle(&client, &targets, mebibytes_per_sec(48))
            .await
            .unwrap();
        clear_throttle(&client, &targets).await.unwrap();
        let calls = client.calls();
        let ops: Vec<_> = calls
            .iter()
            .filter_map(|c| {
                if let MockCall::AlterConfigs { op, .. } = c {
                    Some(*op)
                } else {
                    None
                }
            })
            .collect();
        assert2::assert!(ops == vec![ConfigOp::Set, ConfigOp::Delete]);
    }

    #[test]
    fn partition_keys_dedupes_and_sorts() {
        let ms = vec![
            mv("b", 1, vec![1], vec![2]),
            mv("a", 0, vec![1], vec![2]),
            mv("b", 1, vec![1], vec![3]),
        ];
        let keys = partition_keys(&ms);
        assert2::assert!(keys == vec![("a".to_string(), 0), ("b".to_string(), 1)]);
    }
}
