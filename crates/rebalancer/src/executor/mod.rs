//! Execute-path state machine. `Executor` runs one `Execution` at a
//! time against the cluster via a `ClientFacade`.
//!
//! Slice 43b adds the full state machine (`ApplyThrottle` -> `Submit` ->
//! `Wait` -> `ClearThrottle`) and on-disk persistence with restart resume.

pub mod phases;
pub mod state;
pub mod throttle;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::executor::phases::{
    ClientFacade, PhaseError, apply_throttle, clear_throttle, partition_keys, submit_movements,
};
use crate::executor::state::{InFlightFile, Phase, StateError};
use crate::executor::throttle::{ThrottleTargets, compute_throttle_targets};
use crate::metrics::RebalancerMetrics;
use crate::model::proposal::{Proposal, ProposalStatus};
use crate::model::store::ProposalStore;

/// Configuration controlling the executor's polling cadence and chunking.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub data_dir: PathBuf,
    pub default_throttle_bytes_per_sec: i64,
    pub poll_interval: Duration,
    pub execute_deadline: Duration,
    pub batch_size: usize,
}

/// State shared between `AppState` and the running execution task.
#[derive(Clone)]
pub struct ExecutorState {
    pub store: Arc<ProposalStore>,
    pub config: ExecutorConfig,
    pub metrics: RebalancerMetrics,
    pub in_flight: Arc<Mutex<Option<ExecutionHandle>>>,
}

/// Handle to an active execution task.
pub struct ExecutionHandle {
    pub proposal_id: String,
    pub task: JoinHandle<()>,
    pub cancel: CancellationToken,
    pub started_at: Instant,
}

/// One run of the state machine.
pub struct Execution<C: ClientFacade + 'static> {
    client: Arc<C>,
    state: ExecutorState,
    proposal: Proposal,
    targets: ThrottleTargets,
    throttle_bytes_per_sec: i64,
    cancel: CancellationToken,
    starting_phase: Phase,
}

impl<C: ClientFacade + 'static> Execution<C> {
    /// Build a fresh execution starting from `ApplyThrottle`.
    pub fn new(
        client: Arc<C>,
        state: ExecutorState,
        proposal: Proposal,
        throttle_bytes_per_sec: i64,
        cancel: CancellationToken,
    ) -> Self {
        let targets = compute_throttle_targets(&proposal.movements);
        Self {
            client,
            state,
            proposal,
            targets,
            throttle_bytes_per_sec,
            cancel,
            starting_phase: Phase::ApplyThrottle,
        }
    }

    /// Resume from a persisted phase (recovery on startup).
    pub fn resume(
        client: Arc<C>,
        state: ExecutorState,
        proposal: Proposal,
        in_flight: &InFlightFile,
        cancel: CancellationToken,
    ) -> Self {
        let targets = compute_throttle_targets(&proposal.movements);
        Self {
            client,
            state,
            proposal,
            targets,
            throttle_bytes_per_sec: in_flight.throttle_bytes_per_sec,
            cancel,
            starting_phase: in_flight.phase,
        }
    }

    /// Drive the state machine to a terminal status. Always clears
    /// throttle before returning.
    #[allow(clippy::too_many_lines)]
    pub async fn run(self) {
        let mut phase = self.starting_phase;
        // Seed persisted target/reason from any resume file so that if
        // we resume directly into `ClearThrottle` we still commit the
        // intended terminal status.
        let mut terminal: Option<(ProposalStatus, Option<String>)> = match phase {
            Phase::ClearThrottle => InFlightFile::load(&self.state.config.data_dir)
                .ok()
                .flatten()
                .and_then(|f| f.target_terminal_status.map(|s| (s, f.failure_reason))),
            _ => None,
        };
        let _ = self.persist_phase(phase, None, None);

        loop {
            match phase {
                Phase::ApplyThrottle => match self.do_apply_throttle().await {
                    Ok(()) => {
                        phase = Phase::Submit;
                        let _ = self.persist_phase(phase, None, None);
                    }
                    Err(e) => {
                        let reason = format!("ApplyThrottle: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Failed),
                            Some(reason),
                        );
                    }
                },
                Phase::Submit => match self.do_submit().await {
                    Ok(()) => {
                        phase = Phase::Wait;
                        let _ = self.persist_phase(phase, None, None);
                    }
                    Err(e) => {
                        let reason = format!("Submit: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Failed),
                            Some(reason),
                        );
                    }
                },
                Phase::Wait => match self.do_wait().await {
                    WaitOutcome::Completed => {
                        terminal = Some((ProposalStatus::Completed, None));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Completed),
                            None,
                        );
                    }
                    WaitOutcome::Cancelled => {
                        let _ = self.cancel_in_flight().await;
                        terminal = Some((ProposalStatus::Cancelled, None));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Cancelled),
                            None,
                        );
                    }
                    WaitOutcome::DeadlineExceeded => {
                        let _ = self.cancel_in_flight().await;
                        let reason = "Wait: deadline exceeded".to_string();
                        terminal =
                            Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Failed),
                            Some(reason),
                        );
                    }
                    WaitOutcome::Error(e) => {
                        let reason = format!("Wait: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(
                            phase,
                            Some(ProposalStatus::Failed),
                            Some(reason),
                        );
                    }
                },
                Phase::ClearThrottle => {
                    if let Err(e) = self.do_clear_throttle().await {
                        warn!(
                            error = %e,
                            "clear throttle failed; proposal still moves to terminal"
                        );
                    }
                    let (status, reason) = terminal
                        .as_ref()
                        .map_or((ProposalStatus::Completed, None), |(s, r)| (*s, r.as_deref()));
                    self.commit_terminal(status, reason);
                    let _ = self.cleanup_in_flight_file();
                    return;
                }
            }
        }
    }

    async fn do_apply_throttle(&self) -> Result<(), PhaseError> {
        if self.cancel.is_cancelled() {
            return Err(PhaseError::Broker("cancelled before ApplyThrottle".into()));
        }
        apply_throttle(
            self.client.as_ref(),
            &self.targets,
            self.throttle_bytes_per_sec,
        )
        .await
    }

    async fn do_submit(&self) -> Result<(), PhaseError> {
        if self.cancel.is_cancelled() {
            return Err(PhaseError::Broker("cancelled before Submit".into()));
        }
        submit_movements(
            self.client.as_ref(),
            &self.proposal.movements,
            self.state.config.batch_size,
        )
        .await
    }

    async fn do_wait(&self) -> WaitOutcome {
        let scope = partition_keys(&self.proposal.movements);
        let mut ticker = tokio::time::interval(self.state.config.poll_interval);
        let deadline = tokio::time::Instant::now() + self.state.config.execute_deadline;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if tokio::time::Instant::now() >= deadline {
                        return WaitOutcome::DeadlineExceeded;
                    }
                    match self.client.list_in_flight(&scope).await {
                        Ok(remaining) if remaining.is_empty() => return WaitOutcome::Completed,
                        Ok(_) => {}
                        Err(e) => return WaitOutcome::Error(e),
                    }
                }
                () = self.cancel.cancelled() => return WaitOutcome::Cancelled,
            }
        }
    }

    async fn cancel_in_flight(&self) -> Result<(), PhaseError> {
        let scope = partition_keys(&self.proposal.movements);
        if scope.is_empty() {
            return Ok(());
        }
        self.client.cancel_reassignments(&scope).await
    }

    async fn do_clear_throttle(&self) -> Result<(), PhaseError> {
        clear_throttle(self.client.as_ref(), &self.targets).await
    }

    fn persist_phase(
        &self,
        phase: Phase,
        target: Option<ProposalStatus>,
        reason: Option<String>,
    ) -> Result<(), StateError> {
        let mut f = InFlightFile::new(
            self.proposal.id.clone(),
            phase,
            self.proposal.started_at_ms,
            self.throttle_bytes_per_sec,
        );
        f.target_terminal_status = target;
        f.failure_reason = reason;
        f.write(&self.state.config.data_dir)
    }

    fn cleanup_in_flight_file(&self) -> Result<(), StateError> {
        InFlightFile::delete(&self.state.config.data_dir)
    }

    fn commit_terminal(&self, status: ProposalStatus, reason: Option<&str>) {
        let now = now_ms();
        let id = self.proposal.id.clone();
        let updated = self.state.store.mutate(&id, |p| {
            p.status = status;
            p.terminated_at_ms = now;
            if let Some(r) = reason {
                p.failure_reason = Some(r.to_string());
            }
        });
        match status {
            ProposalStatus::Completed => self.state.metrics.executions_completed_total.inc(),
            ProposalStatus::Failed => self.state.metrics.executions_failed_total.inc(),
            ProposalStatus::Cancelled => self.state.metrics.executions_cancelled_total.inc(),
            ProposalStatus::Computed | ProposalStatus::Executing => 0,
        };
        if updated.is_none() {
            error!(proposal_id = %id, "commit_terminal: proposal vanished from store");
        }
        let in_flight = self.state.in_flight.clone();
        tokio::spawn(async move {
            in_flight.lock().await.take();
        });
        info!(proposal_id = %id, status = ?status, "execution terminal");
    }
}

#[derive(Debug)]
enum WaitOutcome {
    Completed,
    Cancelled,
    DeadlineExceeded,
    Error(PhaseError),
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::phases::tests::{MockCall, MockClient};
    use crate::model::Movement;
    use crate::model::proposal::ProposalSummary;

    fn cfg(dir: &std::path::Path) -> ExecutorConfig {
        ExecutorConfig {
            data_dir: dir.to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(5),
            execute_deadline: Duration::from_secs(5),
            batch_size: 200,
        }
    }

    fn state_with_store(dir: &std::path::Path, p: Proposal) -> ExecutorState {
        let store = Arc::new(ProposalStore::new(20));
        store.insert(p);
        let mut registry =
            prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
        let metrics = RebalancerMetrics::register(&mut registry);
        ExecutorState {
            store,
            config: cfg(dir),
            metrics,
            in_flight: Arc::new(Mutex::new(None)),
        }
    }

    fn proposal_with_movements(id: &str, ms: Vec<Movement>) -> Proposal {
        Proposal {
            id: id.into(),
            status: ProposalStatus::Executing,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: ms,
            started_at_ms: 1,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 50_000_000,
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

    fn kind(c: &MockCall) -> &'static str {
        match c {
            MockCall::AlterConfigs { op, .. } => match op {
                crate::executor::phases::ConfigOp::Set => "set",
                crate::executor::phases::ConfigOp::Delete => "del",
            },
            MockCall::Submit(_) => "submit",
            MockCall::Cancel(_) => "cancel",
            MockCall::ListInFlight(_) => "list",
        }
    }

    #[tokio::test]
    async fn happy_path_apply_submit_wait_clear() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let calls = client.calls();
        let kinds: Vec<&str> = calls.iter().map(kind).collect();
        assert_eq!(kinds.first(), Some(&"set"));
        assert_eq!(kinds.last(), Some(&"del"));
        assert!(kinds.contains(&"submit"));

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Completed);
        assert!(after.terminated_at_ms > 0);

        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[tokio::test]
    async fn submit_failure_routes_through_clear_to_failed() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        client
            .submit_remaining_failures
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);

        let cancel = CancellationToken::new();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Failed);
        assert!(after.failure_reason.as_deref().unwrap().contains("Submit"));
        let kinds: Vec<&str> = client.calls().iter().map(kind).collect();
        assert!(kinds.contains(&"del"));
    }

    #[tokio::test]
    async fn cancel_during_wait_results_in_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        client
            .list_in_flight_remaining
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
        *client.list_scope.lock().unwrap() = vec![("t".into(), 0)];

        let cancel = CancellationToken::new();
        let cancel_for_caller = cancel.clone();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        let handle = tokio::spawn(async move {
            exec.run().await;
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel_for_caller.cancel();
        handle.await.unwrap();

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Cancelled);
        let cancels: usize = client
            .calls()
            .iter()
            .filter(|c| matches!(c, MockCall::Cancel(_)))
            .count();
        assert!(cancels >= 1);
    }

    #[tokio::test]
    async fn resume_from_clear_throttle_commits_target_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let mut f = InFlightFile::new(p.id.clone(), Phase::ClearThrottle, 1, 50_000_000);
        f.target_terminal_status = Some(ProposalStatus::Completed);
        f.write(dir.path()).unwrap();

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        let in_flight = InFlightFile::load(dir.path()).unwrap().unwrap();
        let exec = Execution::resume(client.clone(), state.clone(), p, &in_flight, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Completed);
        let dels: usize = client
            .calls()
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    MockCall::AlterConfigs {
                        op: crate::executor::phases::ConfigOp::Delete,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(dels, 1);
    }
}
