//! Execute-path state machine. `Executor` runs one `Execution` at a
//! time against the cluster via a `ClientFacade`.
//!
//! The full state machine (`ApplyThrottle` -> `Submit` ->
//! `Wait` -> `ClearThrottle`) provides on-disk persistence with restart resume.

pub mod client_impl;
pub mod phases;
pub mod state;
pub mod throttle;

use std::fmt::Write as _;
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
    /// State backend for in-flight executor state. Production: topic-backed
    /// `StateTopic`; tests: `fake::InMemoryBackend`. Replaces the
    /// file-backed `{data_dir}/in_flight.json` store. (`data_dir` is now
    /// used only by the anomaly store.)
    pub state_topic: Arc<dyn crate::state_topic::StateBackend>,
}

/// Handle to an active execution task.
pub struct ExecutionHandle {
    pub proposal_id: String,
    pub task: JoinHandle<()>,
    pub cancel: CancellationToken,
    pub started_at: Instant,
}

/// One run of the state machine.
pub struct Execution<C: ClientFacade + ?Sized + 'static> {
    client: Arc<C>,
    state: ExecutorState,
    proposal: Proposal,
    targets: ThrottleTargets,
    throttle_bytes_per_sec: i64,
    cancel: CancellationToken,
    starting_phase: Phase,
}

impl<C: ClientFacade + ?Sized + 'static> Execution<C> {
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
    // One span per execution run (info): the whole ApplyThrottle→Submit→Wait→
    // ClearThrottle lifecycle. Per-phase steps stay as their existing
    // `info!`/`warn!` events inside the loop (not separate spans).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(proposal_id = %self.proposal.id, starting_phase = ?self.starting_phase),
    )]
    #[allow(clippy::too_many_lines)]
    pub async fn run(self) {
        let mut phase = self.starting_phase;
        // Seed persisted target/reason from any resume file so that if
        // we resume directly into `ClearThrottle` we still commit the
        // intended terminal status.
        let mut terminal: Option<(ProposalStatus, Option<String>)> = match phase {
            Phase::ClearThrottle => self
                .state
                .state_topic
                .loaded()
                .and_then(|f| f.target_terminal_status.map(|s| (s, f.failure_reason))),
            _ => None,
        };
        // Preserve any resumed target/reason on the initial persist so a
        // subsequent crash during ClearThrottle still resumes with the
        // correct terminal target.
        let (init_target, init_reason) = terminal
            .as_ref()
            .map_or((None, None), |(s, r)| (Some(*s), r.clone()));
        let _ = self.persist_phase(phase, init_target, init_reason).await;

        loop {
            // Cancel-from-any-phase short-circuits to Cancelled. The
            // per-phase fns no longer do their own cancel checks; the
            // outer loop owns cancellation routing.
            if self.cancel.is_cancelled()
                && !matches!(phase, Phase::ClearThrottle)
                && terminal.is_none()
            {
                let cancel_note = match self.cancel_in_flight().await {
                    Ok(()) => None,
                    Err(e) => {
                        warn!(
                            error = %e,
                            "cancel_in_flight failed during cancel short-circuit"
                        );
                        Some(format!("cancel_reassignments failed: {e}"))
                    }
                };
                terminal = Some((ProposalStatus::Cancelled, cancel_note.clone()));
                phase = Phase::ClearThrottle;
                let _ = self
                    .persist_phase(phase, Some(ProposalStatus::Cancelled), cancel_note)
                    .await;
                continue;
            }

            match phase {
                Phase::ApplyThrottle => match self.do_apply_throttle().await {
                    Ok(()) => {
                        phase = Phase::Submit;
                        let _ = self.persist_phase(phase, None, None).await;
                    }
                    Err(e) => {
                        let reason = format!("ApplyThrottle: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Failed), Some(reason))
                            .await;
                    }
                },
                Phase::Submit => match self.do_submit().await {
                    Ok(()) => {
                        phase = Phase::Wait;
                        let _ = self.persist_phase(phase, None, None).await;
                    }
                    Err(e) => {
                        let reason = format!("Submit: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Failed), Some(reason))
                            .await;
                    }
                },
                Phase::Wait => match self.do_wait().await {
                    WaitOutcome::Completed => {
                        terminal = Some((ProposalStatus::Completed, None));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Completed), None)
                            .await;
                    }
                    WaitOutcome::Cancelled => {
                        let cancel_note = match self.cancel_in_flight().await {
                            Ok(()) => None,
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    "cancel_in_flight failed during Cancelled path"
                                );
                                Some(format!("cancel_reassignments failed: {e}"))
                            }
                        };
                        terminal = Some((ProposalStatus::Cancelled, cancel_note.clone()));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Cancelled), cancel_note)
                            .await;
                    }
                    WaitOutcome::DeadlineExceeded => {
                        let mut reason = String::from("Wait: deadline exceeded");
                        if let Err(e) = self.cancel_in_flight().await {
                            warn!(
                                error = %e,
                                "cancel_in_flight failed during DeadlineExceeded path"
                            );
                            let _ = write!(reason, "; cancel_reassignments failed: {e}");
                        }
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Failed), Some(reason))
                            .await;
                    }
                    WaitOutcome::Error(e) => {
                        let reason = format!("Wait: {e}");
                        terminal = Some((ProposalStatus::Failed, Some(reason.clone())));
                        phase = Phase::ClearThrottle;
                        let _ = self
                            .persist_phase(phase, Some(ProposalStatus::Failed), Some(reason))
                            .await;
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
                        .map_or((ProposalStatus::Completed, None), |(s, r)| {
                            (*s, r.as_deref())
                        });
                    self.commit_terminal(status, reason);
                    let _ = self.cleanup_in_flight_file().await;
                    return;
                }
            }
        }
    }

    async fn do_apply_throttle(&self) -> Result<(), PhaseError> {
        apply_throttle(
            self.client.as_ref(),
            &self.targets,
            self.throttle_bytes_per_sec,
        )
        .await
    }

    async fn do_submit(&self) -> Result<(), PhaseError> {
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

    async fn persist_phase(
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
        self.state
            .state_topic
            .write(&f)
            .await
            .map_err(StateError::Backend)
    }

    async fn cleanup_in_flight_file(&self) -> Result<(), StateError> {
        self.state
            .state_topic
            .delete()
            .await
            .map_err(StateError::Backend)
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
    use crate::state_topic::{StateBackend, StateTopicError};
    use assert2::{assert, check};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Bounded yield-poll: spin (yielding to the runtime) until `cond` holds,
    /// so a test waits on observable in-process progress deterministically
    /// instead of sleeping a fixed settle.
    async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..200_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never held: {what}");
    }

    fn cfg(dir: &std::path::Path) -> ExecutorConfig {
        ExecutorConfig {
            data_dir: dir.to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(5),
            execute_deadline: Duration::from_secs(5),
            batch_size: 200,
        }
    }

    #[derive(Default)]
    struct RecordingBackend {
        state: std::sync::Mutex<Option<InFlightFile>>,
        writes: std::sync::Mutex<Vec<InFlightFile>>,
        deletes: AtomicUsize,
        loaded: AtomicBool,
    }

    impl RecordingBackend {
        fn new_loaded() -> Self {
            Self {
                loaded: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn with_state(file: InFlightFile) -> Self {
            Self {
                state: std::sync::Mutex::new(Some(file)),
                loaded: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn writes(&self) -> Vec<InFlightFile> {
            self.writes.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl StateBackend for RecordingBackend {
        fn loaded(&self) -> Option<InFlightFile> {
            self.state.lock().unwrap().clone()
        }

        fn is_loaded(&self) -> bool {
            self.loaded.load(Ordering::Acquire)
        }

        async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
            self.writes.lock().unwrap().push(f.clone());
            *self.state.lock().unwrap() = Some(f.clone());
            Ok(())
        }

        async fn delete(&self) -> Result<(), StateTopicError> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            *self.state.lock().unwrap() = None;
            Ok(())
        }
    }

    fn state_with_store(dir: &std::path::Path, p: Proposal) -> ExecutorState {
        state_with_backend(
            dir,
            p,
            Arc::new(crate::state_topic::fake::InMemoryBackend::new_loaded()),
        )
    }

    fn state_with_backend(
        dir: &std::path::Path,
        p: Proposal,
        state_topic: Arc<dyn StateBackend>,
    ) -> ExecutorState {
        let store = Arc::new(ProposalStore::new(20));
        store.insert(p);
        let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
        let metrics = RebalancerMetrics::register(&mut registry);
        ExecutorState {
            store,
            config: cfg(dir),
            metrics,
            in_flight: Arc::new(Mutex::new(None)),
            state_topic,
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
        let before = now_ms();
        exec.run().await;

        let calls = client.calls();
        let kinds: Vec<&str> = calls.iter().map(kind).collect();
        check!(kinds.first() == Some(&"set"));
        check!(kinds.last() == Some(&"del"));
        check!(kinds.contains(&"submit"));

        let after = state.store.get("p1").unwrap();
        check!(after.status == ProposalStatus::Completed);
        check!(after.terminated_at_ms > 0);
        check!(after.terminated_at_ms >= before);

        // After a clean terminal the backend should have been tombstoned.
        check!(state.state_topic.loaded().is_none());
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
        assert!(after.status == ProposalStatus::Failed);
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
        // Wait until the execution has submitted and entered the in-flight
        // wait loop (it polls `list_in_flight`, which never drains here), then
        // cancel — this exercises the cancel-DURING-wait path deterministically
        // rather than racing a fixed settle.
        await_until("execution entered in-flight wait loop", || {
            client
                .calls()
                .iter()
                .any(|c| matches!(c, MockCall::ListInFlight(_)))
        })
        .await;
        cancel_for_caller.cancel();
        handle.await.unwrap();

        let after = state.store.get("p1").unwrap();
        assert!(after.status == ProposalStatus::Cancelled);
        let cancels: usize = client
            .calls()
            .iter()
            .filter(|c| matches!(c, MockCall::Cancel(_)))
            .count();
        assert!(cancels >= 1);
    }

    #[tokio::test]
    async fn cancel_before_submit_results_in_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        cancel.cancel(); // already cancelled before run() starts
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert!(
            after.status == ProposalStatus::Cancelled,
            "cancel-before-Submit must produce Cancelled, not {:?}",
            after.status
        );
        // After cancellation the backend should have been tombstoned.
        assert!(state.state_topic.loaded().is_none());
    }

    #[tokio::test]
    async fn submit_failure_persists_failed_clear_throttle_before_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let backend = Arc::new(RecordingBackend::new_loaded());
        let state = state_with_backend(dir.path(), p.clone(), backend.clone());

        let client = Arc::new(MockClient::new());
        client
            .submit_remaining_failures
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);

        let cancel = CancellationToken::new();
        let exec = Execution::new(client, state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let writes = backend.writes();
        assert!(
            writes.iter().any(|f| {
                f.phase == Phase::ClearThrottle
                    && f.target_terminal_status == Some(ProposalStatus::Failed)
                    && f.failure_reason
                        .as_deref()
                        .is_some_and(|reason| reason.contains("Submit"))
            }),
            "expected a persisted failed ClearThrottle phase, got {writes:?}"
        );
        assert!(backend.deletes.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn resume_from_clear_throttle_commits_target_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);

        // Pre-seed the backend with a ClearThrottle in-flight record so
        // the executor resumes with the correct terminal target.
        let mut in_flight = InFlightFile::new(p.id.clone(), Phase::ClearThrottle, 1, 50_000_000);
        in_flight.target_terminal_status = Some(ProposalStatus::Completed);

        let backend = Arc::new(crate::state_topic::fake::InMemoryBackend::new_loaded());
        *backend.state.lock().unwrap() = Some(in_flight.clone());

        let store = Arc::new(ProposalStore::new(20));
        store.insert(p.clone());
        let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
        let metrics = RebalancerMetrics::register(&mut registry);
        let state = ExecutorState {
            store,
            config: cfg(dir.path()),
            metrics,
            in_flight: Arc::new(Mutex::new(None)),
            state_topic: backend,
        };

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        let exec = Execution::resume(client.clone(), state.clone(), p, &in_flight, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert!(after.status == ProposalStatus::Completed);
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
        assert!(dels == 1);
    }

    #[tokio::test]
    async fn cancelled_resume_from_clear_throttle_preserves_terminal_target() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let mut in_flight = InFlightFile::new(p.id.clone(), Phase::ClearThrottle, 1, 50_000_000);
        in_flight.target_terminal_status = Some(ProposalStatus::Completed);

        let backend = Arc::new(RecordingBackend::with_state(in_flight.clone()));
        let state = state_with_backend(dir.path(), p.clone(), backend);
        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let exec = Execution::resume(client.clone(), state.clone(), p, &in_flight, cancel);

        tokio::time::timeout(Duration::from_secs(1), exec.run())
            .await
            .expect("clear-throttle resume should terminate");

        let after = state.store.get("p1").unwrap();
        assert!(after.status == ProposalStatus::Completed);
        assert!(
            !client
                .calls()
                .iter()
                .any(|c| matches!(c, MockCall::Cancel(_)))
        );
    }
}
