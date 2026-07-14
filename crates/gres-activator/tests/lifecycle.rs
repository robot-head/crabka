use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::assert;
use crabka_gres::{
    FinalCheckpointer, SuspendMonitorOutcome, SuspendPolicy, SuspendRegistry,
    try_suspend_idle_tenant,
};
use crabka_gres_activator::{
    ActivatorError, BackendEndpoint, Readiness, WaitForReadyConfig, WakeCoordinator, WakeRegistry,
    WakeRequest,
};
use crabka_gres_control::{
    FinalCheckpoint, SqlUser, TenantId, TenantName, TenantRecord, TenantState,
};
use crabka_gres_substrate::{
    GroupCommitRequest, InMemoryWalLog, SubstrateError, TransactionalWalWriter, WalFrame,
    WriterGeneration, recover_after_barrier,
};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgwire::server::ActivityTracker;
use tokio::sync::{Mutex, Notify};

const TENANT: &str = "tenant-a";
const ENDPOINT: &str = "127.0.0.1:15432";

#[tokio::test]
async fn full_lifecycle_suspends_wakes_and_recovers_data_intact() {
    let registry = LifecycleRegistry::active();
    let log = InMemoryWalLog::shared();
    log.commit_group(GroupCommitRequest {
        generation: WriterGeneration(0),
        frames: vec![put_frame(0, b"table/item", b"before-suspend")],
    })
    .await
    .expect("workload commit");
    let activity = idle_activity_tracker();
    let checkpointer = FakeFinalCheckpointer::new(final_checkpoint());
    let policy = suspend_policy();

    let outcome = try_suspend_idle_tenant(&policy, &activity, &checkpointer, &mut registry.clone())
        .await
        .expect("suspend attempt");

    assert!(outcome == SuspendMonitorOutcome::Suspended);
    assert!(checkpointer.force_count() == 1);
    let suspended = registry.record().await;
    assert!(suspended.state == TenantState::Suspended);
    assert!(suspended.final_checkpoint == Some(final_checkpoint()));

    let successor_store = Arc::new(MemKv::default());
    let controller = spawn_resume_controller(registry.clone(), log, Arc::clone(&successor_store));
    let coordinator = WakeCoordinator::new(registry.clone());
    let endpoint = coordinator
        .wake_and_wait(&wake_request(), fast_wait())
        .await
        .expect("activator wake");

    controller.await.expect("controller task");
    assert!(endpoint.as_str() == ENDPOINT);
    assert!(registry.record().await.state == TenantState::Active);
    assert!(registry.resume_request_count().await == 1);
    assert!(
        successor_store
            .get(b"table/item")
            .expect("read recovered key")
            == Some(b"before-suspend".to_vec())
    );
}

#[tokio::test]
async fn client_racing_closed_admission_retries_through_activator() {
    let registry = LifecycleRegistry::active();
    let activity = idle_activity_tracker();
    let checkpointer = FakeFinalCheckpointer::new(final_checkpoint());
    let policy = suspend_policy();

    let outcome = try_suspend_idle_tenant(&policy, &activity, &checkpointer, &mut registry.clone())
        .await
        .expect("suspend attempt");
    assert!(outcome == SuspendMonitorOutcome::Suspended);

    let activity = Arc::new(activity);
    assert!(activity.try_open_session().is_none());

    let log = InMemoryWalLog::shared();
    let successor_store = Arc::new(MemKv::default());
    let controller = spawn_resume_controller(registry.clone(), log, successor_store);
    let coordinator = WakeCoordinator::new(registry.clone());
    let endpoint = coordinator
        .wake_and_wait(&wake_request(), fast_wait())
        .await
        .expect("retry through activator");

    controller.await.expect("controller task");
    assert!(endpoint == BackendEndpoint(ENDPOINT.to_string()));
    assert!(registry.record().await.state == TenantState::Active);
}

#[tokio::test]
async fn simultaneous_first_connections_from_two_activators_coalesce_registry_request() {
    let registry = LifecycleRegistry::suspended();
    let first = WakeCoordinator::new(registry.clone());
    let second = WakeCoordinator::new(registry.clone());
    let controller = spawn_mark_active_controller(registry.clone());
    let first_request = wake_request();
    let second_request = wake_request();

    let (first_endpoint, second_endpoint) = tokio::join!(
        first.wake_and_wait(&first_request, fast_wait()),
        second.wake_and_wait(&second_request, fast_wait()),
    );

    controller.await.expect("controller task");
    assert!(first_endpoint.expect("first wake").as_str() == ENDPOINT);
    assert!(second_endpoint.expect("second wake").as_str() == ENDPOINT);
    assert!(registry.resume_request_count().await == 1);
}

#[tokio::test]
async fn suspend_is_blocked_by_open_session() {
    let registry = LifecycleRegistry::active();
    let activity = Arc::new(idle_activity_tracker());
    let _session = activity.try_open_session().expect("open session");
    let checkpointer = FakeFinalCheckpointer::new(final_checkpoint());
    let policy = suspend_policy();

    let outcome = try_suspend_idle_tenant(
        &policy,
        activity.as_ref(),
        &checkpointer,
        &mut registry.clone(),
    )
    .await
    .expect("suspend attempt");

    assert!(outcome == SuspendMonitorOutcome::OpenSessions { count: 1 });
    assert!(checkpointer.force_count() == 0);
    assert!(registry.record().await.state == TenantState::Active);
}

#[tokio::test]
async fn resume_fencing_rejects_zombie_compute_and_accepts_successor() {
    let log = InMemoryWalLog::shared();
    log.commit_group(GroupCommitRequest {
        generation: WriterGeneration(0),
        frames: vec![put_frame(0, b"stable", b"yes")],
    })
    .await
    .expect("pre-suspend commit");
    let store = MemKv::default();

    let (barrier, _outcome) = recover_after_barrier(&store, log.as_ref(), log.as_ref())
        .await
        .expect("successor recovery fences predecessor");
    let zombie_error = log
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![put_frame(1, b"zombie", b"lost")],
        })
        .await
        .expect_err("zombie writer must be fenced");
    log.commit_group(GroupCommitRequest {
        generation: barrier.generation,
        frames: vec![put_frame(1, b"successor", b"wins")],
    })
    .await
    .expect("successor commit");

    assert!(matches!(zombie_error, SubstrateError::Fenced));
    assert!(store.get(b"stable").expect("stable key") == Some(b"yes".to_vec()));
    assert!(store.get(b"zombie").expect("zombie key").is_none());
}

#[derive(Clone)]
struct LifecycleRegistry {
    inner: Arc<Mutex<LifecycleState>>,
    changed: Arc<Notify>,
}

struct LifecycleState {
    record: TenantRecord,
    resume_requests: usize,
}

impl LifecycleRegistry {
    fn active() -> Self {
        Self::with_record(
            base_record(TenantState::Active)
                .mark_active(ENDPOINT)
                .expect("active endpoint"),
        )
    }

    fn suspended() -> Self {
        Self::with_record(
            base_record(TenantState::Active)
                .mark_suspended_after_checkpoint(final_checkpoint())
                .expect("suspended record"),
        )
    }

    fn with_record(record: TenantRecord) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LifecycleState {
                record,
                resume_requests: 0,
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    async fn mark_active(&self) {
        let mut state = self.inner.lock().await;
        state.record = state
            .record
            .clone()
            .mark_active(ENDPOINT)
            .expect("mark active");
        self.changed.notify_waiters();
    }

    async fn wait_for_resume_request(&self) {
        loop {
            if self.record().await.state == TenantState::ResumeRequested {
                return;
            }
            self.changed.notified().await;
        }
    }

    async fn record(&self) -> TenantRecord {
        self.inner.lock().await.record.clone()
    }

    async fn resume_request_count(&self) -> usize {
        self.inner.lock().await.resume_requests
    }
}

#[async_trait::async_trait]
impl SuspendRegistry for LifecycleRegistry {
    async fn mark_suspended(
        &mut self,
        _tenant: &str,
        checkpoint: FinalCheckpoint,
    ) -> std::io::Result<()> {
        let mut state = self.inner.lock().await;
        state.record = state
            .record
            .clone()
            .mark_suspended_after_checkpoint(checkpoint)
            .map_err(std::io::Error::other)?;
        self.changed.notify_waiters();
        Ok(())
    }
}

#[async_trait::async_trait]
impl WakeRegistry for LifecycleRegistry {
    async fn request_resume(&self, request: &WakeRequest) -> Result<(), ActivatorError> {
        let mut state = self.inner.lock().await;
        if state.record.name != *request.tenant() {
            return Err(ActivatorError::TenantMissing(request.tenant().clone()));
        }
        if state.record.state != TenantState::Suspended {
            return Ok(());
        }

        state.record = state.record.clone().request_resume()?;
        state.resume_requests += 1;
        self.changed.notify_waiters();
        Ok(())
    }

    async fn readiness(&self, tenant: &TenantName) -> Result<Readiness, ActivatorError> {
        let state = self.inner.lock().await;
        if state.record.name != *tenant {
            return Ok(Readiness::Missing);
        }
        if state.record.state != TenantState::Active {
            return Ok(Readiness::NotReady);
        }
        Ok(Readiness::Ready(BackendEndpoint(
            state.record.endpoint.clone().expect("active endpoint"),
        )))
    }
}

struct FakeFinalCheckpointer {
    checkpoint: FinalCheckpoint,
    force_count: AtomicUsize,
}

impl FakeFinalCheckpointer {
    fn new(checkpoint: FinalCheckpoint) -> Self {
        Self {
            checkpoint,
            force_count: AtomicUsize::new(0),
        }
    }

    fn force_count(&self) -> usize {
        self.force_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl FinalCheckpointer for FakeFinalCheckpointer {
    async fn latest_checkpoint_bytes(&self) -> std::io::Result<u64> {
        Ok(self.checkpoint.total_bytes)
    }

    async fn force_final_checkpoint(&self) -> std::io::Result<FinalCheckpoint> {
        self.force_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.checkpoint.clone())
    }
}

fn spawn_resume_controller(
    registry: LifecycleRegistry,
    log: Arc<InMemoryWalLog>,
    successor_store: Arc<MemKv>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        registry.wait_for_resume_request().await;
        recover_after_barrier(successor_store.as_ref(), log.as_ref(), log.as_ref())
            .await
            .expect("recover successor");
        registry.mark_active().await;
    })
}

fn spawn_mark_active_controller(registry: LifecycleRegistry) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        registry.wait_for_resume_request().await;
        registry.mark_active().await;
    })
}

fn base_record(state: TenantState) -> TenantRecord {
    TenantRecord::new(
        1,
        TenantId::try_from(TENANT).expect("tenant id"),
        TenantName::try_from(TENANT).expect("tenant name"),
        state,
        SqlUser::try_from("alice").expect("sql user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("tenant record")
}

fn final_checkpoint() -> FinalCheckpoint {
    FinalCheckpoint {
        wal_generation: 0,
        covered_offset: 0,
        manifest_key: "ckpt/tenant-a/0/manifest.json".to_string(),
        total_bytes: 64,
    }
}

fn suspend_policy() -> SuspendPolicy {
    SuspendPolicy {
        tenant: TENANT.to_string(),
        idle_window: Duration::from_millis(1),
        suspend_max_checkpoint_bytes: Some(1024),
    }
}

fn idle_activity_tracker() -> ActivityTracker {
    ActivityTracker::with_last_activity_unix_millis(now_millis().saturating_sub(5_000))
}

fn now_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis();
    u64::try_from(millis).expect("current unix millis fits u64")
}

fn wake_request() -> WakeRequest {
    WakeRequest::for_database(TENANT).expect("wake request")
}

fn fast_wait() -> WaitForReadyConfig {
    WaitForReadyConfig {
        timeout: Duration::from_secs(1),
        poll_interval: Duration::from_millis(5),
    }
}

fn put_frame(journal_seq: u64, key: &[u8], value: &[u8]) -> WalFrame {
    WalFrame {
        journal_seq,
        ops: vec![WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }],
    }
}
