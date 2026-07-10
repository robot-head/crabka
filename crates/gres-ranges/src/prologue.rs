//! Fence-first recovery prologue for a tenant range.

use crabka_pgkv::Kv;

use crate::{RangeId, range0_tail::Range0TailError};

/// A range opened for service after the recovery prologue completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServingRange {
    /// Range now allowed to serve requests.
    pub range: RangeId,
    /// Epoch fenced and served by this compute.
    pub epoch: i16,
    /// Committed barrier offset that bounded replay.
    pub barrier_offset: i64,
    /// Next journal sequence after replay.
    pub next_journal_seq: u64,
}

/// Ordered recovery seams composed by [`recover_range`].
pub struct RecoverRange<'a> {
    /// Range being recovered.
    pub range: RangeId,
    /// Local store receiving replayed records and reseeds.
    pub store: &'a dyn Kv,
    /// Fence/barrier/replay provider.
    pub substrate: &'a dyn RangeRecoverySubstrate,
    /// Range-0-only counter and GTM reseed hooks.
    pub range0_hooks: &'a dyn Range0RecoveryHooks,
    /// In-doubt transaction lock and settle seam.
    pub settlement: &'a dyn InDoubtSettlement,
    /// Serving gate opened only after the prologue settles.
    pub serving_gate: &'a dyn ServingGate,
}

/// Range substrate operations that must execute in fence-first order.
#[async_trait::async_trait]
pub trait RangeRecoverySubstrate: Send + Sync {
    /// Bump/fence the writer epoch before any end-offset read or replay.
    async fn fence_epoch(&self, range: RangeId) -> Result<i16, PrologueError>;
    /// Produce a committed recovery barrier under the fenced epoch.
    async fn produce_barrier(
        &self,
        range: RangeId,
        epoch: i16,
    ) -> Result<ProducedBarrier, PrologueError>;
    /// Replay committed records through the fenced barrier.
    async fn replay_to_barrier(
        &self,
        store: &dyn Kv,
        barrier: ProducedBarrier,
    ) -> Result<ReplaySummary, PrologueError>;
}

/// Range-0-only reseed hooks.
#[async_trait::async_trait]
pub trait Range0RecoveryHooks: Send + Sync {
    /// Reseed counters from recovered range-0 state.
    async fn reseed_counters(&self, store: &dyn Kv) -> Result<(), PrologueError>;
    /// Reseed global transaction metadata from recovered range-0 state.
    async fn reseed_gtm(&self, store: &dyn Kv) -> Result<(), PrologueError>;
}

/// In-doubt recovery and settle seam.
#[async_trait::async_trait]
pub trait InDoubtSettlement: Send + Sync {
    /// Reacquire locks for prepared in-doubt records before settling decisions.
    async fn reacquire_in_doubt_locks(&self, range: RangeId) -> Result<(), PrologueError>;
    /// Try to settle known in-doubt records.
    async fn settle_once(&self, range: RangeId) -> Result<SettleOutcome, PrologueError>;
}

/// Serving gate that is fail-closed until explicitly opened.
#[async_trait::async_trait]
pub trait ServingGate: Send + Sync {
    /// Install recovered writer state and open the range for service.
    async fn mark_served(
        &self,
        range: RangeId,
        epoch: i16,
        barrier_offset: i64,
        next_journal_seq: u64,
    ) -> Result<(), PrologueError>;
}

/// Recovery barrier metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducedBarrier {
    /// Range protected by this barrier.
    pub range: RangeId,
    /// Fenced epoch that produced the barrier.
    pub epoch: i16,
    /// Offset of the committed barrier.
    pub offset: i64,
}

/// Replay output used to start the successor writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaySummary {
    /// Next journal sequence after replay.
    pub next_journal_seq: u64,
}

/// Result of one settle loop pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleOutcome {
    /// No in-doubt markers remain.
    Complete,
    /// In-doubt markers remain; the gate must stay closed.
    InDoubtRemaining,
}

/// Run the fence → barrier → replay → reseed → settle → serve prologue.
pub async fn recover_range(input: RecoverRange<'_>) -> Result<ServingRange, PrologueError> {
    let epoch = input.substrate.fence_epoch(input.range).await?;
    let barrier = input.substrate.produce_barrier(input.range, epoch).await?;
    let replay = input
        .substrate
        .replay_to_barrier(input.store, barrier)
        .await?;

    if input.range.is_coordinator() {
        input.range0_hooks.reseed_counters(input.store).await?;
        input.range0_hooks.reseed_gtm(input.store).await?;
    }

    input
        .settlement
        .reacquire_in_doubt_locks(input.range)
        .await?;
    match input.settlement.settle_once(input.range).await? {
        SettleOutcome::Complete => {}
        SettleOutcome::InDoubtRemaining => return Err(PrologueError::InDoubtRemaining),
    }

    input
        .serving_gate
        .mark_served(input.range, epoch, barrier.offset, replay.next_journal_seq)
        .await?;
    Ok(ServingRange {
        range: input.range,
        epoch,
        barrier_offset: barrier.offset,
        next_journal_seq: replay.next_journal_seq,
    })
}

/// Recovery prologue errors.
#[derive(Debug, thiserror::Error)]
pub enum PrologueError {
    /// Underlying local storage failed.
    #[error(transparent)]
    Kv(#[from] crabka_pgkv::KvError),
    /// Range-0 tail application failed.
    #[error(transparent)]
    Range0Tail(#[from] Range0TailError),
    /// A substrate seam failed.
    #[error("substrate recovery failed: {0}")]
    Substrate(String),
    /// Range-0 reseeding failed and serving must remain closed.
    #[error("range-0 reseed failed: {0}")]
    Reseed(String),
    /// In-doubt markers remain after the bounded settle loop.
    #[error("range recovery still has in-doubt markers")]
    InDoubtRemaining,
    /// Serving gate failed to open.
    #[error("serving gate failed: {0}")]
    Gate(String),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        Fence(RangeId),
        Barrier(RangeId, i16),
        Replay(i64),
        ReseedCounters,
        ReseedGtm,
        Reacquire(RangeId),
        Settle(RangeId),
        Served(RangeId, i16),
    }

    #[derive(Default)]
    struct Events(Mutex<Vec<Event>>);

    impl Events {
        fn push(&self, event: Event) {
            self.0.lock().expect("events lock").push(event);
        }

        fn snapshot(&self) -> Vec<Event> {
            self.0.lock().expect("events lock").clone()
        }
    }

    struct TestSubstrate {
        events: Arc<Events>,
    }

    #[async_trait::async_trait]
    impl RangeRecoverySubstrate for TestSubstrate {
        async fn fence_epoch(&self, range: RangeId) -> Result<i16, PrologueError> {
            self.events.push(Event::Fence(range));
            Ok(3)
        }

        async fn produce_barrier(
            &self,
            range: RangeId,
            epoch: i16,
        ) -> Result<ProducedBarrier, PrologueError> {
            assert!(self.events.snapshot() == vec![Event::Fence(range)]);
            self.events.push(Event::Barrier(range, epoch));
            Ok(ProducedBarrier {
                range,
                epoch,
                offset: 11,
            })
        }

        async fn replay_to_barrier(
            &self,
            _store: &dyn Kv,
            barrier: ProducedBarrier,
        ) -> Result<ReplaySummary, PrologueError> {
            let expected_prefix = vec![
                Event::Fence(barrier.range),
                Event::Barrier(barrier.range, barrier.epoch),
            ];
            assert!(self.events.snapshot() == expected_prefix);
            self.events.push(Event::Replay(barrier.offset));
            Ok(ReplaySummary {
                next_journal_seq: 9,
            })
        }
    }

    struct TestHooks {
        events: Arc<Events>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl Range0RecoveryHooks for TestHooks {
        async fn reseed_counters(&self, _store: &dyn Kv) -> Result<(), PrologueError> {
            self.events.push(Event::ReseedCounters);
            if self.fail {
                return Err(PrologueError::Reseed("counters unavailable".to_owned()));
            }
            Ok(())
        }

        async fn reseed_gtm(&self, _store: &dyn Kv) -> Result<(), PrologueError> {
            self.events.push(Event::ReseedGtm);
            Ok(())
        }
    }

    struct TestSettlement {
        events: Arc<Events>,
        outcome: SettleOutcome,
    }

    #[async_trait::async_trait]
    impl InDoubtSettlement for TestSettlement {
        async fn reacquire_in_doubt_locks(&self, range: RangeId) -> Result<(), PrologueError> {
            self.events.push(Event::Reacquire(range));
            Ok(())
        }

        async fn settle_once(&self, range: RangeId) -> Result<SettleOutcome, PrologueError> {
            self.events.push(Event::Settle(range));
            Ok(self.outcome)
        }
    }

    struct TestGate {
        events: Arc<Events>,
    }

    #[async_trait::async_trait]
    impl ServingGate for TestGate {
        async fn mark_served(
            &self,
            range: RangeId,
            epoch: i16,
            _barrier_offset: i64,
            _next_journal_seq: u64,
        ) -> Result<(), PrologueError> {
            self.events.push(Event::Served(range, epoch));
            Ok(())
        }
    }

    #[tokio::test]
    async fn prologue_happy_path_follows_exact_ordering() {
        let events = Arc::new(Events::default());
        let store = MemKv::default();

        let served = recover_range(input(
            &store,
            events.clone(),
            SettleOutcome::Complete,
            false,
        ))
        .await
        .expect("recover");

        assert!(served == serving_range());
        assert!(events.snapshot() == expected_complete_events());
    }

    #[tokio::test]
    async fn prologue_in_doubt_marker_path_refuses_until_settle() {
        let events = Arc::new(Events::default());
        let store = MemKv::default();

        let error = recover_range(input(
            &store,
            events.clone(),
            SettleOutcome::InDoubtRemaining,
            false,
        ))
        .await
        .expect_err("in doubt");

        assert!(matches!(error, PrologueError::InDoubtRemaining));
        assert!(
            !events
                .snapshot()
                .contains(&Event::Served(RangeId::COORDINATOR, 3))
        );
    }

    #[tokio::test]
    async fn range0_reseed_is_fail_closed() {
        let events = Arc::new(Events::default());
        let store = MemKv::default();

        let error = recover_range(input(&store, events.clone(), SettleOutcome::Complete, true))
            .await
            .expect_err("reseed");

        assert!(matches!(error, PrologueError::Reseed(_)));
        assert!(
            !events
                .snapshot()
                .contains(&Event::Served(RangeId::COORDINATOR, 3))
        );
    }

    #[tokio::test]
    async fn no_list_offsets_as_stable_end_shortcut_before_fencing() {
        let events = Arc::new(Events::default());
        let store = MemKv::default();

        recover_range(input(
            &store,
            events.clone(),
            SettleOutcome::Complete,
            false,
        ))
        .await
        .expect("recover");

        let observed = events.snapshot();
        let fence_at = observed
            .iter()
            .position(|event| matches!(event, Event::Fence(_)))
            .expect("fence event");
        let replay_at = observed
            .iter()
            .position(|event| matches!(event, Event::Replay(_)))
            .expect("replay event");
        assert!(fence_at < replay_at);
    }

    fn input(
        store: &dyn Kv,
        events: Arc<Events>,
        outcome: SettleOutcome,
        fail_reseed: bool,
    ) -> RecoverRange<'_> {
        let substrate = Box::leak(Box::new(TestSubstrate {
            events: events.clone(),
        }));
        let range0_hooks = Box::leak(Box::new(TestHooks {
            events: events.clone(),
            fail: fail_reseed,
        }));
        let settlement = Box::leak(Box::new(TestSettlement {
            events: events.clone(),
            outcome,
        }));
        let serving_gate = Box::leak(Box::new(TestGate { events }));
        RecoverRange {
            range: RangeId::COORDINATOR,
            store,
            substrate,
            range0_hooks,
            settlement,
            serving_gate,
        }
    }

    fn serving_range() -> ServingRange {
        ServingRange {
            range: RangeId::COORDINATOR,
            epoch: 3,
            barrier_offset: 11,
            next_journal_seq: 9,
        }
    }

    fn expected_complete_events() -> Vec<Event> {
        vec![
            Event::Fence(RangeId::COORDINATOR),
            Event::Barrier(RangeId::COORDINATOR, 3),
            Event::Replay(11),
            Event::ReseedCounters,
            Event::ReseedGtm,
            Event::Reacquire(RangeId::COORDINATOR),
            Event::Settle(RangeId::COORDINATOR),
            Event::Served(RangeId::COORDINATOR, 3),
        ]
    }
}
