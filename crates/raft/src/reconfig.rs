//! KIP-853 reconfiguration coordinator: single-voter add, remove, and update
//! with safety guards.

use crabka_metadata::{MetadataRecord, Voter, VoterSet, VotersRecord};

use crate::{NodeId, RaftError};

/// A request to add one voter. The candidate must already be a caught-up observer.
#[derive(Debug, Clone)]
pub struct AddVoter {
    pub voter: Voter,
}

/// A request to remove one voter.
#[derive(Debug, Clone)]
pub struct RemoveVoter {
    pub id: NodeId,
    pub directory_id: uuid::Uuid,
}

/// A request to update one voter's endpoints and supported version range.
#[derive(Debug, Clone)]
pub struct UpdateVoter {
    pub voter: Voter,
}

/// Outcome shared by all three operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfigOutcome {
    Committed,
    NotLeader { leader: Option<NodeId> },
}

/// The raft operations the coordinator needs. `ControllerHandle` implements
/// this trait.
#[async_trait::async_trait]
pub trait ReconfigOps: Send + Sync {
    fn current_voters(&self) -> VoterSet;
    fn leader(&self) -> Option<NodeId>;
    fn is_leader(&self) -> bool;
    /// Highest log index the leader has. The observer-lag checks use it.
    fn leader_last_index(&self) -> u64;
    /// Last replicated index for an observer or learner, if known.
    fn observer_index(&self, id: NodeId) -> Option<u64>;
    async fn add_learner(&self, id: NodeId, node: crate::Node) -> Result<(), RaftError>;
    async fn change_membership(
        &self,
        ids: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError>;
    async fn submit_records(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), RaftError>;
}

pub struct Coordinator<'a, O: ReconfigOps> {
    ops: &'a O,
    lock: &'a tokio::sync::Mutex<()>,
    observer_lag_bound: u64,
}

impl<'a, O: ReconfigOps> Coordinator<'a, O> {
    pub fn new(ops: &'a O, lock: &'a tokio::sync::Mutex<()>, observer_lag_bound: u64) -> Self {
        Self {
            ops,
            lock,
            observer_lag_bound,
        }
    }

    fn not_leader_outcome(&self) -> Option<ReconfigOutcome> {
        (!self.ops.is_leader()).then(|| ReconfigOutcome::NotLeader {
            leader: self.ops.leader(),
        })
    }

    fn try_reconfig_lock(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, RaftError> {
        self.lock
            .try_lock()
            .map_err(|_| RaftError::ReconfigInProgress)
    }

    async fn submit_voters(&self, voters: VoterSet) -> Result<(), RaftError> {
        self.ops
            .submit_records(vec![MetadataRecord::V1Voters(VotersRecord { voters })])
            .await
    }

    /// Adds a single voter.
    ///
    /// This method first registers the candidate as a learner. The candidate
    /// must be caught up within `observer_lag_bound` before promotion. On
    /// success the coordinator commits the new membership and writes an
    /// authoritative `V1Voters` record.
    ///
    /// # Errors
    ///
    /// - [`RaftError::ReconfigInProgress`] if another reconfiguration holds the lock.
    /// - [`RaftError::VoterNotCaughtUp`] if the candidate observer lags too far.
    /// - Any error that the underlying raft operations return.
    #[tracing::instrument(level = "info", skip_all, fields(voter = req.voter.id.0), err)]
    pub async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        if let Some(outcome) = self.not_leader_outcome() {
            return Ok(outcome);
        }
        let _guard = self.try_reconfig_lock()?;
        let current = self.ops.current_voters();
        if current.contains(req.voter.id) {
            return Ok(ReconfigOutcome::Committed); // idempotent
        }
        let node = crate::Node {
            directory_id: req.voter.directory_id,
            endpoints: req.voter.endpoints.clone(),
            kraft_version: req.voter.kraft_version,
        };
        self.ops.add_learner(req.voter.id, node).await?;
        let lag = self
            .ops
            .leader_last_index()
            .saturating_sub(self.ops.observer_index(req.voter.id).unwrap_or(0));
        if lag > self.observer_lag_bound {
            return Err(RaftError::VoterNotCaughtUp {
                id: req.voter.id,
                lag,
            });
        }
        let next = current.with_voter(req.voter.clone());
        self.ops.change_membership(next.ids()).await?;
        self.submit_voters(next).await?;
        Ok(ReconfigOutcome::Committed)
    }

    /// Removes a single voter. This method refuses to drop the last voter.
    ///
    /// # Errors
    ///
    /// - [`RaftError::ReconfigInProgress`] if another reconfiguration holds the lock.
    /// - [`RaftError::ReconfigRejected`] if the removal would leave no voters.
    /// - Any error that the underlying raft operations return.
    #[tracing::instrument(level = "info", skip_all, fields(voter = req.id.0), err)]
    pub async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        if let Some(outcome) = self.not_leader_outcome() {
            return Ok(outcome);
        }
        let _guard = self.try_reconfig_lock()?;
        let current = self.ops.current_voters();
        match current.get(req.id) {
            // No voter with this id: already absent, idempotent no-op.
            None => return Ok(ReconfigOutcome::Committed),
            // A voter with this id exists, but it is a different incarnation than
            // the one targeted (e.g. the node rejoined under a new directory_id
            // after a restart). Do not remove the current voter on a stale request.
            Some(v) if v.directory_id != req.directory_id => {
                return Ok(ReconfigOutcome::Committed);
            }
            Some(_) => {}
        }
        let next = current.without_voter(req.id);
        if next.is_empty() {
            return Err(RaftError::ReconfigRejected(
                "cannot remove the last voter".into(),
            ));
        }
        self.ops.change_membership(next.ids()).await?;
        self.submit_voters(next).await?;
        Ok(ReconfigOutcome::Committed)
    }

    /// Updates an existing voter's endpoints and supported version range.
    ///
    /// # Errors
    ///
    /// - [`RaftError::ReconfigInProgress`] if another reconfiguration holds the lock.
    /// - [`RaftError::ReconfigRejected`] if the voter id is unknown.
    /// - Any error that the underlying raft operations return.
    #[tracing::instrument(level = "info", skip_all, fields(voter = req.voter.id.0), err)]
    pub async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        if let Some(outcome) = self.not_leader_outcome() {
            return Ok(outcome);
        }
        let _guard = self.try_reconfig_lock()?;
        let current = self.ops.current_voters();
        if !current.contains(req.voter.id) {
            return Err(RaftError::ReconfigRejected("unknown voter".into()));
        }
        let next = current.with_voter(req.voter);
        self.submit_voters(next).await?;
        Ok(ReconfigOutcome::Committed)
    }
}
