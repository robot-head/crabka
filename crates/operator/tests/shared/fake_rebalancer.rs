//! In-memory `RebalancerClientLike` for `KafkaRebalance` reconcile tests.
//!
//! This fake records every Connect-RPC that the reconcile issues. It
//! serves one scripted response for each method, so that a test can
//! exercise the state machine of the controller without a live
//! `crabka-rebalancer` process. It follows the `FakeAdminClient` pattern
//! and uses `std::sync::Mutex` for interior mutability behind the `&self`
//! trait methods.

#![allow(dead_code)]

use std::sync::Mutex as StdMutex;

use crabka_operator::{
    ids::{LeaderMovementCount, MaxLeadersCount, MaxReplicasCount, ReplicaMovementCount},
    rebalancer_client::{
        ProposalStatus, ProposalSummary, RebalancerClientLike, RebalancerError, RebalancerProposal,
    },
};
use crabka_units::ByteRate;

/// One recorded Connect-RPC.
#[derive(Debug, Clone, PartialEq)]
pub enum RebalCall {
    CreateProposal(Vec<String>),
    GetProposal(String),
    ExecuteProposal {
        id: String,
        throttle: Option<ByteRate>,
    },
    CancelExecution(String),
}

/// A scripted reply for one method.
///
/// The type is `Clone`, so that the fake can serve the same reply on
/// repeated calls. `RebalancerError` is not `Clone`, so this type models
/// the two error forms structurally and builds a new error on each call.
#[derive(Debug, Clone)]
pub enum FakeResp {
    Ok(RebalancerProposal),
    Rpc { code: String, message: String },
    Transport(String),
}

impl FakeResp {
    fn into_result(self) -> Result<RebalancerProposal, RebalancerError> {
        match self {
            Self::Ok(p) => Ok(p),
            Self::Rpc { code, message } => Err(RebalancerError::Rpc { code, message }),
            Self::Transport(m) => Err(RebalancerError::Transport(m)),
        }
    }
}

#[derive(Default)]
pub struct FakeRebalancerClient {
    pub calls: StdMutex<Vec<RebalCall>>,
    pub create: StdMutex<Option<FakeResp>>,
    pub get: StdMutex<Option<FakeResp>>,
    pub execute: StdMutex<Option<FakeResp>>,
    pub cancel: StdMutex<Option<FakeResp>>,
}

impl FakeRebalancerClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_create(self, r: FakeResp) -> Self {
        *self.create.lock().unwrap() = Some(r);
        self
    }

    #[must_use]
    pub fn with_get(self, r: FakeResp) -> Self {
        *self.get.lock().unwrap() = Some(r);
        self
    }

    #[must_use]
    pub fn with_execute(self, r: FakeResp) -> Self {
        *self.execute.lock().unwrap() = Some(r);
        self
    }

    #[must_use]
    pub fn with_cancel(self, r: FakeResp) -> Self {
        *self.cancel.lock().unwrap() = Some(r);
        self
    }

    pub fn calls(&self) -> Vec<RebalCall> {
        self.calls.lock().unwrap().clone()
    }

    fn serve(slot: &StdMutex<Option<FakeResp>>) -> Result<RebalancerProposal, RebalancerError> {
        slot.lock()
            .unwrap()
            .clone()
            .expect("fake rebalancer: no scripted response for this method")
            .into_result()
    }
}

/// Convenience builder for a scripted proposal.
#[must_use]
pub fn fake_proposal(id: &str, status: ProposalStatus) -> RebalancerProposal {
    RebalancerProposal {
        id: id.into(),
        status,
        summary: ProposalSummary {
            replica_movements: ReplicaMovementCount(2),
            leader_movements: LeaderMovementCount(1),
            max_replicas_before: MaxReplicasCount(8),
            max_replicas_after: MaxReplicasCount(5),
            max_leaders_before: MaxLeadersCount(4),
            max_leaders_after: MaxLeadersCount(2),
        },
        goals_applied: vec!["ReplicaDistribution".into()],
        movement_count: 2,
        failure_reason: None,
    }
}

#[async_trait::async_trait]
impl RebalancerClientLike for FakeRebalancerClient {
    async fn create_proposal(
        &self,
        goals: &[String],
    ) -> Result<RebalancerProposal, RebalancerError> {
        self.calls
            .lock()
            .unwrap()
            .push(RebalCall::CreateProposal(goals.to_vec()));
        Self::serve(&self.create)
    }

    async fn get_proposal(&self, id: &str) -> Result<RebalancerProposal, RebalancerError> {
        self.calls
            .lock()
            .unwrap()
            .push(RebalCall::GetProposal(id.into()));
        Self::serve(&self.get)
    }

    async fn execute_proposal(
        &self,
        id: &str,
        throttle: Option<ByteRate>,
    ) -> Result<RebalancerProposal, RebalancerError> {
        self.calls.lock().unwrap().push(RebalCall::ExecuteProposal {
            id: id.into(),
            throttle,
        });
        Self::serve(&self.execute)
    }

    async fn cancel_execution(&self, id: &str) -> Result<RebalancerProposal, RebalancerError> {
        self.calls
            .lock()
            .unwrap()
            .push(RebalCall::CancelExecution(id.into()));
        Self::serve(&self.cancel)
    }
}
