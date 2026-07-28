//! The `"sr"` group-membership loop: `FindCoordinator` → `JoinGroup` → (leader:
//! select+assign) `SyncGroup` → `Heartbeat`, rejoining on rebalance and leaving
//! on shutdown. Generic over `protocol_type` + opaque JSON metadata/assignment;
//! models `client-consumer`'s coordinator loop without consumer semantics.

use bytes::Bytes;
use crabka_client_core::{Client, ClientSecurity};
use crabka_protocol::owned::{
    find_coordinator_request::FindCoordinatorRequest,
    heartbeat_request::HeartbeatRequest,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    leave_group_request::{LeaveGroupRequest, MemberIdentity},
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
};
use crabka_units::prelude::*;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{
    PrimaryState,
    protocol::{
        SR_PROTOCOL_NAME, SR_PROTOCOL_TYPE, SR_VERSION, SchemaRegistryGroupAssignment,
        SchemaRegistryIdentity, select_master,
    },
};
use crate::config::RegistryRuntimeConfig;

// Kafka group error codes (defined locally to avoid a crabka-broker dependency).
const NONE: i16 = 0;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const ILLEGAL_GENERATION: i16 = 22;
const UNKNOWN_MEMBER_ID: i16 = 25;
const REBALANCE_IN_PROGRESS: i16 = 27;
const MEMBER_ID_REQUIRED: i16 = 79;

/// `PartialEq` but not `Eq`: [`Time`] stores `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ElectionPolicy {
    session_timeout: Time,
    rebalance_timeout: Time,
    heartbeat_interval: Time,
    reconnect_backoff: Time,
}

fn election_policy(runtime: &RegistryRuntimeConfig) -> ElectionPolicy {
    ElectionPolicy {
        session_timeout: runtime.election_session_timeout,
        rebalance_timeout: runtime.election_rebalance_timeout,
        heartbeat_interval: runtime.election_heartbeat_interval,
        reconnect_backoff: runtime.election_reconnect_backoff,
    }
}

pub(super) struct ElectionClient {
    pub bootstrap: String,
    pub client_id: String,
    pub group_id: String,
    pub identity: SchemaRegistryIdentity,
    pub tx: watch::Sender<PrimaryState>,
    /// SR-to-broker Kafka-client security for the coordinator connections.
    /// `None` = plaintext (the pre-security default).
    pub security: Option<ClientSecurity>,
    pub runtime: RegistryRuntimeConfig,
}

impl ElectionClient {
    /// Run until `cancel` fires. Reconnects + rejoins on any error; publishes
    /// `PrimaryState` after each successful `SyncGroup`.
    pub async fn run(self, cancel: CancellationToken) {
        let mut member_id = String::new();
        loop {
            if cancel.is_cancelled() {
                return;
            }
            match self.connect_and_run(&mut member_id, &cancel).await {
                Ok(()) => return, // cancelled mid-loop
                Err(e) => {
                    tracing::warn!(error = %e, "election: reconnecting after error");
                    // unknown member on reconnect: rejoin from scratch
                    member_id.clear();
                    let _ = self.tx.send(PrimaryState::default());
                    if cancel
                        .run_until_cancelled(tokio::time::sleep(
                            election_policy(&self.runtime).reconnect_backoff.to_std(),
                        ))
                        .await
                        .is_none()
                    {
                        return;
                    }
                }
            }
        }
    }

    async fn connect_and_run(
        &self,
        member_id: &mut String,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let coord = self.connect_coordinator().await?;
        let result = self.run_session(&coord, member_id, cancel).await;
        coord.close(); // deterministic teardown on every exit (Ok = cancelled, Err = reconnect)
        result
    }

    async fn run_session(
        &self,
        coord: &Client,
        member_id: &mut String,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        loop {
            let (generation, assignment) = self.join_and_sync(coord, member_id).await?;
            self.publish(&assignment);
            // heartbeat until a rebalance/error forces a rejoin
            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        let _ = coord.send(LeaveGroupRequest {
                            group_id: self.group_id.clone(),
                            member_id: member_id.clone(),
                            members: vec![MemberIdentity { member_id: member_id.clone(), ..Default::default() }],
                            ..Default::default()
                        }).await;
                        return Ok(());
                    }
                    () = tokio::time::sleep(election_policy(&self.runtime).heartbeat_interval.to_std()) => {
                        let hb = coord.send(HeartbeatRequest {
                            group_id: self.group_id.clone(),
                            generation_id: generation,
                            member_id: member_id.clone(),
                            ..Default::default()
                        }).await?;
                        match hb.error_code {
                            NONE => {} // healthy: keep heartbeating
                            REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION => break, // rejoin (keep member_id)
                            UNKNOWN_MEMBER_ID => { member_id.clear(); break; }   // rejoin from scratch
                            NOT_COORDINATOR | COORDINATOR_NOT_AVAILABLE | COORDINATOR_LOAD_IN_PROGRESS => {
                                anyhow::bail!("heartbeat coordinator error {}", hb.error_code); // reconnect
                            }
                            other => { tracing::debug!(code = other, "heartbeat transient"); }
                        }
                    }
                }
            }
        }
    }

    /// `FindCoordinator` via the bootstrap, then a `Client` to the coordinator.
    async fn connect_coordinator(&self) -> anyhow::Result<Client> {
        let boot = Client::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .build()
            .await?;
        let fc = boot
            .send(FindCoordinatorRequest {
                key: self.group_id.clone(),
                key_type: 0, // group
                coordinator_keys: vec![self.group_id.clone()],
                ..Default::default()
            })
            .await;
        boot.close(); // release the bootstrap connection regardless of outcome
        let fc = fc?;
        let (host, port) = fc
            .coordinators
            .first()
            .map(|c| (c.host.clone(), c.port))
            .filter(|(h, _)| !h.is_empty())
            .or_else(|| (!fc.host.is_empty()).then(|| (fc.host.clone(), fc.port)))
            .ok_or_else(|| anyhow::anyhow!("no coordinator for group {}", self.group_id))?;
        Ok(Client::builder()
            .bootstrap(format!("{host}:{port}"))
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .build()
            .await?)
    }

    /// `JoinGroup` (+ `MEMBER_ID_REQUIRED` two-step) then `SyncGroup`; as
    /// leader, select the master and assign it to every member. Returns
    /// (generation, our assignment bytes).
    async fn join_and_sync(
        &self,
        coord: &Client,
        member_id: &mut String,
    ) -> anyhow::Result<(i32, Bytes)> {
        let metadata = Bytes::from(serde_json::to_vec(&self.identity)?);
        let policy = election_policy(&self.runtime);
        let mk_join = |mid: String| JoinGroupRequest {
            group_id: self.group_id.clone(),
            // `JoinGroup` is a generated wire request: the extents render back
            // to raw `int32` milliseconds here.
            session_timeout_ms: policy.session_timeout.millis_i32(),
            rebalance_timeout_ms: policy.rebalance_timeout.millis_i32(),
            member_id: mid,
            protocol_type: SR_PROTOCOL_TYPE.to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: SR_PROTOCOL_NAME.to_string(),
                metadata: metadata.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut jg = coord.send(mk_join(member_id.clone())).await?;
        if jg.error_code == MEMBER_ID_REQUIRED {
            member_id.clone_from(&jg.member_id);
            jg = coord.send(mk_join(member_id.clone())).await?;
        }
        if jg.error_code != NONE {
            anyhow::bail!("JoinGroup error {}", jg.error_code);
        }
        member_id.clone_from(&jg.member_id);
        let assignments = if jg.leader == jg.member_id {
            // leader: decode identities, select master, assign to all members
            let ids: Vec<(String, SchemaRegistryIdentity)> = jg
                .members
                .iter()
                .filter_map(|m| {
                    serde_json::from_slice(&m.metadata)
                        .ok()
                        .map(|id| (m.member_id.clone(), id))
                })
                .collect();
            // cp's leader broadcasts the elected master's member-id + identity.
            let (master_member_id, master_identity) = match select_master(&ids) {
                Some((mid, idn)) => (Some(mid), Some(idn)),
                None => (None, None),
            };
            let assign = Bytes::from(serde_json::to_vec(&SchemaRegistryGroupAssignment {
                error: 0,
                master: master_member_id,
                master_identity,
                version: SR_VERSION,
            })?);
            jg.members
                .iter()
                .map(|m| SyncGroupRequestAssignment {
                    member_id: m.member_id.clone(),
                    assignment: assign.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };
        let sg = coord
            .send(SyncGroupRequest {
                group_id: self.group_id.clone(),
                generation_id: jg.generation_id,
                member_id: member_id.clone(),
                protocol_type: Some(SR_PROTOCOL_TYPE.to_string()),
                protocol_name: jg.protocol_name.clone(),
                assignments,
                ..Default::default()
            })
            .await?;
        if sg.error_code != NONE {
            anyhow::bail!("SyncGroup error {}", sg.error_code);
        }
        Ok((jg.generation_id, sg.assignment))
    }

    fn publish(&self, assignment: &Bytes) {
        let parsed: SchemaRegistryGroupAssignment =
            serde_json::from_slice(assignment).unwrap_or_default();
        // cp's assignment carries the master's identity; we're primary iff it
        // equals ours (the `master` member-id string isn't comparable here
        // because our own member_id lives in the coordinator loop, not the
        // identity — the identity match is the cp-faithful primary signal).
        let is_primary = parsed.master_identity.as_ref() == Some(&self.identity);
        let primary_url = parsed
            .master_identity
            .as_ref()
            .map(SchemaRegistryIdentity::url);
        let _ = self.tx.send(PrimaryState {
            is_primary,
            primary_url,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RegistryRuntimeConfig;

    #[test]
    fn election_policy_uses_configured_runtime() {
        let runtime = RegistryRuntimeConfig {
            election_session_timeout: secs(12),
            election_rebalance_timeout: secs(40),
            election_heartbeat_interval: secs(2),
            election_reconnect_backoff: millis(750),
            ..RegistryRuntimeConfig::default()
        };

        assert2::check!(
            election_policy(&runtime)
                == ElectionPolicy {
                    session_timeout: secs(12),
                    rebalance_timeout: secs(40),
                    heartbeat_interval: secs(2),
                    reconnect_backoff: millis(750),
                }
        );
    }
}
