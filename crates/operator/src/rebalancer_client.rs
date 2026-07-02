//! Connect-RPC client for the standalone `crabka-rebalancer`
//! service.
//!
//! The rebalancer exposes a Connect-RPC service. Connect's
//! unary protocol is a plain `POST` to
//! `/{package}.{Service}/{Method}` with a JSON (or protobuf) body; the
//! response is JSON with HTTP 200 on success, or a non-2xx status with a
//! `{"code","message"}` body on error. We speak the JSON flavor so the
//! operator stays decoupled from the rebalancer's prost/pbjson codegen.
//!
//! JSON shape notes (proto3 JSON / pbjson):
//! - field names are lowerCamelCase (`snapshotAtMs`, `goalsApplied`);
//! - enums serialize as their proto value name (`PROPOSAL_STATUS_COMPUTED`);
//! - 64-bit ints serialize as JSON *strings*; 32-bit ints as numbers;
//! - default-valued fields are omitted entirely.
//!
//! The decode path below is deliberately tolerant of all of the above.

use std::time::Duration;

use serde_json::{Value, json};

/// Test seam mirroring [`crate::context::AdminClientHandle`]. Production
/// wraps [`ConnectRebalancerClient`]; reconcile tests substitute a fake.
///
/// Methods take `&self` (not `&mut self`): the inner `reqwest::Client`
/// is a cheap, shareable connection pool, so no exclusive access is
/// needed and the handle can be a bare `Arc<dyn …>` with no `Mutex`.
#[async_trait::async_trait]
pub trait RebalancerClientLike: Send + Sync {
    /// `CreateProposal` — compute a proposal for the given goals (empty =
    /// the rebalancer's default registry). Returns a `Computed` proposal.
    async fn create_proposal(
        &self,
        goals: &[String],
    ) -> Result<RebalancerProposal, RebalancerError>;

    /// `GetProposal` — fetch the current state of a proposal by id.
    async fn get_proposal(&self, id: &str) -> Result<RebalancerProposal, RebalancerError>;

    /// `ExecuteProposal` — drive a computed proposal through KIP-455 under
    /// an optional KIP-73 throttle. Returns the now-`Executing` proposal.
    async fn execute_proposal(
        &self,
        id: &str,
        throttle_bytes_per_sec: Option<i64>,
    ) -> Result<RebalancerProposal, RebalancerError>;

    /// `CancelExecution` — revert pending reassignments + clear throttle,
    /// transitioning the proposal to `Cancelled`.
    async fn cancel_execution(&self, id: &str) -> Result<RebalancerProposal, RebalancerError>;
}

/// Lifecycle state of a proposal, decoded from the rebalancer's
/// `ProposalStatus` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalStatus {
    Unspecified,
    Computed,
    Executing,
    Completed,
    Failed,
    Cancelled,
}

impl ProposalStatus {
    /// Parse from the pbjson JSON form (enum name string) or the numeric
    /// proto ordinal. Unknown values map to [`ProposalStatus::Unspecified`].
    #[must_use]
    pub fn from_json(v: &Value) -> Self {
        match v {
            Value::String(s) => match s.as_str() {
                "PROPOSAL_STATUS_COMPUTED" => Self::Computed,
                "PROPOSAL_STATUS_EXECUTING" => Self::Executing,
                "PROPOSAL_STATUS_COMPLETED" => Self::Completed,
                "PROPOSAL_STATUS_FAILED" => Self::Failed,
                "PROPOSAL_STATUS_CANCELLED" => Self::Cancelled,
                _ => Self::Unspecified,
            },
            Value::Number(n) => match n.as_i64() {
                Some(1) => Self::Computed,
                Some(2) => Self::Executing,
                Some(3) => Self::Completed,
                Some(4) => Self::Failed,
                Some(5) => Self::Cancelled,
                _ => Self::Unspecified,
            },
            _ => Self::Unspecified,
        }
    }
}

/// Summary stats from a proposal's `ProposalSummary` message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProposalSummary {
    pub replica_movements: i32,
    pub leader_movements: i32,
    pub max_replicas_before: i32,
    pub max_replicas_after: i32,
    pub max_leaders_before: i32,
    pub max_leaders_after: i32,
}

/// The subset of the rebalancer's `Proposal` message the operator acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalancerProposal {
    pub id: String,
    pub status: ProposalStatus,
    pub summary: ProposalSummary,
    pub goals_applied: Vec<String>,
    pub movement_count: usize,
    pub failure_reason: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RebalancerError {
    /// The HTTP request never produced a usable response (connection
    /// refused, DNS failure, timeout, …).
    #[error("rebalancer transport error: {0}")]
    Transport(String),
    /// The rebalancer returned a Connect error (non-2xx with a
    /// `{code,message}` body), e.g. `failed_precondition` /
    /// `not_found` / `unavailable`.
    #[error("rebalancer rpc error [{code}]: {message}")]
    Rpc { code: String, message: String },
    /// The response status was 2xx but the body didn't parse.
    #[error("rebalancer response decode error: {0}")]
    Decode(String),
}

/// Parse a `RebalancerProposal` out of a JSON object. For `ExecuteProposal`
/// / `CancelExecution` the proposal is nested under `proposal`; for
/// `CreateProposal` / `GetProposal` the body *is* the proposal. This
/// helper handles both by unwrapping `proposal` when present.
#[must_use]
pub fn proposal_from_json(body: &Value) -> RebalancerProposal {
    let p = body.get("proposal").unwrap_or(body);
    let summary = p.get("summary").cloned().unwrap_or(Value::Null);
    let goals_applied = p
        .get("goalsApplied")
        .or_else(|| p.get("goals_applied"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    RebalancerProposal {
        id: p
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        status: ProposalStatus::from_json(p.get("status").unwrap_or(&Value::Null)),
        summary: ProposalSummary {
            replica_movements: json_i32(&summary, "replicaMovements", "replica_movements"),
            leader_movements: json_i32(&summary, "leaderMovements", "leader_movements"),
            max_replicas_before: json_i32(&summary, "maxReplicasBefore", "max_replicas_before"),
            max_replicas_after: json_i32(&summary, "maxReplicasAfter", "max_replicas_after"),
            max_leaders_before: json_i32(&summary, "maxLeadersBefore", "max_leaders_before"),
            max_leaders_after: json_i32(&summary, "maxLeadersAfter", "max_leaders_after"),
        },
        goals_applied,
        movement_count: p
            .get("movements")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        failure_reason: p
            .get("failureReason")
            .or_else(|| p.get("failure_reason"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// Read an int32 proto field that pbjson may have emitted as a JSON
/// number, omitted entirely (proto3 default → `0`), or — defensively —
/// stringified. Accepts either the `camelCase` or `snake_case` key.
#[allow(clippy::cast_possible_truncation)]
fn json_i32(obj: &Value, camel: &str, snake: &str) -> i32 {
    let v = obj.get(camel).or_else(|| obj.get(snake));
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) as i32,
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Production `RebalancerClientLike` backed by `reqwest` speaking
/// Connect/JSON over plain HTTP (the in-cluster rebalancer terminates no
/// TLS on its `:9300` Connect port).
pub struct ConnectRebalancerClient {
    base_url: String,
    http: reqwest::Client,
}

const SERVICE_PATH: &str = "crabka.rebalancer.v1.Rebalancer";

impl ConnectRebalancerClient {
    /// Build a client for the rebalancer at `base_url` (e.g.
    /// `http://host:9300`). A trailing slash on `base_url` is tolerated.
    #[must_use]
    pub fn new(base_url: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    /// POST a Connect unary request and return the parsed JSON body.
    async fn call(&self, method: &str, body: Value) -> Result<Value, RebalancerError> {
        let url = format!("{}/{SERVICE_PATH}/{method}", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).expect("request body serializes"))
            .send()
            .await
            .map_err(|e| RebalancerError::Transport(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| RebalancerError::Transport(e.to_string()))?;
        if status.is_success() {
            serde_json::from_str(&text).map_err(|e| RebalancerError::Decode(e.to_string()))
        } else {
            Err(connect_error(&text, status.as_u16()))
        }
    }
}

/// Map a Connect error body (`{"code","message"}`) onto
/// [`RebalancerError::Rpc`]. Falls back to the HTTP status when the body
/// isn't the expected shape.
fn connect_error(text: &str, http_status: u16) -> RebalancerError {
    let parsed: Option<Value> = serde_json::from_str(text).ok();
    let code = parsed
        .as_ref()
        .and_then(|v| v.get("code"))
        .and_then(Value::as_str)
        .map_or_else(|| format!("http_{http_status}"), str::to_string);
    let message = parsed
        .as_ref()
        .and_then(|v| v.get("message"))
        .and_then(Value::as_str)
        .map_or_else(|| text.trim().to_string(), str::to_string);
    RebalancerError::Rpc { code, message }
}

#[async_trait::async_trait]
impl RebalancerClientLike for ConnectRebalancerClient {
    async fn create_proposal(
        &self,
        goals: &[String],
    ) -> Result<RebalancerProposal, RebalancerError> {
        let body = if goals.is_empty() {
            json!({})
        } else {
            json!({ "goals": goals })
        };
        let v = self.call("CreateProposal", body).await?;
        Ok(proposal_from_json(&v))
    }

    async fn get_proposal(&self, id: &str) -> Result<RebalancerProposal, RebalancerError> {
        let v = self.call("GetProposal", json!({ "id": id })).await?;
        Ok(proposal_from_json(&v))
    }

    async fn execute_proposal(
        &self,
        id: &str,
        throttle_bytes_per_sec: Option<i64>,
    ) -> Result<RebalancerProposal, RebalancerError> {
        let mut body = json!({ "id": id });
        if let Some(t) = throttle_bytes_per_sec {
            // proto3 JSON encodes int64 as a string.
            body["throttleBytesPerSec"] = Value::String(t.to_string());
        }
        let v = self.call("ExecuteProposal", body).await?;
        Ok(proposal_from_json(&v))
    }

    async fn cancel_execution(&self, id: &str) -> Result<RebalancerProposal, RebalancerError> {
        let v = self.call("CancelExecution", json!({ "id": id })).await?;
        Ok(proposal_from_json(&v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn status_parses_pbjson_enum_names() {
        for (input, want) in [
            ("PROPOSAL_STATUS_COMPUTED", ProposalStatus::Computed),
            ("PROPOSAL_STATUS_EXECUTING", ProposalStatus::Executing),
            ("PROPOSAL_STATUS_COMPLETED", ProposalStatus::Completed),
            ("PROPOSAL_STATUS_FAILED", ProposalStatus::Failed),
            ("PROPOSAL_STATUS_CANCELLED", ProposalStatus::Cancelled),
        ] {
            assert!(
                ProposalStatus::from_json(&json!(input)) == want,
                "case {input:?}"
            );
        }
    }

    #[test]
    fn status_parses_numeric_ordinals_and_unknown() {
        for (input, want) in [
            (json!(1), ProposalStatus::Computed),
            (json!(2), ProposalStatus::Executing),
            (json!("WAT"), ProposalStatus::Unspecified),
            (Value::Null, ProposalStatus::Unspecified),
        ] {
            assert!(ProposalStatus::from_json(&input) == want, "case {input:?}");
        }
    }

    #[test]
    fn proposal_parses_full_create_response() {
        // Shape pbjson emits for CreateProposal (the body IS the Proposal).
        let body = json!({
            "id": "abc-123",
            "status": "PROPOSAL_STATUS_COMPUTED",
            "goalsApplied": ["RackAware", "ReplicaDistribution"],
            "summary": {
                "replicaMovements": 4,
                "leaderMovements": 2,
                "maxReplicasBefore": 10,
                "maxReplicasAfter": 7,
                "maxLeadersBefore": 6,
                "maxLeadersAfter": 4
            },
            "movements": [{}, {}, {}, {}],
            "throttleBytesPerSec": "52428800"
        });
        let p = proposal_from_json(&body);
        assert!(
            p == RebalancerProposal {
                id: "abc-123".to_string(),
                status: ProposalStatus::Computed,
                summary: ProposalSummary {
                    replica_movements: 4,
                    leader_movements: 2,
                    max_replicas_before: 10,
                    max_replicas_after: 7,
                    max_leaders_before: 6,
                    max_leaders_after: 4,
                },
                goals_applied: vec!["RackAware".to_string(), "ReplicaDistribution".to_string()],
                movement_count: 4,
                failure_reason: None,
            }
        );
    }

    #[test]
    fn proposal_unwraps_nested_proposal_field() {
        // Shape pbjson emits for ExecuteProposal / CancelExecution.
        let body = json!({
            "proposal": {
                "id": "xyz",
                "status": "PROPOSAL_STATUS_EXECUTING"
            }
        });
        let p = proposal_from_json(&body);
        assert!(p.id == "xyz");
        assert!(p.status == ProposalStatus::Executing);
    }

    #[test]
    fn proposal_tolerates_omitted_defaults() {
        // pbjson omits default-valued fields entirely: a zero-movement
        // proposal has no `summary`, no `movements`, no `goalsApplied`.
        let body = json!({
            "id": "empty",
            "status": "PROPOSAL_STATUS_COMPUTED"
        });
        let p = proposal_from_json(&body);
        assert!(
            p == RebalancerProposal {
                id: "empty".to_string(),
                status: ProposalStatus::Computed,
                summary: ProposalSummary {
                    replica_movements: 0,
                    leader_movements: 0,
                    max_replicas_before: 0,
                    max_replicas_after: 0,
                    max_leaders_before: 0,
                    max_leaders_after: 0,
                },
                goals_applied: vec![],
                movement_count: 0,
                failure_reason: None,
            }
        );
    }

    #[test]
    fn failure_reason_decoded_when_present() {
        let body = json!({
            "id": "f",
            "status": "PROPOSAL_STATUS_FAILED",
            "failureReason": "broker 3 unreachable"
        });
        let p = proposal_from_json(&body);
        assert!(p.status == ProposalStatus::Failed);
        assert!(p.failure_reason.as_deref() == Some("broker 3 unreachable"));
    }

    #[test]
    fn connect_error_parses_code_and_message() {
        let e = connect_error(
            r#"{"code":"failed_precondition","message":"proposal not in Computed state"}"#,
            400,
        );
        match e {
            RebalancerError::Rpc { code, message } => {
                assert!(code == "failed_precondition");
                assert!(message == "proposal not in Computed state");
            }
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn connect_error_falls_back_to_http_status() {
        let e = connect_error("upstream exploded", 503);
        match e {
            RebalancerError::Rpc { code, message } => {
                assert!(code == "http_503");
                assert!(message == "upstream exploded");
            }
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[test]
    fn json_i32_accepts_number_string_and_missing() {
        let obj = json!({ "a": 5, "b": "9" });
        for (key, want) in [("a", 5), ("b", 9), ("missing", 0)] {
            assert!(json_i32(&obj, key, key) == want, "case {key:?}");
        }
    }

    #[test]
    fn base_url_trailing_slash_trimmed() {
        let c = ConnectRebalancerClient::new("http://host:9300/");
        assert!(c.base_url == "http://host:9300");
    }
}
