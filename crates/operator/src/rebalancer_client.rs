//! Connect-RPC client for the standalone `crabka-rebalancer`
//! service.
//!
//! The rebalancer gives a Connect-RPC service. The unary protocol of
//! Connect is a plain `POST` to `/{package}.{Service}/{Method}` with a
//! JSON or protobuf body. On success the response is JSON with HTTP 200.
//! On an error the response has a non-2xx status and a
//! `{"code","message"}` body. This client speaks the JSON form, so the
//! operator stays independent of the prost and pbjson codegen of the
//! rebalancer.
//!
//! The JSON shape follows proto3 JSON, which pbjson produces:
//! - the field names are lowerCamelCase, for example `snapshotAtMs` and
//!   `goalsApplied`.
//! - an enum serializes as its proto value name, for example
//!   `PROPOSAL_STATUS_COMPUTED`.
//! - a 64-bit int serializes as a JSON *string*, and a 32-bit int as a
//!   number.
//! - a field with the default value is not present at all.
//!
//! The decode path below accepts all of these forms, and this is on
//! purpose.

use crabka_units::{
    ByteRate, Time,
    convert::{ByteRateExt as _, TimeExt as _},
};
use serde_json::{Value, json};

use crate::ids::{LeaderMovementCount, MaxLeadersCount, MaxReplicasCount, ReplicaMovementCount};

/// Test seam that follows [`crate::context::AdminClientHandle`].
///
/// Production wraps [`ConnectRebalancerClient`]. Reconcile tests
/// substitute a fake.
///
/// The methods take `&self` and not `&mut self`. The inner
/// `reqwest::Client` is a cheap connection pool that many callers can
/// share, so no caller needs exclusive access. The handle can therefore be
/// a plain `Arc<dyn …>` with no `Mutex`.
#[async_trait::async_trait]
pub trait RebalancerClientLike: Send + Sync {
    /// `CreateProposal` computes a proposal for the given goals. An empty
    /// goal list means the default registry of the rebalancer. The method
    /// returns a `Computed` proposal.
    async fn create_proposal(
        &self,
        goals: &[String],
    ) -> Result<RebalancerProposal, RebalancerError>;

    /// `GetProposal` fetches the current state of one proposal by id.
    async fn get_proposal(&self, id: &str) -> Result<RebalancerProposal, RebalancerError>;

    /// `ExecuteProposal` drives a computed proposal through KIP-455, with
    /// an optional KIP-73 throttle. The method returns the proposal, which
    /// is now `Executing`.
    async fn execute_proposal(
        &self,
        id: &str,
        throttle: Option<ByteRate>,
    ) -> Result<RebalancerProposal, RebalancerError>;

    /// `CancelExecution` reverts the pending reassignments and clears the
    /// throttle. The proposal then moves to `Cancelled`.
    async fn cancel_execution(&self, id: &str) -> Result<RebalancerProposal, RebalancerError>;
}

/// Lifecycle state of a proposal, decoded from the `ProposalStatus` enum
/// of the rebalancer.
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
    /// Parses the pbjson JSON form, which is the enum name string, or the
    /// numeric proto ordinal. An unknown value maps to
    /// [`ProposalStatus::Unspecified`].
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

/// Summary statistics from the `ProposalSummary` message of a proposal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProposalSummary {
    pub replica_movements: ReplicaMovementCount,
    pub leader_movements: LeaderMovementCount,
    pub max_replicas_before: MaxReplicasCount,
    pub max_replicas_after: MaxReplicasCount,
    pub max_leaders_before: MaxLeadersCount,
    pub max_leaders_after: MaxLeadersCount,
}

/// The subset of the `Proposal` message of the rebalancer that the
/// operator acts on.
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
    /// The HTTP request gave no usable response. The causes include a
    /// refused connection, a DNS failure, and a timeout.
    #[error("rebalancer transport error: {0}")]
    Transport(String),
    /// The rebalancer returned a Connect error, which is a non-2xx status
    /// with a `{code,message}` body. Examples are `failed_precondition`,
    /// `not_found`, and `unavailable`.
    #[error("rebalancer rpc error [{code}]: {message}")]
    Rpc { code: String, message: String },
    /// The response status was 2xx, but the body did not parse.
    #[error("rebalancer response decode error: {0}")]
    Decode(String),
}

/// Parses a `RebalancerProposal` out of a JSON object.
///
/// For `ExecuteProposal` and `CancelExecution`, the proposal sits below
/// `proposal`. For `CreateProposal` and `GetProposal`, the body *is* the
/// proposal. This helper covers both forms. It unwraps `proposal` when
/// that field is present.
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
            replica_movements: ReplicaMovementCount(json_i32(
                &summary,
                "replicaMovements",
                "replica_movements",
            )),
            leader_movements: LeaderMovementCount(json_i32(
                &summary,
                "leaderMovements",
                "leader_movements",
            )),
            max_replicas_before: MaxReplicasCount(json_i32(
                &summary,
                "maxReplicasBefore",
                "max_replicas_before",
            )),
            max_replicas_after: MaxReplicasCount(json_i32(
                &summary,
                "maxReplicasAfter",
                "max_replicas_after",
            )),
            max_leaders_before: MaxLeadersCount(json_i32(
                &summary,
                "maxLeadersBefore",
                "max_leaders_before",
            )),
            max_leaders_after: MaxLeadersCount(json_i32(
                &summary,
                "maxLeadersAfter",
                "max_leaders_after",
            )),
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

/// Reads an int32 proto field.
///
/// pbjson can write the field as a JSON number. It can also leave the
/// field out, which is the proto3 default `0`. As a defensive measure,
/// this function also accepts the field as a string. It accepts the
/// `camelCase` key and the `snake_case` key.
fn json_i32(obj: &Value, camel: &str, snake: &str) -> i32 {
    let v = obj.get(camel).or_else(|| obj.get(snake));
    match v {
        Some(Value::Number(n)) => n.as_i64().and_then(|v| i32::try_from(v).ok()).unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Production `RebalancerClientLike` on top of `reqwest`.
///
/// It speaks Connect and JSON over plain HTTP. The in-cluster rebalancer
/// terminates no TLS on its `:9300` Connect port.
pub struct ConnectRebalancerClient {
    base_url: String,
    http: reqwest::Client,
}

const SERVICE_PATH: &str = "crabka.rebalancer.v1.Rebalancer";

/// Body of an `ExecuteProposal` request.
///
/// It is a separate type from
/// [`RebalancerClientLike::execute_proposal`], so that a test can check
/// the exact JSON that the rebalancer sees without an HTTP round-trip.
fn execute_body(id: &str, throttle: Option<ByteRate>) -> Value {
    let mut body = json!({ "id": id });
    if let Some(throttle) = throttle {
        // proto3 JSON encodes int64 as a string.
        body["throttleBytesPerSec"] = Value::String(throttle.bytes_per_sec_i64().to_string());
    }
    body
}

impl ConnectRebalancerClient {
    /// Builds a client for the rebalancer at `base_url` with the given
    /// request timeout. A trailing slash on `base_url` is acceptable.
    #[must_use]
    pub fn new(base_url: &str, request_timeout: Time) -> Self {
        let http = reqwest::Client::builder()
            .timeout(request_timeout.to_std())
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    /// Sends a Connect unary request with POST and returns the parsed
    /// JSON body.
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

/// Maps a Connect error body, which is `{"code","message"}`, onto
/// [`RebalancerError::Rpc`].
///
/// When the body does not have that shape, this function uses the HTTP
/// status.
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
        throttle: Option<ByteRate>,
    ) -> Result<RebalancerProposal, RebalancerError> {
        let v = self
            .call("ExecuteProposal", execute_body(id, throttle))
            .await?;
        Ok(proposal_from_json(&v))
    }

    async fn cancel_execution(&self, id: &str) -> Result<RebalancerProposal, RebalancerError> {
        let v = self.call("CancelExecution", json!({ "id": id })).await?;
        Ok(proposal_from_json(&v))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{bytes_per_sec, mebibytes_per_sec, millis, secs};

    use super::*;

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
                    replica_movements: ReplicaMovementCount(4),
                    leader_movements: LeaderMovementCount(2),
                    max_replicas_before: MaxReplicasCount(10),
                    max_replicas_after: MaxReplicasCount(7),
                    max_leaders_before: MaxLeadersCount(6),
                    max_leaders_after: MaxLeadersCount(4),
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
                    replica_movements: ReplicaMovementCount(0),
                    leader_movements: LeaderMovementCount(0),
                    max_replicas_before: MaxReplicasCount(0),
                    max_replicas_after: MaxReplicasCount(0),
                    max_leaders_before: MaxLeadersCount(0),
                    max_leaders_after: MaxLeadersCount(0),
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
        let c = ConnectRebalancerClient::new("http://host:9300/", secs(30));
        assert!(c.base_url == "http://host:9300");
    }

    #[test]
    fn execute_body_encodes_throttle_as_decimal_string() {
        // proto3 JSON maps int64 to a string, so a 50 MiB/s quota must go out
        // as "52428800" — not a number, and not the base-unit float the
        // quantity stores internally.
        for (throttle, want) in [
            (
                Some(mebibytes_per_sec(50)),
                json!({ "id": "p1", "throttleBytesPerSec": "52428800" }),
            ),
            (
                Some(bytes_per_sec(1_000_000)),
                json!({ "id": "p1", "throttleBytesPerSec": "1000000" }),
            ),
            (None, json!({ "id": "p1" })),
        ] {
            assert!(execute_body("p1", throttle) == want, "case {throttle:?}");
        }
    }

    #[tokio::test]
    async fn request_timeout_is_configurable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _connection = listener.accept().await.unwrap();
            tokio::time::sleep(core::time::Duration::from_secs(1)).await;
        });
        let client = ConnectRebalancerClient::new(&format!("http://{addr}"), millis(10));

        let result = tokio::time::timeout(
            core::time::Duration::from_millis(250),
            client.call("CreateProposal", json!({})),
        )
        .await
        .expect("configured request timeout");
        assert!(matches!(result, Err(RebalancerError::Transport(_))));
        server.abort();
    }
}
