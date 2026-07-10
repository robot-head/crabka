//! Kill-broker orchestration for the `failover` scenario. Uses the
//! in-cluster `kube` API via the Job's `ServiceAccount` — the
//! `ServiceAccount` needs `pods: get,list,delete` in the target namespace,
//! and is only mounted when a failover scenario actually requests it.
//!
//! `partition0_leader` resolves the topic metadata first and then maps the
//! partition leader's broker id back to the matching `StatefulSet` pod. If that
//! metadata probe fails, the caller can fall back to the first broker pod by
//! name-sort for backwards-compatible smoke runs.

use anyhow::{Context, Result, anyhow};
use crabka_client_core::{Client as KafkaClient, security::ClientSecurity};
use crabka_protocol::owned::metadata_response::MetadataResponse;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Client as KubeClient,
    api::{Api, DeleteParams, ListParams},
};

use crate::scenario::Stack;

/// Discover an in-cluster Kubernetes client (uses the in-pod
/// `serviceAccount` token). When running outside the cluster — useful for
/// `cargo test` — this returns an `Err` and the caller should report the
/// failover as skipped.
pub async fn try_client() -> Result<KubeClient> {
    KubeClient::try_default()
        .await
        .context("build in-cluster kube client")
}

/// Query topic metadata and return partition 0's current leader broker id.
pub async fn partition0_leader_from_metadata(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<i32> {
    let client = KafkaClient::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("bench-failover-targeter")
        .maybe_security(security)
        .build()
        .await
        .context("build metadata client")?;
    let md = client
        .refresh_metadata()
        .await
        .context("refresh metadata")?;
    let leader = partition0_leader_id(&md, topic)
        .ok_or_else(|| anyhow!("metadata did not contain {topic} partition 0 leader"))?;
    client.close();
    Ok(leader)
}

/// Delete the requested broker pod. When `leader_id` is `Some`, the pod whose
/// ordinal matches that broker id is targeted; otherwise the first matching pod
/// is deleted for backwards-compatible smoke runs. `grace_period_seconds = 0`
/// means SIGKILL.
pub async fn kill_broker_pod(
    client: &KubeClient,
    stack: Stack,
    namespace: &str,
    leader_id: Option<i32>,
) -> Result<String> {
    let prefix = stack.broker_pod_regex().trim_start_matches('^');

    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = pods
        .list(&ListParams::default())
        .await
        .context("list pods in namespace")?;
    let mut names: Vec<String> = list
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .filter(|n| n.starts_with(prefix))
        .collect();
    names.sort();
    let Some(target) = choose_target_pod(&names, stack, leader_id) else {
        return Err(anyhow!("no broker pod matched prefix {prefix}"));
    };

    let dp = DeleteParams::default().grace_period(0);
    pods.delete(&target, &dp)
        .await
        .with_context(|| format!("delete pod {target}"))?;

    Ok(target)
}

/// Delete the first broker pod (alphabetical) matching the stack's pod-name
/// regex root. Kept as a compatibility wrapper for non-partition-targeted runs.
pub async fn kill_first_broker(
    client: &KubeClient,
    stack: Stack,
    namespace: &str,
) -> Result<String> {
    kill_broker_pod(client, stack, namespace, None).await
}

fn partition0_leader_id(md: &MetadataResponse, topic: &str) -> Option<i32> {
    md.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .map(|p| p.leader_id)
        .filter(|id| *id >= 0)
}

fn choose_target_pod(names: &[String], stack: Stack, leader_id: Option<i32>) -> Option<String> {
    let prefix = stack.broker_pod_regex().trim_start_matches('^');
    let mut matching: Vec<&String> = names.iter().filter(|n| n.starts_with(prefix)).collect();
    matching.sort();

    if let Some(id) = leader_id {
        let broker_suffix = format!("-{id}");
        let nodepool_suffix = format!("-{id}-0");
        if let Some(name) = matching
            .iter()
            .find(|n| n.ends_with(&broker_suffix) || n.ends_with(&nodepool_suffix))
        {
            return Some((*name).clone());
        }
    }

    matching.first().map(|n| (*n).clone())
}

#[cfg(test)]
mod tests {
    use crabka_protocol::owned::metadata_response::{
        MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
    };

    use super::*;

    #[test]
    fn partition0_leader_is_read_from_metadata() {
        let md = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("bench-topic".into()),
                partitions: vec![
                    MetadataResponsePartition {
                        partition_index: 1,
                        leader_id: 0,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        partition_index: 0,
                        leader_id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(partition0_leader_id(&md, "bench-topic") == Some(2));
    }

    #[test]
    fn target_pod_uses_actual_leader_id_for_each_stack_naming_shape() {
        let crabka_multi = vec![
            "demo-broker-0-0".to_string(),
            "demo-broker-1-0".to_string(),
            "demo-broker-2-0".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&crabka_multi, Stack::Crabka, Some(1)).as_deref()
                == Some("demo-broker-1-0")
        );

        let crabka_single_pool = vec![
            "demo-brokers-0".to_string(),
            "demo-brokers-1".to_string(),
            "demo-brokers-2".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&crabka_single_pool, Stack::Crabka, Some(2)).as_deref()
                == Some("demo-brokers-2")
        );

        let kafka = vec![
            "demo-kafka-0".to_string(),
            "demo-kafka-1".to_string(),
            "demo-kafka-2".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&kafka, Stack::Kafka, Some(1)).as_deref() == Some("demo-kafka-1")
        );
    }

    #[test]
    fn target_pod_falls_back_to_first_matching_broker_when_leader_unknown() {
        let pods = vec![
            "demo-kafka-2".to_string(),
            "demo-kafka-0".to_string(),
            "demo-kafka-1".to_string(),
        ];

        assert2::assert!(
            choose_target_pod(&pods, Stack::Kafka, None).as_deref() == Some("demo-kafka-0")
        );
    }
}
