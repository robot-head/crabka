//! Kill-broker orchestration for the `failover` scenario. Uses the
//! in-cluster `kube` API via the Job's `ServiceAccount` — the
//! `ServiceAccount` needs `pods: get,list,delete` in the target namespace,
//! and is only mounted when a failover scenario actually requests it.
//!
//! Today we always target the first broker pod by name-sort
//! (`partition0_leader` is the documented value but we use a simple
//! heuristic — the first pod in alphabetical pod-name order is
//! deterministically `*-0`, which the Strimzi range assignor and the
//! Crabka `KRaft` default both elect as the partition-0 leader on a freshly
//! created topic). Future revisions can query partition metadata via
//! `crabka_client_admin::topics::list_topics` for stricter targeting.

use anyhow::{Context, Result, anyhow};
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, DeleteParams, ListParams};

use crate::scenario::Stack;

/// Discover an in-cluster Kubernetes client (uses the in-pod
/// `serviceAccount` token). When running outside the cluster — useful for
/// `cargo test` — this returns an `Err` and the caller should report the
/// failover as skipped.
pub async fn try_client() -> Result<Client> {
    Client::try_default()
        .await
        .context("build in-cluster kube client")
}

/// Delete the first broker pod (alphabetical) matching the stack's
/// pod-name regex root. `grace_period_seconds = 0` means SIGKILL.
pub async fn kill_first_broker(client: &Client, stack: Stack, namespace: &str) -> Result<String> {
    // Convert "^demo-brokers-" → "demo-brokers-" for the prefix match.
    // The trailing pod ordinal (-0, -1, ...) is what gives us a
    // deterministic election target since StatefulSet pod names are
    // ordinal-suffixed.
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
    let Some(target) = names.first() else {
        return Err(anyhow!("no broker pod matched prefix {prefix}"));
    };

    let dp = DeleteParams::default().grace_period(0);
    pods.delete(target, &dp)
        .await
        .with_context(|| format!("delete pod {target}"))?;

    Ok(target.clone())
}
