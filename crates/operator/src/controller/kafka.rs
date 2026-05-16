//! Kafka CRD reconciler.
//!
//! Slice 17 stub: the only thing this reconciler does is patch the
//! `status.conditions` subresource with `Ready=True, reason=Stub`.
//! Real workload reconciliation (`StatefulSet` / `Service` / `ConfigMap`)
//! lands in slice 18.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use kube::ResourceExt as _;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use serde_json::json;

use crate::context::Context;
use crate::crd::{Kafka, KafkaCondition, KafkaStatus};

const FIELD_MANAGER: &str = "crabka-operator";

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
}

/// Run the `Kafka` controller forever. Returns only on irrecoverable
/// stream error (the kube-rs `Controller` re-establishes watches on
/// recoverable errors internally).
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    tracing::info!(%ns, %name, "reconciling Kafka");

    let api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let status = KafkaStatus {
        conditions: vec![KafkaCondition {
            type_: "Ready".into(),
            status: "True".into(),
            reason: "Stub".into(),
            message: "Slice 17 placeholder: no workload reconciliation yet.".into(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
        }],
    };
    let patch = json!({ "status": status });
    api.patch_status(
        &name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(Action::requeue(Duration::from_mins(1)))
}

pub fn error_policy(_obj: Arc<Kafka>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}
