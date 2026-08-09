//! Metrics reconcile.
//!
//! This module renders `PodMonitor` and `ServiceMonitor` objects, applies
//! them with a dynamic SSA apply against `monitoring.coreos.com/v1`, and
//! holds the `reconcile_metrics` orchestrator. The two variants are
//! mutually exclusive. The operator garbage-collects the leftover objects
//! of the abandoned variant on a best-effort basis.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use kube::{
    Resource as _,
    api::{Api, DeleteParams, DynamicObject, Patch, PatchParams},
    core::{ApiResource, GroupVersionKind},
};
use serde_json::json;

use crate::{
    context::Context,
    controller::{
        common::{
            APP_LABEL, FIELD_MANAGER, ReconcileError, apply_object, common_labels, owner_ref,
        },
        kafka_node_pool::METRICS_PORT,
    },
    crd::{Kafka, PodMonitorSpec, ServiceMonitorSpec},
};

pub(crate) fn render_pod_monitor(
    owner: &Kafka,
    cfg: &PodMonitorSpec,
) -> Result<serde_json::Value, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    Ok(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": {
            "name": format!("{name}-broker"),
            "namespace": ns,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "namespaceSelector": { "matchNames": [ns] },
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": APP_LABEL,
                    "app.kubernetes.io/instance": name,
                }
            },
            "podMetricsEndpoints": [{
                "port": "metrics",
                "path": "/metrics",
                "interval": cfg.interval.as_deref().unwrap_or("30s"),
                "scrapeTimeout": cfg.scrape_timeout.as_deref().unwrap_or("10s"),
            }],
        }
    }))
}

pub(crate) fn render_service_monitor(
    owner: &Kafka,
    cfg: &ServiceMonitorSpec,
) -> Result<serde_json::Value, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    Ok(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "name": format!("{name}-broker"),
            "namespace": ns,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "namespaceSelector": { "matchNames": [ns] },
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": APP_LABEL,
                    "app.kubernetes.io/instance": name,
                }
            },
            "endpoints": [{
                "port": "metrics",
                "path": "/metrics",
                "interval": cfg.interval.as_deref().unwrap_or("30s"),
                "scrapeTimeout": cfg.scrape_timeout.as_deref().unwrap_or("10s"),
            }],
        }
    }))
}

pub(crate) fn render_metrics_service(
    owner: &Kafka,
    cfg: &ServiceMonitorSpec,
) -> Result<Service, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let mut labels = common_labels(&name, &owner.spec.kafka_version, None);
    for (k, v) in &cfg.labels {
        labels.entry(k.clone()).or_insert_with(|| v.clone());
    }
    let mut selector = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".to_string(), APP_LABEL.to_string());
    selector.insert("app.kubernetes.io/instance".to_string(), name.clone());

    let svc: Service = serde_json::from_value(json!({
        "metadata": {
            "name": format!("{name}-broker-metrics"),
            "namespace": owner.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "clusterIP": "None",
            "selector": selector,
            "ports": [{
                "name": "metrics",
                "port": METRICS_PORT,
                "protocol": "TCP",
                "targetPort": "metrics",
            }],
        }
    }))?;
    Ok(svc)
}

async fn apply_dynamic(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), ReconcileError> {
    let (group, version) = api_version
        .split_once('/')
        .ok_or_else(|| ReconcileError::Malformed("apiVersion missing '/'".into()))?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let obj: DynamicObject = serde_json::from_value(body.clone())?;
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    match api.patch(name, &pp, &Patch::Apply(&obj)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(status)) if status.code == 404 => {
            Err(ReconcileError::PrometheusOperatorCrdsMissing)
        }
        Err(e) => Err(e.into()),
    }
}

async fn delete_dynamic_if_exists(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
) -> Result<(), ReconcileError> {
    let (group, version) = api_version
        .split_once('/')
        .ok_or_else(|| ReconcileError::Malformed("apiVersion missing '/'".into()))?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(status)) if status.code == 404 => Ok(()),
        Err(kube::Error::Api(status)) if status.code == 405 => {
            Err(ReconcileError::PrometheusOperatorCrdsMissing)
        }
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip_all, fields(cluster = %name, namespace = %namespace))]
pub(crate) async fn reconcile_metrics(
    ctx: &Context,
    owner: &Kafka,
    name: &str,
    namespace: &str,
) -> Option<Result<(), ReconcileError>> {
    let cfg = owner.spec.metrics_config.as_ref()?;

    if cfg.pod_monitor.is_some() && cfg.service_monitor.is_some() {
        return Some(Err(ReconcileError::MetricsMutuallyExclusive));
    }

    if let Some(pm) = &cfg.pod_monitor {
        let body = match render_pod_monitor(owner, pm) {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let pm_name = format!("{name}-broker");
        if let Err(e) = apply_dynamic(
            &ctx.client,
            namespace,
            "monitoring.coreos.com/v1",
            "PodMonitor",
            "podmonitors",
            &pm_name,
            &body,
        )
        .await
        {
            return Some(Err(e));
        }
        let _ = delete_dynamic_if_exists(
            &ctx.client,
            namespace,
            "monitoring.coreos.com/v1",
            "ServiceMonitor",
            "servicemonitors",
            &pm_name,
        )
        .await;
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
        let _ = svc_api
            .delete(&format!("{name}-broker-metrics"), &DeleteParams::default())
            .await;
    } else if let Some(sm) = &cfg.service_monitor {
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
        let svc = match render_metrics_service(owner, sm) {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };
        let svc_name = format!("{name}-broker-metrics");
        if let Err(e) = apply_object(&svc_api, &svc_name, &svc).await {
            return Some(Err(e));
        }
        let body = match render_service_monitor(owner, sm) {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let sm_name = format!("{name}-broker");
        if let Err(e) = apply_dynamic(
            &ctx.client,
            namespace,
            "monitoring.coreos.com/v1",
            "ServiceMonitor",
            "servicemonitors",
            &sm_name,
            &body,
        )
        .await
        {
            return Some(Err(e));
        }
        let _ = delete_dynamic_if_exists(
            &ctx.client,
            namespace,
            "monitoring.coreos.com/v1",
            "PodMonitor",
            "podmonitors",
            &sm_name,
        )
        .await;
    }

    Some(Ok(()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::crd::KafkaSpec;

    fn test_kafka() -> Kafka {
        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
                gres_registry: None,
            },
        );
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn render_pod_monitor_minimal_defaults() {
        let pm = render_pod_monitor(&test_kafka(), &PodMonitorSpec::default()).unwrap();
        assert!(
            pm == json!({
                "apiVersion": "monitoring.coreos.com/v1",
                "kind": "PodMonitor",
                "metadata": {
                    "name": "demo-broker",
                    "namespace": "default",
                    "labels": {
                        "app.kubernetes.io/instance": "demo",
                        "app.kubernetes.io/managed-by": "crabka-operator",
                        "app.kubernetes.io/name": "crabka-broker",
                        "app.kubernetes.io/version": "0.1.1",
                    },
                    "ownerReferences": [{
                        "apiVersion": "crabka.io/v1alpha1",
                        "blockOwnerDeletion": true,
                        "controller": true,
                        "kind": "Kafka",
                        "name": "demo",
                        "uid": "00000000-0000-0000-0000-000000000001",
                    }],
                },
                "spec": {
                    "namespaceSelector": { "matchNames": ["default"] },
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": "crabka-broker",
                            "app.kubernetes.io/instance": "demo",
                        }
                    },
                    "podMetricsEndpoints": [{
                        "port": "metrics",
                        "path": "/metrics",
                        "interval": "30s",
                        "scrapeTimeout": "10s",
                    }],
                }
            })
        );
    }

    #[test]
    fn render_pod_monitor_overrides_and_labels() {
        let pm_spec = PodMonitorSpec {
            interval: Some("15s".into()),
            scrape_timeout: Some("5s".into()),
            labels: [("team".to_string(), "platform".to_string())].into(),
        };
        let pm = render_pod_monitor(&test_kafka(), &pm_spec).unwrap();
        assert!(
            pm == json!({
                "apiVersion": "monitoring.coreos.com/v1",
                "kind": "PodMonitor",
                "metadata": {
                    "name": "demo-broker",
                    "namespace": "default",
                    "labels": {
                        "app.kubernetes.io/instance": "demo",
                        "app.kubernetes.io/managed-by": "crabka-operator",
                        "app.kubernetes.io/name": "crabka-broker",
                        "app.kubernetes.io/version": "0.1.1",
                        "team": "platform",
                    },
                    "ownerReferences": [{
                        "apiVersion": "crabka.io/v1alpha1",
                        "blockOwnerDeletion": true,
                        "controller": true,
                        "kind": "Kafka",
                        "name": "demo",
                        "uid": "00000000-0000-0000-0000-000000000001",
                    }],
                },
                "spec": {
                    "namespaceSelector": { "matchNames": ["default"] },
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": "crabka-broker",
                            "app.kubernetes.io/instance": "demo",
                        }
                    },
                    "podMetricsEndpoints": [{
                        "port": "metrics",
                        "path": "/metrics",
                        "interval": "15s",
                        "scrapeTimeout": "5s",
                    }],
                }
            })
        );
    }

    #[test]
    fn render_service_monitor_kind_and_endpoints_key() {
        let sm = render_service_monitor(&test_kafka(), &ServiceMonitorSpec::default()).unwrap();
        assert!(
            sm == json!({
                "apiVersion": "monitoring.coreos.com/v1",
                "kind": "ServiceMonitor",
                "metadata": {
                    "name": "demo-broker",
                    "namespace": "default",
                    "labels": {
                        "app.kubernetes.io/instance": "demo",
                        "app.kubernetes.io/managed-by": "crabka-operator",
                        "app.kubernetes.io/name": "crabka-broker",
                        "app.kubernetes.io/version": "0.1.1",
                    },
                    "ownerReferences": [{
                        "apiVersion": "crabka.io/v1alpha1",
                        "blockOwnerDeletion": true,
                        "controller": true,
                        "kind": "Kafka",
                        "name": "demo",
                        "uid": "00000000-0000-0000-0000-000000000001",
                    }],
                },
                "spec": {
                    "namespaceSelector": { "matchNames": ["default"] },
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": "crabka-broker",
                            "app.kubernetes.io/instance": "demo",
                        }
                    },
                    "endpoints": [{
                        "port": "metrics",
                        "path": "/metrics",
                        "interval": "30s",
                        "scrapeTimeout": "10s",
                    }],
                }
            })
        );
    }

    #[test]
    fn render_metrics_service_is_headless_with_named_port() {
        let svc = render_metrics_service(&test_kafka(), &ServiceMonitorSpec::default()).unwrap();
        let spec = svc.spec.unwrap();
        assert!(spec.cluster_ip.as_deref() == Some("None"));
        assert!(
            spec.ports
                == Some(vec![k8s_openapi::api::core::v1::ServicePort {
                    app_protocol: None,
                    name: Some("metrics".into()),
                    node_port: None,
                    port: 9404,
                    protocol: Some("TCP".into()),
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                            "metrics".into()
                        )
                    ),
                }])
        );
    }
}
