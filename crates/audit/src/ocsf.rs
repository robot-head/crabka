//! OCSF (Open Cybersecurity Schema Framework) serialization for audit events.

use serde_json::json;

use crate::event::{AuditEvent, AuditOutcome, LifecycleKind};

/// Product identity stamped into every OCSF record's `metadata`.
#[derive(Debug, Clone)]
pub struct ProductInfo {
    pub vendor_name: String,
    pub name: String,
    pub version: String,
}

const SCHEMA_VERSION: &str = "1.3.0";

fn status_id(outcome: AuditOutcome) -> i64 {
    match outcome {
        AuditOutcome::Success => 1,
        AuditOutcome::Failure => 2,
    }
}

fn metadata(product: &ProductInfo) -> serde_json::Value {
    json!({
        "version": SCHEMA_VERSION,
        "product": {
            "vendor_name": product.vendor_name,
            "name": product.name,
            "version": product.version,
        }
    })
}

fn ocsf_authentication(
    outcome: AuditOutcome,
    mechanism: &str,
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    reason: Option<&String>,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3002 Authentication, activity 1 = Logon.
    let class_uid = 3002_i64;
    let activity_id = 1_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Logon",
        "time": time_ms,
        "status_id": status_id(outcome),
        "status_detail": reason,
        "auth_protocol": mechanism,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "metadata": metadata(product),
    })
}

fn ocsf_authorization_denied(
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    resource_type: &str,
    resource_name: &str,
    operation: &str,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3003 Authorize Session, activity 2 = Deny.
    let class_uid = 3003_i64;
    let activity_id = 2_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Deny",
        "time": time_ms,
        "status_id": 2,
        "operation": operation,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": [ { "type": resource_type, "name": resource_name } ],
        "metadata": metadata(product),
    })
}

fn ocsf_admin_operation(
    outcome: AuditOutcome,
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    operation: &str,
    resources: &[crate::event::AuditResource],
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown/Other (operation in `api`).
    let class_uid = 6003_i64;
    let activity_id = 0_i64;
    let res: Vec<serde_json::Value> = resources
        .iter()
        .map(|r| json!({ "type": r.resource_type, "name": r.name }))
        .collect();
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "time": time_ms,
        "status_id": status_id(outcome),
        "api": { "operation": operation, "service": { "name": "kafka" } },
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": res,
        "metadata": metadata(product),
    })
}

fn ocsf_lifecycle(
    kind: LifecycleKind,
    node_id: i64,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6002 Application Lifecycle.
    let class_uid = 6002_i64;
    let (activity_id, activity_name) = match kind {
        LifecycleKind::BrokerStarted => (1_i64, "BrokerStarted"),
        LifecycleKind::BrokerStopping => (4, "BrokerStopping"),
        LifecycleKind::ConfigApplied => (3, "ConfigApplied"),
        LifecycleKind::TlsReloaded => (3, "TlsReloaded"),
    };
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": activity_name,
        "time": time_ms,
        "status_id": 1,
        "device": { "uid": node_id.to_string(), "type_id": 1 },
        "metadata": metadata(product),
    })
}

/// Serialize an [`AuditEvent`] to an OCSF JSON object.
#[must_use]
pub fn to_ocsf(event: &AuditEvent, product: &ProductInfo) -> serde_json::Value {
    match event {
        AuditEvent::Authentication {
            outcome,
            mechanism,
            principal,
            source,
            reason,
            time_ms,
        } => ocsf_authentication(
            *outcome,
            mechanism,
            principal,
            source,
            reason.as_ref(),
            *time_ms,
            product,
        ),
        AuditEvent::AuthorizationDenied {
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            time_ms,
        } => ocsf_authorization_denied(
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            *time_ms,
            product,
        ),
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms,
        } => ocsf_admin_operation(
            *outcome, principal, source, operation, resources, *time_ms, product,
        ),
        AuditEvent::Lifecycle {
            kind,
            node_id,
            time_ms,
        } => ocsf_lifecycle(*kind, *node_id, *time_ms, product),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::event::*;

    fn product() -> ProductInfo {
        ProductInfo {
            vendor_name: "Crabka".into(),
            name: "crabka-broker".into(),
            version: "0.3.7".into(),
        }
    }

    #[test]
    fn authentication_failure_maps_to_3002() {
        let ev = AuditEvent::Authentication {
            outcome: AuditOutcome::Failure,
            mechanism: "SASL/PLAIN".into(),
            principal: AuditPrincipal {
                name: "alice".into(),
                auth_method: "SaslPlain".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.1".into(),
                port: 51120,
            },
            reason: Some("authentication failed".into()),
            time_ms: 1_700_000_000_000,
        };
        let j = to_ocsf(&ev, &product());
        check!(j["class_uid"] == 3002);
        check!(j["category_uid"] == 3);
        check!(j["status_id"] == 2);
        check!(j["time"] == 1_700_000_000_000_i64);
        check!(j["actor"]["user"]["name"] == "alice");
        check!(j["src_endpoint"]["ip"] == "10.0.0.1");
        check!(j["src_endpoint"]["port"] == 51120);
        check!(j["auth_protocol"] == "SASL/PLAIN");
        check!(j["metadata"]["product"]["vendor_name"] == "Crabka");
    }

    #[test]
    fn authorization_denied_maps_to_3003_failure() {
        let ev = AuditEvent::AuthorizationDenied {
            principal: AuditPrincipal {
                name: "bob".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.2".into(),
                port: 4444,
            },
            resource_type: "Topic".into(),
            resource_name: "secrets".into(),
            operation: "Write".into(),
            time_ms: 5,
        };
        let j = to_ocsf(&ev, &product());
        check!(j["class_uid"] == 3003);
        check!(j["status_id"] == 2);
        check!(j["type_uid"] == 300_302_i64);
        check!(j["actor"]["user"]["name"] == "bob");
        check!(j["resources"][0]["type"] == "Topic");
        check!(j["resources"][0]["name"] == "secrets");
        check!(j["operation"] == "Write");
    }

    #[test]
    fn admin_operation_maps_to_6003_with_resources() {
        let ev = AuditEvent::AdminOperation {
            outcome: AuditOutcome::Success,
            principal: AuditPrincipal {
                name: "admin".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.3".into(),
                port: 9092,
            },
            operation: "CreateTopics".into(),
            resources: vec![AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
            time_ms: 6,
        };
        let j = to_ocsf(&ev, &product());
        check!(j["class_uid"] == 6003);
        check!(j["category_uid"] == 6);
        check!(j["status_id"] == 1);
        check!(j["api"]["operation"] == "CreateTopics");
        check!(j["resources"][0]["name"] == "orders");
    }

    #[test]
    fn lifecycle_maps_to_6002() {
        let ev = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 7,
        };
        let j = to_ocsf(&ev, &product());
        check!(j["class_uid"] == 6002);
        check!(j["status_id"] == 1);
        check!(j["activity_name"] == "BrokerStarted");
        check!(j["device"]["uid"] == "1");
    }
}
