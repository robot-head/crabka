//! Internal audit event model — the source of truth for the KSI-MLA-LET catalog.

use serde::Serialize;

/// Outcome of an audited action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AuditOutcome {
    Success,
    Failure,
}

/// The actor responsible for an audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditPrincipal {
    pub name: String,
    pub auth_method: String,
}

/// Network source of the action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEndpoint {
    pub ip: String,
    pub port: u16,
}

/// A resource affected by an admin operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditResource {
    pub resource_type: String,
    pub name: String,
}

/// Broker lifecycle transitions worth auditing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LifecycleKind {
    BrokerStarted,
    BrokerStopping,
    ConfigApplied,
    TlsReloaded,
}

/// OCSF class grouping used for record headers/routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventClass {
    Authentication,
    Authorization,
    ApiActivity,
    ApplicationLifecycle,
    /// Internal meta-record (signed chain checkpoint); not an OCSF event.
    Checkpoint,
}

impl AuditEventClass {
    /// Stable lowercase identifier, used as the `event_class` record header value.
    #[must_use]
    pub fn as_header(self) -> &'static str {
        match self {
            AuditEventClass::Authentication => "authentication",
            AuditEventClass::Authorization => "authorization",
            AuditEventClass::ApiActivity => "api_activity",
            AuditEventClass::ApplicationLifecycle => "application_lifecycle",
            AuditEventClass::Checkpoint => "checkpoint",
        }
    }

    /// Compact tag for spool framing.
    #[must_use]
    pub fn tag(self) -> u8 {
        match self {
            AuditEventClass::Authentication => 0,
            AuditEventClass::Authorization => 1,
            AuditEventClass::ApiActivity => 2,
            AuditEventClass::ApplicationLifecycle => 3,
            AuditEventClass::Checkpoint => 4,
        }
    }

    /// Inverse of [`Self::tag`].
    #[must_use]
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(AuditEventClass::Authentication),
            1 => Some(AuditEventClass::Authorization),
            2 => Some(AuditEventClass::ApiActivity),
            3 => Some(AuditEventClass::ApplicationLifecycle),
            4 => Some(AuditEventClass::Checkpoint),
            _ => None,
        }
    }

    /// Inverse of [`Self::as_header`].
    #[must_use]
    pub fn from_header(s: &str) -> Option<Self> {
        match s {
            "authentication" => Some(AuditEventClass::Authentication),
            "authorization" => Some(AuditEventClass::Authorization),
            "api_activity" => Some(AuditEventClass::ApiActivity),
            "application_lifecycle" => Some(AuditEventClass::ApplicationLifecycle),
            "checkpoint" => Some(AuditEventClass::Checkpoint),
            _ => None,
        }
    }
}

/// A single auditable security event. Times are caller-supplied epoch-millis so
/// the crate stays pure and deterministically testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditEvent {
    Authentication {
        outcome: AuditOutcome,
        mechanism: String,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        reason: Option<String>,
        time_ms: i64,
    },
    AuthorizationDenied {
        principal: AuditPrincipal,
        source: AuditEndpoint,
        resource_type: String,
        resource_name: String,
        operation: String,
        time_ms: i64,
    },
    AdminOperation {
        outcome: AuditOutcome,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        operation: String,
        resources: Vec<AuditResource>,
        time_ms: i64,
    },
    Lifecycle {
        kind: LifecycleKind,
        node_id: i64,
        time_ms: i64,
    },
}

impl AuditEvent {
    /// The OCSF class this event maps to.
    #[must_use]
    pub fn class(&self) -> AuditEventClass {
        match self {
            AuditEvent::Authentication { .. } => AuditEventClass::Authentication,
            AuditEvent::AuthorizationDenied { .. } => AuditEventClass::Authorization,
            AuditEvent::AdminOperation { .. } => AuditEventClass::ApiActivity,
            AuditEvent::Lifecycle { .. } => AuditEventClass::ApplicationLifecycle,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn class_tag_round_trips_and_header_maps() {
        for c in [
            AuditEventClass::Authentication,
            AuditEventClass::Authorization,
            AuditEventClass::ApiActivity,
            AuditEventClass::ApplicationLifecycle,
            AuditEventClass::Checkpoint,
        ] {
            check!(AuditEventClass::from_tag(c.tag()) == Some(c));
            check!(AuditEventClass::from_header(c.as_header()) == Some(c));
        }
        check!(AuditEventClass::from_tag(99) == None);
        check!(AuditEventClass::from_header("nope") == None);
    }

    #[test]
    fn event_class_maps_each_variant() {
        let authn = AuditEvent::Authentication {
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
        check!(authn.class() == AuditEventClass::Authentication);

        let denied = AuditEvent::AuthorizationDenied {
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
            time_ms: 1,
        };
        check!(denied.class() == AuditEventClass::Authorization);

        let admin = AuditEvent::AdminOperation {
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
            time_ms: 2,
        };
        check!(admin.class() == AuditEventClass::ApiActivity);

        let life = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 3,
        };
        check!(life.class() == AuditEventClass::ApplicationLifecycle);
    }
}
