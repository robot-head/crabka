//! Crabka audit subsystem — event model, OCSF serialization, and write pipeline.
//!
//! See `docs/superpowers/specs/2026-06-18-crabka-fedramp-mla-audit-design.md`.

pub mod event;

pub use event::{
    AuditEndpoint, AuditEvent, AuditEventClass, AuditOutcome, AuditPrincipal, AuditResource,
    LifecycleKind,
};
