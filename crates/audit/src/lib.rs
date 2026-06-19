//! Crabka audit subsystem — event model, OCSF serialization, and write pipeline.
//!
//! See `docs/superpowers/specs/2026-06-18-crabka-fedramp-mla-audit-design.md`.

pub mod chain;
pub mod checkpoint;
pub mod event;
pub mod log;
pub mod ocsf;
pub mod signing;
pub mod sink;

pub use chain::{ChainState, GENESIS_HEAD, chain_hash};
pub use checkpoint::{Checkpoint, EVENT_CLASS_CHECKPOINT};
pub use event::{
    AuditEndpoint, AuditEvent, AuditEventClass, AuditOutcome, AuditPrincipal, AuditResource,
    LifecycleKind,
};
pub use log::{AuditLog, AuditWriter};
pub use ocsf::{ProductInfo, to_ocsf};
pub use signing::{
    FileEd25519Signer, SigningKeyProvider, checkpoint_signing_bytes, verify_signature,
};
pub use sink::{AuditError, AuditRecord, AuditSink, MemorySink};
