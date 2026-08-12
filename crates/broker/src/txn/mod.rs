//! Transaction subsystem for the Crabka broker.

pub(crate) mod bootstrap;
pub(crate) mod coordinator;
pub(crate) mod decision;
/// Compositional end-to-end model of the exactly-once read guarantee. It
/// composes the txn-coordinator decision cores with the LSO mechanics and the
/// `read_committed` fetch-visibility core.
#[cfg(test)]
#[path = "eos_composition_model.rs"]
mod eos_composition_model;
/// KIP-939 idle-transaction reaper. It aborts a timed-out transaction, and
/// skips a 2PC transaction, which has no timeout.
pub(crate) mod expiration;
pub(crate) mod handlers;
pub(crate) mod log_record;
pub(crate) mod marker;
pub(crate) mod partitioner;
pub(crate) mod state;
/// KIP-939 two-phase-commit decision cores: timeout resolution and the
/// safety-critical idle-abort predicate. They are pure, and [`two_pc_model`]
/// model-checks them.
pub(crate) mod two_pc;
/// Exhaustive stateright model of the KIP-939 2PC timeout-safety property.
#[cfg(test)]
#[path = "two_pc_model.rs"]
mod two_pc_model;
pub(crate) mod util;
pub(crate) mod version;
