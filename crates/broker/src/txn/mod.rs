//! Transaction subsystem for the Crabka broker.

pub(crate) mod bootstrap;
pub(crate) mod coordinator;
pub(crate) mod decision;
/// Compositional end-to-end model of the exactly-once read guarantee: composes
/// the txn-coordinator decision cores with the LSO mechanics and the
/// `read_committed` fetch-visibility core.
#[cfg(test)]
#[path = "eos_composition_model.rs"]
mod eos_composition_model;
/// KIP-939: idle-transaction reaper that aborts timed-out transactions while
/// skipping 2PC (no-timeout) transactions.
pub(crate) mod expiration;
pub(crate) mod handlers;
pub(crate) mod log_record;
pub(crate) mod marker;
pub(crate) mod partitioner;
pub(crate) mod state;
/// KIP-939 two-phase-commit decision cores (timeout resolution + the
/// safety-critical idle-abort predicate). Pure; model-checked in
/// [`two_pc_model`].
pub(crate) mod two_pc;
/// Exhaustive stateright model of the KIP-939 2PC timeout-safety property.
#[cfg(test)]
#[path = "two_pc_model.rs"]
mod two_pc_model;
pub(crate) mod util;
// TxnVersion + resolver are retained for transaction-version negotiation.
#[allow(dead_code)]
pub(crate) mod version;
