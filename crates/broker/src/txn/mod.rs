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
pub(crate) mod handlers;
pub(crate) mod log_record;
pub(crate) mod marker;
pub(crate) mod partitioner;
pub(crate) mod state;
pub(crate) mod util;
// TxnVersion + resolver are retained for transaction-version negotiation.
#[allow(dead_code)]
pub(crate) mod version;
