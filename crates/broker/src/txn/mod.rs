//! Transaction subsystem for the Crabka broker.

pub(crate) mod bootstrap;
pub(crate) mod coordinator;
pub(crate) mod decision;
pub(crate) mod handlers;
pub(crate) mod log_record;
pub(crate) mod marker;
pub(crate) mod partitioner;
pub(crate) mod state;
pub(crate) mod util;
// TxnVersion + resolver are retained for transaction-version negotiation.
#[allow(dead_code)]
pub(crate) mod version;
