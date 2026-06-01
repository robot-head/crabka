//! KIP-932 share-state persister RPC handlers (api keys 83–87). Each handler
//! decodes the typed request, gates every `(topic, partition)` on
//! [`crate::share_coordinator::coordinator::ShareCoordinator::is_leader`] for
//! its state partition (returning per-partition `NOT_COORDINATOR` otherwise),
//! delegates to the matching coordinator method, and maps the result to a
//! per-partition `error_code`.
//!
//! These are inter-broker RPCs and carry no per-connection ACL context, so the
//! plain 4-arg [`crate::handlers::HandlerFn`] form fits (see
//! [`crate::txn::handlers::write_txn_markers`]).

pub(crate) mod delete;
pub(crate) mod initialize;
pub(crate) mod read;
pub(crate) mod read_summary;
pub(crate) mod write;
