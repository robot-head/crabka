//! Interactive Queries v2 (KIP-796 / 960 / 968): the `StateQueryRequest` →
//! `KafkaStreams::query` → `StateQueryResult` envelope and its query objects.
//! Coexists with the v1 `ReadOnly*Store` views (see `runtime::iq_view`) on a
//! separate channel; v1 is untouched.

pub(crate) mod dispatch;
pub mod query;
pub mod request;
pub mod result;

pub use query::{KeyQuery, Query, RangeQuery, WindowKeyQuery, WindowRangeQuery};
pub use request::{Position, PositionBound, StateQuery, StateQueryRequest};
pub use result::{FailureReason, QueryResult, StateQueryResult};
