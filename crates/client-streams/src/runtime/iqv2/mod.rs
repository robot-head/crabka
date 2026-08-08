//! Interactive Queries v2 (KIP-796 / 960 / 968): the `StateQueryRequest` →
//! `KafkaStreams::query` → `StateQueryResult` envelope and its query objects.
//!
//! This module uses a channel of its own, separate from the v1
//! `ReadOnly*Store` views in `runtime::iq_view`. It does not change v1.

pub(crate) mod dispatch;
pub mod query;
pub mod request;
pub mod result;

pub use query::{
    KeyQuery, MultiVersionedKeyQuery, Query, RangeQuery, VersionedKeyQuery, WindowKeyQuery,
    WindowRangeQuery,
};
pub use request::{Position, PositionBound, StateQuery, StateQueryRequest};
pub use result::{FailureReason, QueryResult, StateQueryResult};
