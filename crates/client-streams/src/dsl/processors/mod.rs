//! Concrete `Processor` impls the DSL lowering attaches to the Processor-API
//! `Topology`. Each op's lowering thunk constructs one of these, capturing the
//! user closure; the four KV type parameters are inferred from the impl.
pub(crate) mod aggregate;
pub(crate) mod change;
pub(crate) mod cogroup_merge;
pub(crate) mod fk;
pub(crate) mod global_join;
pub(crate) mod join;
pub(crate) mod join_grace;
pub(crate) mod ktable_join;
pub(crate) mod outer_join_store;
pub(crate) mod session_aggregate;
pub(crate) mod sliding_window_aggregate;
pub(crate) mod stateless;
pub(crate) mod stream_join;
pub(crate) mod suppress;
pub(crate) mod table;
pub(crate) mod table_aggregate;
pub(crate) mod tuple_forwarder;
pub(crate) mod window_aggregate;
