//! State stores + changelog backing (sub-project #3).
pub mod api;
pub mod memory;
pub(crate) mod registry;
pub use api::{KeyValueStore, StateStore};
pub use memory::InMemoryKeyValueStore;
