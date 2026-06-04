//! State stores + changelog backing (sub-project #3).
pub mod api;
pub mod memory;
pub use api::{KeyValueStore, StateStore};
pub use memory::InMemoryKeyValueStore;
