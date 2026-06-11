//! Record cache (`statestore.cache.max.bytes`): write-back LRU layered over state
//! stores. `entry` -> `named` -> `thread`. Store wrappers live in sibling modules
//! (added in later tasks).
//!
//! The cache core (`LruCacheEntry` / `NamedCache` / `ThreadCache`) is consumed by
//! the store wrappers and runtime wiring added in later record-caching tasks, so
//! several public-in-crate items are unused at this point in the slice.
#![allow(dead_code)]

pub(crate) mod entry;
pub(crate) mod named;
pub(crate) mod thread;
