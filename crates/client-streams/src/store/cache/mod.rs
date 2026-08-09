//! Record cache (`statestore.cache.max.bytes`): a write-back LRU layered over the
//! state stores.
//!
//! The layers are `entry`, then `named`, then `thread`. The store wrappers live
//! in sibling modules, and later tasks add them.
//!
//! The store wrappers and the runtime wiring consume the cache core, which is
//! `LruCacheEntry`, `NamedCache`, and `ThreadCache`. Later record-caching tasks
//! add that wiring, so several public-in-crate items are unused at this point in
//! the slice.
#![allow(dead_code)]

pub(crate) mod entry;
pub(crate) mod named;
pub(crate) mod thread;

pub(crate) mod kv;
pub(crate) mod session;
pub(crate) mod window;
