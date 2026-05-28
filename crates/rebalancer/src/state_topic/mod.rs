//! Slice 43i: rebalancer state persistence via an internal compacted
//! topic on the Crabka cluster being managed. Replaces the slice-43b
//! `{data_dir}/in_flight.json` file. Survives pod restart; prerequisite
//! for multi-replica HA (slice 43j).

mod error;
pub(crate) mod serde_format;
pub(crate) mod topic_admin;

pub use error::StateTopicError;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;

use crate::executor::state::InFlightFile;

/// In-memory mirror of the latest record under the `STATE_KEY` on the
/// state topic. Populated by `StateTopicLoader` at startup and by
/// `StateTopic::write` / `delete` thereafter.
#[derive(Debug, Default)]
pub struct LoadedState {
    pub value: ArcSwap<Option<InFlightFile>>,
    pub is_loaded: AtomicBool,
}

impl LoadedState {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            value: ArcSwap::from_pointee(None),
            is_loaded: AtomicBool::new(false),
        })
    }

    pub fn current(&self) -> Option<InFlightFile> {
        let guard = self.value.load();
        let opt: &Option<InFlightFile> = &guard;
        opt.clone()
    }

    pub fn is_loaded(&self) -> bool {
        self.is_loaded.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub(crate) fn store(&self, value: Option<InFlightFile>) {
        self.value.store(Arc::new(value));
    }

    #[allow(dead_code)]
    pub(crate) fn mark_loaded(&self) {
        self.is_loaded.store(true, Ordering::Release);
    }
}

/// The fixed key under which the executor's state is published. Single
/// in-flight record per topic; tombstone (null value) clears it.
pub const STATE_KEY: &str = "in_flight";
