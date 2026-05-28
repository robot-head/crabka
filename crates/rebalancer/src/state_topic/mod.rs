//! Slice 43i: rebalancer state persistence via an internal compacted
//! topic on the Crabka cluster being managed. Replaces the slice-43b
//! `{data_dir}/in_flight.json` file. Survives pod restart; prerequisite
//! for multi-replica HA (slice 43j).

mod error;
pub mod loader;
pub(crate) mod producer;
pub(crate) mod serde_format;
pub mod topic_admin;

pub use error::StateTopicError;
pub use loader::StateTopicLoader;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use bytes::Bytes;
use crabka_client_core::Client;

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

    pub(crate) fn store(&self, value: Option<InFlightFile>) {
        self.value.store(Arc::new(value));
    }

    pub(crate) fn mark_loaded(&self) {
        self.is_loaded.store(true, Ordering::Release);
    }
}

/// The fixed key under which the executor's state is published. Single
/// in-flight record per topic; tombstone (null value) clears it.
pub const STATE_KEY: &str = "in_flight";

/// Backend abstraction for the executor's persisted state. The
/// production impl is `StateTopic`; tests use an in-memory fake
/// (`fake::InMemoryBackend`) to drive the executor's state machine
/// without a broker.
#[async_trait::async_trait]
pub trait StateBackend: Send + Sync {
    /// Snapshot the latest known in-flight record. Returns `None` if
    /// the topic is empty / tombstoned, or if the load hasn't
    /// completed yet (caller must check `is_loaded` first).
    fn loaded(&self) -> Option<InFlightFile>;

    /// `true` once the loader has finished its initial replay.
    fn is_loaded(&self) -> bool;

    /// Persist an in-flight record. Production: produces to the topic
    /// AND mirrors locally into `LoadedState` so the executor's next
    /// `loaded()` call sees the write without waiting for the loader
    /// to round-trip it back.
    async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError>;

    /// Tombstone the state key.
    async fn delete(&self) -> Result<(), StateTopicError>;
}

/// Topic-backed `StateBackend` impl — produces to the state topic
/// and reads from the shared `LoadedState` mirror.
#[derive(Clone)]
pub struct StateTopic {
    client: Arc<Client>,
    topic: String,
    state: Arc<LoadedState>,
}

impl StateTopic {
    #[must_use]
    pub fn new(client: Arc<Client>, topic: String, state: Arc<LoadedState>) -> Self {
        Self {
            client,
            topic,
            state,
        }
    }
}

#[async_trait::async_trait]
impl StateBackend for StateTopic {
    fn loaded(&self) -> Option<InFlightFile> {
        self.state.current()
    }

    fn is_loaded(&self) -> bool {
        self.state.is_loaded()
    }

    async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
        let value: Bytes = serde_format::encode(f)?;
        producer::produce_state(&self.client, &self.topic, STATE_KEY, Some(value)).await?;
        self.state.store(Some(f.clone()));
        Ok(())
    }

    async fn delete(&self) -> Result<(), StateTopicError> {
        producer::produce_state(&self.client, &self.topic, STATE_KEY, None).await?;
        self.state.store(None);
        Ok(())
    }
}

pub mod fake {
    //! In-memory `StateBackend` for executor unit tests and integration
    //! tests. Doesn't touch a broker.

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    use crate::executor::state::InFlightFile;
    use crate::state_topic::{StateBackend, StateTopicError};

    #[derive(Default)]
    pub struct InMemoryBackend {
        pub state: Mutex<Option<InFlightFile>>,
        pub loaded_flag: AtomicBool,
    }

    impl InMemoryBackend {
        /// Construct in a "fully loaded, empty" state — `is_loaded()`
        /// returns `true`, `loaded()` returns `None`. The most common
        /// fixture for executor tests that don't care about the
        /// resume-from-state path.
        #[must_use]
        pub fn new_loaded() -> Self {
            Self {
                state: Mutex::new(None),
                loaded_flag: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl StateBackend for InMemoryBackend {
        fn loaded(&self) -> Option<InFlightFile> {
            self.state.lock().unwrap().clone()
        }
        fn is_loaded(&self) -> bool {
            self.loaded_flag.load(Ordering::Acquire)
        }
        async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
            *self.state.lock().unwrap() = Some(f.clone());
            Ok(())
        }
        async fn delete(&self) -> Result<(), StateTopicError> {
            *self.state.lock().unwrap() = None;
            Ok(())
        }
    }
}
