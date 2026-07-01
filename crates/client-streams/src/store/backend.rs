//! Which storage engine backs the state stores. `InMemory` is the test default
//! and a valid production option; `Turso` persists under a state dir.
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::turso::TursoBytes;

#[derive(Clone, Debug, Default)]
pub enum StoreBackend {
    #[default]
    InMemory,
    Turso {
        state_dir: std::path::PathBuf,
    },
}

impl StoreBackend {
    /// Open a byte backend for one store. `app_id`/`store` form the Turso file path.
    #[tracing::instrument(
        name = "streams.store.backend_open",
        level = "info",
        skip_all,
        fields(app_id = %app_id, store = %store),
    )]
    pub(crate) async fn open(&self, app_id: &str, store: &str) -> Box<dyn ByteKeyValueStore> {
        match self {
            Self::InMemory => Box::new(InMemoryBytes::default()),
            Self::Turso { state_dir } => {
                let dir = state_dir.join(app_id);
                std::fs::create_dir_all(&dir).expect("create state dir");
                let path = dir.join(format!("{store}.db"));
                Box::new(TursoBytes::open(path.to_str().expect("utf8 path")).await)
            }
        }
    }
}
