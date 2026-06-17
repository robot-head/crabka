//! Postgres logical-decoding source connector for Crabka Connect.

pub mod config;
pub mod error;

pub mod model;

pub mod offset;

pub mod pgoutput;

pub mod schema;

pub mod source {
    #[derive(Debug, Clone)]
    pub struct PostgresWalSource;
}

pub use config::PostgresSourceConfig;
pub use error::PostgresConnectError;
pub use model::{ColumnValue, EntityDifference, EntityKey, Operation, TableSchema};
pub use offset::PgLsn;
pub use source::PostgresWalSource;
