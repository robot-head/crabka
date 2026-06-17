//! Postgres logical-decoding source connector for Crabka Connect.

pub mod config;
pub mod error;

pub mod model {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ColumnValue;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct EntityDifference;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct EntityKey;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Operation {}

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TableSchema;
}

pub mod offset;

pub mod pgoutput {}

pub mod schema {}

pub mod source {
    #[derive(Debug, Clone)]
    pub struct PostgresWalSource;
}

pub use config::PostgresSourceConfig;
pub use error::PostgresConnectError;
pub use model::{ColumnValue, EntityDifference, EntityKey, Operation, TableSchema};
pub use offset::PgLsn;
pub use source::PostgresWalSource;
