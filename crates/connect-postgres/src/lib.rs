//! Postgres logical-decoding source connector for Crabka Connect.

mod catalog;
pub mod config;
pub mod error;

pub mod model;

pub mod offset;

pub mod pgoutput;

pub mod schema;

pub mod source;

pub use config::PostgresSourceConfig;
pub use error::PostgresConnectError;
pub use model::{ColumnValue, EntityDifference, EntityKey, Operation, TableSchema};
pub use offset::PgLsn;
pub use source::PostgresWalSource;
