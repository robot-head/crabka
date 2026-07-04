use crate::{PgLsn, ids::TransactionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityKey {
    pub table: String,
    pub columns: Vec<ColumnValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDifference {
    pub table: String,
    pub key: EntityKey,
    pub op: Operation,
    pub before: Vec<ColumnValue>,
    pub after: Vec<ColumnValue>,
    pub lsn: PgLsn,
    pub txid: Option<TransactionId>,
    pub commit_timestamp_ms: Option<i64>,
    pub schema: TableSchema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub type_name: String,
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnValue {
    pub name: String,
    pub value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    Null,
    UnchangedToast,
    Bool(bool),
    Int(i64),
    Float(String),
    Text(String),
    Bytes(Vec<u8>),
}
