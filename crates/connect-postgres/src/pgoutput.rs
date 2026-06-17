use std::collections::BTreeMap;

use crate::model::{
    ColumnSchema, ColumnValue, EntityDifference, EntityKey, Operation, TableSchema,
};
use crate::{PgLsn, PostgresConnectError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationEvent {
    pub relation_id: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowEventKind {
    Insert,
    Update { old: Vec<ColumnValue> },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowEvent {
    pub relation_id: u32,
    pub lsn: PgLsn,
    pub txid: Option<i64>,
    pub commit_timestamp_ms: Option<i64>,
    pub kind: RowEventKind,
    pub values: Vec<ColumnValue>,
}

#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    relations: BTreeMap<u32, TableSchema>,
}

impl RelationCache {
    pub fn apply_relation(&mut self, event: RelationEvent) {
        self.relations.insert(
            event.relation_id,
            TableSchema {
                schema: event.schema,
                table: event.table,
                columns: event.columns,
            },
        );
    }

    pub fn translate(&self, event: RowEvent) -> Result<EntityDifference, PostgresConnectError> {
        let schema = self.relations.get(&event.relation_id).ok_or_else(|| {
            PostgresConnectError::Backend(format!(
                "missing relation metadata for relation id {}",
                event.relation_id
            ))
        })?;
        let table = format!("{}.{}", schema.schema, schema.table);
        let key = EntityKey {
            table: table.clone(),
            columns: key_columns(schema, &event.values),
        };

        let (op, before, after) = match event.kind {
            RowEventKind::Insert => (Operation::Insert, Vec::new(), event.values),
            RowEventKind::Update { old } => (Operation::Update, old, event.values),
            RowEventKind::Delete => (Operation::Delete, event.values, Vec::new()),
        };

        Ok(EntityDifference {
            table,
            key,
            op,
            before,
            after,
            lsn: event.lsn,
            txid: event.txid,
            commit_timestamp_ms: event.commit_timestamp_ms,
            schema: schema.clone(),
        })
    }
}

fn key_columns(schema: &TableSchema, values: &[ColumnValue]) -> Vec<ColumnValue> {
    schema
        .columns
        .iter()
        .filter(|column| column.key)
        .filter_map(|column| {
            values
                .iter()
                .find(|value| value.name == column.name)
                .cloned()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{RelationCache, RelationEvent, RowEvent, RowEventKind};
    use crate::PgLsn;
    use crate::model::{ColumnSchema, ColumnValue, Operation, ScalarValue};

    fn orders_relation(type_name: &str) -> RelationEvent {
        RelationEvent {
            relation_id: 7,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    type_name: "int8".to_owned(),
                    key: true,
                },
                ColumnSchema {
                    name: "status".to_owned(),
                    type_name: type_name.to_owned(),
                    key: false,
                },
            ],
        }
    }

    fn id(value: i64) -> ColumnValue {
        ColumnValue {
            name: "id".to_owned(),
            value: ScalarValue::Int(value),
        }
    }

    fn status(value: &str) -> ColumnValue {
        ColumnValue {
            name: "status".to_owned(),
            value: ScalarValue::Text(value.to_owned()),
        }
    }

    #[test]
    fn insert_translates_to_entity_difference_with_key() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let values = vec![id(42), status("paid")];
        let difference = cache
            .translate(RowEvent {
                relation_id: 7,
                lsn: PgLsn(0x16_b374_d848),
                txid: Some(99),
                commit_timestamp_ms: Some(1_700_000_000_000),
                kind: RowEventKind::Insert,
                values: values.clone(),
            })
            .expect("relation should translate");

        check!(difference.table == "public.orders");
        check!(difference.key.table == "public.orders");
        check!(difference.key.columns == vec![id(42)]);
        check!(difference.op == Operation::Insert);
        check!(difference.before == Vec::new());
        check!(difference.after == values);
        check!(difference.lsn == PgLsn(0x16_b374_d848));
        check!(difference.txid == Some(99));
        check!(difference.commit_timestamp_ms == Some(1_700_000_000_000));
        check!(difference.schema.table == "orders");
        check!(difference.schema.columns[0].key);
    }

    #[test]
    fn delete_translates_to_before_only_difference() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let values = vec![id(42), status("cancelled")];
        let difference = cache
            .translate(RowEvent {
                relation_id: 7,
                lsn: PgLsn(0x2a),
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Delete,
                values: values.clone(),
            })
            .expect("relation should translate");

        check!(difference.table == "public.orders");
        check!(difference.key.columns == vec![id(42)]);
        check!(difference.op == Operation::Delete);
        check!(difference.before == values);
        check!(difference.after == Vec::new());
    }

    #[test]
    fn relation_refresh_changes_table_schema() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));
        cache.apply_relation(orders_relation("varchar"));

        let difference = cache
            .translate(RowEvent {
                relation_id: 7,
                lsn: PgLsn(0x2b),
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![id(7), status("new")],
            })
            .expect("relation should translate");

        check!(difference.schema.columns[1].type_name == "varchar");
    }
}
