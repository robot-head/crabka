use assert2::assert;
use crabka_pgcatalog::{
    BOOTSTRAP_ROLE, CatalogError, Column, RelationName, Table, TableCreation, TableId,
    TableIdSource, TableOptions, create_foreign_table_ops, create_schema_ops, create_server,
    create_table_ops, create_table_with_options_ops, drop_table_ops, get_table, read_next_table_id,
    relation_name_of, rename_table_ops, set_next_table_id_op, table_by_id,
};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgtypes::ColumnType;

fn columns() -> Vec<Column> {
    vec![
        Column::new("id", ColumnType::Int4),
        Column::new("name", ColumnType::Text),
    ]
}

fn op_key(op: &WriteOp) -> &[u8] {
    match op {
        WriteOp::Put { key, .. }
        | WriteOp::ConditionalPut { key, .. }
        | WriteOp::Delete { key } => key,
    }
}

fn apply(kv: &dyn Kv, ops: &[WriteOp]) {
    kv.write_batch(ops).expect("catalog batch");
}

fn create_schema(kv: &dyn Kv, name: &str) {
    apply(
        kv,
        &create_schema_ops(kv, name, "postgres").expect("create schema ops"),
    );
}

fn create(kv: &dyn Kv, name: &RelationName) -> TableId {
    let (id, ops) = create_table_ops(kv, name, columns()).expect("create table ops");
    apply(kv, &ops);
    id
}

fn ordinary_table(id: TableId, name: &RelationName) -> Table {
    Table {
        owner: BOOTSTRAP_ROLE.into(),
        id,
        name: name.clone(),
        columns: columns(),
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        checks: Vec::new(),
    }
}

#[test]
fn created_table_resolves_by_id_to_the_same_table_its_name_resolves_to() {
    let kv = MemKv::new();
    let orders = RelationName::public("orders");
    let id = create(&kv, &orders);

    assert!(relation_name_of(&kv, id).expect("index lookup") == Some(orders.clone()));
    assert!(table_by_id(&kv, id).expect("table by id") == ordinary_table(id, &orders));
    assert!(
        table_by_id(&kv, id).expect("table by id") == get_table(&kv, &orders).expect("by name")
    );
}

#[test]
fn relation_name_of_reports_the_schema_the_relation_lives_in() {
    let kv = MemKv::new();
    create_schema(&kv, "sales");
    let public_orders = RelationName::public("orders");
    let sales_orders = RelationName::new("sales", "orders");
    let ids = [create(&kv, &public_orders), create(&kv, &sales_orders)];

    let names = ids.map(|id| relation_name_of(&kv, id).expect("index lookup"));
    assert!(names == [Some(public_orders), Some(sales_orders)]);
}

#[test]
fn rename_moves_the_id_to_the_new_name_and_schema() {
    let cases = [
        (
            RelationName::public("orders"),
            RelationName::public("fulfilled_orders"),
        ),
        (
            RelationName::public("events"),
            RelationName::new("archive", "events"),
        ),
    ];

    for (from, to) in cases {
        let kv = MemKv::new();
        create_schema(&kv, "archive");
        let id = create(&kv, &from);

        apply(&kv, &rename_table_ops(&kv, &from, &to).expect("rename ops"));

        assert!(relation_name_of(&kv, id).expect("index lookup") == Some(to.clone()));
        assert!(table_by_id(&kv, id).expect("table by id") == ordinary_table(id, &to));
    }
}

#[test]
fn dropped_table_leaves_its_id_unresolvable() {
    let kv = MemKv::new();
    let orders = RelationName::public("orders");
    let id = create(&kv, &orders);

    apply(&kv, &drop_table_ops(&kv, &orders).expect("drop ops"));

    assert!(relation_name_of(&kv, id).expect("index lookup").is_none());
    assert!(table_by_id(&kv, id) == Err(CatalogError::UndefinedTable(format!("table id {id}"))));
}

#[test]
fn id_no_relation_was_ever_created_under_is_unresolvable() {
    let kv = MemKv::new();
    create(&kv, &RelationName::public("orders"));

    assert!(relation_name_of(&kv, 42).expect("index lookup").is_none());
    assert!(table_by_id(&kv, 42) == Err(CatalogError::UndefinedTable("table id 42".into())));
}

#[test]
fn foreign_table_resolves_by_id() {
    let kv = MemKv::new();
    create_server(&kv, "kafka_srv", "kafka_fdw", Vec::new()).expect("create server");
    let ft = RelationName::public("ft");
    let (id, ops) = create_foreign_table_ops(
        &kv,
        &ft,
        vec![Column::new("value", ColumnType::Text)],
        "kafka_srv",
        vec![("topic".into(), "ft".into())],
        TableCreation::bootstrap(),
    )
    .expect("create foreign table ops");
    apply(&kv, &ops);

    assert!(relation_name_of(&kv, id).expect("index lookup") == Some(ft.clone()));
    assert!(table_by_id(&kv, id).expect("table by id") == get_table(&kv, &ft).expect("by name"));
}

/// Everything a creation batch says about where its id came from.
#[derive(Debug, PartialEq, Eq)]
struct Allocation {
    returned_id: TableId,
    indexed_name: Option<RelationName>,
    stored_id: TableId,
    counter_writes: usize,
    counter_after: TableId,
}

#[test]
fn table_id_source_decides_whether_the_shared_counter_moves() {
    let table = RelationName::public("t");
    let cases = [
        (
            TableIdSource::Counter,
            Allocation {
                returned_id: 5,
                indexed_name: Some(table.clone()),
                stored_id: 5,
                counter_writes: 1,
                counter_after: 6,
            },
        ),
        (
            TableIdSource::Reserved(77),
            Allocation {
                returned_id: 77,
                indexed_name: Some(table.clone()),
                stored_id: 77,
                counter_writes: 0,
                counter_after: 5,
            },
        ),
    ];

    for (source, expected) in cases {
        let kv = MemKv::new();
        // Seed the counter away from its default so "unchanged" is observable.
        apply(&kv, &[set_next_table_id_op(5)]);

        let (returned_id, ops) = create_table_with_options_ops(
            &kv,
            &table,
            columns(),
            TableOptions::default(),
            Vec::new(),
            TableCreation {
                owner: BOOTSTRAP_ROLE,
                id: source,
            },
        )
        .expect("create table ops");
        let counter_key = crabka_pgkv::key::meta_next_table_id_key();
        let counter_writes = ops.iter().filter(|op| op_key(op) == counter_key).count();
        apply(&kv, &ops);

        let observed = Allocation {
            returned_id,
            indexed_name: relation_name_of(&kv, returned_id).expect("index lookup"),
            stored_id: get_table(&kv, &table).expect("by name").id,
            counter_writes,
            counter_after: read_next_table_id(&kv).expect("counter"),
        };
        assert!(observed == expected);
    }
}
