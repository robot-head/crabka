//! The owning role is part of a table's durable schema record: it survives a
//! serialize/deserialize round trip, it is what `ALTER TABLE` preserves while
//! it rewrites the column and `CHECK` lists, and the reader still refuses a
//! record it does not fully understand.

use assert2::assert;
use crabka_pgcatalog::{
    BOOTSTRAP_ROLE, CheckConstraint, Column, ForeignTableMeta, RelationName, Table, TableCreation,
    TableOptions, create_server, create_table_with_options_ops, get_table,
    replace_table_schema_ops,
    serde::{SCHEMA_VERSION, deserialize_schema, serialize_schema},
};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgtypes::ColumnType;

fn columns() -> Vec<Column> {
    vec![
        Column::new("id", ColumnType::Int4),
        Column::new("label", ColumnType::Text),
    ]
}

fn checks() -> Vec<CheckConstraint> {
    vec![CheckConstraint {
        name: "positive".into(),
        expr: "id > 0".into(),
        validated: true,
    }]
}

fn create(kv: &dyn Kv, name: &RelationName, owner: &str) {
    let (_, ops) = create_table_with_options_ops(
        kv,
        name,
        columns(),
        TableOptions::default(),
        checks(),
        TableCreation {
            owner,
            id: crabka_pgcatalog::TableIdSource::Counter,
        },
    )
    .expect("create table ops");
    kv.write_batch(&ops).expect("catalog batch");
}

/// The whole decoded record, compared as one value, for every combination of
/// owner name and table shape the encoder has to carry it through.
#[test]
fn a_schema_record_round_trips_its_owner() {
    let foreign = ForeignTableMeta {
        server: "kafka_srv".into(),
        options: vec![("topic".into(), "orders".into())],
    };
    let cases = [
        (BOOTSTRAP_ROLE, TableOptions::default(), None, Vec::new()),
        (
            "regress_owner",
            TableOptions::default(),
            None,
            vec![CheckConstraint {
                name: "positive".into(),
                expr: "id > 0".into(),
                validated: false,
            }],
        ),
        (
            "sharded_owner",
            TableOptions { sharded: true },
            None,
            Vec::new(),
        ),
        (
            "foreign_owner",
            TableOptions::default(),
            Some(foreign),
            Vec::new(),
        ),
        // A quoted-identifier owner: the encoding is length-prefixed bytes, so
        // an owner with a delimiter in it must survive unchanged.
        ("odd owner.name", TableOptions::default(), None, Vec::new()),
    ];

    for (owner, options, meta, table_checks) in cases {
        let bytes = serialize_schema(7, &columns(), options, owner, meta.as_ref(), &table_checks);

        assert!(bytes[0] == SCHEMA_VERSION);
        assert!(
            deserialize_schema(&bytes).expect("decode")
                == (
                    7,
                    columns(),
                    options,
                    owner.to_string(),
                    meta,
                    table_checks.clone()
                )
        );
    }
}

/// The record the catalog actually stores, read back as a whole `Table`.
#[test]
fn a_created_table_reads_back_owned_by_the_role_it_was_created_under() {
    let kv = MemKv::new();
    let orders = RelationName::public("orders");
    create(&kv, &orders, "regress_owner");

    assert!(
        get_table(&kv, &orders).expect("stored table")
            == Table {
                id: 1,
                name: orders,
                owner: "regress_owner".into(),
                columns: columns(),
                sharded: false,
                sharding: None,
                foreign: None,
                checks: checks(),
            }
    );
}

/// `ALTER TABLE` rewrites the column and `CHECK` lists through one encoder, and
/// the owner it is handed is the one that lands in the record — so both a
/// preserved owner and a changed one go through the same seam.
#[test]
fn replacing_a_schema_record_writes_the_owner_it_is_given() {
    for owner in ["regress_owner", "successor"] {
        let kv = MemKv::new();
        let orders = RelationName::public("orders");
        create(&kv, &orders, "regress_owner");

        let widened = [columns(), vec![Column::new("note", ColumnType::Text)]].concat();
        let ops = replace_table_schema_ops(&kv, &orders, &widened, &checks(), owner)
            .expect("replace ops");
        kv.write_batch(&ops).expect("catalog batch");

        assert!(
            get_table(&kv, &orders).expect("stored table")
                == Table {
                    id: 1,
                    name: orders,
                    owner: owner.into(),
                    columns: widened,
                    sharded: false,
                    sharding: None,
                    foreign: None,
                    checks: checks(),
                }
        );
    }
}

/// A foreign table's owner rides the same record as its server metadata.
#[test]
fn a_foreign_table_reads_back_owned_by_the_role_it_was_created_under() {
    let kv = MemKv::new();
    create_server(&kv, "kafka_srv", "kafka_fdw", Vec::new()).expect("create server");
    let remote = RelationName::public("remote");
    let (_, ops) = crabka_pgcatalog::create_foreign_table_ops(
        &kv,
        &remote,
        vec![Column::new("value", ColumnType::Text)],
        "kafka_srv",
        vec![("topic".into(), "remote".into())],
        TableCreation {
            owner: "regress_owner",
            id: crabka_pgcatalog::TableIdSource::Counter,
        },
    )
    .expect("create foreign table ops");
    kv.write_batch(&ops).expect("catalog batch");

    assert!(get_table(&kv, &remote).expect("stored table").owner == "regress_owner");
}

/// The reader stays strict. A tolerant one would decode a record written with
/// an option bit it does not know as though the bit were clear — which, once
/// row-level security occupies one of those bits, is a silent total bypass.
#[test]
fn a_record_carrying_an_unknown_option_bit_is_refused() {
    let encode = |options| serialize_schema(7, &columns(), options, BOOTSTRAP_ROLE, None, &[]);
    let clear = encode(TableOptions::default());
    let set = encode(TableOptions { sharded: true });
    // The only byte the one known option changes is the option-flag byte, so
    // the difference locates it without the test knowing the layout.
    let flags = clear
        .iter()
        .zip(&set)
        .position(|(left, right)| left != right)
        .expect("a set option changes the record");

    let mut unknown = clear;
    unknown[flags] |= 0b1000_0000;

    assert!(deserialize_schema(&unknown).is_err());
}
