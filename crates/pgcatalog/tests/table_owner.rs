//! The owning role is part of a table's durable schema record: it survives a
//! serialize/deserialize round trip, it is what `ALTER TABLE` preserves while
//! it rewrites the column and `CHECK` lists, and the reader still refuses a
//! record it does not fully understand. The materialized-view metadata rides
//! the same record and is checked here for the same reasons.

use assert2::assert;
use crabka_pgcatalog::{
    BOOTSTRAP_ROLE, CheckConstraint, Column, ForeignTableMeta, MaterializedView, RelationName,
    Table, TableCreation, TableOptions, create_server, create_table_with_options_ops, get_table,
    is_materialized_view, replace_table_schema_ops,
    serde::{SCHEMA_VERSION, deserialize_schema, serialize_schema},
    set_materialized_populated_op,
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
    create_as(kv, name, owner, None);
}

fn create_as(
    kv: &dyn Kv,
    name: &RelationName,
    owner: &str,
    materialized: Option<&MaterializedView>,
) {
    let (_, ops) = create_table_with_options_ops(
        kv,
        name,
        columns(),
        TableOptions::default(),
        checks(),
        TableCreation {
            owner,
            id: crabka_pgcatalog::TableIdSource::Counter,
            materialized,
        },
    )
    .expect("create table ops");
    kv.write_batch(&ops).expect("catalog batch");
}

/// The whole decoded record, compared as one value, for every combination of
/// owner name and relation shape the encoder has to carry it through.
///
/// The two relation-kind payloads are exercised one at a time and neither at
/// all, which is what pins them as independent: a foreign table still decodes
/// with no materialized-view metadata, and a materialized view with no foreign
/// metadata.
#[test]
fn a_schema_record_round_trips_its_owner() {
    let foreign = ForeignTableMeta {
        server: "kafka_srv".into(),
        options: vec![("topic".into(), "orders".into())],
    };
    let cases = [
        (
            BOOTSTRAP_ROLE,
            TableOptions::default(),
            None,
            None,
            Vec::new(),
        ),
        (
            "regress_owner",
            TableOptions::default(),
            None,
            None,
            vec![CheckConstraint {
                name: "positive".into(),
                expr: "id > 0".into(),
                validated: false,
            }],
        ),
        (
            "sharded_owner",
            TableOptions {
                sharded: true,
                row_security: false,
                force_row_security: false,
            },
            None,
            None,
            Vec::new(),
        ),
        (
            "foreign_owner",
            TableOptions::default(),
            Some(foreign),
            None,
            Vec::new(),
        ),
        (
            "matview_owner",
            TableOptions::default(),
            None,
            Some(MaterializedView {
                definition: "SELECT id, label FROM orders".into(),
                populated: true,
            }),
            Vec::new(),
        ),
        // An unpopulated matview differs from a populated one by a single byte,
        // and that byte is what makes a scan of it `55000` rather than a read
        // of an empty heap.
        (
            "matview_owner",
            TableOptions::default(),
            None,
            Some(MaterializedView {
                definition: "SELECT id, label FROM orders".into(),
                populated: false,
            }),
            Vec::new(),
        ),
        // A definition is stored as written: multi-byte text and the newlines a
        // multi-line query is laid out with both survive byte for byte, because
        // `pg_matviews.definition` deparses from exactly these bytes.
        (
            "matview_owner",
            TableOptions::default(),
            None,
            Some(MaterializedView {
                definition: "SELECT '价格 ✓'::text AS \"étiquette\"\n  FROM orders\n WHERE id > 0"
                    .into(),
                populated: true,
            }),
            vec![CheckConstraint {
                name: "positive".into(),
                expr: "id > 0".into(),
                validated: true,
            }],
        ),
        // A quoted-identifier owner: the encoding is length-prefixed bytes, so
        // an owner with a delimiter in it must survive unchanged.
        (
            "odd owner.name",
            TableOptions::default(),
            None,
            None,
            Vec::new(),
        ),
    ];

    for (owner, options, meta, materialized, table_checks) in cases {
        let bytes = serialize_schema(
            7,
            &columns(),
            options,
            owner,
            meta.as_ref(),
            materialized.as_ref(),
            &table_checks,
        );

        assert!(bytes[0] == SCHEMA_VERSION);
        assert!(
            deserialize_schema(&bytes).expect("decode")
                == (
                    7,
                    columns(),
                    options,
                    owner.to_string(),
                    meta,
                    table_checks.clone(),
                    materialized,
                )
        );
    }
}

/// A materialized view is created as one schema record, reads back as a whole
/// `Table`, and answers [`is_materialized_view`] — while an ordinary table
/// created through the same battery answers `false` and carries no metadata.
#[test]
fn a_created_materialized_view_reads_back_with_its_query_and_flag() {
    for populated in [true, false] {
        let kv = MemKv::new();
        let matview = MaterializedView {
            definition: "SELECT id, label FROM orders".into(),
            populated,
        };
        let summary = RelationName::public("summary");
        let orders = RelationName::public("orders");
        create_as(&kv, &summary, "regress_owner", Some(&matview));
        create(&kv, &orders, "regress_owner");

        assert!(
            get_table(&kv, &summary).expect("stored matview")
                == Table {
                    id: 1,
                    name: summary.clone(),
                    owner: "regress_owner".into(),
                    columns: columns(),
                    sharded: false,
                    row_security: false,
                    force_row_security: false,
                    sharding: None,
                    foreign: None,
                    materialized: Some(matview),
                    checks: checks(),
                }
        );
        assert!(is_materialized_view(&kv, &summary).expect("matview lookup"));
        assert!(!is_materialized_view(&kv, &orders).expect("table lookup"));
        assert!(get_table(&kv, &orders).expect("stored table").materialized == None);
        // A name that is no relation at all is not a materialized view either,
        // rather than an error every caller has to translate.
        assert!(
            !is_materialized_view(&kv, &RelationName::public("absent")).expect("absent lookup")
        );
    }
}

/// `REFRESH MATERIALIZED VIEW` moves the population flag and nothing else: the
/// query text, owner, columns and `CHECK` list all come back as they were.
#[test]
fn flipping_the_population_flag_leaves_the_rest_of_the_record_alone() {
    for (created, refreshed) in [(false, true), (true, false), (true, true)] {
        let kv = MemKv::new();
        let summary = RelationName::public("summary");
        create_as(
            &kv,
            &summary,
            "regress_owner",
            Some(&MaterializedView {
                definition: "SELECT id, label FROM orders".into(),
                populated: created,
            }),
        );
        let before = get_table(&kv, &summary).expect("stored matview");

        kv.write_batch(&[set_materialized_populated_op(&before, refreshed)])
            .expect("catalog batch");

        assert!(
            get_table(&kv, &summary).expect("stored matview")
                == Table {
                    materialized: Some(MaterializedView {
                        definition: "SELECT id, label FROM orders".into(),
                        populated: refreshed,
                    }),
                    ..before
                }
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
                row_security: false,
                force_row_security: false,
                sharding: None,
                foreign: None,
                materialized: None,
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
        let stored = get_table(&kv, &orders).expect("stored table");
        let ops = replace_table_schema_ops(
            &kv,
            &orders,
            &Table {
                columns: widened.clone(),
                checks: checks(),
                owner: owner.into(),
                ..stored
            },
        )
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
                    row_security: false,
                    force_row_security: false,
                    sharding: None,
                    foreign: None,
                    materialized: None,
                    checks: checks(),
                }
        );
    }
}

/// A foreign table's owner rides the same record as its server metadata.
#[test]
fn a_foreign_table_reads_back_owned_by_the_role_it_was_created_under() {
    let kv = MemKv::new();
    crabka_pgcatalog::create_fdw(&kv, "kafka_fdw", Vec::new()).expect("create fdw");
    create_server(&kv, "kafka_srv", "kafka_fdw", Vec::new()).expect("create server");
    let remote = RelationName::public("remote");
    let (_, ops) = crabka_pgcatalog::create_foreign_table_ops(
        &kv,
        &remote,
        vec![Column::new("value", ColumnType::Text)],
        "kafka_srv",
        vec![("topic".into(), "remote".into())],
        Vec::new(),
        TableCreation {
            owner: "regress_owner",
            id: crabka_pgcatalog::TableIdSource::Counter,
            materialized: None,
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
    let encode =
        |options| serialize_schema(7, &columns(), options, BOOTSTRAP_ROLE, None, None, &[]);
    let clear = encode(TableOptions::default());
    let set = encode(TableOptions {
        sharded: true,
        row_security: false,
        force_row_security: false,
    });
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
