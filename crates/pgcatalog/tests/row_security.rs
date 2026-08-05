//! Row-security state is durable and survives the relation lifecycle: the two
//! `pg_class` flags round-trip through the schema record and stay reachable
//! from a `Table`, and a relation's policies follow it through a rename and go
//! away with it on a drop.
//!
//! Both lifecycle cases are security properties rather than housekeeping. A
//! policy that stops applying to the relation it was written for exposes the
//! rows it was meant to hide; a policy that outlives its relation attaches
//! itself to whatever is created on that table id next.

use assert2::assert;
use crabka_pgcatalog::{
    BOOTSTRAP_ROLE, Column, RelationName, Table, TableCreation, TableId, TableIdSource,
    TableOptions, create_table_with_options_ops, drop_table_ops, get_table,
    policy::{Policy, PolicyCommand, create_policy_ops, list_policies, policies_for_table},
    rename_table_ops, replace_table_schema_ops, set_row_security_ops,
};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgtypes::ColumnType;

fn columns() -> Vec<Column> {
    vec![
        Column::new("id", ColumnType::Int4),
        Column::new("owner", ColumnType::Text),
    ]
}

fn apply(kv: &MemKv, ops: &[crabka_pgkv::WriteOp]) {
    kv.write_batch(ops).expect("catalog batch");
}

fn create(kv: &MemKv, name: &RelationName, options: TableOptions) -> TableId {
    create_reserved(kv, name, options, TableIdSource::Counter)
}

fn create_reserved(
    kv: &MemKv,
    name: &RelationName,
    options: TableOptions,
    id: TableIdSource,
) -> TableId {
    let (table_id, ops) = create_table_with_options_ops(
        kv,
        name,
        columns(),
        options,
        Vec::new(),
        TableCreation {
            owner: BOOTSTRAP_ROLE,
            id,
        },
    )
    .expect("create table ops");
    apply(kv, &ops);
    table_id
}

fn expected_table(id: TableId, name: &RelationName, options: TableOptions) -> Table {
    Table {
        id,
        name: name.clone(),
        owner: BOOTSTRAP_ROLE.into(),
        columns: columns(),
        sharded: options.sharded,
        row_security: options.row_security,
        force_row_security: options.force_row_security,
        sharding: None,
        foreign: None,
        checks: Vec::new(),
    }
}

fn policy(name: &str, table_id: TableId) -> Policy {
    Policy {
        oid: 0,
        name: name.into(),
        table_id,
        command: PolicyCommand::All,
        permissive: true,
        roles: Vec::new(),
        using: Some("owner = current_user".into()),
        with_check: None,
    }
}

fn add_policy(kv: &MemKv, name: &str, table_id: TableId) {
    let ops = create_policy_ops(kv, &policy(name, table_id)).expect("create policy ops");
    apply(kv, &ops);
}

fn policy_names(kv: &MemKv, table_id: TableId) -> Vec<String> {
    policies_for_table(kv, table_id)
        .expect("policy scan")
        .into_iter()
        .map(|policy| policy.name)
        .collect()
}

/// Every combination of the flags, read back as a whole `Table` — the flags are
/// only useful if code holding a relation holds them too.
#[test]
fn the_row_security_flags_reach_a_table_they_were_created_with() {
    for row_security in [false, true] {
        for force_row_security in [false, true] {
            let kv = MemKv::new();
            let orders = RelationName::public("orders");
            let options = TableOptions {
                sharded: false,
                row_security,
                force_row_security,
            };
            let id = create(&kv, &orders, options);
            assert!(
                get_table(&kv, &orders).expect("stored table")
                    == expected_table(id, &orders, options)
            );
        }
    }
}

#[test]
fn setting_the_row_security_flags_rewrites_only_them() {
    let kv = MemKv::new();
    let orders = RelationName::public("orders");
    let sharded = TableOptions {
        sharded: true,
        ..TableOptions::default()
    };
    let id = create(&kv, &orders, sharded);

    let ops = set_row_security_ops(&kv, &orders, true, true).expect("set flags");
    apply(&kv, &ops);

    assert!(
        get_table(&kv, &orders).expect("stored table")
            == expected_table(
                id,
                &orders,
                TableOptions {
                    sharded: true,
                    row_security: true,
                    force_row_security: true,
                }
            )
    );
}

/// `ALTER TABLE … ADD COLUMN` re-encodes the whole schema record. Dropping the
/// flags there would disable row security on the next DDL statement.
#[test]
fn replacing_a_schema_record_preserves_the_row_security_flags() {
    let kv = MemKv::new();
    let orders = RelationName::public("orders");
    let options = TableOptions {
        sharded: false,
        row_security: true,
        force_row_security: true,
    };
    let id = create(&kv, &orders, options);

    let widened = [columns(), vec![Column::new("note", ColumnType::Text)]].concat();
    let ops =
        replace_table_schema_ops(&kv, &orders, &widened, &[], BOOTSTRAP_ROLE).expect("replace ops");
    apply(&kv, &ops);

    assert!(
        get_table(&kv, &orders).expect("stored table")
            == Table {
                columns: widened,
                ..expected_table(id, &orders, options)
            }
    );
}

/// A rename must not detach a relation from its policies. Policy keys carry the
/// table id, which a rename preserves, so the relation stays protected and
/// there is no key for a rename to leave behind.
#[test]
fn renaming_a_relation_keeps_its_policies_and_its_row_security_flags() {
    let kv = MemKv::new();
    let before = RelationName::public("document");
    let after = RelationName::new("regress_rls", "papers");
    let options = TableOptions {
        sharded: false,
        row_security: true,
        force_row_security: true,
    };
    let id = create(&kv, &before, options);
    add_policy(&kv, "p_select", id);
    add_policy(&kv, "p_insert", id);
    let stored = policies_for_table(&kv, id).expect("policy scan");

    let ops = rename_table_ops(&kv, &before, &after).expect("rename ops");
    apply(&kv, &ops);

    let renamed = get_table(&kv, &after).expect("stored table");
    assert!(renamed == expected_table(id, &after, options));
    // The same policy records, still attached to the same relation.
    assert!(policies_for_table(&kv, renamed.id).expect("policy scan") == stored);
    assert!(list_policies(&kv).expect("policy list") == stored);
}

/// A dropped relation's policies go with it. Table ids are handed out from a
/// counter, so a policy left behind would silently govern whatever relation is
/// created on that id next.
#[test]
fn dropping_a_relation_deletes_its_policies_so_a_recycled_id_inherits_none() {
    let kv = MemKv::new();
    let document = RelationName::public("document");
    let options = TableOptions {
        sharded: false,
        row_security: true,
        force_row_security: false,
    };
    let id = create(&kv, &document, options);
    add_policy(&kv, "p_select", id);
    add_policy(&kv, "p_insert", id);

    let ops = drop_table_ops(&kv, &document).expect("drop ops");
    apply(&kv, &ops);

    assert!(policies_for_table(&kv, id).expect("policy scan").is_empty());
    assert!(list_policies(&kv).expect("policy list").is_empty());

    let recycled = RelationName::public("unrelated");
    let recycled_id = create_reserved(
        &kv,
        &recycled,
        TableOptions::default(),
        TableIdSource::Reserved(id),
    );
    assert!(recycled_id == id);
    let table = get_table(&kv, &recycled).expect("stored table");
    assert!(!table.row_security);
    assert!(
        policies_for_table(&kv, table.id)
            .expect("policy scan")
            .is_empty()
    );
}

#[test]
fn dropping_one_relation_leaves_another_relations_policies_alone() {
    let kv = MemKv::new();
    let document = RelationName::public("document");
    let category = RelationName::public("category");
    let document_id = create(&kv, &document, TableOptions::default());
    let category_id = create(&kv, &category, TableOptions::default());
    add_policy(&kv, "p_document", document_id);
    add_policy(&kv, "p_category", category_id);

    let ops = drop_table_ops(&kv, &document).expect("drop ops");
    apply(&kv, &ops);

    assert!(policy_names(&kv, document_id).is_empty());
    assert!(policy_names(&kv, category_id) == vec!["p_category"]);
}
