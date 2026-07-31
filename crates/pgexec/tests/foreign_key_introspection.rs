//! D-6: `FOREIGN KEY` introspection end to end — constraints created with SQL
//! and read back through `pg_catalog` and `information_schema` the way `psql`
//! and ORM preambles read them.
//!
//! Every expected value here is a capture from a live `PostgreSQL` 18.4, not a
//! restatement of what crabka happens to produce.

use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
    engine
        .connect()
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next()
        .expect("one result")
}

/// Run one or more statements for their effect.
async fn exec(engine: &SqlEngine, sql: &str) {
    engine
        .connect()
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} failed: {error:?}"));
}

/// Every row of a single-column result, in order.
async fn column(engine: &SqlEngine, sql: &str) -> Vec<Option<String>> {
    grid(engine, sql)
        .await
        .into_iter()
        .map(|row| row.into_iter().next().expect("one column"))
        .collect()
}

/// The whole result as text, so a test can compare an entire expected table.
async fn grid(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    match run(engine, sql).await {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| cell.as_ref().map(text_of))
                    .collect::<Vec<_>>()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn text_of(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("valid text cell")
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// One expected `pg_constraint` row, laid out the way the oracle's table is:
/// the cells space-separated in column order, then `confdelsetcols` — the only
/// nullable column in the projections below.
fn fk_row(cells: &str, confdelsetcols: Option<&str>) -> Vec<Option<String>> {
    let mut row: Vec<Option<String>> = cells
        .split_whitespace()
        .map(|cell| Some(cell.to_string()))
        .collect();
    row.push(confdelsetcols.map(str::to_string));
    row
}

/// The oracle's `pp`/`cc` pair: one parent with both a primary key and a unique
/// constraint, and one child carrying the four foreign keys whose
/// `pg_constraint` row values the capture records.
async fn oracle_fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE pp (id int4 PRIMARY KEY, k int4 UNIQUE)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE cc (\
         a int4 CONSTRAINT cc_a_fkey REFERENCES pp (id), \
         b int4 CONSTRAINT cc_full REFERENCES pp (id) \
           MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL, \
         c int4 CONSTRAINT cc_def REFERENCES pp (k) \
           ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED)",
    )
    .await;
    exec(
        &engine,
        "ALTER TABLE cc ADD CONSTRAINT cc_nv FOREIGN KEY (a) REFERENCES pp (id) \
         ON DELETE RESTRICT NOT VALID",
    )
    .await;
    engine
}

/// The oracle's `pg_constraint` capture for `cc`'s four foreign keys, column
/// order `conname contype condeferrable condeferred convalidated confrelid
/// confupdtype confdeltype confmatchtype conkey confkey`, then
/// `confdelsetcols`. Action codes are `a` `NO ACTION`, `r` `RESTRICT`, `c`
/// `CASCADE`, `n` `SET NULL`, `d` `SET DEFAULT`; match codes `s` `SIMPLE` and
/// `f` `FULL`.
const ORACLE_CONSTRAINT_ROWS: &[(&str, Option<&str>)] = &[
    ("cc_a_fkey f f f t pp a a s {1} {1}", None),
    ("cc_def    f t t t pp a d s {3} {2}", None),
    ("cc_full   f f f t pp c n f {2} {1}", None),
    ("cc_nv     f f f f pp a r s {1} {1}", None),
];

/// The whole `contype = 'f'` projection the capture records, compared as
/// complete rows. `confrelid` is joined back to `pg_class` because the oracle
/// names the parent relation rather than an oid whose numeric value is crabka's
/// own business.
#[tokio::test]
async fn pg_constraint_foreign_key_rows_match_the_oracle() {
    let engine = oracle_fixture().await;
    let listed = grid(
        &engine,
        "SELECT con.conname, con.contype, con.condeferrable, con.condeferred, con.convalidated, \
                parent.relname, con.confupdtype, con.confdeltype, con.confmatchtype, \
                con.conkey, con.confkey, con.confdelsetcols \
         FROM pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class parent ON parent.oid = con.confrelid \
         WHERE con.contype = 'f' ORDER BY con.conname",
    )
    .await;
    let expected: Vec<_> = ORACLE_CONSTRAINT_ROWS
        .iter()
        .map(|(cells, confdelsetcols)| fk_row(cells, *confdelsetcols))
        .collect();
    assert2::assert!(listed == expected);
}

/// `convalidated` is the one `pg_constraint` flag `ALTER TABLE … VALIDATE
/// CONSTRAINT` moves, and `pg_get_constraintdef` drops its `NOT VALID` suffix
/// with it. A client watching for unvalidated constraints reads exactly this.
#[tokio::test]
async fn validating_a_not_valid_foreign_key_flips_convalidated_and_its_definition() {
    let engine = oracle_fixture().await;
    let probe = "SELECT convalidated, pg_catalog.pg_get_constraintdef(oid) \
                 FROM pg_catalog.pg_constraint WHERE conname = 'cc_nv'";

    let before = grid(&engine, probe).await;
    assert2::assert!(
        before
            == vec![some(&[
                "f",
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE RESTRICT NOT VALID",
            ])]
    );

    exec(&engine, "ALTER TABLE cc VALIDATE CONSTRAINT cc_nv").await;
    let after = grid(&engine, probe).await;
    assert2::assert!(
        after
            == vec![some(&[
                "t",
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE RESTRICT",
            ])]
    );
}

/// `confdelsetcols` is NULL unless an `ON DELETE SET … (columns)` list was
/// written — `PostgreSQL` does not fill it with a copy of `conkey` for the
/// implicit all-columns case.
#[tokio::test]
async fn confdelsetcols_records_only_an_explicit_set_column_list() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE ppc (m int4, n int4, PRIMARY KEY (m, n))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE implicit (a int4, b int4, \
         CONSTRAINT implicit_fk FOREIGN KEY (a, b) REFERENCES ppc (m, n) ON DELETE SET NULL)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE listed (a int4, b int4, \
         CONSTRAINT listed_fk FOREIGN KEY (a, b) REFERENCES ppc (m, n) ON DELETE SET NULL (b))",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT conname, conkey, confdelsetcols FROM pg_catalog.pg_constraint \
         WHERE contype = 'f' ORDER BY conname",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                fk_row("implicit_fk {1,2}", None),
                fk_row("listed_fk {1,2}", Some("{2}")),
            ]
    );
}

/// A composite foreign key stores `conkey` and `confkey` in the order the FK
/// clause wrote them, paired positionally — not sorted, and not permuted into
/// the referenced index's order. `FOREIGN KEY (b, a) REFERENCES pperm(y, x)`
/// over a `(x, y)` primary key is `{2,1}` on both sides.
#[tokio::test]
async fn composite_foreign_keys_keep_both_key_lists_in_clause_order() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE pperm (x int4, y int4, PRIMARY KEY (x, y))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE cperm (a int4, b int4, FOREIGN KEY (b, a) REFERENCES pperm (y, x))",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT conname, conkey, confkey, pg_catalog.pg_get_constraintdef(oid) \
         FROM pg_catalog.pg_constraint WHERE contype = 'f'",
    )
    .await;
    assert2::assert!(
        listed
            == vec![some(&[
                "cperm_b_a_fkey",
                "{2,1}",
                "{2,1}",
                "FOREIGN KEY (b, a) REFERENCES pperm(y, x)",
            ])]
    );
}

/// (child DDL, constraint name, the `pg_get_constraintdef` text `PostgreSQL`
/// 18.4 returns for it).
const DEFINITION_CASES: &[(&str, &str, &str)] = &[
    (
        "CREATE TABLE d01 (a int4 CONSTRAINT d01_fk REFERENCES pp (id))",
        "d01_fk",
        "FOREIGN KEY (a) REFERENCES pp(id)",
    ),
    (
        "CREATE TABLE d02 (a int4 CONSTRAINT d02_fk REFERENCES pp (id) MATCH FULL)",
        "d02_fk",
        "FOREIGN KEY (a) REFERENCES pp(id) MATCH FULL",
    ),
    (
        "CREATE TABLE d03 (a int4 CONSTRAINT d03_fk REFERENCES pp (id) \
         MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL)",
        "d03_fk",
        "FOREIGN KEY (a) REFERENCES pp(id) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL",
    ),
    // Written delete-first; PostgreSQL always prints `ON UPDATE` first.
    (
        "CREATE TABLE d04 (a int4 CONSTRAINT d04_fk REFERENCES pp (id) \
         ON DELETE CASCADE ON UPDATE RESTRICT)",
        "d04_fk",
        "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE RESTRICT ON DELETE CASCADE",
    ),
    (
        "CREATE TABLE d05 (c int4 CONSTRAINT d05_fk REFERENCES pp (k) \
         ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED)",
        "d05_fk",
        "FOREIGN KEY (c) REFERENCES pp(k) ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED",
    ),
    (
        "CREATE TABLE d06 (a int4 CONSTRAINT d06_fk REFERENCES pp (id) DEFERRABLE)",
        "d06_fk",
        "FOREIGN KEY (a) REFERENCES pp(id) DEFERRABLE",
    ),
    (
        "CREATE TABLE d07 (a int4); \
         ALTER TABLE d07 ADD CONSTRAINT d07_fk FOREIGN KEY (a) REFERENCES pp (id) \
         ON DELETE RESTRICT NOT VALID",
        "d07_fk",
        "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE RESTRICT NOT VALID",
    ),
    (
        "CREATE TABLE d08 (a int4, b int4, CONSTRAINT d08_fk FOREIGN KEY (a, b) \
         REFERENCES ppc (m, n) ON DELETE SET NULL (b))",
        "d08_fk",
        "FOREIGN KEY (a, b) REFERENCES ppc(m, n) ON DELETE SET NULL (b)",
    ),
    (
        "CREATE TABLE d09 (a int4, b int4, CONSTRAINT d09_fk FOREIGN KEY (b, a) \
         REFERENCES ppc (n, m))",
        "d09_fk",
        "FOREIGN KEY (b, a) REFERENCES ppc(n, m)",
    ),
    (
        "CREATE TABLE d10 (\"Ref Col\" int4 CONSTRAINT d10_fk \
         REFERENCES \"Odd Parent\" (\"Key Col\"))",
        "d10_fk",
        "FOREIGN KEY (\"Ref Col\") REFERENCES \"Odd Parent\"(\"Key Col\")",
    ),
];

/// `pg_get_constraintdef` is byte-exact across the whole spelling matrix:
/// `MATCH FULL` before the actions, `ON UPDATE` before `ON DELETE`, the
/// deferral after both, `NOT VALID` last, `MATCH SIMPLE`/`NO ACTION` silent,
/// the `ON DELETE SET …` column list attached to its action, composite lists in
/// clause order, and identifiers quoted only where they need it.
#[tokio::test]
async fn pg_get_constraintdef_matches_the_oracle_across_the_spelling_matrix() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE pp (id int4 PRIMARY KEY, k int4 UNIQUE)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE ppc (m int4, n int4, PRIMARY KEY (m, n))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE \"Odd Parent\" (\"Key Col\" int4 PRIMARY KEY)",
    )
    .await;
    for (ddl, _, _) in DEFINITION_CASES {
        exec(&engine, ddl).await;
    }

    let listed = grid(
        &engine,
        "SELECT conname, pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint \
         WHERE contype = 'f' ORDER BY conname",
    )
    .await;
    let expected: Vec<_> = DEFINITION_CASES
        .iter()
        .map(|(_, name, definition)| some(&[name, definition]))
        .collect();
    assert2::assert!(listed == expected);
}

/// The referenced relation is emitted **unqualified** — `REFERENCES pp(id)`,
/// never `public.pp(id)` — even though `pg_get_indexdef` on the very same
/// relation does qualify. Neither should be "fixed" to match the other.
#[tokio::test]
async fn the_referenced_relation_is_emitted_unqualified() {
    let engine = oracle_fixture().await;
    let definition = column(
        &engine,
        "SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint \
         WHERE conname = 'cc_a_fkey'",
    )
    .await;
    assert2::assert!(definition == some(&["FOREIGN KEY (a) REFERENCES pp(id)"]));

    let index = column(
        &engine,
        "SELECT pg_catalog.pg_get_indexdef(c.oid) FROM pg_catalog.pg_class c \
         WHERE c.relname = 'pp_pkey'",
    )
    .await;
    assert2::assert!(index == some(&["CREATE UNIQUE INDEX pp_pkey ON public.pp USING btree (id)"]));
}

/// (referential tail, `match_option`, `update_rule`, `delete_rule`).
const RULE_CASES: &[(&str, &str, &str, &str)] = &[
    ("", "NONE", "NO ACTION", "NO ACTION"),
    ("MATCH SIMPLE", "NONE", "NO ACTION", "NO ACTION"),
    ("MATCH FULL", "FULL", "NO ACTION", "NO ACTION"),
    (
        "ON UPDATE RESTRICT ON DELETE CASCADE",
        "NONE",
        "RESTRICT",
        "CASCADE",
    ),
    (
        "ON UPDATE SET NULL ON DELETE SET DEFAULT",
        "NONE",
        "SET NULL",
        "SET DEFAULT",
    ),
    (
        "MATCH FULL ON UPDATE CASCADE ON DELETE RESTRICT",
        "FULL",
        "CASCADE",
        "RESTRICT",
    ),
    (
        "ON UPDATE NO ACTION ON DELETE SET NULL",
        "NONE",
        "NO ACTION",
        "SET NULL",
    ),
];

/// `information_schema.referential_constraints` spells the SQL standard's
/// names: `NONE` — not `SIMPLE` — for `MATCH SIMPLE`, `FULL` for `MATCH FULL`,
/// and the referential actions written out rather than as `pg_constraint`'s
/// one-character codes.
#[tokio::test]
async fn referential_constraints_spell_the_match_option_and_the_rules() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE pp (id int4 PRIMARY KEY)").await;
    for (index, (tail, ..)) in RULE_CASES.iter().enumerate() {
        exec(
            &engine,
            &format!(
                "CREATE TABLE rule{index} (a int4 CONSTRAINT rule{index}_fk \
                 REFERENCES pp (id) {tail})"
            ),
        )
        .await;
    }

    let listed = grid(
        &engine,
        "SELECT constraint_name, match_option, update_rule, delete_rule \
         FROM information_schema.referential_constraints ORDER BY constraint_name",
    )
    .await;
    let expected: Vec<_> = RULE_CASES
        .iter()
        .enumerate()
        .map(|(index, (_, match_option, update_rule, delete_rule))| {
            some(&[
                &format!("rule{index}_fk"),
                match_option,
                update_rule,
                delete_rule,
            ])
        })
        .collect();
    assert2::assert!(listed == expected);
}

/// `unique_constraint_name` names the constraint the foreign key targets — but
/// a bare `CREATE UNIQUE INDEX` is not a constraint, so the whole
/// catalog/schema/name triple comes back NULL for a key that targets one.
#[tokio::test]
async fn unique_constraint_name_is_null_when_the_referent_is_a_bare_unique_index() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE pp (id int4 PRIMARY KEY, k int4 UNIQUE)",
    )
    .await;
    exec(&engine, "CREATE TABLE uix (a int4)").await;
    exec(&engine, "CREATE UNIQUE INDEX uix_a_uq ON uix (a)").await;
    exec(
        &engine,
        "CREATE TABLE to_pkey (a int4 CONSTRAINT to_pkey_fk REFERENCES pp (id))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE to_unique (a int4 CONSTRAINT to_unique_fk REFERENCES pp (k))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE to_index (a int4 CONSTRAINT to_index_fk REFERENCES uix (a))",
    )
    .await;

    let listed = grid(
        &engine,
        "SELECT constraint_name, unique_constraint_catalog, unique_constraint_schema, \
                unique_constraint_name \
         FROM information_schema.referential_constraints ORDER BY constraint_name",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                vec![Some("to_index_fk".to_string()), None, None, None],
                some(&["to_pkey_fk", "postgres", "public", "pp_pkey"]),
                some(&["to_unique_fk", "postgres", "public", "pp_k_key"]),
            ]
    );
}

/// A foreign key is the one constraint kind crabka can defer, so it is the one
/// whose `information_schema.table_constraints` row reports anything but
/// `NO`/`NO`.
#[tokio::test]
async fn table_constraints_reports_a_foreign_keys_real_deferral() {
    let engine = oracle_fixture().await;
    let listed = grid(
        &engine,
        "SELECT constraint_name, table_name, constraint_type, is_deferrable, initially_deferred \
         FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' \
         ORDER BY constraint_name",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["cc_a_fkey", "cc", "FOREIGN KEY", "NO", "NO"]),
                some(&["cc_def", "cc", "FOREIGN KEY", "YES", "YES"]),
                some(&["cc_full", "cc", "FOREIGN KEY", "NO", "NO"]),
                some(&["cc_nv", "cc", "FOREIGN KEY", "NO", "NO"]),
            ]
    );
}

/// `key_column_usage` lists a foreign key's *referencing* columns, numbered
/// 1..n in clause order, with a non-NULL `position_in_unique_constraint`.
#[tokio::test]
async fn key_column_usage_numbers_the_referencing_columns() {
    let engine = oracle_fixture().await;
    let listed = grid(
        &engine,
        "SELECT constraint_name, table_name, column_name, ordinal_position, \
                position_in_unique_constraint \
         FROM information_schema.key_column_usage WHERE table_name = 'cc' \
         ORDER BY constraint_name, ordinal_position",
    )
    .await;
    assert2::assert!(
        listed
            == vec![
                some(&["cc_a_fkey", "cc", "a", "1", "1"]),
                some(&["cc_def", "cc", "c", "1", "1"]),
                some(&["cc_full", "cc", "b", "1", "1"]),
                some(&["cc_nv", "cc", "a", "1", "1"]),
            ]
    );
}

/// `position_in_unique_constraint` is the paired referenced column's position
/// **within the referenced index**, not its position in the FK clause. A
/// permuted composite makes the two disagree: `ordinal_position` 1 pairs with
/// index position 2 and vice versa.
#[tokio::test]
async fn position_in_unique_constraint_follows_the_referenced_index_order() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE pperm (x int4, y int4, PRIMARY KEY (x, y))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE cperm (a int4, b int4, FOREIGN KEY (b, a) REFERENCES pperm (y, x))",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT column_name, ordinal_position, position_in_unique_constraint \
         FROM information_schema.key_column_usage WHERE constraint_name = 'cperm_b_a_fkey' \
         ORDER BY ordinal_position",
    )
    .await;
    assert2::assert!(listed == vec![some(&["b", "1", "2"]), some(&["a", "2", "1"])]);
}

/// `constraint_column_usage` is `key_column_usage`'s mirror: it attributes a
/// foreign key to the columns it *references*, on the parent relation.
#[tokio::test]
async fn constraint_column_usage_names_the_referenced_table_and_columns() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE ppc (m int4, n int4, PRIMARY KEY (m, n))",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE ccc (a int4, b int4, CONSTRAINT ccc_fk FOREIGN KEY (b, a) \
         REFERENCES ppc (n, m))",
    )
    .await;
    let listed = grid(
        &engine,
        "SELECT u.constraint_name, u.table_name, u.column_name \
         FROM information_schema.constraint_column_usage u \
         JOIN information_schema.table_constraints t \
           ON t.constraint_name = u.constraint_name \
         WHERE t.constraint_type = 'FOREIGN KEY' ORDER BY u.column_name",
    )
    .await;
    assert2::assert!(listed == vec![some(&["ccc_fk", "ppc", "m"]), some(&["ccc_fk", "ppc", "n"]),]);
}

/// A relation name resolved through `regclass` selects the same constraints as
/// the relation's own `pg_class` oid, on both the referencing (`conrelid`) and
/// the referenced (`confrelid`) side.
#[tokio::test]
async fn regclass_resolves_the_owning_and_referenced_relations() {
    let engine = oracle_fixture().await;
    let on_the_child = column(
        &engine,
        "SELECT conname FROM pg_catalog.pg_constraint \
         WHERE conrelid = 'cc'::regclass AND contype = 'f' ORDER BY conname",
    )
    .await;
    assert2::assert!(on_the_child == some(&["cc_a_fkey", "cc_def", "cc_full", "cc_nv"]));

    let on_the_parent = column(
        &engine,
        "SELECT conname FROM pg_catalog.pg_constraint \
         WHERE confrelid = 'pp'::regclass AND contype = 'f' ORDER BY conname",
    )
    .await;
    assert2::assert!(on_the_parent == on_the_child);

    // …and both oids round-trip back to the relation names through `pg_class`.
    let named = grid(
        &engine,
        "SELECT con.conname, child.relname, parent.relname \
         FROM pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class child ON child.oid = con.conrelid \
         JOIN pg_catalog.pg_class parent ON parent.oid = con.confrelid \
         WHERE con.contype = 'f' ORDER BY con.conname",
    )
    .await;
    assert2::assert!(
        named
            == vec![
                some(&["cc_a_fkey", "cc", "pp"]),
                some(&["cc_def", "cc", "pp"]),
                some(&["cc_full", "cc", "pp"]),
                some(&["cc_nv", "cc", "pp"]),
            ]
    );
}

/// The `dpar`/`dchild` pair whose `psql \d` rendering the capture records.
async fn psql_fixture(engine: &SqlEngine, extra: &[&str]) {
    exec(
        engine,
        "CREATE TABLE dpar (id int4 PRIMARY KEY, k int4 UNIQUE)",
    )
    .await;
    exec(
        engine,
        "CREATE TABLE dchild (\
         a int4 CONSTRAINT dchild_a_fkey REFERENCES dpar (id), \
         b int4, \
         c int4 CONSTRAINT dchild_def REFERENCES dpar (k) \
           ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED)",
    )
    .await;
    for sql in extra {
        exec(engine, sql).await;
    }
}

/// `psql`'s `\d <child>` block: four-space indent, the constraint name
/// double-quoted, and `pg_get_constraintdef` verbatim after it.
fn foreign_key_block(header: &str, lines: &[String]) -> String {
    let mut out = String::from(header);
    for line in lines {
        out.push_str("\n    ");
        out.push_str(line);
    }
    out
}

/// The child half of `\d`, rendered from the same rows `psql` selects.
#[tokio::test]
async fn psql_backslash_d_renders_the_childs_foreign_key_block() {
    let engine = SqlEngine::new();
    psql_fixture(
        &engine,
        &[
            "ALTER TABLE dchild ADD CONSTRAINT dchild_full FOREIGN KEY (b) REFERENCES dpar (id) \
             MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL",
            "ALTER TABLE dchild ADD CONSTRAINT dchild_nv FOREIGN KEY (a) REFERENCES dpar (id) \
             ON DELETE RESTRICT NOT VALID",
        ],
    )
    .await;

    let listed = grid(
        &engine,
        "SELECT con.conname, pg_catalog.pg_get_constraintdef(con.oid, true) \
         FROM pg_catalog.pg_constraint con \
         WHERE con.conrelid = 'dchild'::regclass AND con.contype = 'f' \
         ORDER BY con.conname",
    )
    .await;
    let lines: Vec<String> = listed
        .iter()
        .map(|row| {
            let name = row[0].as_deref().expect("conname");
            let definition = row[1].as_deref().expect("constraintdef");
            format!("\"{name}\" {definition}")
        })
        .collect();

    assert2::assert!(
        foreign_key_block("Foreign-key constraints:", &lines)
            == "Foreign-key constraints:\n    \
                \"dchild_a_fkey\" FOREIGN KEY (a) REFERENCES dpar(id)\n    \
                \"dchild_def\" FOREIGN KEY (c) REFERENCES dpar(k) ON DELETE SET DEFAULT \
                DEFERRABLE INITIALLY DEFERRED\n    \
                \"dchild_full\" FOREIGN KEY (b) REFERENCES dpar(id) MATCH FULL ON UPDATE CASCADE \
                ON DELETE SET NULL\n    \
                \"dchild_nv\" FOREIGN KEY (a) REFERENCES dpar(id) ON DELETE RESTRICT NOT VALID"
    );
}

/// The parent half of `\d`. `psql` builds its `Referenced by:` section by
/// filtering `pg_constraint` on `confrelid`, so a wrong `confrelid` makes the
/// parent's `\d` silently empty rather than visibly wrong.
#[tokio::test]
async fn psql_backslash_d_finds_a_parents_children_through_confrelid() {
    let engine = SqlEngine::new();
    psql_fixture(&engine, &[]).await;

    let listed = grid(
        &engine,
        "SELECT con.conname, child.relname, pg_catalog.pg_get_constraintdef(con.oid, true) \
         FROM pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class child ON child.oid = con.conrelid \
         WHERE con.confrelid = 'dpar'::regclass AND con.contype = 'f' AND con.conparentid = 0 \
         ORDER BY con.conname",
    )
    .await;
    let lines: Vec<String> = listed
        .iter()
        .map(|row| {
            let name = row[0].as_deref().expect("conname");
            let table = row[1].as_deref().expect("relname");
            let definition = row[2].as_deref().expect("constraintdef");
            format!("TABLE \"{table}\" CONSTRAINT \"{name}\" {definition}")
        })
        .collect();

    assert2::assert!(
        foreign_key_block("Referenced by:", &lines)
            == "Referenced by:\n    \
                TABLE \"dchild\" CONSTRAINT \"dchild_a_fkey\" FOREIGN KEY (a) REFERENCES \
                dpar(id)\n    \
                TABLE \"dchild\" CONSTRAINT \"dchild_def\" FOREIGN KEY (c) REFERENCES dpar(k) \
                ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED"
    );

    // A relation nothing references has an empty section rather than an error.
    let unreferenced = column(
        &engine,
        "SELECT conname FROM pg_catalog.pg_constraint \
         WHERE confrelid = 'dchild'::regclass AND contype = 'f'",
    )
    .await;
    assert2::assert!(unreferenced == Vec::<Option<String>>::new());
}
