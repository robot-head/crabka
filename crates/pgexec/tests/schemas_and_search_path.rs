//! Relations live in schemas, and the `search_path` decides which one an
//! unqualified name reaches.
//!
//! Every expectation here was captured from `postgres:18.4` before it was
//! written down; the cases that contradict a careful reading of the
//! documentation are called out where they appear.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// One session, so a `SET` and the statements that depend on it stay together.
struct Client {
    session: crabka_pgexec::SqlSession,
}

impl Client {
    fn new(engine: &SqlEngine) -> Self {
        Self {
            session: engine.connect(),
        }
    }

    async fn run(&mut self, sql: &str) -> QueryResult {
        self.session
            .simple_query(sql)
            .await
            .expect("query succeeds")
            .into_iter()
            .next_back()
            .expect("at least one result")
    }

    async fn fails(&mut self, sql: &str) -> crabka_pgwire::error::PgError {
        self.session
            .simple_query(sql)
            .await
            .expect_err("statement is refused")
    }

    /// The statement's rows as text, so a case compares a whole table rather
    /// than a chain of per-cell assertions.
    async fn rows(&mut self, sql: &str) -> Vec<Vec<Option<String>>> {
        text_rows(&self.run(sql).await)
    }

    /// The single scalar a one-row, one-column query answers with.
    async fn scalar(&mut self, sql: &str) -> Option<String> {
        let rows = self.rows(sql).await;
        assert!(rows.len() == 1);
        rows[0][0].clone()
    }
}

fn text_rows(result: &QueryResult) -> Vec<Vec<Option<String>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell: &Option<Cell>| {
                        cell.as_ref()
                            .map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf-8 cell"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn one(value: &str) -> Vec<Vec<Option<String>>> {
    vec![vec![Some(value.to_string())]]
}

/// The same relation name in two schemas resolves by the search path, and each
/// stays reachable by its qualified name.
#[tokio::test]
async fn the_search_path_decides_which_of_two_same_named_relations_is_read() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE SCHEMA s2").await;
    client.run("CREATE TABLE s1.t (x int)").await;
    client.run("CREATE TABLE s2.t (x int)").await;
    client.run("INSERT INTO s1.t VALUES (1)").await;
    client.run("INSERT INTO s2.t VALUES (2)").await;

    for (path, unqualified) in [("s1, s2", "1"), ("s2, s1", "2")] {
        client.run(&format!("SET search_path = {path}")).await;
        assert!(client.rows("SELECT x FROM t").await == one(unqualified));
        // A qualified name ignores the path entirely.
        assert!(client.rows("SELECT x FROM s1.t").await == one("1"));
        assert!(client.rows("SELECT x FROM s2.t").await == one("2"));
    }
}

/// A write resolves the same way a read does — the defect class this wave
/// exists to close is an operation that silently does not shadow.
#[tokio::test]
async fn every_operation_resolves_through_the_same_search_path() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE SCHEMA s2").await;
    client.run("CREATE TABLE s1.t (x int)").await;
    client.run("CREATE TABLE s2.t (x int)").await;

    client.run("SET search_path = s2, s1").await;
    client.run("INSERT INTO t VALUES (99)").await;
    client.run("UPDATE t SET x = 100").await;
    assert!(client.rows("SELECT x FROM s2.t").await == one("100"));
    assert!(client.rows("SELECT x FROM s1.t").await.is_empty());

    client.run("DELETE FROM t").await;
    assert!(client.rows("SELECT x FROM s2.t").await.is_empty());

    client.run("DROP TABLE t").await;
    // The shadowed relation is now the visible one.
    assert!(client.rows("SELECT x FROM t").await.is_empty());
    assert!(client.fails("SELECT x FROM s2.t").await.code == "42P01");
}

/// `CREATE` lands in the first *existing* explicit entry, and a nonexistent one
/// is skipped rather than refused. Oracle: `SET search_path = nosuch, s1, s2;
/// CREATE TABLE lands_where (x int)` puts the table in `s1`.
#[tokio::test]
async fn creation_lands_in_the_first_existing_explicit_entry() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE SCHEMA s2").await;
    client.run("SET search_path = nosuch, s1, s2").await;
    client.run("CREATE TABLE lands_where (x int)").await;

    assert!(
        client
            .rows(
                "SELECT table_schema FROM information_schema.tables \
                 WHERE table_name = 'lands_where'",
            )
            .await
            == one("s1")
    );
    assert!(client.scalar("SELECT current_schema").await == Some("s1".to_string()));
}

/// A path naming nothing that exists is not an error until something has to be
/// created in it, which is `3F000 no schema has been selected to create in`.
#[tokio::test]
async fn a_path_with_no_existing_entry_refuses_only_a_creation() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("SET search_path = notme").await;

    assert!(client.scalar("SELECT current_schema").await == None);
    assert!(client.rows("SELECT current_schemas(true)").await == one("{pg_catalog}"));
    // `pg_catalog` is implicit regardless, so the catalog is still readable.
    client.run("SELECT count(*) FROM pg_class").await;

    let error = client.fails("CREATE TABLE t (x int)").await;
    assert!(error.code == "3F000");
}

/// `SHOW search_path` reproduces what `SET` was given, including the quoting a
/// plain join would lose and an entry holding a comma, which a join cannot
/// represent at all. Every expected value is `postgres:18.4`'s.
#[tokio::test]
async fn show_search_path_round_trips_every_written_spelling() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    let cases = [
        (
            r#"SET search_path = "MySchema", public"#,
            r#""MySchema", public"#,
        ),
        ("SET search_path = MySchema", "myschema"),
        (r"SET search_path = 'a,b', public", r#""a,b", public"#),
        (r#"SET search_path = '"unbalanced'"#, r#""""unbalanced""#),
        (r#"SET search_path = "$user", public"#, r#""$user", public"#),
        (r#"SET search_path = 'x y', "q,r""#, r#""x y", "q,r""#),
        ("SET search_path = pg_catalog, public", "pg_catalog, public"),
        ("SET search_path = public, public", "public, public"),
    ];
    for (set, shown) in cases {
        client.run(set).await;
        assert!(client.rows("SHOW search_path").await == one(shown));
    }
}

/// `current_schema`/`current_schemas` read the real path, filtered against the
/// catalog. Oracle: after `SET search_path = pg_catalog, public`,
/// `current_schemas(false)` is `{pg_catalog,public}` and `current_schema` is
/// `pg_catalog`.
#[tokio::test]
async fn current_schema_functions_follow_the_search_path() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;

    let cases = [
        (
            r#"SET search_path = "$user", public"#,
            Some("public"),
            "{public}",
            "{pg_catalog,public}",
        ),
        (
            "SET search_path = pg_catalog, public",
            Some("pg_catalog"),
            "{pg_catalog,public}",
            "{pg_catalog,public}",
        ),
        (
            "SET search_path = s1, public",
            Some("s1"),
            "{s1,public}",
            "{pg_catalog,s1,public}",
        ),
        // A nonexistent entry is filtered out entirely rather than reported.
        (
            "SET search_path = nosuch, s1",
            Some("s1"),
            "{s1}",
            "{pg_catalog,s1}",
        ),
        ("SET search_path = ''", None, "{}", "{pg_catalog}"),
    ];
    for (set, schema, explicit, implicit) in cases {
        client.run(set).await;
        assert!(client.scalar("SELECT current_schema").await == schema.map(str::to_string));
        assert!(client.rows("SELECT current_schemas(false)").await == one(explicit));
        assert!(client.rows("SELECT current_schemas(true)").await == one(implicit));
    }
}

/// A missing schema is `3F000` for a utility statement and `42P01` for a
/// reference — the split the resolver's disposition argument exists for, and
/// one the plan this wave started from had backwards.
#[tokio::test]
async fn a_missing_schema_is_reported_differently_by_statement_kind() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    let cases = [
        ("SELECT * FROM nope.t", "42P01"),
        ("INSERT INTO nope.t VALUES (1)", "42P01"),
        ("DROP TABLE nope.t", "3F000"),
        ("CREATE TABLE nope.t (x int)", "3F000"),
        ("TRUNCATE nope.t", "3F000"),
    ];
    for (sql, code) in cases {
        assert!(client.fails(sql).await.code == code);
    }
    // `IF EXISTS` skips the missing schema rather than reporting it.
    client.run("DROP TABLE IF EXISTS nope.t").await;
}

/// A relation whose *name* contains a dot and a relation of that name in a
/// schema of that name are different relations with different contents. No
/// flattened `schema.relation` string can hold both, which is the whole
/// argument for the two-part catalog key.
#[tokio::test]
async fn a_dotted_relation_name_and_a_qualified_one_are_different_relations() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run(r#"CREATE TABLE "a.b" (x int)"#).await;
    client.run("CREATE SCHEMA a").await;
    client.run("CREATE TABLE a.b (y int)").await;
    client.run(r#"INSERT INTO "a.b" VALUES (1)"#).await;
    client.run("INSERT INTO a.b VALUES (2)").await;

    assert!(client.rows(r#"SELECT x FROM "a.b""#).await == one("1"));
    assert!(client.rows("SELECT y FROM a.b").await == one("2"));
}

/// A relation whose name holds a `/` is an ordinary relation. It used to be
/// storable and queryable but invisible to every catalog projection, because
/// the catalog scan recovered a name by rejecting any key suffix holding a
/// slash — which also meant `DROP SCHEMA … CASCADE` walked straight past it.
#[tokio::test]
async fn a_relation_whose_name_holds_a_slash_is_visible_to_the_catalog() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run(r#"CREATE TABLE s1."a/b" (x int)"#).await;
    client.run(r#"INSERT INTO s1."a/b" VALUES (7)"#).await;

    assert!(client.rows(r#"SELECT x FROM s1."a/b""#).await == one("7"));
    assert!(
        client
            .rows("SELECT relname FROM pg_class WHERE relname = 'a/b'")
            .await
            == one("a/b")
    );
    assert!(
        client
            .rows("SELECT table_schema FROM information_schema.tables WHERE table_name = 'a/b'",)
            .await
            == one("s1")
    );

    client.run("DROP SCHEMA s1 CASCADE").await;
    assert!(client.fails(r#"SELECT x FROM s1."a/b""#).await.code == "42P01");
}

/// `DROP SCHEMA … CASCADE` accounts for every kind of relation the schema
/// holds, and `RESTRICT` refuses while any of them remains.
#[tokio::test]
async fn drop_schema_cascade_finds_every_relation_in_the_schema() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE TABLE s1.t (x int)").await;
    client.run("CREATE VIEW s1.v AS SELECT x FROM s1.t").await;
    client.run("CREATE SEQUENCE s1.q").await;

    assert!(client.fails("DROP SCHEMA s1").await.code == "2BP01");
    client.run("DROP SCHEMA s1 CASCADE").await;

    for sql in [
        "SELECT x FROM s1.t",
        "SELECT x FROM s1.v",
        "SELECT nextval('s1.q')",
    ] {
        assert!(!client.fails(sql).await.code.is_empty());
    }
    // The schema itself is gone, so re-creating it is not a duplicate.
    client.run("CREATE SCHEMA s1").await;
}

/// The implicit `pg_catalog` entry genuinely shadows: a user relation called
/// `pg_class` in `public` does not displace the catalog relation for an
/// unqualified read.
#[tokio::test]
async fn the_implicit_pg_catalog_entry_shadows_a_user_relation() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE public.pg_class (x int)").await;
    client.run("INSERT INTO public.pg_class VALUES (1)").await;

    // `pg_class` still names the catalog relation, which has more than one row
    // and a `relname` column the user relation does not have.
    client.run("SELECT relname FROM pg_class").await;
}

/// A relation moves nowhere when it is renamed: `RENAME TO` takes an
/// unqualified name and leaves the relation in the schema it was already in.
#[tokio::test]
async fn rename_keeps_a_relation_in_its_own_schema() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE TABLE s1.t (x int)").await;
    client.run("INSERT INTO s1.t VALUES (5)").await;
    client.run("ALTER TABLE s1.t RENAME TO renamed").await;

    assert!(client.rows("SELECT x FROM s1.renamed").await == one("5"));
    assert!(
        client
            .rows(
                "SELECT table_schema FROM information_schema.tables \
                 WHERE table_name = 'renamed'",
            )
            .await
            == one("s1")
    );
}

/// A default constraint or index name is built from the relation's own name,
/// not from its qualified spelling, and the object lands beside the table in
/// the table's schema. On `postgres:18.4`,
/// `CREATE TABLE nm.t (id serial PRIMARY KEY, y int UNIQUE, z int CHECK (z > 0))`
/// followed by `CREATE INDEX ON nm.t (y)` gives constraints `t_pkey`,
/// `t_y_key`, `t_z_check` (beside the `t_id_not_null` the `serial` brings) and
/// relations `t`, `t_id_seq`, `t_pkey`, `t_y_key`, `t_y_idx` — every one of
/// them unqualified, all of them in `nm`.
#[tokio::test]
async fn default_constraint_names_do_not_carry_the_schema() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA nm").await;
    client
        .run("CREATE TABLE nm.t (id serial PRIMARY KEY, y int UNIQUE, z int CHECK (z > 0))")
        .await;
    client.run("CREATE INDEX ON nm.t (y)").await;

    assert!(
        client
            .rows(
                "SELECT c.relname FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'nm' ORDER BY c.relname",
            )
            .await
            == vec![
                vec![Some("t".to_string())],
                vec![Some("t_id_seq".to_string())],
                vec![Some("t_pkey".to_string())],
                vec![Some("t_y_idx".to_string())],
                vec![Some("t_y_key".to_string())],
            ]
    );
    assert!(
        client
            .rows(
                "SELECT conname FROM pg_constraint \
                 WHERE conrelid = (SELECT c.oid FROM pg_class c \
                                   JOIN pg_namespace n ON n.oid = c.relnamespace \
                                   WHERE n.nspname = 'nm' AND c.relname = 't') \
                 ORDER BY conname",
            )
            .await
            == vec![
                vec![Some("t_id_not_null".to_string())],
                vec![Some("t_pkey".to_string())],
                vec![Some("t_y_key".to_string())],
                vec![Some("t_z_check".to_string())],
            ]
    );
}
