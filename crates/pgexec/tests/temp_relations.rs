//! Temporary relations: a namespace per session, first in the search path,
//! emptied when the session ends.
//!
//! Every expectation here was captured from a live `postgres:18.4` and is
//! written down as that server answered. This includes the places where the
//! answer contradicts a careful reading of the documentation. The one recorded
//! divergence is noted where it appears. This engine has no notice channel, so
//! the `NOTICE` 18.4 emits when it converts a view to a temporary view is
//! silently absent.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// A refused statement as a whole value: its SQLSTATE and its primary message.
type Refusal = (String, String);

fn refused(code: &str, message: &str) -> Refusal {
    (code.to_string(), message.to_string())
}

/// The name every session's temporary namespace is folded to, so a case can
/// compare a whole schema list without knowing the backend id behind it.
const TEMP_PLACEHOLDER: &str = "pg_temp_<n>";

/// One session, so a `SET`, a temporary relation and the statements that depend
/// on either stay together.
struct Client {
    session: SqlSession,
}

impl Client {
    fn new(engine: &SqlEngine) -> Self {
        Self {
            session: engine.connect(),
        }
    }

    /// A session with a chosen backend id, which is what makes "a later
    /// session inherits this namespace" reachable from a test.
    fn with_pid(engine: &SqlEngine, pid: i32) -> Self {
        Self {
            session: engine.connect_with_pid(pid),
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

    /// Whether the statement was accepted, and if not how it was refused.
    async fn outcome(&mut self, sql: &str) -> Result<(), Refusal> {
        match self.session.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(error) => Err((error.code, error.message)),
        }
    }

    async fn refusal(&mut self, sql: &str) -> Refusal {
        self.outcome(sql).await.expect_err("statement is refused")
    }

    /// The statement's rows as text, so a case compares a whole table instead
    /// of a chain of per-cell assertions.
    async fn rows(&mut self, sql: &str) -> Vec<Vec<Option<String>>> {
        text_rows(&self.run(sql).await)
    }

    /// The single scalar a one-row, one-column query answers with.
    async fn scalar(&mut self, sql: &str) -> Option<String> {
        let rows = self.rows(sql).await;
        assert!(rows.len() == 1);
        rows[0][0].clone()
    }

    /// `current_schemas(implicit)` as a list.
    async fn schemas(&mut self, implicit: bool) -> Vec<String> {
        let literal = self
            .scalar(&format!("SELECT current_schemas({implicit})"))
            .await
            .expect("current_schemas answers with an array");
        literal
            .trim_start_matches('{')
            .trim_end_matches('}')
            .split(',')
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// This session's temporary namespace, read back from the session and not
    /// derived. The backend id is not the test's to know.
    async fn temp_namespace(&mut self) -> String {
        self.schemas(true)
            .await
            .into_iter()
            .find(|schema| is_temp_namespace(schema))
            .expect("the session has a temporary namespace")
    }

    async fn terminate(&mut self) {
        self.session.terminate().await;
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

fn row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

fn is_temp_namespace(name: &str) -> bool {
    name.strip_prefix("pg_temp_")
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// A schema list with the session-specific backend id folded away, so a case
/// asserts the whole list and its order instead of single entries.
fn masked(schemas: &[String]) -> Vec<String> {
    schemas
        .iter()
        .map(|schema| {
            if is_temp_namespace(schema) {
                TEMP_PLACEHOLDER.to_string()
            } else {
                schema.clone()
            }
        })
        .collect()
}

/// The relations that match `predicate`, each with the namespace that holds it
/// and its persistence. `pg_class` joined to `pg_namespace` is where both facts
/// are visible at once.
fn relations(predicate: &str) -> String {
    format!(
        "SELECT c.relname, n.nspname, c.relpersistence \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE {predicate} ORDER BY c.relname"
    )
}

/// The case a plausible implementation gets wrong while it gets everything else
/// right.
///
/// With a permanent `public.shad` that holds 1 and a temporary `shad` that
/// holds 99, 18.4 answers `SELECT x FROM shad` with 99 and
/// `SELECT x FROM public.shad` with 1. An unqualified `DROP TABLE shad` takes
/// the TEMPORARY one, and `SELECT x FROM shad` afterwards is 1. The permanent
/// relation was never touched and is visible again.
#[tokio::test]
async fn an_unqualified_drop_takes_the_temporary_relation_and_spares_the_permanent_one() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE public.shad (x int)").await;
    client.run("INSERT INTO public.shad VALUES (1)").await;
    client.run("CREATE TEMP TABLE shad (x int)").await;
    client.run("INSERT INTO shad VALUES (99)").await;

    assert!(client.rows("SELECT x FROM shad").await == one("99"));
    assert!(client.rows("SELECT x FROM public.shad").await == one("1"));

    client.run("DROP TABLE shad").await;

    assert!(client.rows("SELECT x FROM shad").await == one("1"));
    assert!(client.rows("SELECT x FROM public.shad").await == one("1"));
}

/// The temporary namespace is placed implicitly, ahead of the implicit
/// `pg_catalog`.
///
/// Before any temporary relation exists, 18.4 reports `current_schemas(true)`
/// as `{pg_catalog,public}`. After `CREATE TEMP TABLE t (x int)` it is
/// `{pg_temp_<n>,pg_catalog,public}`. `current_schema` stays `public`
/// throughout, because the implicit placement affects lookup and not the
/// creation slot.
#[tokio::test]
async fn the_temporary_namespace_leads_the_implicit_search_path() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    assert!(masked(&client.schemas(true).await) == ["pg_catalog", "public"]);

    client.run("CREATE TEMP TABLE t (x int)").await;

    assert!(masked(&client.schemas(true).await) == [TEMP_PLACEHOLDER, "pg_catalog", "public"]);
    assert!(client.scalar("SELECT current_schema").await == Some("public".to_string()));
}

/// A written `pg_temp` sits where it was written and suppresses the
/// implicit-first placement.
///
/// 18.4 reports `current_schemas(true)` as `{pg_catalog,public,pg_temp_<n>}`
/// after `SET search_path = public, pg_temp`. With `SET search_path = pg_temp`
/// alone, `current_schema` is `pg_temp_<n>`, and a following
/// `CREATE TABLE inpath (x int)` with no `TEMP` keyword lands there with
/// `relpersistence = 't'`.
#[tokio::test]
async fn a_written_pg_temp_sits_where_it_was_written() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TEMP TABLE t (x int)").await;
    let temp = client.temp_namespace().await;

    client.run("SET search_path = public, pg_temp").await;
    assert!(masked(&client.schemas(true).await) == ["pg_catalog", "public", TEMP_PLACEHOLDER]);

    client.run("SET search_path = pg_temp").await;
    assert!(client.scalar("SELECT current_schema").await == Some(temp.clone()));

    client.run("CREATE TABLE inpath (x int)").await;
    assert!(
        client.rows(&relations("c.relname = 'inpath'")).await == vec![row(&["inpath", &temp, "t"])]
    );
}

/// `pg_temp` as a written qualifier creates a TEMPORARY relation whether or not
/// the `TEMP` keyword is there.
///
/// 18.4 puts both `CREATE TABLE pg_temp.viaqual (x int)` and
/// `CREATE TEMP TABLE pg_temp.ok (x int)` in `pg_temp_<n>` with
/// `relpersistence = 't'`, and it accepts the redundant spelling instead of a
/// refusal.
#[tokio::test]
async fn a_pg_temp_qualifier_creates_a_temporary_relation() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE pg_temp.viaqual (x int)").await;
    client.run("CREATE TEMP TABLE pg_temp.ok (x int)").await;
    let temp = client.temp_namespace().await;

    assert!(
        client
            .rows(&relations("c.relname IN ('viaqual', 'ok')"))
            .await
            == vec![row(&["ok", &temp, "t"]), row(&["viaqual", &temp, "t"])]
    );
}

/// The error names the qualifier AS WRITTEN, never the namespace it expands to.
///
/// Once the session has a temporary namespace, 18.4 still reports
/// `SELECT * FROM pg_temp.nothere` as
/// `42P01 relation "pg_temp.nothere" does not exist`, and it does not name
/// `pg_temp_<n>`.
///
/// The complementary case is in `qualified_names.rs`. There the session has no
/// temporary namespace yet, the same `SELECT` is `42P01`, and
/// `DROP TABLE pg_temp.nothere` is `3F000 schema "pg_temp" does not exist`.
#[tokio::test]
async fn a_pg_temp_reference_reports_the_qualifier_as_written() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TEMP TABLE present (x int)").await;

    assert!(
        client.refusal("SELECT * FROM pg_temp.nothere").await
            == refused("42P01", "relation \"pg_temp.nothere\" does not exist")
    );
}

/// A temporary relation cannot be put in a schema that is not a temporary one.
///
/// 18.4 refuses both an ordinary schema and `pg_catalog` with
/// `42P16 cannot create temporary relation in non-temporary schema`.
#[tokio::test]
async fn a_temporary_relation_is_refused_in_a_non_temporary_schema() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;

    for sql in [
        "CREATE TEMP TABLE s1.nope (x int)",
        "CREATE TEMP TABLE pg_catalog.nope (x int)",
    ] {
        assert!(
            client.refusal(sql).await
                == refused(
                    "42P16",
                    "cannot create temporary relation in non-temporary schema"
                ),
            "{sql}"
        );
    }
}

/// A temporary namespace belongs to one session.
///
/// In 18.4, a reference to another session's namespace by its `pg_temp_<n>`
/// spelling is `42P16 cannot create relations in temporary schemas of other
/// sessions`. This holds with and without the `TEMP` keyword. The keyword
/// changes nothing, because the schema decides.
#[tokio::test]
async fn another_sessions_temporary_namespace_refuses_a_creation() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut b = Client::new(&engine);
    a.run("CREATE TEMP TABLE mine (x int)").await;
    let temp_a = a.temp_namespace().await;

    for keyword in ["TABLE", "TEMP TABLE"] {
        let sql = format!("CREATE {keyword} {temp_a}.other (y int)");
        assert!(
            b.refusal(&sql).await
                == refused(
                    "42P16",
                    "cannot create relations in temporary schemas of other sessions"
                ),
            "{sql}"
        );
    }
}

/// Inside an explicit block both `ON COMMIT` dispositions are held until the
/// `COMMIT`.
///
/// 18.4 shows one row in each of `oc_del` and `oc_drop` before the `COMMIT`.
/// Afterwards `oc_del` holds 0 rows and `oc_drop` is gone, which is `42P01`.
#[tokio::test]
async fn on_commit_dispositions_fire_when_the_block_commits() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("BEGIN").await;
    client
        .run("CREATE TEMP TABLE oc_del (x int) ON COMMIT DELETE ROWS")
        .await;
    client
        .run("CREATE TEMP TABLE oc_drop (x int) ON COMMIT DROP")
        .await;
    client.run("INSERT INTO oc_del VALUES (1)").await;
    client.run("INSERT INTO oc_drop VALUES (1)").await;

    assert!(client.scalar("SELECT count(*) FROM oc_del").await == Some("1".to_string()));
    assert!(client.scalar("SELECT count(*) FROM oc_drop").await == Some("1".to_string()));

    client.run("COMMIT").await;

    assert!(client.scalar("SELECT count(*) FROM oc_del").await == Some("0".to_string()));
    assert!(client.fails("SELECT * FROM oc_drop").await.code == "42P01");
}

/// Outside a block every statement is its own transaction, so an `ON COMMIT`
/// disposition fires the moment the statement that armed it ends.
///
/// 18.4 leaves nothing behind for `ON COMMIT DROP`, and the following `SELECT`
/// is `42P01`. An `ON COMMIT DELETE ROWS` table is empty again after the
/// separate `INSERT` commits, so the count is 0.
#[tokio::test]
async fn outside_a_block_an_on_commit_disposition_fires_immediately() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    client
        .run("CREATE TEMP TABLE oc2 (x int) ON COMMIT DROP")
        .await;
    assert!(client.fails("SELECT * FROM oc2").await.code == "42P01");

    client
        .run("CREATE TEMP TABLE oc3 (x int) ON COMMIT DELETE ROWS")
        .await;
    client.run("INSERT INTO oc3 VALUES (1)").await;
    assert!(client.scalar("SELECT count(*) FROM oc3").await == Some("0".to_string()));
}

/// `ON COMMIT DELETE ROWS` on a *partitioned* temp parent empties nothing by
/// itself, and a partition keeps whatever its own disposition asked for.
///
/// 18.4 collects the `DELETE ROWS` relations at commit and hands them to
/// `heap_truncate`, which skips anything without storage — so the partitioned
/// parent is a no-op there and each partition is emptied only if it declared
/// `DELETE ROWS` in its own right.
///
/// This engine reached the same commit-time truncation through a `TRUNCATE`
/// that says `ONLY` to mean "this relation, not its tree". While `ONLY` was
/// ignored on a partitioned parent that walked into every leaf, so a partition
/// that asked to `PRESERVE ROWS` was emptied anyway — the parent's disposition
/// silently overriding the child's.
#[tokio::test]
async fn on_commit_delete_rows_leaves_a_preserve_rows_partition_alone() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("BEGIN").await;
    client
        .run("CREATE TEMP TABLE ocp (a int) PARTITION BY LIST (a) ON COMMIT DELETE ROWS")
        .await;
    client
        .run(
            "CREATE TEMP TABLE ocp_keep PARTITION OF ocp FOR VALUES IN (1) ON COMMIT PRESERVE ROWS",
        )
        .await;
    client
        .run("CREATE TEMP TABLE ocp_wipe PARTITION OF ocp FOR VALUES IN (2) ON COMMIT DELETE ROWS")
        .await;
    client.run("INSERT INTO ocp VALUES (1), (2)").await;
    client.run("COMMIT").await;

    // The partition that asked to keep its rows still has them, and the one
    // that asked to be emptied is empty. The parent reports their sum, which is
    // also the witness that the commit truncated something rather than nothing.
    assert!(client.scalar("SELECT count(*) FROM ocp_keep").await == Some("1".to_string()));
    assert!(client.scalar("SELECT count(*) FROM ocp_wipe").await == Some("0".to_string()));
    assert!(client.scalar("SELECT count(*) FROM ocp").await == Some("1".to_string()));
}

/// A partitioned temp parent whose partitions all say `DELETE ROWS` does end up
/// empty — each partition empties itself, not the parent on their behalf.
#[tokio::test]
async fn on_commit_delete_rows_empties_partitions_that_asked_for_it() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("BEGIN").await;
    client
        .run("CREATE TEMP TABLE ocq (a int) PARTITION BY LIST (a) ON COMMIT DELETE ROWS")
        .await;
    client
        .run("CREATE TEMP TABLE ocq_1 PARTITION OF ocq FOR VALUES IN (1) ON COMMIT DELETE ROWS")
        .await;
    client.run("INSERT INTO ocq VALUES (1)").await;
    assert!(client.scalar("SELECT count(*) FROM ocq").await == Some("1".to_string()));
    client.run("COMMIT").await;
    assert!(client.scalar("SELECT count(*) FROM ocq").await == Some("0".to_string()));
}

/// `ON COMMIT` governs a relation that cannot outlive its session, so 18.4
/// refuses it on a permanent table with
/// `42P16 ON COMMIT can only be used on temporary tables`.
#[tokio::test]
async fn on_commit_is_refused_on_a_permanent_table() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    assert!(
        client
            .refusal("CREATE TABLE permoc (x int) ON COMMIT DELETE ROWS")
            .await
            == refused("42P16", "ON COMMIT can only be used on temporary tables")
    );
}

/// A relation that is gone by the end of the transaction has no rows to empty
/// and nothing to drop, so its `ON COMMIT` disposition has nothing left to do.
///
/// 18.4 accepts every statement in each of these sequences. This includes the
/// `DROP SCHEMA`-shaped `DISCARD TEMP` and the `TRUNCATE` that leaves the
/// relation in place.
///
/// The trailing `SELECT 1` is the half that matters most. The disposition is
/// discharged at the end of *every* transaction. A disposition that cannot be
/// discharged is therefore not one bad `COMMIT`. It is a session that answers
/// every later statement with the same error.
#[tokio::test]
async fn a_relation_that_does_not_survive_its_transaction_discharges_its_disposition() {
    let cases: &[(&str, &[&str])] = &[
        (
            "delete rows, dropped inside the block",
            &[
                "BEGIN",
                "CREATE TEMP TABLE ocd (x int) ON COMMIT DELETE ROWS",
                "INSERT INTO ocd VALUES (1)",
                "DROP TABLE ocd",
                "COMMIT",
            ],
        ),
        (
            "drop, dropped inside the block",
            &[
                "BEGIN",
                "CREATE TEMP TABLE ocp (x int) ON COMMIT DROP",
                "DROP TABLE ocp",
                "COMMIT",
            ],
        ),
        (
            "delete rows, dropped by a later autocommit statement",
            &[
                "CREATE TEMP TABLE oca (f1 int, f2 text) ON COMMIT DELETE ROWS",
                "INSERT INTO oca VALUES (1, 'foo'), (2, 'bar')",
                "DROP TABLE oca",
            ],
        ),
        (
            "delete rows, emptied by DISCARD TEMP inside the block",
            &[
                "BEGIN",
                "CREATE TEMP TABLE ocs (x int) ON COMMIT DELETE ROWS",
                "DISCARD TEMP",
                "COMMIT",
            ],
        ),
        (
            "delete rows, truncated inside the block",
            &[
                "BEGIN",
                "CREATE TEMP TABLE oct (x int) ON COMMIT DELETE ROWS",
                "INSERT INTO oct VALUES (1), (2)",
                "TRUNCATE oct",
                "COMMIT",
            ],
        ),
    ];

    for (label, statements) in cases {
        let engine = SqlEngine::new();
        let mut client = Client::new(&engine);
        let mut outcomes = Vec::new();
        for sql in statements.iter().chain(std::iter::once(&"SELECT 1")) {
            outcomes.push((*sql, client.outcome(sql).await));
        }

        let accepted: Vec<_> = statements
            .iter()
            .chain(std::iter::once(&"SELECT 1"))
            .map(|sql| (*sql, Ok(())))
            .collect();
        assert!(outcomes == accepted, "{label}");
    }
}

/// `ON COMMIT` is keyed to the relation, not to the name it holds.
///
/// A same-transaction `DROP TABLE` frees the name, and a second
/// `CREATE TEMP TABLE` takes it with `ON COMMIT PRESERVE ROWS`. 18.4 then keeps
/// the new table's two rows at the `COMMIT`. The dropped table's `DELETE ROWS`
/// does not follow its name onto the relation that replaced it.
#[tokio::test]
async fn on_commit_follows_the_relation_and_not_the_name_it_held() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("BEGIN").await;
    client
        .run("CREATE TEMP TABLE reuse (x int) ON COMMIT DELETE ROWS")
        .await;
    client.run("INSERT INTO reuse VALUES (1)").await;
    client.run("DROP TABLE reuse").await;
    client
        .run("CREATE TEMP TABLE reuse (x int) ON COMMIT PRESERVE ROWS")
        .await;
    client.run("INSERT INTO reuse VALUES (7), (8)").await;
    client.run("COMMIT").await;

    assert!(client.rows("SELECT x FROM reuse ORDER BY x").await == vec![row(&["7"]), row(&["8"])]);
}

/// `DISCARD` drops every temporary relation but keeps the namespace.
///
/// In 18.4 the following `SELECT` is `42P01`, while `current_schemas(true)`
/// still reports `pg_temp_<n>` first. `DISCARD ALL` does the same, even with
/// its `RESET ALL`.
#[tokio::test]
async fn discard_drops_the_temporary_relations_and_keeps_the_namespace() {
    for discard in ["DISCARD TEMP", "DISCARD ALL"] {
        let engine = SqlEngine::new();
        let mut client = Client::new(&engine);
        client.run("CREATE TEMP TABLE gone (x int)").await;
        client.run("INSERT INTO gone VALUES (1)").await;

        client.run(discard).await;

        assert!(
            client.fails("SELECT * FROM gone").await.code == "42P01",
            "{discard}"
        );
        assert!(
            masked(&client.schemas(true).await) == [TEMP_PLACEHOLDER, "pg_catalog", "public"],
            "{discard}"
        );
    }
}

/// A temporary relation is reachable by name only from the session that made
/// it. 18.4 answers another session's `SELECT * FROM mine` with
/// `42P01 relation "mine" does not exist`.
#[tokio::test]
async fn another_session_cannot_resolve_a_temporary_relation_by_name() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut b = Client::new(&engine);
    a.run("CREATE TEMP TABLE mine (x int)").await;

    assert!(
        b.refusal("SELECT * FROM mine").await
            == refused("42P01", "relation \"mine\" does not exist")
    );
}

/// `pg_class` DOES show another session's temporary relation. This was verified
/// on 18.4, where the row carries the other session's `pg_temp_<a>` and
/// `relpersistence = 't'`. This is deliberate and must not be "fixed" into
/// hiding. Name resolution is what is per-session, and the catalog is not.
#[tokio::test]
async fn pg_class_shows_another_sessions_temporary_relation() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut b = Client::new(&engine);
    a.run("CREATE TEMP TABLE mine (x int)").await;
    let temp_a = a.temp_namespace().await;

    assert!(b.rows(&relations("c.relname = 'mine'")).await == vec![row(&["mine", &temp_a, "t"])]);
}

/// `information_schema` is the other way round from `pg_class`. 18.4 shows a
/// session its own temporary relation in `tables` and `columns`, and it shows
/// another session's relation in neither.
#[tokio::test]
async fn information_schema_hides_another_sessions_temporary_relation() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut b = Client::new(&engine);
    a.run("CREATE TEMP TABLE mine (x int)").await;
    let temp_a = a.temp_namespace().await;

    let tables =
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'mine'";
    let columns = "SELECT table_schema, column_name FROM information_schema.columns WHERE table_name = 'mine'";

    assert!(a.rows(tables).await == vec![row(&[&temp_a, "mine"])]);
    assert!(a.rows(columns).await == vec![row(&[&temp_a, "x"])]);
    assert!(b.rows(tables).await.is_empty());
    assert!(b.rows(columns).await.is_empty());
}

/// The namespaces themselves are public knowledge, and the relations in them
/// are not. 18.4 lists every session's `pg_temp_<n>` in `pg_namespace` and in
/// `information_schema.schemata`, to both sessions.
#[tokio::test]
async fn every_sessions_temporary_namespace_is_listed() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut b = Client::new(&engine);
    a.run("CREATE TEMP TABLE ta (x int)").await;
    b.run("CREATE TEMP TABLE tb (x int)").await;
    let temp_a = a.temp_namespace().await;
    let temp_b = b.temp_namespace().await;
    assert!(temp_a != temp_b);

    let mut expected = vec![row(&[&temp_a]), row(&[&temp_b])];
    expected.sort();

    for sql in [
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'pg_temp%' ORDER BY nspname",
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name LIKE 'pg_temp%' ORDER BY schema_name",
    ] {
        assert!(a.rows(sql).await == expected, "session a: {sql}");
        assert!(b.rows(sql).await == expected, "session b: {sql}");
    }
}

/// Persistence follows the namespace, so everything a `CREATE TEMP TABLE`
/// brings with it is temporary too.
///
/// 18.4 puts `tseq`, the `serial` column's sequence `tseq_id_seq` and the
/// primary key's index `tseq_pkey` all in `pg_temp_<n>` with
/// `relpersistence = 't'`. It puts the permanent counterparts all in `public`
/// with `'p'`.
///
/// This case compares the whole contents of both namespaces, so nothing a
/// `CREATE` brings with it can go unaccounted for.
#[tokio::test]
async fn relpersistence_covers_a_temporary_tables_index_and_sequence() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE TEMP TABLE tseq (id serial PRIMARY KEY, y int)")
        .await;
    client
        .run("CREATE TABLE perm (id serial PRIMARY KEY, y int)")
        .await;
    let temp = client.temp_namespace().await;

    assert!(
        client
            .rows(&relations(&format!("n.nspname IN ('public', '{temp}')")))
            .await
            == vec![
                row(&["perm", "public", "p"]),
                row(&["perm_id_seq", "public", "p"]),
                row(&["perm_pkey", "public", "p"]),
                row(&["tseq", &temp, "t"]),
                row(&["tseq_id_seq", &temp, "t"]),
                row(&["tseq_pkey", &temp, "t"]),
            ]
    );
}

/// A view over a temporary relation cannot outlive it, so 18.4 silently
/// converts the view.
///
/// `CREATE VIEW v AS SELECT * FROM src` over a temporary `src` lands in
/// `pg_temp_<n>` with `relpersistence = 't'`, even though nothing in the
/// statement said `TEMP`.
///
/// 18.4 also emits `NOTICE: view "v" will be a temporary view`. This engine has
/// no notice channel, so the conversion is silent. That is a recorded
/// divergence, and this case does not assert it.
#[tokio::test]
async fn a_view_over_a_temporary_relation_is_converted_to_a_temporary_view() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TEMP TABLE src (x int)").await;
    client.run("CREATE VIEW v AS SELECT * FROM src").await;
    let temp = client.temp_namespace().await;

    assert!(client.rows(&relations("c.relname = 'v'")).await == vec![row(&["v", &temp, "t"])]);
}

/// The conversion cannot happen where the statement pinned the view to an
/// ordinary schema. 18.4 then reports the same refusal a temporary table gets
/// there: `42P16 cannot create temporary relation in non-temporary schema`.
#[tokio::test]
async fn a_qualified_view_over_a_temporary_relation_is_refused() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA vq").await;
    client.run("CREATE TEMP TABLE src (x int)").await;

    assert!(
        client
            .refusal("CREATE VIEW vq.qual AS SELECT * FROM src")
            .await
            == refused(
                "42P16",
                "cannot create temporary relation in non-temporary schema"
            )
    );
}

/// `CREATE TEMP VIEW` puts the view in the session's temporary namespace even
/// when everything it reads is permanent, and the view dies with the session.
///
/// In 18.4, `CREATE TEMP VIEW tv AS SELECT * FROM permsrc` lands `tv` in
/// `pg_temp_<n>` with `relpersistence = 't'`. A written qualifier that names an
/// ordinary schema is
/// `42P16 cannot create temporary relation in non-temporary schema`.
#[tokio::test]
async fn a_temporary_view_lives_in_the_temporary_namespace() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE permsrc (x int)").await;
    client.run("CREATE SCHEMA tvq").await;
    client
        .run("CREATE TEMP VIEW tv AS SELECT * FROM permsrc")
        .await;
    let temp = client.temp_namespace().await;

    assert!(client.rows(&relations("c.relname = 'tv'")).await == vec![row(&["tv", &temp, "t"])]);
    assert!(
        client
            .refusal("CREATE TEMP VIEW tvq.nope AS SELECT * FROM permsrc")
            .await
            == refused(
                "42P16",
                "cannot create temporary relation in non-temporary schema"
            )
    );

    client.run("DISCARD TEMP").await;
    assert!(client.fails("SELECT x FROM tv").await.code == "42P01");
}

/// A foreign key may not cross the temporary boundary in either direction, and
/// 18.4 words the two refusals from the constrained table's side.
///
/// Both spellings of the same constraint report identically. The spellings are
/// inline at `CREATE TABLE` and a later `ALTER TABLE … ADD FOREIGN KEY`. The
/// matching pairs are accepted.
#[tokio::test]
async fn a_foreign_key_may_not_cross_the_temporary_boundary() {
    let cases = [
        ("TABLE", "perm", Ok(())),
        ("TEMP TABLE", "tpar", Ok(())),
        (
            "TABLE",
            "tpar",
            Err(refused(
                "42P16",
                "constraints on permanent tables may reference only permanent tables",
            )),
        ),
        (
            "TEMP TABLE",
            "perm",
            Err(refused(
                "42P16",
                "constraints on temporary tables may reference only temporary tables",
            )),
        ),
    ];

    for (keyword, parent, expected) in cases {
        for inline in [true, false] {
            let case = format!("CREATE {keyword} kid REFERENCES {parent} (inline={inline})");
            let engine = SqlEngine::new();
            let mut client = Client::new(&engine);
            client.run("CREATE TABLE perm (id int PRIMARY KEY)").await;
            client
                .run("CREATE TEMP TABLE tpar (id int PRIMARY KEY)")
                .await;

            let actual = if inline {
                client
                    .outcome(&format!(
                        "CREATE {keyword} kid (a int REFERENCES {parent}(id))"
                    ))
                    .await
            } else {
                client.run(&format!("CREATE {keyword} kid (a int)")).await;
                client
                    .outcome(&format!(
                        "ALTER TABLE kid ADD FOREIGN KEY (a) REFERENCES {parent}(id)"
                    ))
                    .await
            };
            assert!(actual == expected, "{case}");
        }
    }
}

/// A session's temporary relations die with the session.
///
/// `terminate` is what the wire layer calls once the message loop ends, however
/// it ends. After that call the relation is gone from `pg_class`. This case
/// observes that from a second session, which is the only place the row was
/// ever visible from.
#[tokio::test]
async fn terminating_a_session_drops_its_temporary_relations() {
    let engine = SqlEngine::new();
    let mut a = Client::new(&engine);
    let mut observer = Client::new(&engine);
    a.run("CREATE TEMP TABLE mine (x int)").await;
    let temp_a = a.temp_namespace().await;
    assert!(
        observer.rows(&relations("c.relname = 'mine'")).await == vec![row(&["mine", &temp_a, "t"])]
    );

    a.terminate().await;

    assert!(
        observer
            .rows(&relations("c.relname = 'mine'"))
            .await
            .is_empty()
    );
}

/// A backend that never reached `terminate` leaves its relations behind under
/// the name the next session of that backend id will use. That session purges
/// the namespace before it creates anything in it.
///
/// Here the first session is dropped outright, with no `terminate`. Its
/// leftover is still in `pg_class`, and a second session on the same backend id
/// clears it as soon as that session creates a temporary relation of its own.
#[tokio::test]
async fn a_session_inheriting_a_backend_id_purges_the_leftovers() {
    let engine = SqlEngine::new();
    let pid = 987_654;
    let mut observer = Client::new(&engine);

    let mut first = Client::with_pid(&engine, pid);
    first.run("CREATE TEMP TABLE leftover (x int)").await;
    let temp = first.temp_namespace().await;
    drop(first);

    assert!(
        observer.rows(&relations("c.relname = 'leftover'")).await
            == vec![row(&["leftover", &temp, "t"])]
    );

    let mut second = Client::with_pid(&engine, pid);
    second.run("CREATE TEMP TABLE fresh (x int)").await;

    assert!(
        observer
            .rows(&relations("c.relname = 'leftover'"))
            .await
            .is_empty()
    );
    assert!(
        observer.rows(&relations("c.relname = 'fresh'")).await == vec![row(&["fresh", &temp, "t"])]
    );
}

/// A *deparsed* reference to a temporary relation spells the namespace
/// `pg_temp`, never `pg_temp_<backend id>`.
///
/// `PostgreSQL` routes every deparsed reference and every object identity
/// through `get_namespace_name_or_temp`, which substitutes the alias for the
/// reading session's own namespace. Measured on `postgres:18.4` against a
/// temporary `probe_t`:
///
/// ```text
/// pg_get_indexdef(…)          CREATE INDEX probe_i ON pg_temp.probe_t USING btree (b)
/// pg_get_serial_sequence(…)   pg_temp.probe_t_s_seq
/// pg_get_triggerdef(…)        CREATE TRIGGER probe_tg AFTER INSERT ON pg_temp.probe_t …
/// ```
///
/// The backend id is the one part of a resolved name that no two runs have to
/// agree on, so this case never writes one down. Where a spelling *does* keep
/// the number, the case reads the number back from the session.
#[tokio::test]
async fn a_deparsed_reference_spells_the_temporary_namespace_pg_temp() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE TEMP TABLE dpr (id serial PRIMARY KEY, y int)")
        .await;
    client.run("CREATE INDEX dpr_y ON dpr (y)").await;
    client
        .run(
            "CREATE FUNCTION dpr_noop() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEW; END $$",
        )
        .await;
    client
        .run(
            "CREATE TRIGGER dpr_tg AFTER INSERT ON dpr FOR EACH ROW \
             EXECUTE FUNCTION dpr_noop()",
        )
        .await;

    assert!(
        client
            .rows(
                "SELECT pg_get_indexdef('dpr_y'::regclass), \
                        pg_get_serial_sequence('dpr', 'id'), \
                        (SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'dpr_tg')"
            )
            .await
            == vec![row(&[
                "CREATE INDEX dpr_y ON pg_temp.dpr USING btree (y)",
                "pg_temp.dpr_id_seq",
                "CREATE TRIGGER dpr_tg AFTER INSERT ON pg_temp.dpr FOR EACH ROW \
                 EXECUTE FUNCTION dpr_noop()",
            ])]
    );
}

/// A constraint definition qualifies the table it references only when an
/// unqualified reference would not reach it, and the qualifier it then uses is
/// `pg_temp`.
///
/// Reaching that branch takes a shadow: `pg_temp` listed last on the search
/// path and a `public` relation of the same name in front of it. Measured on
/// `postgres:18.4`, which answers
/// `FOREIGN KEY (a) REFERENCES pg_temp.shadow_t(a)`.
#[tokio::test]
async fn a_shadowed_temporary_reference_is_qualified_with_pg_temp() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE TABLE shadow_t (a int PRIMARY KEY)")
        .await;
    client
        .run("CREATE TEMP TABLE shadow_t (a int PRIMARY KEY)")
        .await;
    client
        .run("CREATE TEMP TABLE shadow_child (a int REFERENCES shadow_t (a))")
        .await;
    client.run("SET search_path = public, pg_temp").await;

    assert!(
        client
            .rows(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conname = 'shadow_child_a_fkey'"
            )
            .await
            == one("FOREIGN KEY (a) REFERENCES pg_temp.shadow_t(a)")
    );
}

/// An event trigger's `ddl_commands` row spells a temporary namespace
/// `pg_temp` in both `schema_name` and `object_identity`, and so does the
/// `sql_drop` row — which also reports `is_temporary`.
///
/// Measured on `postgres:18.4`, where `CREATE TEMP TABLE et_t(a int primary
/// key, s serial)` under a `ddl_command_end` trigger reports
/// `schema = pg_temp` beside `identity = pg_temp.et_t` on every row it emits,
/// and `DROP TABLE et_t` reports the same under `sql_drop`:
///
/// ```text
/// orig=t normal=f istemp=t type=table schema=pg_temp identity=pg_temp.it_t
/// orig=t normal=f istemp=f type=table schema=public  identity=public.it_p
/// ```
///
/// `is_temporary` is asserted in both directions on purpose. It cannot be
/// recovered from `schema_name`, because that column carries the display
/// spelling `pg_temp`, which is the alias rather than a namespace name — so a
/// producer that forgot to record it could not be caught by reading the row.
#[tokio::test]
async fn an_event_trigger_names_a_temporary_namespace_pg_temp() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE TABLE et_audit (phase text, nsp text, identity text, istemp bool)")
        .await;
    client
        .run(
            "CREATE FUNCTION et_created() RETURNS event_trigger LANGUAGE plpgsql AS \
             $$ BEGIN \
                INSERT INTO et_audit \
                SELECT 'create', schema_name, object_identity, false \
                FROM pg_event_trigger_ddl_commands(); \
                RETURN NULL; \
              END $$",
        )
        .await;
    client
        .run(
            "CREATE FUNCTION et_dropped() RETURNS event_trigger LANGUAGE plpgsql AS \
             $$ BEGIN \
                INSERT INTO et_audit \
                SELECT 'drop', schema_name, object_identity, is_temporary \
                FROM pg_event_trigger_dropped_objects(); \
                RETURN NULL; \
              END $$",
        )
        .await;
    client
        .run(
            "CREATE EVENT TRIGGER et_created ON ddl_command_end \
             WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION et_created()",
        )
        .await;
    client
        .run(
            "CREATE EVENT TRIGGER et_dropped ON sql_drop \
             WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION et_dropped()",
        )
        .await;

    client
        .run("CREATE TEMP TABLE et_t (a int PRIMARY KEY)")
        .await;
    client.run("CREATE TABLE et_p (a int PRIMARY KEY)").await;
    client.run("DROP TABLE et_t").await;
    client.run("DROP TABLE et_p").await;

    let audited = client
        .rows("SELECT DISTINCT phase, nsp FROM et_audit ORDER BY phase, nsp")
        .await;
    assert!(
        audited
            == vec![
                row(&["create", "pg_temp"]),
                row(&["create", "public"]),
                row(&["drop", "pg_temp"]),
                row(&["drop", "public"]),
            ]
    );
    assert!(
        client
            .rows(
                "SELECT DISTINCT identity, istemp FROM et_audit \
                 WHERE phase = 'drop' AND identity LIKE '%et_%' ORDER BY identity"
            )
            .await
            == vec![row(&["pg_temp.et_t", "t"]), row(&["public.et_p", "f"]),]
    );
}

/// `regclass` and `regproc` keep the backend id. They answer the question "what
/// text reads back as exactly this object", and `pg_temp` reads back as the
/// *reader's* namespace, which is a different question.
///
/// Measured on `postgres:18.4`: a temporary `shadow_t` behind a `public`
/// namesake casts to `pg_temp_51.shadow_t`, and a `pg_temp.probe_trig` casts to
/// `pg_temp_46.probe_trig`. The number is not this case's to know, so it is
/// read back from the session rather than written down.
#[tokio::test]
async fn regclass_keeps_the_backend_id_that_a_deparsed_reference_drops() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE keeper (a int)").await;
    client.run("CREATE TEMP TABLE keeper (a int)").await;
    client.run("SET search_path = public, pg_temp").await;
    let temp = client.temp_namespace().await;

    assert!(
        client
            .rows(
                "SELECT c.oid::regclass::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname = 'keeper' AND n.nspname NOT IN ('public')"
            )
            .await
            == one(&format!("{temp}.keeper"))
    );
}
