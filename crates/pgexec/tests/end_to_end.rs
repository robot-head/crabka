use std::{error::Error, sync::Arc, time::Duration};

use bytes::BytesMut;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{
    NoTls,
    types::{Format, IsNull, ToSql, Type, to_sql_checked},
};

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

#[tokio::test]
async fn views_store_schema_expand_current_rows_and_drop_atomically() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE sales (id int4, amount int4); \
             INSERT INTO sales VALUES (1, 10), (2, 20); \
             CREATE VIEW \"Recent Sales\" AS \
                 SELECT id, amount * 2 AS \"Total\" FROM sales WHERE id > 1",
        )
        .await
        .expect("create view");

    let rows = client
        .query("SELECT \"Total\" FROM \"Recent Sales\" WHERE id = 2", &[])
        .await
        .expect("query view");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 40);

    client
        .batch_execute("INSERT INTO sales VALUES (3, 30)")
        .await
        .expect("insert after create view");
    let rows = client
        .query("SELECT id FROM \"Recent Sales\" ORDER BY id", &[])
        .await
        .expect("view reads current rows");
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    let duplicate = client
        .batch_execute("CREATE VIEW \"Recent Sales\" AS SELECT id FROM sales")
        .await
        .expect_err("duplicate view must fail");
    assert_eq!(sqlstate(&duplicate), "42P07");

    client
        .batch_execute("DROP VIEW \"Recent Sales\"")
        .await
        .expect("drop view");
    let missing = client
        .batch_execute("DROP VIEW \"Recent Sales\"")
        .await
        .expect_err("missing view must fail");
    assert_eq!(sqlstate(&missing), "42P01");
    client
        .batch_execute("DROP VIEW IF EXISTS \"Recent Sales\"")
        .await
        .expect("DROP VIEW IF EXISTS is a no-op for a missing view");
}

#[tokio::test]
async fn views_reject_comma_joins_and_drop_view_if_exists_rejects_tables() {
    let client = connect(spawn().await).await;

    let comma_join = client
        .batch_execute("CREATE VIEW invalid_join AS SELECT * FROM missing_left, missing_right")
        .await
        .expect_err("CREATE VIEW must reject comma joins before resolving relations");
    assert_eq!(sqlstate(&comma_join), "0A000");

    client
        .batch_execute("CREATE TABLE ordinary_table (id int4)")
        .await
        .expect("create ordinary table");
    let wrong_object_type = client
        .batch_execute("DROP VIEW IF EXISTS ordinary_table")
        .await
        .expect_err("DROP VIEW IF EXISTS must not hide a table");
    assert_eq!(sqlstate(&wrong_object_type), "42809");
}

#[tokio::test]
async fn drop_index_removes_a_quoted_nonunique_index_without_affecting_table_constraints() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE inventory (id int4 PRIMARY KEY, name text); \
             INSERT INTO inventory VALUES (1, 'first'); \
             CREATE INDEX \"Inventory By Name\" ON inventory (name); \
             DROP INDEX \"Inventory By Name\"",
        )
        .await
        .expect("drop quoted index");

    let rows = client
        .query("SELECT id, name FROM inventory", &[])
        .await
        .expect("table remains readable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "first");

    let duplicate = client
        .batch_execute("INSERT INTO inventory VALUES (1, 'duplicate')")
        .await
        .expect_err("primary key remains enforced");
    assert_eq!(sqlstate(&duplicate), "23505");
}

#[tokio::test]
async fn drop_index_reports_missing_wrong_type_and_protected_constraint_errors() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE inventory (id int4 PRIMARY KEY, name text); \
             CREATE VIEW inventory_view AS SELECT id FROM inventory; \
             CREATE SEQUENCE inventory_sequence",
        )
        .await
        .expect("create relations");

    let missing = client
        .batch_execute("DROP INDEX missing_index")
        .await
        .expect_err("missing index must fail");
    assert_eq!(sqlstate(&missing), "42704");
    client
        .batch_execute("DROP INDEX IF EXISTS missing_index")
        .await
        .expect("IF EXISTS makes a missing index a no-op");

    for relation in ["inventory", "inventory_view", "inventory_sequence"] {
        let wrong_type = client
            .batch_execute(&format!("DROP INDEX IF EXISTS {relation}"))
            .await
            .expect_err("IF EXISTS must not hide another relation type");
        assert_eq!(sqlstate(&wrong_type), "42809", "{relation}");
    }

    let protected = client
        .batch_execute("DROP INDEX inventory_pkey")
        .await
        .expect_err("primary-key index must remain protected");
    assert_eq!(sqlstate(&protected), "2BP01");

    let unsupported = client
        .batch_execute("DROP INDEX CONCURRENTLY inventory_pkey")
        .await
        .expect_err("unsupported DROP INDEX syntax must fail clearly");
    assert_eq!(sqlstate(&unsupported), "42601");

    client
        .batch_execute("CREATE GLOBAL INDEX inventory_name_global_idx ON inventory (name)")
        .await
        .expect("create global index metadata");
    let global = client
        .batch_execute("DROP INDEX inventory_name_global_idx")
        .await
        .expect_err("global index cleanup is not implemented");
    assert_eq!(sqlstate(&global), "0A000");
}

#[tokio::test]
async fn alter_table_rename_moves_metadata_and_preserves_rows_and_indexes() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE \"Old Orders\" (id int4 PRIMARY KEY, customer text); \
             INSERT INTO \"Old Orders\" VALUES (1, 'Ada'); \
             CREATE INDEX orders_customer_idx ON \"Old Orders\" (customer); \
             ALTER TABLE \"Old Orders\" RENAME TO \"New Orders\"",
        )
        .await
        .expect("rename table");

    let rows = client
        .query("SELECT id, customer FROM \"New Orders\"", &[])
        .await
        .expect("renamed table remains readable");
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Ada");

    client
        .batch_execute("INSERT INTO \"New Orders\" VALUES (2, 'Bea')")
        .await
        .expect("renamed table remains writable through its preserved indexes");
    let duplicate = client
        .batch_execute("INSERT INTO \"New Orders\" VALUES (1, 'Duplicate')")
        .await
        .expect_err("primary-key index remains attached after rename");
    assert_eq!(sqlstate(&duplicate), "23505");

    let missing = client
        .batch_execute("ALTER TABLE missing RENAME TO replacement")
        .await
        .expect_err("missing table must fail");
    assert_eq!(sqlstate(&missing), "42P01");
    let duplicate = client
        .batch_execute("ALTER TABLE \"New Orders\" RENAME TO \"New Orders\"")
        .await
        .expect_err("duplicate target must fail");
    assert_eq!(sqlstate(&duplicate), "42P07");
    client
        .batch_execute("CREATE VIEW order_view AS SELECT id FROM \"New Orders\"")
        .await
        .expect("create view");
    let wrong_type = client
        .batch_execute("ALTER TABLE order_view RENAME TO other")
        .await
        .expect_err("view is not a table");
    assert_eq!(sqlstate(&wrong_type), "42809");
    // A view reads the table under its QUOTED spelling, whose source span the
    // token-level rewrite cannot substitute, so the rename is refused rather
    // than left silently pointing at a name that no longer exists.
    let dependency = client
        .batch_execute("ALTER TABLE \"New Orders\" RENAME TO blocked")
        .await
        .expect_err("a view reference the rewrite cannot prove must block the rename");
    assert_eq!(sqlstate(&dependency), "0A000");

    // Renaming a column no view reads is unaffected, in both spellings, and the
    // view keeps returning its rows.
    client
        .batch_execute("ALTER TABLE \"New Orders\" RENAME COLUMN customer TO buyer")
        .await
        .expect("column rename");
    client
        .batch_execute("ALTER TABLE \"New Orders\" RENAME buyer TO client_name")
        .await
        .expect("optional COLUMN spelling");
    let view_rows = client
        .query("SELECT id FROM order_view ORDER BY id", &[])
        .await
        .expect("the view still reads");
    assert_eq!(view_rows.len(), 2);
    let renamed = client
        .query("SELECT client_name FROM \"New Orders\" ORDER BY id", &[])
        .await
        .expect("the renamed column is readable");
    assert_eq!(renamed[0].get::<_, String>(0), "Ada");
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

fn sqlstate(err: &tokio_postgres::Error) -> &str {
    err.as_db_error().expect("db error").code().code()
}

#[derive(Debug)]
struct BadInt4Binary;

impl ToSql for BadInt4Binary {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::INT4 {
            return Err("BadInt4Binary only supports int4".into());
        }
        out.extend_from_slice(&[0, 1]);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT4
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct TextScalarParam {
    value: &'static str,
    ty: Type,
}

impl ToSql for TextScalarParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty != &self.ty {
            return Err("text scalar parameter received an unexpected type".into());
        }
        out.extend_from_slice(self.value.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2 || *ty == Type::FLOAT4
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct RawTextScalarParam {
    bytes: &'static [u8],
    ty: Type,
}

impl ToSql for RawTextScalarParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty != &self.ty {
            return Err("raw text scalar parameter received an unexpected type".into());
        }
        out.extend_from_slice(self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2 || *ty == Type::FLOAT4
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct MalformedBinaryScalar {
    ty: Type,
    bytes: &'static [u8],
}

impl ToSql for MalformedBinaryScalar {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty != &self.ty {
            return Err("malformed binary scalar received an unexpected type".into());
        }
        out.extend_from_slice(self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2 || *ty == Type::FLOAT4
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct ByteaTextParam(&'static [u8]);

impl ToSql for ByteaTextParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::BYTEA {
            return Err("ByteaTextParam only supports bytea".into());
        }
        out.extend_from_slice(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct ByteaBinaryParam(&'static [u8]);

impl ToSql for ByteaBinaryParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::BYTEA {
            return Err("ByteaBinaryParam only supports bytea".into());
        }
        out.extend_from_slice(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Binary
    }

    to_sql_checked!();
}

enum ByteaParameter {
    Binary(&'static [u8]),
    Null,
}

#[derive(Debug)]
struct NumericBinaryParam(&'static [u8]);

impl ToSql for NumericBinaryParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::NUMERIC {
            return Err("NumericBinaryParam only supports numeric".into());
        }
        out.extend_from_slice(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct DateBinaryParam([u8; 4]);

impl ToSql for DateBinaryParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::DATE {
            return Err("DateBinaryParam only supports date".into());
        }
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE
    }

    to_sql_checked!();
}

#[tokio::test]
async fn create_insert_select_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("insert");
    // Extended protocol with binary results (exercises describe + binary cells).
    let rows = client
        .query(
            "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5",
            &[],
        )
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let first: &str = rows[0].get(0);
    let second: &str = rows[1].get(0);
    assert_eq!((first, second), ("c", "b"));
}

#[tokio::test]
async fn unique_local_index_rejects_duplicate_insert() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create table");
    client
        .batch_execute("CREATE UNIQUE INDEX t_name_idx ON t (name)")
        .await
        .expect("create unique index");
    client
        .batch_execute("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("first insert");

    let err = client
        .batch_execute("INSERT INTO t VALUES (2, 'a')")
        .await
        .expect_err("duplicate unique key");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn create_table_column_unique_rejects_duplicate_insert() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text UNIQUE)")
        .await
        .expect("create table with column unique");
    client
        .batch_execute("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("first insert");

    let err = client
        .batch_execute("INSERT INTO t VALUES (2, 'a')")
        .await
        .expect_err("duplicate unique key");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn create_table_table_unique_rejects_duplicate_tuple() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (a int4, b int4, UNIQUE (a, b))")
        .await
        .expect("create table with table unique");
    client
        .batch_execute("INSERT INTO t VALUES (1, 1), (1, 2)")
        .await
        .expect("distinct tuples insert");

    let err = client
        .batch_execute("INSERT INTO t VALUES (1, 1)")
        .await
        .expect_err("duplicate unique tuple");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn create_table_primary_key_rejects_duplicate_and_null() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4 PRIMARY KEY, name text)")
        .await
        .expect("create table with primary key");
    client
        .batch_execute("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("first insert");

    let duplicate = client
        .batch_execute("INSERT INTO t VALUES (1, 'b')")
        .await
        .expect_err("duplicate primary key");
    assert_eq!(
        duplicate.as_db_error().expect("db error").code().code(),
        "23505"
    );

    let null = client
        .batch_execute("INSERT INTO t VALUES (NULL, 'missing')")
        .await
        .expect_err("null primary key");
    assert_eq!(null.as_db_error().expect("db error").code().code(), "23502");
}

#[tokio::test]
async fn create_table_unique_constraint_works_inside_explicit_transaction() {
    let client = connect(spawn().await).await;

    client
        .batch_execute(
            "BEGIN; \
             CREATE TABLE t (id int4, name text UNIQUE); \
             INSERT INTO t VALUES (1, 'a'); \
             COMMIT;",
        )
        .await
        .expect("create unique table and insert in transaction");

    let err = client
        .batch_execute("INSERT INTO t VALUES (2, 'a')")
        .await
        .expect_err("duplicate unique key after commit");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn unique_local_index_update_self_noop_and_conflict() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, name text);
             CREATE UNIQUE INDEX t_name_idx ON t (name);
             INSERT INTO t VALUES (1, 'a'), (2, 'b');",
        )
        .await
        .expect("seed unique table");

    client
        .batch_execute("UPDATE t SET name = 'a' WHERE id = 1")
        .await
        .expect("self no-op update");
    let err = client
        .batch_execute("UPDATE t SET name = 'b' WHERE id = 1")
        .await
        .expect_err("update conflicts with another row");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn unique_local_index_delete_then_reinsert_and_nulls() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, name text);
             CREATE UNIQUE INDEX t_name_idx ON t (name);
             INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, NULL);
             DELETE FROM t WHERE id = 1;
             INSERT INTO t VALUES (4, 'a');",
        )
        .await
        .expect("deleted key can be reused and nulls are distinct");

    let rows = client
        .query("SELECT count(*)::int4 FROM t", &[])
        .await
        .expect("count rows");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 3);
}

#[tokio::test]
async fn unique_local_index_creation_rejects_existing_duplicates() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1, 'a'), (2, 'a')",
        )
        .await
        .expect("seed duplicates");

    let err = client
        .batch_execute("CREATE UNIQUE INDEX t_name_idx ON t (name)")
        .await
        .expect_err("duplicate existing keys");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");
}

#[tokio::test]
async fn unique_local_index_serializes_concurrent_insert_conflicts() {
    let port = spawn().await;
    let client = connect(port).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, name text); \
             CREATE UNIQUE INDEX t_name_idx ON t (name)",
        )
        .await
        .expect("create unique table");

    let first = connect(port).await;
    let second = connect(port).await;
    let first_inserted = Arc::new(tokio::sync::Notify::new());
    let commit_first = Arc::new(tokio::sync::Notify::new());

    let first_task = {
        let first_inserted = Arc::clone(&first_inserted);
        let commit_first = Arc::clone(&commit_first);
        tokio::spawn(async move {
            first.batch_execute("BEGIN").await.expect("begin first");
            first
                .batch_execute("INSERT INTO t VALUES (1, 'dupe')")
                .await
                .expect("first insert");
            first_inserted.notify_one();
            commit_first.notified().await;
            first.batch_execute("COMMIT").await.expect("commit first");
        })
    };

    first_inserted.notified().await;
    let second_task = tokio::spawn(async move {
        second
            .batch_execute("INSERT INTO t VALUES (2, 'dupe')")
            .await
            .expect_err("second insert must lose unique race")
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    commit_first.notify_one();

    first_task.await.expect("first task joins");
    let err = second_task.await.expect("second task joins");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");

    let rows = client
        .query("SELECT count(*)::int4 FROM t WHERE name = 'dupe'", &[])
        .await
        .expect("count duplicate key rows");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn unique_local_index_serializes_concurrent_update_conflicts() {
    let port = spawn().await;
    let client = connect(port).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, name text); \
             CREATE UNIQUE INDEX t_name_idx ON t (name); \
             INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        )
        .await
        .expect("create unique table");

    let first = connect(port).await;
    let second = connect(port).await;
    let first_updated = Arc::new(tokio::sync::Notify::new());
    let commit_first = Arc::new(tokio::sync::Notify::new());

    let first_task = {
        let first_updated = Arc::clone(&first_updated);
        let commit_first = Arc::clone(&commit_first);
        tokio::spawn(async move {
            first.batch_execute("BEGIN").await.expect("begin first");
            first
                .batch_execute("UPDATE t SET name = 'dupe' WHERE id = 1")
                .await
                .expect("first update");
            first_updated.notify_one();
            commit_first.notified().await;
            first.batch_execute("COMMIT").await.expect("commit first");
        })
    };

    first_updated.notified().await;
    let second_task = tokio::spawn(async move {
        second
            .batch_execute("UPDATE t SET name = 'dupe' WHERE id = 2")
            .await
            .expect_err("second update must lose unique race")
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    commit_first.notify_one();

    first_task.await.expect("first task joins");
    let err = second_task.await.expect("second task joins");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");

    let rows = client
        .query("SELECT count(*)::int4 FROM t WHERE name = 'dupe'", &[])
        .await
        .expect("count duplicate key rows");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn unique_local_index_repeatable_read_insert_checks_current_committed_rows() {
    let port = spawn().await;
    let setup = connect(port).await;
    setup
        .batch_execute(
            "CREATE TABLE t (id int4, name text); \
             CREATE UNIQUE INDEX t_name_idx ON t (name)",
        )
        .await
        .expect("create unique table");

    let stale_reader = connect(port).await;
    stale_reader
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin repeatable read transaction");

    let writer = connect(port).await;
    writer
        .batch_execute("INSERT INTO t VALUES (1, 'dupe')")
        .await
        .expect("commit duplicate key after begin");

    let rows = stale_reader
        .query("SELECT count(*)::int4 FROM t WHERE name = 'dupe'", &[])
        .await
        .expect("repeatable read keeps stale read snapshot");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 0);

    let err = stale_reader
        .batch_execute("INSERT INTO t VALUES (2, 'dupe')")
        .await
        .expect_err("unique check must see current committed duplicate");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");

    stale_reader
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback failed transaction");
}

#[tokio::test]
async fn unique_local_index_repeatable_read_update_checks_current_committed_rows() {
    let port = spawn().await;
    let setup = connect(port).await;
    setup
        .batch_execute(
            "CREATE TABLE t (id int4, name text); \
             CREATE UNIQUE INDEX t_name_idx ON t (name); \
             INSERT INTO t VALUES (1, 'original')",
        )
        .await
        .expect("create unique table");

    let stale_reader = connect(port).await;
    stale_reader
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin repeatable read transaction");

    let writer = connect(port).await;
    writer
        .batch_execute("INSERT INTO t VALUES (2, 'dupe')")
        .await
        .expect("commit duplicate key after begin");

    let rows = stale_reader
        .query("SELECT count(*)::int4 FROM t WHERE name = 'dupe'", &[])
        .await
        .expect("repeatable read keeps stale read snapshot");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 0);

    let err = stale_reader
        .batch_execute("UPDATE t SET name = 'dupe' WHERE id = 1")
        .await
        .expect_err("unique check must see current committed duplicate");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");

    stale_reader
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback failed transaction");
}

#[tokio::test]
async fn unique_local_index_backfill_serializes_with_concurrent_insert() {
    let port = spawn().await;
    let client = connect(port).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1, 'dupe')")
        .await
        .expect("seed table");

    let writer = connect(port).await;
    let ddl = connect(port).await;
    let writer_inserted = Arc::new(tokio::sync::Notify::new());
    let commit_writer = Arc::new(tokio::sync::Notify::new());

    let writer_task = {
        let writer_inserted = Arc::clone(&writer_inserted);
        let commit_writer = Arc::clone(&commit_writer);
        tokio::spawn(async move {
            writer.batch_execute("BEGIN").await.expect("begin writer");
            writer
                .batch_execute("INSERT INTO t VALUES (2, 'dupe')")
                .await
                .expect("insert duplicate before index exists");
            writer_inserted.notify_one();
            commit_writer.notified().await;
            writer.batch_execute("COMMIT").await.expect("commit writer");
        })
    };

    writer_inserted.notified().await;
    let ddl_task = tokio::spawn(async move {
        ddl.batch_execute("CREATE UNIQUE INDEX t_name_idx ON t (name)")
            .await
            .expect_err("backfill must see committed concurrent duplicate")
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    commit_writer.notify_one();

    writer_task.await.expect("writer task joins");
    let err = ddl_task.await.expect("ddl task joins");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "23505");

    let rows = client
        .query("SELECT count(*)::int4 FROM t WHERE name = 'dupe'", &[])
        .await
        .expect("count duplicate key rows");
    let count: i32 = rows[0].get(0);
    assert_eq!(count, 2);
}

#[tokio::test]
async fn unique_global_index_fails_clear() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create table");

    let err = client
        .batch_execute("CREATE UNIQUE GLOBAL INDEX t_name_idx ON t (name)")
        .await
        .expect_err("unique global index is unsupported");

    assert_eq!(err.as_db_error().expect("db error").code().code(), "0A000");
}

#[tokio::test]
async fn column_defaults_and_not_null_are_enforced_on_insert_and_update() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4 NOT NULL, name text DEFAULT 'anon')")
        .await
        .expect("create table with constraints");

    client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect("omitted column uses default");
    client
        .batch_execute("INSERT INTO t VALUES (2, DEFAULT)")
        .await
        .expect("explicit DEFAULT uses default");

    let rows = client
        .query("SELECT id, name FROM t ORDER BY id", &[])
        .await
        .expect("select defaults");
    let first_id: i32 = rows[0].get(0);
    let first_name: &str = rows[0].get(1);
    let second_id: i32 = rows[1].get(0);
    let second_name: &str = rows[1].get(1);
    assert_eq!(
        (first_id, first_name, second_id, second_name),
        (1, "anon", 2, "anon")
    );

    let insert_err = client
        .batch_execute("INSERT INTO t (name) VALUES ('missing id')")
        .await
        .expect_err("omitted not-null column fails");
    assert_eq!(
        insert_err.as_db_error().expect("db error").code().code(),
        "23502"
    );

    let update_err = client
        .batch_execute("UPDATE t SET id = NULL WHERE id = 1")
        .await
        .expect_err("not-null update fails");
    assert_eq!(
        update_err.as_db_error().expect("db error").code().code(),
        "23502"
    );
}

#[tokio::test]
async fn unsupported_create_table_constraints_fail_loudly() {
    let client = connect(spawn().await).await;

    for sql in [
        "CREATE TABLE fk_t (id int4 REFERENCES other_t (id))",
        "CREATE TABLE fk_u (id int4, FOREIGN KEY (id) REFERENCES other_t (id))",
    ] {
        let err = client
            .batch_execute(sql)
            .await
            .expect_err("unsupported constraint");
        assert_eq!(
            err.as_db_error().expect("db error").code().code(),
            "0A000",
            "{sql}"
        );
    }
}

#[tokio::test]
async fn select_expression_typed_int4() {
    let client = connect(spawn().await).await;
    let rows = client
        .query("SELECT 2 + 3 AS five", &[])
        .await
        .expect("select");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 5);
}

#[tokio::test]
async fn undefined_table_errors_but_session_survives() {
    let client = connect(spawn().await).await;
    let err = client
        .batch_execute("SELECT * FROM nope")
        .await
        .expect_err("no table");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P01");
    // Session still usable.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn wire_transaction_commit_and_rollback() {
    let mut client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");

    // Rollback path: tokio-postgres transaction dropped without commit.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (1,'a')")
            .await
            .expect("insert");
        // drop without commit → ROLLBACK sent over the wire
    }
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 0, "rolled-back insert must be gone");

    // Commit path.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (2,'b')")
            .await
            .expect("insert");
        tx.commit().await.expect("commit");
    }
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 2);
}

#[tokio::test]
async fn wire_update_delete_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("insert");

    let updated = client
        .execute("UPDATE t SET name = 'z' WHERE id > 1", &[])
        .await
        .expect("update");
    assert_eq!(updated, 2, "UPDATE must report 2 affected rows");

    let deleted = client
        .execute("DELETE FROM t WHERE id = 1", &[])
        .await
        .expect("delete");
    assert_eq!(deleted, 1, "DELETE must report 1 affected row");

    let rows = client
        .query("SELECT id, name FROM t ORDER BY id", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get::<_, &str>(1)).collect();
    assert_eq!(names, vec!["z", "z"]);
}

#[tokio::test]
async fn parameterized_extended_queries_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");

    let scalar_select = client
        .prepare_typed("SELECT $1", &[Type::INT4])
        .await
        .expect("prepare scalar select");
    assert_eq!(scalar_select.columns()[0].type_(), &Type::INT4);
    let scalar_rows = client
        .query(&scalar_select, &[&5_i32])
        .await
        .expect("scalar parameterized select");
    let scalar: i32 = scalar_rows[0].get(0);
    assert_eq!(scalar, 5);

    let insert = client
        .prepare_typed(
            "INSERT INTO t (id, name) VALUES ($1, $2)",
            &[Type::INT4, Type::TEXT],
        )
        .await
        .expect("prepare insert");
    let inserted = client
        .execute(&insert, &[&1_i32, &"one"])
        .await
        .expect("insert first row");
    assert_eq!(inserted, 1);
    client
        .execute(&insert, &[&2_i32, &"two"])
        .await
        .expect("insert second row");

    let select = client
        .prepare_typed("SELECT name FROM t WHERE id = $1", &[Type::INT4])
        .await
        .expect("prepare select");
    let rows = client
        .query(&select, &[&2_i32])
        .await
        .expect("select by parameter");
    let name: &str = rows[0].get(0);
    assert_eq!(name, "two");

    let update = client
        .prepare_typed(
            "UPDATE t SET name = $1 WHERE id = $2",
            &[Type::TEXT, Type::INT4],
        )
        .await
        .expect("prepare update");
    let updated = client
        .execute(&update, &[&"zwei", &2_i32])
        .await
        .expect("update by parameter");
    assert_eq!(updated, 1);

    let delete = client
        .prepare_typed("DELETE FROM t WHERE name = $1", &[Type::TEXT])
        .await
        .expect("prepare delete");
    let deleted = client
        .execute(&delete, &[&"one"])
        .await
        .expect("delete by parameter");
    assert_eq!(deleted, 1);

    let remaining = client
        .query("SELECT id, name FROM t ORDER BY id", &[])
        .await
        .expect("select remaining row");
    assert_eq!(remaining.len(), 1);
    let id: i32 = remaining[0].get(0);
    let remaining_name: &str = remaining[0].get(1);
    assert_eq!((id, remaining_name), (2, "zwei"));
}

#[tokio::test]
async fn extended_bind_decode_error_aborts_explicit_transaction() {
    let client = connect(spawn().await).await;
    let statement = client
        .prepare_typed("SELECT $1", &[Type::INT4])
        .await
        .expect("prepare int4 parameter");

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .query(&statement, &[&BadInt4Binary])
        .await
        .expect_err("bad binary int4 parameter must fail");
    assert_eq!(sqlstate(&err), "22P03");

    let err = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("failed transaction rejects following statements");
    assert_eq!(sqlstate(&err), "25P02");

    client.batch_execute("ROLLBACK").await.expect("rollback");
    let rows = client
        .query("SELECT 1::int4", &[])
        .await
        .expect("rollback clears failed transaction");
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}

#[tokio::test]
async fn extended_bind_int2_and_float4_text_binary_and_null_parameters_roundtrip() {
    let client = connect(spawn().await).await;

    let int2 = client
        .prepare_typed("SELECT $1::int4", &[Type::INT2])
        .await
        .expect("prepare int2 parameter");
    for parameter in [TextScalarParam {
        value: "-123",
        ty: Type::INT2,
    }] {
        let rows = client
            .query(&int2, &[&parameter])
            .await
            .expect("bind text int2 parameter");
        assert_eq!(rows[0].get::<_, i32>(0), -123);
    }
    let rows = client
        .query(&int2, &[&321_i16])
        .await
        .expect("bind binary int2 parameter");
    assert_eq!(rows[0].get::<_, i32>(0), 321);
    let rows = client
        .query(&int2, &[&None::<i16>])
        .await
        .expect("bind null int2 parameter");
    assert_eq!(rows[0].get::<_, Option<i32>>(0), None);

    let float4 = client
        .prepare_typed("SELECT $1::float8", &[Type::FLOAT4])
        .await
        .expect("prepare float4 parameter");
    let rows = client
        .query(
            &float4,
            &[&TextScalarParam {
                value: "1.25",
                ty: Type::FLOAT4,
            }],
        )
        .await
        .expect("bind text float4 parameter");
    assert_eq!(rows[0].get::<_, f64>(0).to_bits(), 1.25_f64.to_bits());
    let rows = client
        .query(&float4, &[&-3.5_f32])
        .await
        .expect("bind binary float4 parameter");
    assert_eq!(rows[0].get::<_, f64>(0).to_bits(), (-3.5_f64).to_bits());
    let rows = client
        .query(&float4, &[&f32::NAN])
        .await
        .expect("bind binary float4 NaN parameter");
    assert!(rows[0].get::<_, f64>(0).is_nan());
    let rows = client
        .query(&float4, &[&f32::INFINITY])
        .await
        .expect("bind binary float4 infinity parameter");
    assert!(rows[0].get::<_, f64>(0).is_infinite());
    let rows = client
        .query(&float4, &[&None::<f32>])
        .await
        .expect("bind null float4 parameter");
    assert_eq!(rows[0].get::<_, Option<f64>>(0), None);
}

#[tokio::test]
async fn extended_bind_int2_and_float4_malformed_binary_parameters_fail() {
    let client = connect(spawn().await).await;
    let int2 = client
        .prepare_typed("SELECT $1", &[Type::INT2])
        .await
        .expect("prepare int2 parameter");
    let float4 = client
        .prepare_typed("SELECT $1", &[Type::FLOAT4])
        .await
        .expect("prepare float4 parameter");

    for (statement, parameter) in [
        (
            &int2,
            MalformedBinaryScalar {
                ty: Type::INT2,
                bytes: &[0],
            },
        ),
        (
            &float4,
            MalformedBinaryScalar {
                ty: Type::FLOAT4,
                bytes: &[0, 0, 0],
            },
        ),
    ] {
        let err = client
            .query(statement, &[&parameter])
            .await
            .expect_err("malformed binary scalar parameter must fail");
        assert_eq!(sqlstate(&err), "22P03");
    }

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .query(
            &float4,
            &[&MalformedBinaryScalar {
                ty: Type::FLOAT4,
                bytes: &[0, 0, 0],
            }],
        )
        .await
        .expect_err("malformed float4 parameter aborts transaction");
    assert_eq!(sqlstate(&err), "22P03");
    let err = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("failed transaction rejects following statements");
    assert_eq!(sqlstate(&err), "25P02");
    client.batch_execute("ROLLBACK").await.expect("rollback");
}

#[tokio::test]
async fn extended_bind_int2_and_float4_text_decode_errors_have_postgres_sqlstates() {
    let client = connect(spawn().await).await;
    let int2 = client
        .prepare_typed("SELECT $1", &[Type::INT2])
        .await
        .expect("prepare int2 parameter");
    let float4 = client
        .prepare_typed("SELECT $1", &[Type::FLOAT4])
        .await
        .expect("prepare float4 parameter");

    let err = client
        .query(
            &int2,
            &[&TextScalarParam {
                value: "not-a-smallint",
                ty: Type::INT2,
            }],
        )
        .await
        .expect_err("invalid int2 text parameter must fail");
    assert_eq!(sqlstate(&err), "22P02");

    let err = client
        .query(
            &float4,
            &[&TextScalarParam {
                value: "not-a-real",
                ty: Type::FLOAT4,
            }],
        )
        .await
        .expect_err("invalid float4 text parameter must fail");
    assert_eq!(sqlstate(&err), "22P02");

    let err = client
        .query(
            &int2,
            &[&TextScalarParam {
                value: "32768",
                ty: Type::INT2,
            }],
        )
        .await
        .expect_err("overflowing int2 text parameter must fail");
    assert_eq!(sqlstate(&err), "22003");

    let err = client
        .query(
            &float4,
            &[&RawTextScalarParam {
                bytes: &[0xff],
                ty: Type::FLOAT4,
            }],
        )
        .await
        .expect_err("invalid UTF-8 text parameter must fail");
    assert_eq!(sqlstate(&err), "22021");
}

#[tokio::test]
async fn extended_bind_bytea_text_requires_valid_legacy_escapes() {
    let client = connect(spawn().await).await;
    let statement = client
        .prepare_typed("SELECT $1", &[Type::BYTEA])
        .await
        .expect("prepare bytea parameter");

    for (encoded, expected) in [
        (br"\xdeadbeef".as_slice(), vec![0xde, 0xad, 0xbe, 0xef]),
        (br"\\".as_slice(), vec![b'\\']),
        (br"\001\377".as_slice(), vec![1, 255]),
        (
            br"prefix\\\101suffix".as_slice(),
            b"prefix\\Asuffix".to_vec(),
        ),
    ] {
        let rows = client
            .query(&statement, &[&ByteaTextParam(encoded)])
            .await
            .expect("valid legacy bytea bind");
        assert_eq!(rows[0].get::<_, Vec<u8>>(0), expected, "{encoded:?}");
    }

    for encoded in [
        br"\x0".as_slice(),
        br"\x0g",
        br"\a",
        br"\12x",
        br"\1",
        br"\12",
        b"\\",
    ] {
        let err = client
            .query(&statement, &[&ByteaTextParam(encoded)])
            .await
            .expect_err("malformed legacy bytea escape must fail");
        assert_eq!(sqlstate(&err), "22P02", "{encoded:?}");
    }

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .query(&statement, &[&ByteaTextParam(br"\a")])
        .await
        .expect_err("malformed bytea bind aborts transaction");
    assert_eq!(sqlstate(&err), "22P02");
    let err = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("failed transaction rejects following statements");
    assert_eq!(sqlstate(&err), "25P02");
    client.batch_execute("ROLLBACK").await.expect("rollback");
}

#[tokio::test]
async fn extended_bind_bytea_binary_and_null_parameters_roundtrip() {
    let client = connect(spawn().await).await;
    let statement = client
        .prepare_typed("SELECT $1", &[Type::BYTEA])
        .await
        .expect("prepare bytea parameter");

    for (description, parameter, expected) in [
        (
            "binary bytea preserves all bytes",
            ByteaParameter::Binary(&[0, 255]),
            Some(vec![0, 255]),
        ),
        ("NULL bytea remains SQL NULL", ByteaParameter::Null, None),
    ] {
        let rows = match parameter {
            ByteaParameter::Binary(bytes) => {
                client.query(&statement, &[&ByteaBinaryParam(bytes)]).await
            }
            ByteaParameter::Null => client.query(&statement, &[&None::<Vec<u8>>]).await,
        }
        .expect(description);

        assert_eq!(
            rows[0].get::<_, Option<Vec<u8>>>(0),
            expected,
            "{description}"
        );
    }
}

#[tokio::test]
async fn extended_bind_supports_additional_text_and_binary_parameter_types() {
    let client = connect(spawn().await).await;

    for (ty, value) in [(Type::VARCHAR, "valid"), (Type::BPCHAR, "valid")] {
        let statement = client
            .prepare_typed("SELECT $1::text", &[ty])
            .await
            .expect("prepare text-like parameter");
        let rows = client
            .query(&statement, &[&value])
            .await
            .expect("bind text-like parameter");
        assert_eq!(rows[0].get::<_, &str>(0), value);
    }

    // An EXPLICIT cast to a length-constrained string type truncates rather than
    // erroring, whether the value arrives as a literal or as a bound parameter —
    // `EXECUTE p('abcd')` over `$1::varchar(3)` is `abc` on PostgreSQL too. The
    // assignment direction (below) is the one that raises 22001.
    for (ty, sql) in [
        (Type::VARCHAR, "SELECT $1::varchar(3)::text"),
        (Type::BPCHAR, "SELECT $1::character(3)::text"),
    ] {
        let statement = client
            .prepare_typed(sql, &[ty])
            .await
            .expect("prepare length-constrained text-like parameter");
        for value in ["abc", "abcd"] {
            let rows = client
                .query(&statement, &[&value])
                .await
                .expect("bind text-like parameter");
            assert_eq!(rows[0].get::<_, &str>(0), "abc", "{sql} with {value}");
        }
    }

    // Storing an over-long value through a bound parameter is an assignment, so
    // it is 22001 — the same split the scalar string types make everywhere.
    client
        .simple_query("CREATE TABLE bound_typmod (v varchar(3), c character(3))")
        .await
        .expect("create length-constrained table");
    for (column, ty) in [("v", Type::VARCHAR), ("c", Type::BPCHAR)] {
        let sql = format!("INSERT INTO bound_typmod ({column}) VALUES ($1)");
        let statement = client
            .prepare_typed(&sql, &[ty])
            .await
            .expect("prepare length-constrained insert");
        client
            .execute(&statement, &[&"abc"])
            .await
            .expect("value within typmod stores");
        let err = client
            .execute(&statement, &[&"abcd"])
            .await
            .expect_err("assigning over the typmod must fail");
        assert_eq!(sqlstate(&err), "22001", "{sql}");
    }

    let numeric = client
        .prepare_typed("SELECT $1::numeric::text", &[Type::NUMERIC])
        .await
        .expect("prepare numeric parameter");
    let rows = client
        .query(
            &numeric,
            &[&NumericBinaryParam(&[
                0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 13, 72,
            ])],
        )
        .await
        .expect("bind binary numeric parameter");
    assert_eq!(rows[0].get::<_, &str>(0), "12.34");

    let date = client
        .prepare_typed("SELECT $1::date::text", &[Type::DATE])
        .await
        .expect("prepare date parameter");
    let rows = client
        .query(&date, &[&DateBinaryParam(8_767_i32.to_be_bytes())])
        .await
        .expect("bind binary date parameter");
    assert_eq!(rows[0].get::<_, &str>(0), "2024-01-02");
}

#[tokio::test]
async fn parameter_edge_cases_report_expected_sqlstates() {
    let client = connect(spawn().await).await;

    let err = client
        .batch_execute("SELECT $1")
        .await
        .expect_err("simple protocol parameters are rejected");
    assert_eq!(sqlstate(&err), "0A000");

    // `regconfig` (OID 3734) stands in for "an OID the engine does not support"
    // — `json`/`jsonb` used to play that role and are now supported.
    let err = client
        .prepare_typed("SELECT $1", &[Type::REGCONFIG])
        .await
        .expect_err("unsupported parameter OID is rejected");
    assert_eq!(sqlstate(&err), "42P18");
}

#[tokio::test]
async fn extended_prepare_type_error_aborts_explicit_transaction() {
    let client = connect(spawn().await).await;

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .prepare_typed("SELECT $1", &[Type::REGCONFIG])
        .await
        .expect_err("unsupported prepare parameter OID must fail");
    assert_eq!(sqlstate(&err), "42P18");

    let err = client
        .prepare("SELECT 1")
        .await
        .expect_err("failed transaction rejects extended prepare");
    assert_eq!(sqlstate(&err), "25P02");

    let err = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("failed transaction rejects following statements");
    assert_eq!(sqlstate(&err), "25P02");

    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .prepare_typed("SELECT $1", &[Type::INT4])
        .await
        .expect("rollback clears failed transaction");
}

#[tokio::test]
async fn null_and_bool_parameters_roundtrip() {
    let client = connect(spawn().await).await;
    let null_statement = client
        .prepare_typed("SELECT $1::int4 IS NULL", &[Type::INT4])
        .await
        .expect("prepare nullable int4");
    let rows = client
        .query(&null_statement, &[&None::<i32>])
        .await
        .expect("execute nullable int4");
    assert!(rows[0].get::<_, bool>(0));

    let bool_statement = client
        .prepare_typed("SELECT NOT $1", &[Type::BOOL])
        .await
        .expect("prepare bool parameter");
    let rows = client
        .query(&bool_statement, &[&true])
        .await
        .expect("execute bool parameter");
    assert!(!rows[0].get::<_, bool>(0));
}

/// Wire-protocol version of the blocking UPDATE test.
///
/// conn1 opens a transaction and locks a row via UPDATE; conn2's UPDATE on
/// the same row blocks over the wire. After conn1 commits, conn2 completes
/// and reports exactly 1 affected row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_concurrent_update_blocks_then_succeeds() {
    // Each connection needs its own port/engine so they share the same engine.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let engine = Arc::new(SqlEngine::new());
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::clone(&engine),
        Arc::new(crabka_pgwire::session::SessionConfig::trust()),
    ));

    let conn1 = connect(port).await;
    let conn2 = connect(port).await;

    // Set up the table.
    conn1
        .batch_execute("CREATE TABLE t (id int4, v text)")
        .await
        .expect("create");
    conn1
        .batch_execute("INSERT INTO t VALUES (1,'orig')")
        .await
        .expect("insert");

    // T1: open a transaction and lock row 1.
    conn1
        .batch_execute("BEGIN; UPDATE t SET v='a' WHERE id=1")
        .await
        .expect("t1 begin+update");

    // T2: issue an UPDATE that will block.
    let t2 = tokio::spawn(async move {
        conn2
            .execute("UPDATE t SET v='b' WHERE id=1", &[])
            .await
            .expect("t2 update")
    });

    // let T2 reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;
    conn1.batch_execute("COMMIT").await.expect("t1 commit");

    let affected = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(affected, 1, "t2 must have updated exactly 1 row");
}

// ── SP40: foreign-table (Kafka FDW) executor seam ────────────────────────────

use crabka_pgcatalog::{ForeignServer, Table, UserMapping};
use crabka_pgexec::{
    ExecError,
    clock::EvalCtx,
    foreign::{ForeignScanner, ScanBounds},
};
use crabka_pgtypes::Datum;

/// A fake `ForeignScanner` for tests: returns canned rows aligned to the foreign
/// table's column layout (envelope columns first, then value columns). Records the
/// last server/mapping it was handed so a test can assert they were resolved.
/// `import_tables` are the canned `(name, value_columns)` pairs IMPORT FOREIGN
/// SCHEMA materializes (after the requested filter is applied); the `FakeScanner`
/// supplies the standard `topic`/`value_format=raw` OPTIONS for each.
struct FakeScanner {
    rows: Vec<Vec<Datum>>,
    import_tables: Vec<(String, Vec<crabka_pgcatalog::Column>)>,
}

impl ForeignScanner for FakeScanner {
    fn scan(
        &self,
        table: &Table,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        _bounds: &ScanBounds,
        _ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        // Every canned row must match the table's full column width (envelope + value).
        for r in &self.rows {
            assert_eq!(
                r.len(),
                table.columns.len(),
                "fake scanner row width must match the foreign table column count"
            );
        }
        Ok(self.rows.clone())
    }

    fn import_schema(
        &self,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        filter: &crabka_pgexec::foreign::ImportFilter,
    ) -> Result<Vec<crabka_pgexec::foreign::ImportedTable>, ExecError> {
        Ok(self
            .import_tables
            .iter()
            .filter(|(name, _)| filter.retains(name))
            .map(|(name, columns)| crabka_pgexec::foreign::ImportedTable {
                name: name.clone(),
                columns: columns.clone(),
                options: vec![
                    ("topic".to_string(), name.clone()),
                    ("value_format".to_string(), "raw".to_string()),
                ],
            })
            .collect())
    }
}

/// Spawn a server whose engine has `scanner` registered as the foreign scanner.
async fn spawn_with_scanner(scanner: Arc<dyn ForeignScanner>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(scanner);
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(engine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

/// DDL round-trip: CREATE SERVER + CREATE FOREIGN TABLE + DROP FOREIGN TABLE all
/// succeed and report the `PostgreSQL` command tags. No scanner is needed (no scan
/// runs), so this uses the default (scanner-less) `spawn()`.
#[tokio::test]
async fn create_drop_foreign_objects_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw \
             OPTIONS (bootstrap 'h:9092', registry_url 'http://r')",
        )
        .await
        .expect("create server");
    client
        .batch_execute(
            "CREATE FOREIGN TABLE orders (id int4) SERVER s \
             OPTIONS (topic 'orders', value_format 'avro')",
        )
        .await
        .expect("create foreign table");
    // Describe/plan of a foreign table resolves its schema (envelope + value columns)
    // from the catalog without a scan.
    let fields = client
        .prepare("SELECT _partition, _offset, id FROM orders")
        .await
        .expect("describe foreign table");
    assert_eq!(
        fields.columns().len(),
        3,
        "three projected columns described"
    );
    client
        .batch_execute("DROP FOREIGN TABLE orders")
        .await
        .expect("drop foreign table");
    // Gone: a re-select errors 42P01.
    let err = client
        .batch_execute("SELECT id FROM orders")
        .await
        .expect_err("dropped");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P01");
}

/// Read path: with a fake scanner registered, a `SELECT` from a foreign table
/// returns the canned rows, and projection + WHERE compose over those rows exactly
/// like an ordinary table.
#[tokio::test]
async fn foreign_select_reads_scanner_rows_with_projection_and_where() {
    // Canned rows for `orders (id int4, amount int4)`: layout is the 5 envelope
    // columns (_partition int4, _offset int8, _timestamp timestamptz, _key bytea,
    // _headers text) followed by the 2 value columns.
    let row = |partition: i32, offset: i64, id: i32, amount: i32| {
        vec![
            Datum::Int4(partition),
            Datum::Int8(offset),
            Datum::Timestamptz(jiff::Timestamp::UNIX_EPOCH),
            Datum::Bytea(vec![0xDE, 0xAD]),
            Datum::Text("{}".into()),
            Datum::Int4(id),
            Datum::Int4(amount),
        ]
    };
    let scanner = Arc::new(FakeScanner {
        rows: vec![row(0, 100, 1, 10), row(0, 101, 2, 20), row(1, 200, 3, 30)],
        import_tables: Vec::new(),
    });
    let client = connect(spawn_with_scanner(scanner).await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092')",
        )
        .await
        .expect("create server");
    client
        .batch_execute(
            "CREATE FOREIGN TABLE orders (id int4, amount int4) SERVER s OPTIONS (topic 'orders')",
        )
        .await
        .expect("create foreign table");

    // Full scan: all three canned rows come back, envelope + value columns present.
    let rows = client
        .query(
            "SELECT _partition, _offset, id, amount FROM orders ORDER BY id",
            &[],
        )
        .await
        .expect("select foreign");
    assert_eq!(rows.len(), 3, "all canned rows returned");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    let partitions: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("_partition")).collect();
    assert_eq!(partitions, vec![0, 0, 1]);

    // Projection + WHERE compose over the scanner rows: only amount >= 20 survive.
    let filtered = client
        .query("SELECT id FROM orders WHERE amount >= 20 ORDER BY id", &[])
        .await
        .expect("select foreign with where");
    let ids: Vec<i32> = filtered.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(ids, vec![2, 3], "WHERE filters the scanner rows");
}

/// No-scanner path: with no foreign scanner registered, a `SELECT` from a foreign
/// table returns the clear 0A000 ("foreign tables require the `kafka` feature").
/// DDL still works (no scan), so the table can be created without a scanner.
#[tokio::test]
async fn foreign_select_without_scanner_is_unsupported() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092')",
        )
        .await
        .expect("create server");
    client
        .batch_execute("CREATE FOREIGN TABLE orders (id int4) SERVER s OPTIONS (topic 'orders')")
        .await
        .expect("create foreign table");
    let err = client
        .batch_execute("SELECT id FROM orders")
        .await
        .expect_err("no scanner registered");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000", "feature_not_supported");
    assert!(
        db.message().contains("foreign tables require"),
        "clear message, got: {}",
        db.message()
    );
}

/// IMPORT FOREIGN SCHEMA through the full pgwire path: the fake scanner reports
/// two tables; `EXCEPT (payments)` drops one; the survivor becomes a queryable
/// foreign table (the scanner returns no rows here, so the SELECT is empty but
/// the table — with its envelope + value columns — must exist and be selectable).
#[tokio::test]
async fn import_foreign_schema_materializes_foreign_tables() {
    use crabka_pgcatalog::Column;
    use crabka_pgtypes::ColumnType;

    let scanner = Arc::new(FakeScanner {
        rows: Vec::new(),
        import_tables: vec![
            (
                "orders".to_string(),
                vec![Column::new("id", ColumnType::Int8)],
            ),
            (
                "payments".to_string(),
                vec![Column::new("amount", ColumnType::Float8)],
            ),
        ],
    });
    let client = connect(spawn_with_scanner(scanner).await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092')",
        )
        .await
        .expect("create server");

    client
        .batch_execute("IMPORT FOREIGN SCHEMA kafka EXCEPT (payments) FROM SERVER s")
        .await
        .expect("import foreign schema");

    // `orders` is now a queryable foreign table: its value column `id` plus the
    // envelope columns are present (the scanner returns no rows → empty result).
    let rows = client
        .query("SELECT _partition, id FROM orders", &[])
        .await
        .expect("select imported orders");
    assert_eq!(rows.len(), 0, "fake scanner returns no rows");

    // `payments` was excluded — querying it is a 42P01 (undefined table).
    let err = client
        .batch_execute("SELECT amount FROM payments")
        .await
        .expect_err("payments must not exist");
    assert_eq!(
        err.as_db_error().expect("db").code().code(),
        "42P01",
        "excluded table must be undefined"
    );
}

// ---- ALTER TABLE … ADD PRIMARY KEY ----

#[tokio::test]
async fn alter_table_add_primary_key_backfills_and_enforces_future_writes() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE b (bid int4, bbalance int4); \
             INSERT INTO b VALUES (1, 10), (2, 20), (3, 30); \
             ALTER TABLE b ADD PRIMARY KEY (bid)",
        )
        .await
        .expect("ADD PRIMARY KEY on a populated table with clean data");

    // Future duplicates violate the backfilled unique index, named <table>_pkey.
    let dup = client
        .batch_execute("INSERT INTO b VALUES (2, 99)")
        .await
        .expect_err("duplicate key after ADD PRIMARY KEY");
    let db = dup.as_db_error().expect("db error");
    assert!(db.code().code() == "23505");
    assert!(db.message().contains("b_pkey"));

    // The key column became NOT NULL for future writes.
    let null = client
        .batch_execute("INSERT INTO b VALUES (NULL, 0)")
        .await
        .expect_err("NULL key after ADD PRIMARY KEY");
    assert!(sqlstate(&null) == "23502");

    // Distinct keys still insert; existing rows are intact.
    client
        .batch_execute("INSERT INTO b VALUES (4, 40)")
        .await
        .expect("distinct key inserts");
    let rows = client
        .query("SELECT count(*)::int4 FROM b", &[])
        .await
        .expect("count");
    assert!(rows[0].get::<_, i32>(0) == 4);

    // The pkey index is constraint-backed: DROP INDEX is refused like a
    // CREATE TABLE-time primary key's index (2BP01).
    let drop = client
        .batch_execute("DROP INDEX b_pkey")
        .await
        .expect_err("constraint-backed index must not drop");
    assert!(sqlstate(&drop) == "2BP01");
}

#[tokio::test]
async fn alter_table_add_multi_column_primary_key_enforces_composite_keys() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (a int4, b int4); \
             INSERT INTO t VALUES (1, 1), (1, 2), (2, 1); \
             ALTER TABLE t ADD PRIMARY KEY (a, b)",
        )
        .await
        .expect("composite ADD PRIMARY KEY");

    client
        .batch_execute("INSERT INTO t VALUES (1, 3)")
        .await
        .expect("distinct composite key inserts");
    let dup = client
        .batch_execute("INSERT INTO t VALUES (1, 2)")
        .await
        .expect_err("duplicate composite key");
    assert!(sqlstate(&dup) == "23505");
    let null = client
        .batch_execute("INSERT INTO t VALUES (5, NULL)")
        .await
        .expect_err("NULL in any key column");
    assert!(sqlstate(&null) == "23502");
}

#[tokio::test]
async fn alter_table_add_primary_key_rejects_existing_duplicates_all_or_nothing() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, v text); \
             INSERT INTO t VALUES (1, 'a'), (1, 'b'), (2, 'c')",
        )
        .await
        .expect("seed duplicate keys");

    let err = client
        .batch_execute("ALTER TABLE t ADD PRIMARY KEY (id)")
        .await
        .expect_err("existing duplicates must fail the ADD");
    assert!(sqlstate(&err) == "23505");

    // Nothing committed: duplicates and NULL keys still insert, and no pkey
    // index metadata leaked (42704 — the index does not exist).
    client
        .batch_execute("INSERT INTO t VALUES (1, 'still duplicable'), (NULL, 'still nullable')")
        .await
        .expect("failed ADD PRIMARY KEY must leave no constraint behind");
    let missing = client
        .batch_execute("DROP INDEX t_pkey")
        .await
        .expect_err("no index metadata may remain");
    assert!(sqlstate(&missing) == "42704");
}

#[tokio::test]
async fn alter_table_add_primary_key_reports_duplicates_before_nulls() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4, v text); \
             INSERT INTO t VALUES (1, 'a'), (1, 'b'), (NULL, 'c')",
        )
        .await
        .expect("seed NULLs and duplicates");

    // PostgreSQL builds the unique index before attaching NOT NULL, so the
    // duplicate wins over the NULL: 23505 naming the index build.
    let err = client
        .batch_execute("ALTER TABLE t ADD PRIMARY KEY (id)")
        .await
        .expect_err("duplicate data must fail the ADD");
    let db = err.as_db_error().expect("db error");
    assert!(db.code().code() == "23505");
    assert!(db.message() == "could not create unique index \"t_pkey\"");

    // With the duplicate gone the NULL check is reached, in PG's spelling.
    client
        .batch_execute("DELETE FROM t WHERE v = 'b'")
        .await
        .expect("remove the duplicate");
    let nulls = client
        .batch_execute("ALTER TABLE t ADD PRIMARY KEY (id)")
        .await
        .expect_err("existing NULL must fail the ADD");
    let db = nulls.as_db_error().expect("db error");
    assert!(db.code().code() == "23502");
    assert!(db.message() == "column \"id\" of relation \"t\" contains null values");

    // The column did not become NOT NULL: another NULL still inserts.
    client
        .batch_execute("INSERT INTO t VALUES (NULL, 'd')")
        .await
        .expect("failed ADD PRIMARY KEY must not mark columns NOT NULL");
}

#[tokio::test]
async fn alter_table_add_primary_key_rejects_a_second_primary_key() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4 PRIMARY KEY, v int4)")
        .await
        .expect("create with CREATE TABLE-time primary key");
    let err = client
        .batch_execute("ALTER TABLE t ADD PRIMARY KEY (v)")
        .await
        .expect_err("a second primary key is invalid");
    let db = err.as_db_error().expect("db error");
    assert!(db.code().code() == "42P16");
    assert!(db.message() == "multiple primary keys for table \"t\" are not allowed");

    // The ALTER-added primary key records the same constraint marker, so a
    // second ALTER is rejected identically.
    client
        .batch_execute("CREATE TABLE u (a int4, b int4); ALTER TABLE u ADD PRIMARY KEY (a)")
        .await
        .expect("first ALTER-added primary key");
    let again = client
        .batch_execute("ALTER TABLE u ADD PRIMARY KEY (b)")
        .await
        .expect_err("second ALTER-added primary key");
    assert!(sqlstate(&again) == "42P16");
}

#[tokio::test]
async fn alter_table_add_primary_key_fails_clear_on_sharded_missing_and_bad_targets() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE s (id int4) SHARDED")
        .await
        .expect("create sharded table");
    let sharded = client
        .batch_execute("ALTER TABLE s ADD PRIMARY KEY (id)")
        .await
        .expect_err("sharded tables have no global enforcement");
    assert!(sqlstate(&sharded) == "0A000");

    let missing_table = client
        .batch_execute("ALTER TABLE nope ADD PRIMARY KEY (id)")
        .await
        .expect_err("undefined table");
    assert!(sqlstate(&missing_table) == "42P01");

    client
        .batch_execute("CREATE TABLE t (id int4)")
        .await
        .expect("create plain table");
    let missing_column = client
        .batch_execute("ALTER TABLE t ADD PRIMARY KEY (nope)")
        .await
        .expect_err("undefined column");
    assert!(sqlstate(&missing_column) == "42703");

    client
        .batch_execute("CREATE VIEW v AS SELECT id FROM t")
        .await
        .expect("create view");
    let view = client
        .batch_execute("ALTER TABLE v ADD PRIMARY KEY (id)")
        .await
        .expect_err("views are not tables");
    assert!(sqlstate(&view) == "42809");
}

#[tokio::test]
async fn alter_table_add_named_constraint_primary_key_uses_the_given_name() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (id int4); \
             INSERT INTO t VALUES (1); \
             ALTER TABLE t ADD CONSTRAINT custom_pk PRIMARY KEY (id)",
        )
        .await
        .expect("named constraint form");
    let dup = client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect_err("duplicate under the named constraint");
    let db = dup.as_db_error().expect("db error");
    assert!(db.code().code() == "23505");
    assert!(db.message().contains("custom_pk"));
}
