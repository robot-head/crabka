use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

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

#[tokio::test]
async fn varchar_and_char_roundtrip_and_enforce_length() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (v varchar(3), c char(3))")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES ('abc', 'a')")
        .await
        .expect("insert");

    let rows = client
        .query("SELECT v, c FROM t WHERE v = 'abc'", &[])
        .await
        .expect("select");
    let v: &str = rows[0].get(0);
    let c: &str = rows[0].get(1);
    assert_eq!((v, c), ("abc", "a  "));

    let err = client
        .batch_execute("INSERT INTO t VALUES ('abcd', 'ok')")
        .await
        .expect_err("varchar length is enforced");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "22001");
}

#[tokio::test]
async fn varchar_and_char_catalog_metadata_is_visible() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (v varchar(5), c character(2))")
        .await
        .expect("create");

    let rows = client
        .query(
            "SELECT attname, atttypid, atttypmod FROM pg_attribute \
             WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't') \
             ORDER BY attname",
            &[],
        )
        .await
        .expect("catalog query");

    let c_name: &str = rows[0].get(0);
    let c_oid: i32 = rows[0].get(1);
    let c_typmod: i32 = rows[0].get(2);
    let v_name: &str = rows[1].get(0);
    let v_oid: i32 = rows[1].get(1);
    let v_typmod: i32 = rows[1].get(2);
    assert_eq!((c_name, c_oid, c_typmod), ("c", 1042, 6));
    assert_eq!((v_name, v_oid, v_typmod), ("v", 1043, 9));

    let type_rows = client
        .query(
            "SELECT typname FROM pg_type WHERE oid IN (1042, 1043) ORDER BY oid",
            &[],
        )
        .await
        .expect("pg_type query");
    let bpchar: &str = type_rows[0].get(0);
    let varchar: &str = type_rows[1].get(0);
    assert_eq!((bpchar, varchar), ("bpchar", "varchar"));
}

#[tokio::test]
async fn uuid_roundtrip_where_catalog_and_row_description() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id uuid)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES ('550E8400-E29B-41D4-A716-446655440000'::uuid)")
        .await
        .expect("insert");

    let rows = client
        .query(
            "SELECT id::text FROM t WHERE id = '550e8400-e29b-41d4-a716-446655440000'::uuid",
            &[],
        )
        .await
        .expect("select");
    let id: &str = rows[0].get(0);
    assert_eq!(id, "550e8400-e29b-41d4-a716-446655440000");

    let row_description_rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("typed select");
    assert_eq!(row_description_rows[0].columns()[0].type_().oid(), 2950);

    let type_rows = client
        .query("SELECT typname, typlen FROM pg_type WHERE oid = 2950", &[])
        .await
        .expect("pg_type uuid");
    let typname: &str = type_rows[0].get(0);
    let typlen: i32 = type_rows[0].get(1);
    assert_eq!((typname, typlen), ("uuid", 16));

    let attr_rows = client
        .query(
            "SELECT atttypid FROM pg_attribute \
             WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't') \
             AND attname = 'id'",
            &[],
        )
        .await
        .expect("pg_attribute uuid");
    let atttypid: i32 = attr_rows[0].get(0);
    assert_eq!(atttypid, 2950);
}

#[tokio::test]
async fn uuid_rejects_invalid_input_with_22p02() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id uuid)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES ('not-a-uuid'::uuid)")
        .await
        .expect_err("invalid uuid is rejected");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "22P02");
}
