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
    let error = err.as_db_error().expect("db error");
    assert_eq!(error.code().code(), "22001");
    assert_eq!(
        error.message(),
        "value too long for type character varying(3)"
    );

    client
        .batch_execute("INSERT INTO t VALUES (2, 3)")
        .await
        .expect("scalar values coerce through varchar/char I/O on assignment");

    let validation = client
        .query_one(
            "SELECT pg_input_is_valid('abc  ', 'varchar(3)'), \
                    pg_input_is_valid('abcd', 'varchar(3)')",
            &[],
        )
        .await
        .expect("typmod soft input");
    assert!(validation.get::<_, bool>(0));
    assert!(!validation.get::<_, bool>(1));
    let error_info = client
        .query_one(
            "SELECT message FROM pg_input_error_info('abcd', 'varchar(3)')",
            &[],
        )
        .await
        .expect("typmod soft-input diagnostics");
    assert_eq!(
        error_info.get::<_, &str>(0),
        "value too long for type character varying(3)"
    );
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
    let typlen: i16 = type_rows[0].get(1);
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
async fn uuid_columns_coerce_bare_literals_for_comparisons() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id uuid); INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000'::uuid)")
        .await
        .expect("fixture");
    let rows = client
        .query(
            "SELECT id::text FROM t WHERE id = '550e8400e29b41d4a716446655440000'",
            &[],
        )
        .await
        .expect("uuid literal comparison");
    assert_eq!(
        rows[0].get::<_, &str>(0),
        "550e8400-e29b-41d4-a716-446655440000"
    );
}

#[tokio::test]
async fn enum_columns_coerce_bare_literals_for_comparisons() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .expect("create enum");
    client
        .batch_execute(
            "CREATE TABLE t (value mood); INSERT INTO t VALUES ('sad'), ('ok'), ('happy')",
        )
        .await
        .expect("fixture");

    let between = client
        .query(
            "SELECT count(*) FROM t WHERE value BETWEEN 'sad' AND 'ok'",
            &[],
        )
        .await
        .expect("bare enum BETWEEN literals");
    assert_eq!(between[0].get::<_, i64>(0), 2);

    let in_list = client
        .query("SELECT value::text FROM t WHERE value IN ('happy')", &[])
        .await
        .expect("bare enum IN literal");
    assert_eq!(in_list[0].get::<_, &str>(0), "happy");

    let any = client
        .query_one("SELECT 'sad' = ANY(ARRAY['sad', 'happy']::mood[])", &[])
        .await
        .expect("bare enum ANY literal");
    assert!(any.get::<_, bool>(0));

    let grouped_any = client
        .query(
            "SELECT value::text FROM t GROUP BY value HAVING 'sad' = ANY(ARRAY[value])",
            &[],
        )
        .await
        .expect("grouped bare enum ANY literal");
    assert_eq!(grouped_any[0].get::<_, &str>(0), "sad");

    client
        .batch_execute(
            "CREATE FUNCTION echo_mood(anyenum) RETURNS text AS $$ \
             BEGIN RETURN $1::text || 'omg'; END $$ LANGUAGE plpgsql",
        )
        .await
        .expect("create anyenum function");
    let echoed = client
        .query_one("SELECT echo_mood('sad'::mood)", &[])
        .await
        .expect("call anyenum function");
    assert_eq!(echoed.get::<_, &str>(0), "sadomg");

    client
        .batch_execute(
            "CREATE FUNCTION echo_mood(mood) RETURNS text AS $$ \
             BEGIN RETURN $1::text || 'exact'; END $$ LANGUAGE plpgsql",
        )
        .await
        .expect("create concrete enum overload");
    let exact = client
        .query_one("SELECT echo_mood('sad'::mood)", &[])
        .await
        .expect("prefer concrete enum overload");
    assert_eq!(exact.get::<_, &str>(0), "sadexact");
}

#[tokio::test]
async fn enum_support_functions_use_typed_nulls_as_bounds() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .expect("create enum");

    let row = client
        .query_one(
            "SELECT enum_first(NULL::mood)::text, enum_last(NULL::mood)::text, \
                    enum_range(NULL::mood)::text, enum_range(NULL, 'ok'::mood)::text, \
                    enum_range('ok'::mood, NULL)::text",
            &[],
        )
        .await
        .expect("enum support functions");
    assert_eq!(row.get::<_, &str>(0), "sad");
    assert_eq!(row.get::<_, &str>(1), "happy");
    assert_eq!(row.get::<_, &str>(2), "{sad,ok,happy}");
    assert_eq!(row.get::<_, &str>(3), "{sad,ok}");
    assert_eq!(row.get::<_, &str>(4), "{ok,happy}");
}

#[tokio::test]
async fn pg_enum_keeps_alter_type_sort_orders() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TYPE planets AS ENUM ('venus', 'earth', 'mars'); \
             ALTER TYPE planets ADD VALUE 'uranus'; \
             ALTER TYPE planets ADD VALUE 'mercury' BEFORE 'venus'; \
             ALTER TYPE planets ADD VALUE 'saturn' BEFORE 'uranus'; \
             ALTER TYPE planets ADD VALUE 'jupiter' AFTER 'mars'; \
             ALTER TYPE planets ADD VALUE 'neptune'",
        )
        .await
        .expect("alter enum labels");

    let rows = client
        .query(
            "SELECT enumlabel, enumsortorder::text FROM pg_enum \
             WHERE enumtypid = 'planets'::regtype ORDER BY enumsortorder",
            &[],
        )
        .await
        .expect("read enum catalog rows");
    let actual = rows
        .iter()
        .map(|row| (row.get::<_, &str>(0), row.get::<_, &str>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            ("mercury", "0"),
            ("venus", "1"),
            ("earth", "2"),
            ("mars", "3"),
            ("jupiter", "3.25"),
            ("saturn", "3.5"),
            ("uranus", "4"),
            ("neptune", "5"),
        ]
    );
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

#[tokio::test]
async fn range_and_multirange_containment_are_strict() {
    let client = connect(spawn().await).await;
    let row = client
        .query_one(
            "SELECT \
                 int4range(1, 2) @> NULL::int4, \
                 'empty'::int4range @> NULL::int4, \
                 NULL::int4 <@ int4range(1, 2), \
                 int4multirange(int4range(1, 2)) @> NULL::int4, \
                 NULL::int4 <@ int4multirange(int4range(1, 2)), \
                 NULL::int4range @> 1, \
                 1 <@ NULL::int4range, \
                 NULL::int4multirange @> 1, \
                 1 <@ NULL::int4multirange, \
                 int4range(1, 2) @> NULL, \
                 NULL <@ int4range(1, 2), \
                 int4multirange(int4range(1, 2)) @> NULL, \
                 NULL <@ int4multirange(int4range(1, 2)), \
                 ARRAY[1] @> NULL, \
                 NULL <@ ARRAY[1], \
                 '{}'::jsonb @> NULL, \
                 NULL <@ '{}'::jsonb, \
                 int4range(1, 2) && NULL, \
                 ARRAY[1] && NULL",
            &[],
        )
        .await
        .expect("strict containment query");

    let values: [Option<bool>; 19] = std::array::from_fn(|index| row.get(index));
    assert_eq!(values, [None; 19]);

    let oidvector = client
        .query_one(
            "SELECT '1 2'::oidvector @> '1', \
                    '1' <@ '1 2'::oidvector, \
                    '1 2'::oidvector && '2 3'",
            &[],
        )
        .await
        .expect("oidvector literals adopt the typed operand");
    assert_eq!(
        [
            oidvector.get::<_, bool>(0),
            oidvector.get::<_, bool>(1),
            oidvector.get::<_, bool>(2),
        ],
        [true; 3]
    );

    let multirange_arithmetic = client
        .query_one(
            "SELECT \
                 int4multirange(int4range(1, 2)) + '{[3,4)}' = \
                     '{[1,2),[3,4)}'::int4multirange, \
                 '{[3,4)}' + int4multirange(int4range(1, 2)) = \
                     '{[1,2),[3,4)}'::int4multirange, \
                 int4multirange(int4range(1, 5)) - '{[2,3)}' = \
                     '{[1,2),[3,5)}'::int4multirange, \
                 int4multirange(int4range(1, 5)) * '{[3,7)}' = \
                     '{[3,5)}'::int4multirange",
            &[],
        )
        .await
        .expect("multirange arithmetic literals adopt the typed operand");
    assert_eq!(
        [
            multirange_arithmetic.get::<_, bool>(0),
            multirange_arithmetic.get::<_, bool>(1),
            multirange_arithmetic.get::<_, bool>(2),
            multirange_arithmetic.get::<_, bool>(3),
        ],
        [true; 4]
    );

    for sql in [
        "SELECT 1 WHERE int4range(1, 2) @> NULL::text",
        "SELECT 1 WHERE int4range(1, 2) && NULL::text",
        "SELECT 1 WHERE int4range(1, 2) &< NULL::text",
        "SELECT 1 WHERE int4range(1, 2) &> NULL::text",
        "SELECT 1 WHERE int4range(1, 2) -|- NULL::text",
        "SELECT 1 WHERE int4range(1, 2) << NULL::text",
        "SELECT 1 WHERE int4range(1, 2) >> NULL::text",
        "SELECT 1 WHERE (int4range(1, 2) + NULL::text) IS NULL",
        "SELECT 1 WHERE (int4range(1, 2) - NULL::text) IS NULL",
        "SELECT 1 WHERE (int4range(1, 2) * NULL::text) IS NULL",
        "SELECT 1 WHERE ARRAY[1] && NULL::text",
        "SELECT '1 2'::oidvector @> ARRAY[1]",
        "SELECT ARRAY[1] <@ '1 2'::oidvector",
        "SELECT '1 2'::oidvector && ARRAY[2]",
    ] {
        let error = client
            .query(sql, &[])
            .await
            .expect_err("operator types are resolved before strict NULL handling");
        assert_eq!(
            error.as_db_error().expect("database error").code().code(),
            "42883",
            "{sql}"
        );
    }

    for sql in [
        "SELECT int4range(1, 2) @> '3'",
        "SELECT '3' <@ int4range(1, 2)",
    ] {
        let error = client
            .query(sql, &[])
            .await
            .expect_err("a bare string adopts the range type before execution");
        assert_eq!(
            error.as_db_error().expect("database error").code().code(),
            "22P02",
            "{sql}"
        );
    }

    for sql in [
        "SELECT NULL @> NULL",
        "SELECT NULL @> '{}'",
        "SELECT NULL && NULL",
        "SELECT NULL &< NULL",
        "SELECT NULL &> NULL",
        "SELECT NULL -|- NULL",
        "SELECT NULL << NULL",
        "SELECT NULL >> NULL",
        "SELECT NULL + NULL",
        "SELECT NULL - NULL",
        "SELECT NULL * NULL",
    ] {
        let error = client
            .query(sql, &[])
            .await
            .expect_err("all-unknown operator call is ambiguous");
        assert_eq!(
            error.as_db_error().expect("database error").code().code(),
            "42725",
            "{sql}"
        );
    }
}
