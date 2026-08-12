//! Full-text-search values, functions, operators, storage, and wire OIDs.

use std::sync::Arc;

use crabka_pgcatalog::RelationName;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn connect() -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

async fn connect_to(engine: &SqlEngine) -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(engine.clone_handle()),
        Arc::new(SessionConfig::trust()),
    ));
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    client
        .simple_query(sql)
        .await
        .expect(sql)
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
}

#[tokio::test]
async fn functions_and_operators_match_postgres_shapes() {
    let client = connect().await;
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('english', 'The Fat Rats')")
            .await
            .as_deref(),
        Some("'fat':2 'rat':3")
    );
    assert_eq!(
        scalar(&client, "SELECT plainto_tsquery('english', 'The Fat Rats')")
            .await
            .as_deref(),
        Some("'fat' & 'rat'")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT phraseto_tsquery('english', 'The Cat and Rats')"
        )
        .await
        .as_deref(),
        Some("'cat' <2> 'rat'")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT to_tsvector('english', 'fat cats') @@ to_tsquery('english', 'cat')",
        )
        .await
        .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(&client, "SELECT 'fat cats' @@ plainto_tsquery('cat')")
            .await
            .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(&client, "SELECT 'cat'::tsquery && 'rat'::tsquery")
            .await
            .as_deref(),
        Some("'cat' & 'rat'")
    );
    assert_eq!(
        scalar(&client, "SELECT 'cat'::tsquery || 'rat'::tsquery")
            .await
            .as_deref(),
        Some("'cat' | 'rat'")
    );
    assert_eq!(
        scalar(&client, "SELECT 'cat'::tsquery <-> 'rat'::tsquery")
            .await
            .as_deref(),
        Some("'cat' <-> 'rat'")
    );
    assert_eq!(
        scalar(&client, "SELECT !! 'cat'::tsquery").await.as_deref(),
        Some("!'cat'")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsquery('english', 'cat & the')")
            .await
            .as_deref(),
        Some("'cat'")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT to_tsvector('simple', 'fat') @@ websearch_to_tsquery('simple', 'fat OR rat dog')",
        )
        .await
        .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(&client, "SELECT querytree('cat & !dog'::tsquery)")
            .await
            .as_deref(),
        Some("'cat'")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT 'cat'::tsvector = 'cat'::tsvector AND 'cat'::tsquery = 'cat'::tsquery",
        )
        .await
        .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT 'cat'::tsquery @> 'cat' AND 'cat'::tsquery <@ 'cat'",
        )
        .await
        .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(&client, "SELECT 'cat'::tsquery && 'rat'")
            .await
            .as_deref(),
        Some("'cat' & 'rat'")
    );
    assert_eq!(
        scalar(
            &client,
            "PREPARE fulltext AS SELECT to_tsvector('simple', 'cat') @@ $1; EXECUTE fulltext('cat')",
        )
        .await
        .as_deref(),
        Some("t")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT setweight(to_tsvector('simple', 'cat rat'), 'A')",
        )
        .await
        .as_deref(),
        Some("'cat':1A 'rat':2A")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT length(to_tsvector('simple', 'cat cat rat'))"
        )
        .await
        .as_deref(),
        Some("2")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT ts_delete(to_tsvector('simple', 'cat rat'), 'cat')",
        )
        .await
        .as_deref(),
        Some("'rat':2")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT ts_headline('english', 'The fat rats ran', plainto_tsquery('english', 'rat'))",
        )
        .await
        .as_deref(),
        Some("The fat <b>rats</b> ran")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT jsonb_to_tsvector('english', '{\"a\":\"The Fat Rats\",\"b\":123}'::jsonb, '[\"string\",\"numeric\"]'::jsonb)",
        )
        .await
        .as_deref(),
        Some("'123':5 'fat':2 'rat':3")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT jsonb_to_tsvector('english', '{\"cat\":\"The Fat Rats\",\"dog\":123}'::jsonb, '\"all\"'::jsonb)",
        )
        .await
        .as_deref(),
        Some("'123':9 'cat':1 'dog':7 'fat':4 'rat':5")
    );
}

#[tokio::test]
async fn gin_index_backfills_tracks_writes_and_drives_exact_query_scans() {
    let engine = SqlEngine::new();
    let client = connect_to(&engine).await;
    client
        .simple_query(
            "CREATE TABLE docs (id int PRIMARY KEY, search tsvector); \
             INSERT INTO docs VALUES \
               (1, to_tsvector('english', 'fat cats')), \
               (2, to_tsvector('english', 'loyal dogs')), \
               (3, to_tsvector('english', 'quiet birds')); \
             CREATE INDEX docs_search_gin ON docs USING gin (search)",
        )
        .await
        .expect("build GIN index");
    assert_eq!(
        scalar(
            &client,
            "SELECT pg_get_indexdef('docs_search_gin'::regclass)"
        )
        .await
        .as_deref(),
        Some("CREATE INDEX docs_search_gin ON public.docs USING gin (search)")
    );
    client
        .simple_query(
            "INSERT INTO docs VALUES \
               (4, to_tsvector('english', 'new cats')), \
               (5, to_tsvector('simple', 'cats')); \
             UPDATE docs SET search = to_tsvector('english', 'cats and dogs') WHERE id = 2; \
             DELETE FROM docs WHERE id = 1",
        )
        .await
        .expect("maintain GIN postings");

    // An unindexed corrupt row makes a sequential table scan fail. The exact
    // query succeeds only when the GIN posting probe limits heap rechecks.
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &RelationName::public("docs"))
        .expect("table");
    engine
        .kv_handle()
        .write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, 999, 999),
            value: vec![0],
        }])
        .expect("inject unreachable row");
    assert_eq!(
        scalar(
            &client,
            "SELECT id FROM docs WHERE search @@ plainto_tsquery('english', 'cat') ORDER BY id",
        )
        .await
        .as_deref(),
        Some("2")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT id FROM docs WHERE search @@ plainto_tsquery('english', 'cat') AND id = 4",
        )
        .await
        .as_deref(),
        Some("4")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT id FROM docs WHERE search @@ 'cats'::tsquery AND id = 5",
        )
        .await
        .as_deref(),
        Some("5")
    );
}

#[tokio::test]
async fn values_round_trip_through_tables_and_report_builtin_oids() {
    let client = connect().await;
    client
        .simple_query("CREATE TABLE docs (id int PRIMARY KEY, body text, search tsvector)")
        .await
        .expect("create");
    client
        .simple_query(
            "INSERT INTO docs VALUES (1, 'The quick brown fox', to_tsvector('english', 'The quick brown fox'))",
        )
        .await
        .expect("insert");
    assert_eq!(
        scalar(
            &client,
            "SELECT body FROM docs WHERE search @@ plainto_tsquery('english', 'quick fox')",
        )
        .await
        .as_deref(),
        Some("The quick brown fox")
    );

    let row = client
        .query_one(
            "SELECT to_tsvector('english', 'cat'), plainto_tsquery('english', 'cat')",
            &[],
        )
        .await
        .expect("typed row");
    assert_eq!(row.columns()[0].type_().oid(), 3614);
    assert_eq!(row.columns()[1].type_().oid(), 3615);

    client
        .simple_query(
            "CREATE TABLE search_defaults (search tsvector DEFAULT ''::tsvector, query tsquery DEFAULT ''::tsquery); \
             INSERT INTO search_defaults DEFAULT VALUES",
        )
        .await
        .expect("text-search defaults");
    assert_eq!(
        scalar(
            &client,
            "SELECT search = ''::tsvector AND query = ''::tsquery FROM search_defaults",
        )
        .await
        .as_deref(),
        Some("t")
    );
}

#[tokio::test]
async fn default_configuration_is_session_settable() {
    let client = connect().await;
    client
        .simple_query("SET default_text_search_config = 'simple'")
        .await
        .expect("set");
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('The Rats')")
            .await
            .as_deref(),
        Some("'rats':2 'the':1")
    );
}

#[tokio::test]
async fn configuration_and_dictionary_ddl_is_durable_and_catalog_visible() {
    let engine = SqlEngine::new();
    let client = connect_to(&engine).await;
    client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY my_dict (TEMPLATE = simple); \
             CREATE TEXT SEARCH CONFIGURATION my_english (COPY = english); \
             ALTER TEXT SEARCH CONFIGURATION my_english ADD MAPPING FOR asciiword WITH my_dict; \
             CREATE TEXT SEARCH CONFIGURATION s1 (COPY = simple); \
             CREATE TEXT SEARCH CONFIGURATION s2 (COPY = s1); \
             CREATE TEXT SEARCH CONFIGURATION a.cfg (COPY = simple); \
             CREATE TEXT SEARCH CONFIGURATION b.cfg (COPY = english); \
             CREATE TEXT SEARCH CONFIGURATION shared (COPY = simple); \
             CREATE TEXT SEARCH DICTIONARY shared (TEMPLATE = simple); \
             CREATE TEXT SEARCH CONFIGURATION parser_cfg (PARSER = pg_catalog.default)",
        )
        .await
        .expect("create text-search objects");
    drop(client);
    let client = connect_to(&engine).await;
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('my_english', 'The Rats')")
            .await
            .as_deref(),
        Some("'rat':2")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT cfgname FROM pg_catalog.pg_ts_config WHERE cfgname = 'my_english'",
        )
        .await
        .as_deref(),
        Some("my_english")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT dictname FROM pg_catalog.pg_ts_dict WHERE dictname = 'my_dict'",
        )
        .await
        .as_deref(),
        Some("my_dict")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('s2', 'The Rats')")
            .await
            .as_deref(),
        Some("'rats':2 'the':1")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('a.cfg', 'The Rats')")
            .await
            .as_deref(),
        Some("'rats':2 'the':1")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('b.cfg', 'The Rats')")
            .await
            .as_deref(),
        Some("'rat':2")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('shared', 'The Rats')")
            .await
            .as_deref(),
        Some("'rats':2 'the':1")
    );
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('parser_cfg', 'The Rats')")
            .await
            .as_deref(),
        Some("'rats':2 'the':1")
    );
    client
        .simple_query("DROP TEXT SEARCH CONFIGURATION a.cfg")
        .await
        .expect("drop one qualified configuration");
    assert_eq!(
        scalar(&client, "SELECT to_tsvector('b.cfg', 'The Rats')")
            .await
            .as_deref(),
        Some("'rat':2")
    );
    client
        .simple_query(
            "ALTER TEXT SEARCH CONFIGURATION my_english RENAME TO renamed_english; \
             DROP TEXT SEARCH CONFIGURATION renamed_english; \
             DROP TEXT SEARCH DICTIONARY my_dict",
        )
        .await
        .expect("rename and drop text-search objects");
}

/// `json_to_tsvector` refuses a document whose escapes do not decode.
///
/// Upstream's `iterate_json_values` builds its lexer with `need_escapes`, so
/// the document is validated before any lexeme is produced. gres decoded
/// without validating, which put a `\u0000` or a dropped unpaired surrogate
/// into a stored `tsvector` -- the same corruption the `json` accessors were
/// closed against, reached through the one entry point that was missed.
///
/// The well-formed case is here too: a fix that refused every document would
/// satisfy the first two assertions and break the function.
#[tokio::test]
async fn json_to_tsvector_validates_the_documents_escapes() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let client = connect_to(&engine).await;

    let nul = client
        .simple_query(
            r#"SELECT json_to_tsvector('english', json '{"a": "one \u0000 two"}', '["string"]'::jsonb)"#,
        )
        .await
        .expect_err("a NUL escape must be refused");
    assert!(nul.code().map(|c| c.code().to_owned()) == Some("22P05".to_owned()));

    let orphan = client
        .simple_query(
            r#"SELECT json_to_tsvector('english', json '{"a": "one \ud800 two"}', '["string"]'::jsonb)"#,
        )
        .await
        .expect_err("an unpaired surrogate must be refused");
    assert!(orphan.code().map(|c| c.code().to_owned()) == Some("22P02".to_owned()));

    // A well-formed document still produces its lexemes, and the escape is
    // decoded on the way: `caf\u00e9` has to reach the parser as `café` for
    // the english configuration to stem it to one token.
    assert_eq!(
        scalar(
            &client,
            r#"SELECT json_to_tsvector('english', json '{"a": "caf\u00e9 cats"}', '["string"]'::jsonb)"#,
        )
        .await,
        Some("'café':1 'cat':2".to_owned())
    );
}
