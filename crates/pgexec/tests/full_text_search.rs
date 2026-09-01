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
            "SELECT ts_rank_cd('''a'':1 ''b'':2'::tsvector, 'a & b')",
        )
        .await
        .as_deref(),
        Some("0.1")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT ts_rank('''a'':1 ''s'':2C d g'::tsvector, 'a | s')",
        )
        .await
        .as_deref(),
        Some("0.091189064")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT tsvector_to_array('base:7 hidden:6 rebel:1 spaceship:2'::tsvector)",
        )
        .await
        .as_deref(),
        Some("{base,hidden,rebel,spaceship}")
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT tsvectorin(tsvectorout('base:7'::tsvector))"
        )
        .await
        .as_deref(),
        Some("'base':7")
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

/// `ts_lexize` reports the lexemes of one dictionary, including SQL-created
/// ISpell dictionaries backed by PostgreSQL's regression sample.
#[tokio::test]
async fn ts_lexize_answers_for_the_dictionaries_crabka_has() {
    let client = connect().await;
    for (sql, expected) in [
        ("SELECT ts_lexize('english_stem', 'skies')", "{sky}"),
        ("SELECT ts_lexize('english_stem', 'identity')", "{ident}"),
        ("SELECT ts_lexize('english_stem', 'the')", "{}"),
        ("SELECT ts_lexize('simple', 'SkIeS')", "{skies}"),
        ("SELECT ts_lexize('simple', 'the')", "{the}"),
        ("SELECT ts_lexize('pg_catalog.simple', 'The')", "{the}"),
    ] {
        assert2::assert!(
            scalar(&client, sql).await.as_deref() == Some(expected),
            "{sql}"
        );
    }
    assert2::assert!(scalar(&client, "SELECT ts_lexize('simple', NULL)").await == None);

    // A dictionary of crabka's own, built on the snowball template, stems like
    // the built-in it copies.
    client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY mystem (TEMPLATE = snowball, Language = english)",
        )
        .await
        .expect("create dictionary");
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('mystem', 'skies')")
            .await
            .as_deref()
            == Some("{sky}")
    );

    client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY ispell (TEMPLATE = ispell, DictFile = ispell_sample, AffFile = ispell_sample)",
        )
        .await
        .expect("create ispell dictionary");
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('ispell', 'bookings')")
            .await
            .as_deref()
            == Some("{booking,book}")
    );
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('ispell', 'Booking')")
            .await
            .as_deref()
            == Some("{booking,book}")
    );
    for (name, dict_file, aff_file, token, expected) in [
        (
            "hunspell_long",
            "hunspell_sample_long",
            "hunspell_sample_long",
            "ex-machina",
            "{ex-,machina}",
        ),
        (
            "hunspell_num",
            "hunspell_sample_num",
            "hunspell_sample_num",
            "sk",
            "{sky}",
        ),
    ] {
        client
            .simple_query(&format!(
                "CREATE TEXT SEARCH DICTIONARY {name} (TEMPLATE = ispell, DictFile = {dict_file}, AffFile = {aff_file})"
            ))
            .await
            .expect("create hunspell dictionary");
        let sql = format!("SELECT ts_lexize('{name}', '{token}')");
        assert2::assert!(
            scalar(&client, &sql).await.as_deref() == Some(expected),
            "{sql}"
        );
    }

    client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY synonym (TEMPLATE = synonym, Synonyms = synonym_sample)",
        )
        .await
        .expect("create synonym dictionary");
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('synonym', 'PoStGrEs')")
            .await
            .as_deref()
            == Some("{pgsql}")
    );
    client
        .simple_query("ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = 1)")
        .await
        .expect("enable case-sensitive synonym dictionary");
    assert2::assert!(scalar(&client, "SELECT ts_lexize('synonym', 'PoStGrEs')").await == None);
    client
        .simple_query("ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = off)")
        .await
        .expect("disable case-sensitive synonym dictionary");
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('synonym', 'indices')")
            .await
            .as_deref()
            == Some("{index}")
    );
    assert2::assert!(
        scalar(
            &client,
            "SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'synonym'",
        )
        .await
        .as_deref()
            == Some("synonyms = 'synonym_sample', casesensitive = 'off'")
    );
    let error = client
        .simple_query("ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = 2)")
        .await
        .expect_err("case-sensitive synonym setting must be Boolean");
    assert2::assert!(
        error
            .as_db_error()
            .map(|error| error.message().to_owned())
            .as_deref()
            == Some("casesensitive requires a Boolean value")
    );
    client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY thesaurus (TEMPLATE = thesaurus, DictFile = thesaurus_sample, Dictionary = english_stem)",
        )
        .await
        .expect("create thesaurus dictionary");
    assert2::assert!(
        scalar(&client, "SELECT ts_lexize('thesaurus', 'one')")
            .await
            .as_deref()
            == Some("{1}")
    );
    client
        .simple_query("CREATE TEXT SEARCH CONFIGURATION ispell_tst (COPY = english)")
        .await
        .expect("create ispell configuration");
    client
        .simple_query(
            "ALTER TEXT SEARCH CONFIGURATION ispell_tst ALTER MAPPING FOR word, numword WITH ispell, english_stem",
        )
        .await
        .expect("map ISpell configuration");
    assert2::assert!(
        scalar(
            &client,
            "SELECT to_tsvector('ispell_tst', 'Booking the skies after rebookings for footballklubber from a foot')",
        )
        .await
        .as_deref()
            == Some("'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3")
    );
    assert2::assert!(
        scalar(
            &client,
            "SELECT to_tsquery('ispell_tst', 'footballklubber')"
        )
        .await
        .as_deref()
            == Some("'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'")
    );
    client
        .simple_query("CREATE TEXT SEARCH CONFIGURATION hunspell_tst (COPY = ispell_tst)")
        .await
        .expect("copy ISpell configuration");
    client
        .simple_query("ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING REPLACE ispell WITH hunspell_long")
        .await
        .expect("replace inherited ISpell mapping");
    assert2::assert!(
        scalar(
            &client,
            "SELECT to_tsquery('hunspell_tst', 'footballklubber')"
        )
        .await
        .as_deref()
            == Some("'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'")
    );
    client
        .simple_query("CREATE TEXT SEARCH CONFIGURATION synonym_tst (COPY = english)")
        .await
        .expect("create synonym configuration");
    client
        .simple_query(
            "ALTER TEXT SEARCH CONFIGURATION synonym_tst ALTER MAPPING FOR asciiword WITH synonym, english_stem",
        )
        .await
        .expect("map synonym configuration");
    client
        .simple_query("CREATE TEXT SEARCH CONFIGURATION thesaurus_tst (COPY = synonym_tst)")
        .await
        .expect("copy synonym configuration");
    client
        .simple_query(
            "ALTER TEXT SEARCH CONFIGURATION thesaurus_tst ALTER MAPPING FOR asciiword WITH synonym, thesaurus, english_stem",
        )
        .await
        .expect("map thesaurus configuration");
    assert2::assert!(
        scalar(
            &client,
            "SELECT to_tsvector('thesaurus_tst', 'one postgres one two one two three one')"
        )
        .await
        .as_deref()
            == Some("'1':1,5 '12':3 '123':4 'pgsql':2")
    );
    assert2::assert!(
        scalar(&client, "SELECT to_tsvector('thesaurus_tst', 'Booking tickets is looking like a booking a tickets')")
            .await
            .as_deref()
            == Some("'card':3,10 'invit':2,9 'like':6 'look':5 'order':1,8")
    );
    assert2::assert!(
        scalar(&client, "SELECT to_tsvector('thesaurus_tst', 'Supernovae star is very new star and usually called supernovae (abbreviation SN)')")
            .await
            .as_deref()
            == Some("'abbrevi':10 'call':8 'new':4 'sn':1,9,11 'star':5 'usual':7")
    );
    client
        .simple_query("CREATE TEXT SEARCH CONFIGURATION mapping_tst (COPY = english)")
        .await
        .expect("create mapping configuration");
    client
        .simple_query(
            "ALTER TEXT SEARCH CONFIGURATION mapping_tst ADD MAPPING FOR word WITH ispell",
        )
        .await
        .expect("add mapping");
    assert2::assert!(
        scalar(&client, "SELECT to_tsvector('mapping_tst', 'skies')")
            .await
            .as_deref()
            == Some("'sky':1")
    );
    client
        .simple_query("ALTER TEXT SEARCH CONFIGURATION mapping_tst DROP MAPPING FOR word")
        .await
        .expect("drop mapping");
    let error = client
        .simple_query("ALTER TEXT SEARCH CONFIGURATION mapping_tst DROP MAPPING FOR word")
        .await
        .expect_err("missing mapping errors without IF EXISTS");
    assert2::assert!(
        error
            .as_db_error()
            .map(|error| error.message().to_owned())
            .as_deref()
            == Some("mapping for token type \"word\" does not exist")
    );
    client
        .simple_query("ALTER TEXT SEARCH CONFIGURATION mapping_tst DROP MAPPING IF EXISTS FOR word")
        .await
        .expect("missing mapping is accepted with IF EXISTS");
    for sql in [
        "ALTER TEXT SEARCH CONFIGURATION mapping_tst DROP MAPPING FOR not_a_token, not_a_token",
        "ALTER TEXT SEARCH CONFIGURATION mapping_tst DROP MAPPING IF EXISTS FOR not_a_token, not_a_token",
        "ALTER TEXT SEARCH CONFIGURATION mapping_tst ADD MAPPING FOR not_a_token WITH ispell",
    ] {
        let error = client
            .simple_query(sql)
            .await
            .expect_err("unknown token types are always refused");
        assert2::assert!(
            error
                .as_db_error()
                .map(|error| error.message().to_owned())
                .as_deref()
                == Some("token type \"not_a_token\" does not exist")
        );
    }
    let error = client
        .simple_query(
            "CREATE TEXT SEARCH DICTIONARY ispell_case (TEMPLATE = ispell, \"DictFile\" = ispell_sample, AffFile = ispell_sample)",
        )
        .await
        .expect_err("quoted ISpell options keep their case and are rejected");
    assert2::assert!(
        error
            .as_db_error()
            .map(|error| error.message().to_owned())
            .as_deref()
            == Some("unrecognized Ispell parameter: \"DictFile\"")
    );

    for name in ["snowball"] {
        let refused = client
            .simple_query(&format!("SELECT ts_lexize('{name}', 'skies')"))
            .await
            .expect_err("a dictionary crabka does not have must be refused");
        assert2::assert!(refused.code().map(|c| c.code().to_owned()) == Some("42704".to_owned()));
        assert2::assert!(
            refused
                .as_db_error()
                .map(|e| e.message().to_owned())
                .as_deref()
                == Some(&*format!(
                    "text search dictionary \"{name}\" does not exist"
                ))
        );
    }
}
