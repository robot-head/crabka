//! The object-identifier (`reg*`) type family and the `to_reg*` functions.
//!
//! The contract these pin is the one `regproc.sql` is built around: the cast
//! form raises when the object does not exist and `to_reg*` answers NULL for
//! the *same* input, so every positive case here has a negative twin and every
//! error is checked as `(SQLSTATE, message)` rather than "it failed".
//!
//! Every expectation was captured from the pinned `PostgreSQL` 18.4 build,
//! including the asymmetries that look like typos: `regoper` prints `0` for
//! `InvalidOid` where the other nine print `-`, `regproc` refuses an overloaded
//! name that `regprocedure` resolves, and `regoper`'s ambiguity message is the
//! only one in the family that does not quote the name.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// One session, so a `CREATE` and the statements that depend on it stay
/// together.
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
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .into_iter()
            .next_back()
            .expect("at least one result")
    }

    /// The first column of the single row `sql` returns.
    async fn scalar(&mut self, sql: &str) -> Option<String> {
        let result = self.run(sql).await;
        let QueryResult::Rows { rows, .. } = &result else {
            panic!("expected rows from {sql}, got {result:?}");
        };
        assert!(rows.len() == 1, "{sql}");
        rows[0][0]
            .as_ref()
            .map(|cell: &Cell| String::from_utf8(cell.text.to_vec()).expect("utf-8 cell"))
    }
}

/// The `(SQLSTATE, message)` of a statement that is refused.
async fn fails(client: &mut Client, sql: &str) -> (String, String) {
    let error = client
        .session
        .simple_query(sql)
        .await
        .expect_err("statement is refused");
    (error.code, error.message)
}

/// Every spelling that must round-trip: the written name and what the type's
/// output function prints for the oid it resolves to.
///
/// `abs(numeric)` and `||/` are chosen because they are the two shapes whose
/// rendering is *not* the input: `regprocedure` and `regoperator` re-spell the
/// operand types through `format_type`.
#[tokio::test]
async fn every_reg_type_round_trips_a_written_name() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, written, printed) in [
        ("regproc", "now", "now"),
        ("regproc", "pg_catalog.now", "now"),
        ("regprocedure", "abs(numeric)", "abs(numeric)"),
        ("regprocedure", "pg_catalog.abs(numeric)", "abs(numeric)"),
        ("regoper", "||/", "||/"),
        ("regoper", "pg_catalog.||/", "||/"),
        ("regoperator", "+(int4,int4)", "+(integer,integer)"),
        (
            "regoperator",
            "pg_catalog.+(int4,int4)",
            "+(integer,integer)",
        ),
        // A prefix operator has no left operand, and `NONE` stands in for it.
        (
            "regoperator",
            "||/(NONE,float8)",
            "||/(NONE,double precision)",
        ),
        ("regclass", "pg_class", "pg_class"),
        ("regtype", "int4", "integer"),
        ("regconfig", "simple", "simple"),
        ("regdictionary", "simple", "simple"),
        ("regnamespace", "pg_catalog", "pg_catalog"),
        ("regnamespace", "\"pg_catalog\"", "pg_catalog"),
        // An uppercase collation name needs quoting on the way in *and* out.
        ("regcollation", "\"POSIX\"", "\"POSIX\""),
        ("regcollation", "pg_catalog.\"POSIX\"", "\"POSIX\""),
        ("regrole", "postgres", "postgres"),
        ("regrole", "\"postgres\"", "postgres"),
    ] {
        let sql = format!("SELECT '{written}'::{ty}::text");
        assert!(client.scalar(&sql).await == Some(printed.into()), "{sql}");
        // The function spelling is the same cast, which is the form
        // `regproc.sql` uses throughout.
        let call = format!("SELECT {ty}('{written}')::text");
        assert!(client.scalar(&call).await == Some(printed.into()), "{call}");
    }
}

/// The cast raises; `to_reg*` answers NULL for the identical input.
///
/// The messages are `PostgreSQL`'s own, and the split across SQLSTATEs is the
/// point: a missing relation is 42P01, a missing function 42883, a missing
/// schema 3F000 and a missing type, role or collation 42704.
#[tokio::test]
async fn a_missing_object_errors_through_the_cast_and_is_null_through_to_reg() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, written, sqlstate, message) in [
        (
            "regproc",
            "know",
            "42883",
            "function \"know\" does not exist",
        ),
        (
            "regproc",
            "ng_catalog.now",
            "42883",
            "function \"ng_catalog.now\" does not exist",
        ),
        (
            "regprocedure",
            "absinthe(numeric)",
            "42883",
            "function \"absinthe(numeric)\" does not exist",
        ),
        (
            "regprocedure",
            "ng_catalog.abs(numeric)",
            "42883",
            "function \"ng_catalog.abs(numeric)\" does not exist",
        ),
        ("regoper", "||//", "42883", "operator does not exist: ||//"),
        (
            "regoper",
            "ng_catalog.||/",
            "42883",
            "operator does not exist: ng_catalog.||/",
        ),
        (
            "regoperator",
            "++(int4,int4)",
            "42883",
            "operator does not exist: ++(int4,int4)",
        ),
        (
            "regoperator",
            "ng_catalog.+(int4,int4)",
            "42883",
            "operator does not exist: ng_catalog.+(int4,int4)",
        ),
        (
            "regclass",
            "pg_classes",
            "42P01",
            "relation \"pg_classes\" does not exist",
        ),
        ("regtype", "int3", "42704", "type \"int3\" does not exist"),
        (
            "regcollation",
            "notacollation",
            "42704",
            "collation \"notacollation\" for encoding \"UTF8\" does not exist",
        ),
        (
            // The quotes are consumed by the name parser, so the message echoes
            // the *parsed* parts — `NameListToString` re-quotes nothing.
            "regcollation",
            "ng_catalog.\"POSIX\"",
            "42704",
            "collation \"ng_catalog.POSIX\" for encoding \"UTF8\" does not exist",
        ),
        (
            "regconfig",
            "no_such_config",
            "42704",
            "text search configuration \"no_such_config\" does not exist",
        ),
        (
            "regdictionary",
            "no_such_dictionary",
            "42704",
            "text search dictionary \"no_such_dictionary\" does not exist",
        ),
        (
            "regrole",
            "nosuchrole",
            "42704",
            "role \"nosuchrole\" does not exist",
        ),
        (
            // An unquoted name folds to lower case before it is looked up, so
            // the message names the folded spelling.
            "regrole",
            "Nonexistent",
            "42704",
            "role \"nonexistent\" does not exist",
        ),
        (
            "regrole",
            "\"Nonexistent\"",
            "42704",
            "role \"Nonexistent\" does not exist",
        ),
        (
            "regnamespace",
            "Nonexistent",
            "3F000",
            "schema \"nonexistent\" does not exist",
        ),
        // `regrole` and `regnamespace` name an object that has no schema, so a
        // dotted name is a syntax error rather than a missing object.
        ("regrole", "foo.bar", "42602", "invalid name syntax"),
        ("regnamespace", "foo.bar", "42602", "invalid name syntax"),
    ] {
        let cast = format!("SELECT '{written}'::{ty}");
        assert!(
            fails(&mut client, &cast).await == (sqlstate.into(), message.into()),
            "{cast}"
        );
        // Only nine of the eleven have a `to_reg*`.
        if matches!(ty, "regconfig" | "regdictionary") {
            continue;
        }
        let soft = format!("SELECT to_{ty}('{written}')");
        assert!(client.scalar(&soft).await == None, "{soft}");
    }
}

/// `regproc` refuses a name that matches more than one overload; `regprocedure`,
/// given the argument types, does not. `regoper` refuses the same way — but its
/// message does not quote the name, because an operator name is not an
/// identifier.
#[tokio::test]
async fn an_ambiguous_name_is_42725_and_to_reg_still_answers_null() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, written, message) in [
        ("regproc", "abs", "more than one function named \"abs\""),
        (
            "regproc",
            "pg_catalog.abs",
            "more than one function named \"pg_catalog.abs\"",
        ),
        ("regoper", "-", "more than one operator named -"),
    ] {
        let cast = format!("SELECT '{written}'::{ty}");
        assert!(
            fails(&mut client, &cast).await == ("42725".into(), message.into()),
            "{cast}"
        );
        let soft = format!("SELECT to_{ty}('{written}')");
        assert!(client.scalar(&soft).await == None, "{soft}");
    }
    // The same names, resolved once the argument types pin one overload.
    assert!(
        client
            .scalar("SELECT 'abs(numeric)'::regprocedure::text")
            .await
            == Some("abs(numeric)".into())
    );
    assert!(
        client
            .scalar("SELECT '-(int4,int4)'::regoperator::text")
            .await
            == Some("-(integer,integer)".into())
    );
}

/// `regoperator`'s own parse errors, which are 22P02 rather than a missing
/// object — and which `to_regoperator` still swallows.
#[tokio::test]
async fn a_malformed_argument_list_is_22p02() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, written, sqlstate, message) in [
        ("regoperator", "-", "22P02", "expected a left parenthesis"),
        (
            "regoperator",
            "+(int4,int4",
            "22P02",
            "expected a right parenthesis",
        ),
        ("regoperator", "+(int4)", "42P02", "missing argument"),
        (
            "regoperator",
            "+(int4,int4,int4)",
            "54023",
            "too many arguments",
        ),
        (
            "regprocedure",
            "abs(numeric",
            "22P02",
            "expected a right parenthesis",
        ),
        (
            "regprocedure",
            "abs(nosuchtype)",
            "42704",
            "type \"nosuchtype\" does not exist",
        ),
    ] {
        let cast = format!("SELECT '{written}'::{ty}");
        assert!(
            fails(&mut client, &cast).await == (sqlstate.into(), message.into()),
            "{cast}"
        );
    }
    // 22P02 is soft, so `to_reg*` absorbs it; 42P02 and 54023 are not, and
    // PostgreSQL propagates them too.
    assert!(client.scalar("SELECT to_regoperator('-')").await == None);
    assert!(client.scalar("SELECT to_regprocedure('abs(numeric')").await == None);
}

/// `InvalidOid` and an oid no catalog row matches, per type.
///
/// The two `0` answers are not a typo: `regoperout`/`regoperatorout` print `0`
/// where the other nine print `-`, and their input functions have no `-`
/// shortcut at all — `'-'::regoper` is an *operator name*.
#[tokio::test]
async fn invalid_and_unknown_oids_render_per_type() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, invalid) in [
        ("regproc", "-"),
        ("regprocedure", "-"),
        ("regoper", "0"),
        ("regoperator", "0"),
        ("regclass", "-"),
        ("regtype", "-"),
        ("regconfig", "-"),
        ("regdictionary", "-"),
        ("regnamespace", "-"),
        ("regrole", "-"),
        ("regcollation", "-"),
    ] {
        let sql = format!("SELECT 0::oid::{ty}::text");
        assert!(client.scalar(&sql).await == Some(invalid.into()), "{sql}");
        // An oid no row matches falls back to `oidout`, which is UNSIGNED — so
        // the largest oid prints as itself rather than as -1.
        for oid in ["999999", "4294967295"] {
            let sql = format!("SELECT {oid}::{ty}::text");
            assert!(client.scalar(&sql).await == Some(oid.into()), "{sql}");
        }
        // `'-'` is `InvalidOid` for nine of the eleven.
        if matches!(ty, "regoper" | "regoperator") {
            continue;
        }
        let sql = format!("SELECT '-'::{ty}::text");
        assert!(client.scalar(&sql).await == Some(invalid.into()), "{sql}");
    }
    // `regoper` reads `-` as a name, and there is more than one operator so
    // spelled.
    assert!(
        fails(&mut client, "SELECT '-'::regoper").await
            == ("42725".into(), "more than one operator named -".into())
    );
}

/// Every member reports its own type, and its identity is the oid — so a value
/// compares, orders and groups as the integer it is.
#[tokio::test]
async fn each_type_reports_itself_and_compares_as_an_oid() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ty in [
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regclass",
        "regtype",
        "regconfig",
        "regdictionary",
        "regnamespace",
        "regrole",
        "regcollation",
    ] {
        let sql = format!("SELECT pg_typeof(1::oid::{ty})");
        assert!(client.scalar(&sql).await == Some(ty.into()), "{sql}");
        let sql = format!("SELECT (1::oid::{ty})::oid");
        assert!(client.scalar(&sql).await == Some("1".into()), "{sql}");
        let sql = format!("SELECT 1::oid::{ty} = 1::oid::{ty}");
        assert!(client.scalar(&sql).await == Some("t".into()), "{sql}");
        let sql = format!("SELECT 1::oid::{ty} < 2::oid::{ty}");
        assert!(client.scalar(&sql).await == Some("t".into()), "{sql}");
    }
    // The oid crosses the spellings, which is what makes a catalog join work.
    assert!(client.scalar("SELECT 1299 = 'now'::regproc").await == Some("t".into()));
    assert!(client.scalar("SELECT 597 = '||/'::regoper").await == Some("t".into()));
    assert!(
        client
            .scalar("SELECT 951 = '\"POSIX\"'::regcollation")
            .await
            == Some("t".into())
    );
}

/// A `reg*` column stores its oid and reads back as the name — which is what
/// makes the type worth having on a table rather than only in an expression.
#[tokio::test]
async fn a_reg_column_stores_the_oid_and_reads_back_the_name() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (ty, written, printed) in [
        ("regproc", "now", "now"),
        ("regprocedure", "abs(numeric)", "abs(numeric)"),
        ("regoper", "||/", "||/"),
        ("regoperator", "+(int4,int4)", "+(integer,integer)"),
        ("regtype", "int4", "integer"),
        ("regconfig", "simple", "simple"),
        ("regdictionary", "simple", "simple"),
        ("regnamespace", "pg_catalog", "pg_catalog"),
        ("regrole", "postgres", "postgres"),
        ("regcollation", "\"POSIX\"", "\"POSIX\""),
    ] {
        client.run(&format!("CREATE TABLE t_{ty}(a {ty})")).await;
        client
            .run(&format!("INSERT INTO t_{ty} VALUES ('{written}'::{ty})"))
            .await;
        let sql = format!("SELECT a::text FROM t_{ty}");
        assert!(client.scalar(&sql).await == Some(printed.into()), "{sql}");
        // `\d` reads the column type through `format_type`, which must name it.
        let sql = format!(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = 't_{ty}'::regclass AND attnum = 1"
        );
        assert!(client.scalar(&sql).await == Some(ty.into()), "{sql}");
    }
}

/// Renaming the object a stored value names changes what the value prints,
/// because the name is derived on the way out rather than stored — the property
/// that makes `regclass` follow a `RENAME` in `PostgreSQL`.
#[tokio::test]
async fn a_stored_value_follows_a_rename_and_falls_back_when_dropped() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE target(a int)").await;
    client.run("CREATE TABLE holder(a regclass)").await;
    client
        .run("INSERT INTO holder VALUES ('target'::regclass)")
        .await;
    assert!(client.scalar("SELECT a::text FROM holder").await == Some("target".into()));
    client.run("ALTER TABLE target RENAME TO renamed").await;
    assert!(client.scalar("SELECT a::text FROM holder").await == Some("renamed".into()));
    let oid = client
        .scalar("SELECT a::oid::text FROM holder")
        .await
        .expect("stored oid");
    client.run("DROP TABLE renamed").await;
    assert!(client.scalar("SELECT a::text FROM holder").await == Some(oid));
}

/// The `to_reg*` functions are declared over `text`, so anything else is 42883
/// — `PostgreSQL` has no implicit conversion into `text`.
#[tokio::test]
async fn to_reg_rejects_a_non_text_argument() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for name in [
        "to_regproc",
        "to_regprocedure",
        "to_regoper",
        "to_regoperator",
        "to_regclass",
        "to_regtype",
        "to_regtypemod",
        "to_regnamespace",
        "to_regrole",
        "to_regcollation",
    ] {
        for (argument, spelled) in [
            ("1::int4", "integer"),
            ("1::int8", "bigint"),
            ("true", "boolean"),
            ("1.5", "numeric"),
            ("'2020-01-01'::date", "date"),
        ] {
            let sql = format!("SELECT {name}({argument})");
            assert!(
                fails(&mut client, &sql).await
                    == (
                        "42883".into(),
                        format!("function {name}({spelled}) does not exist")
                    ),
                "{sql}"
            );
        }
        // Wrong arity is the same 42883, with the argument types spelled out.
        let sql = format!("SELECT {name}()");
        assert!(
            fails(&mut client, &sql).await
                == ("42883".into(), format!("function {name}() does not exist")),
            "{sql}"
        );
        let sql = format!("SELECT {name}('a','b')");
        assert!(
            fails(&mut client, &sql).await
                == (
                    "42883".into(),
                    format!("function {name}(unknown, unknown) does not exist")
                ),
            "{sql}"
        );
        // A `text` argument is accepted, which is the declared signature.
        let sql = format!("SELECT {name}('nosuchthing'::text)");
        assert!(client.scalar(&sql).await == None, "{sql}");
    }
    // `regconfig` and `regdictionary` have no `to_reg*` in PostgreSQL.
    for name in ["to_regconfig", "to_regdictionary"] {
        let sql = format!("SELECT {name}('simple')");
        assert!(fails(&mut client, &sql).await.0 == "42883", "{sql}");
    }
}

/// The type-as-function spelling is a *cast*, and `PostgreSQL` reads it as one
/// only for a binary coercion or a string. `1::int8::regclass` is fine while
/// `regclass(1::int8)` is not, because `pg_cast` reaches `regclass` from
/// `bigint` through a conversion function rather than a relabel.
#[tokio::test]
async fn the_type_as_function_form_accepts_only_binary_coercible_arguments() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ty in [
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regclass",
        "regtype",
        "regconfig",
        "regdictionary",
        "regnamespace",
        "regrole",
        "regcollation",
    ] {
        for (argument, spelled) in [
            ("1::int8", "bigint"),
            ("true", "boolean"),
            ("1.5", "numeric"),
            ("'2020-01-01'::date", "date"),
        ] {
            let sql = format!("SELECT {ty}({argument})");
            assert!(
                fails(&mut client, &sql).await
                    == (
                        "42883".into(),
                        format!("function {ty}({spelled}) does not exist")
                    ),
                "{sql}"
            );
        }
        // `int4` and `oid` are binary-coercible, so both are read as the oid.
        for argument in ["1::int4", "1::oid"] {
            let sql = format!("SELECT {ty}({argument})::text");
            assert!(client.scalar(&sql).await == Some("1".into()), "{sql}");
        }
        // The cast operator is wider than the function spelling, exactly as in
        // PostgreSQL.
        let sql = format!("SELECT 1::int8::{ty}::text");
        assert!(client.scalar(&sql).await == Some("1".into()), "{sql}");
    }
}

/// The four types that existed before this family was completed, as a floor.
#[tokio::test]
async fn the_pre_existing_four_are_unchanged() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (sql, expected) in [
        ("SELECT 'pg_class'::regclass::text", "pg_class"),
        ("SELECT 'pg_class'::regclass::oid", "1259"),
        ("SELECT 'int4'::regtype::text", "integer"),
        ("SELECT 'int4'::regtype::oid", "23"),
        ("SELECT 'anyrange'::regtype::text", "anyrange"),
        ("SELECT 'abs(numeric)'::regprocedure::text", "abs(numeric)"),
        ("SELECT 'boolin(cstring)'::regprocedure::oid", "1242"),
        ("SELECT 'pg_catalog'::regnamespace::text", "pg_catalog"),
        ("SELECT 'pg_catalog'::regnamespace::oid", "11"),
        ("SELECT 'public'::regnamespace::text", "public"),
        ("SELECT 999999::regclass::text", "999999"),
        ("SELECT 999999::regnamespace::text", "999999"),
        ("SELECT 23 = 'integer'::regtype", "t"),
        (
            "SELECT 2200::pg_catalog.regnamespace::pg_catalog.text",
            "public",
        ),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
    for (sql, sqlstate, message) in [
        (
            "SELECT 'nosuchrel'::regclass",
            "42P01",
            "relation \"nosuchrel\" does not exist",
        ),
        (
            "SELECT 'nosuchtype'::regtype",
            "42704",
            "type \"nosuchtype\" does not exist",
        ),
        (
            "SELECT 'nosuchschema'::regnamespace",
            "3F000",
            "schema \"nosuchschema\" does not exist",
        ),
        (
            "SELECT 'nosuchfn(int4)'::regprocedure",
            "42883",
            "function \"nosuchfn(int4)\" does not exist",
        ),
    ] {
        assert!(
            fails(&mut client, sql).await == (sqlstate.into(), message.into()),
            "{sql}"
        );
    }
}

/// A `reg*` input function reached through `pg_input_error_info` reports the
/// same missing-object error the cast does, rather than the generic
/// `invalid input syntax` the pure value cast would.
///
/// The last two rows are the boundary this shares with `to_reg*`: a *hard*
/// error propagates out of the soft-error API too, which is how `PostgreSQL`'s
/// own `regproc.sql` records them.
#[tokio::test]
async fn the_soft_error_api_reports_the_input_functions_own_error() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (input, ty, message, sqlstate) in [
        (
            "ng_catalog.pg_class",
            "regclass",
            "relation \"ng_catalog.pg_class\" does not exist",
            "42P01",
        ),
        (
            "no_such_config",
            "regconfig",
            "text search configuration \"no_such_config\" does not exist",
            "42704",
        ),
        (
            "Nonexistent",
            "regnamespace",
            "schema \"nonexistent\" does not exist",
            "3F000",
        ),
        (
            "ng_catalog.||/",
            "regoper",
            "operator does not exist: ng_catalog.||/",
            "42883",
        ),
        ("-", "regoper", "more than one operator named -", "42725"),
        ("-", "regoperator", "expected a left parenthesis", "22P02"),
        (
            "ng_catalog.now",
            "regproc",
            "function \"ng_catalog.now\" does not exist",
            "42883",
        ),
        (
            "no_such_type",
            "regtype",
            "type \"no_such_type\" does not exist",
            "42704",
        ),
    ] {
        let sql = format!(
            "SELECT message || '|' || sql_error_code FROM pg_input_error_info('{input}', '{ty}')"
        );
        assert!(
            client.scalar(&sql).await == Some(format!("{message}|{sqlstate}")),
            "{sql}"
        );
    }
    // A valid input reports no error at all.
    assert!(
        client
            .scalar("SELECT message FROM pg_input_error_info('pg_class', 'regclass')")
            .await
            == None
    );
    assert!(
        client
            .scalar("SELECT pg_input_is_valid('pg_class', 'regclass')")
            .await
            == Some("t".into())
    );
    assert!(
        client
            .scalar("SELECT pg_input_is_valid('nope', 'regclass')")
            .await
            == Some("f".into())
    );
    for (input, sqlstate, message) in [
        (
            "way.too.many.names",
            "42601",
            "improper qualified name (too many dotted names): way.too.many.names",
        ),
        (
            "no_such_catalog.schema.name",
            "0A000",
            "cross-database references are not implemented: no_such_catalog.schema.name",
        ),
    ] {
        let sql = format!("SELECT * FROM pg_input_error_info('{input}', 'regtype')");
        assert!(
            fails(&mut client, &sql).await == (sqlstate.into(), message.into()),
            "{sql}"
        );
    }
}

/// The shared helpers this family reaches into, exercised through paths that
/// name no `reg*` type at all.
///
/// `func::input_error` gained a `reg*` branch, `func::int_arg` gained a `reg*`
/// arm, `func::builtin_format_type` gained eleven rows, `exec::resolve_type_name`
/// became schema-aware, and `exec::coerce` gained a `reg*` assignment arm. Each
/// is checked here on an argument that is not an object identifier, because a
/// corpus that only ever feeds the new types their own values cannot see a
/// regression in any of them.
#[tokio::test]
async fn the_shared_helpers_still_answer_for_other_types() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    // `input_error` over the types it already served.
    for (input, ty, message) in [
        (
            "abc",
            "int4",
            "invalid input syntax for type integer: \"abc\"",
        ),
        ("(1,4", "int4range", "malformed range literal: \"(1,4\""),
        (
            "01010001",
            "bit(10)",
            "bit string length 8 does not match type bit(10)",
        ),
    ] {
        let sql = format!("SELECT message FROM pg_input_error_info('{input}', '{ty}')");
        assert!(client.scalar(&sql).await == Some(message.into()), "{sql}");
    }
    for (input, ty, valid) in [
        ("1", "int4", "t"),
        ("x", "int4", "f"),
        ("2020-01-01", "date", "t"),
        ("nope", "date", "f"),
        ("{\"a\": 1}", "jsonb", "t"),
    ] {
        let sql = format!("SELECT pg_input_is_valid('{input}', '{ty}')");
        assert!(client.scalar(&sql).await == Some(valid.into()), "{sql}");
    }
    // `int_arg`: the oid and integer spellings a catalog function takes, and the
    // types it must still refuse.
    client.run("CREATE TABLE helper(a int, b text)").await;
    client
        .run("CREATE VIEW helperv AS SELECT a FROM helper")
        .await;
    assert!(
        client
            .scalar("SELECT pg_get_viewdef('helperv')")
            .await
            .is_some()
    );
    assert!(
        client
            .scalar("SELECT pg_get_viewdef('helperv'::regclass)")
            .await
            == client.scalar("SELECT pg_get_viewdef('helperv')").await
    );
    assert!(
        fails(&mut client, "SELECT pg_get_viewdef(true)").await
            == (
                "42804".into(),
                "function does not accept an argument of type boolean".into()
            )
    );
    // `builtin_format_type` over the oids it already knew, through each of the
    // spellings `format_type`'s declared `oid` argument accepts.
    for (oid, name) in [
        ("23", "integer"),
        ("25", "text"),
        ("16", "boolean"),
        ("1700", "numeric"),
        ("1082", "date"),
    ] {
        for spelling in [
            oid.to_string(),
            format!("{oid}::oid"),
            format!("{oid}::regtype"),
        ] {
            let sql = format!("SELECT format_type({spelling}, -1)");
            assert!(client.scalar(&sql).await == Some(name.into()), "{sql}");
        }
    }
    assert!(client.scalar("SELECT format_type(NULL, -1)").await == None);
    assert!(
        fails(&mut client, "SELECT format_type('x'::text, -1)")
            .await
            .0
            == "42883"
    );
    // `coerce`: ordinary assignment is unchanged in both directions.
    client
        .run("INSERT INTO helper VALUES (1, 'x'), (2, 'y')")
        .await;
    assert!(client.scalar("SELECT count(*)::text FROM helper").await == Some("2".into()));
    assert!(
        fails(
            &mut client,
            "INSERT INTO helper VALUES ('2020-01-01'::date, 'z')"
        )
        .await
        .0 == "42804"
    );
    // Ordinary scalar work, which shares the function resolver this family
    // dispatches ahead of.
    for (sql, expected) in [
        ("SELECT 1 + 1", "2"),
        ("SELECT abs(-3)", "3"),
        ("SELECT length('abc')", "3"),
        ("SELECT upper('ab')", "AB"),
        ("SELECT true AND false", "f"),
        ("SELECT 'a' || 'b'", "ab"),
        ("SELECT substring('abcdef' from 2 for 3)", "bcd"),
        ("SELECT pg_typeof(1)", "integer"),
        ("SELECT pg_typeof('x'::text)", "text"),
        ("SELECT pg_typeof(true)", "boolean"),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
    assert!(fails(&mut client, "SELECT nosuchfunc(1)").await.0 == "42883");
}

/// `pg_type` describes every member, because `type_sanity` and every driver's
/// type cache read the catalog rather than the executor's table.
#[tokio::test]
async fn pg_type_describes_the_whole_family() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (name, oid, array) in [
        ("regproc", "24", "1008"),
        ("regprocedure", "2202", "2207"),
        ("regoper", "2203", "2208"),
        ("regoperator", "2204", "2209"),
        ("regclass", "2205", "2210"),
        ("regtype", "2206", "2211"),
        ("regconfig", "3734", "3735"),
        ("regdictionary", "3769", "3770"),
        ("regnamespace", "4089", "4090"),
        ("regrole", "4096", "4097"),
        ("regcollation", "4191", "4192"),
    ] {
        let sql = format!(
            "SELECT oid || '|' || typlen || '|' || typcategory || '|' || typarray \
             FROM pg_type WHERE typname = '{name}'"
        );
        assert!(
            client.scalar(&sql).await == Some(format!("{oid}|4|N|{array}")),
            "{sql}"
        );
        let sql = format!("SELECT format_type({oid}, -1)");
        assert!(client.scalar(&sql).await == Some(name.into()), "{sql}");
    }
}

/// `to_regtype` and `to_regtypemod` are the two halves of one parse: the first
/// keeps the oid the type name resolves to and the second keeps the modifier
/// the name carried, which is why they are always written as a pair.
///
/// Two of the answers here are not in the string at all. Bare `bit` is the
/// grammar's `BIT`, which means `bit(1)`, and bare `character` is
/// `character(1)`; `"bit"` in quotes and `bpchar` under `pg_type`'s own name
/// are ordinary catalog lookups for those same two types and carry nothing.
/// Everything else is the type's own `typmodin`: `varchar` and `character` add
/// the four-byte varlena header to the declared length, the date/time types
/// store the fractional-seconds precision raw, and `numeric` packs precision
/// above scale.
#[tokio::test]
async fn to_regtypemod_answers_the_modifier_the_name_carries() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (written, expected) in [
        ("text", Some("-1")),
        ("int4", Some("-1")),
        ("timestamp(4)", Some("4")),
        ("timestamptz(2)", Some("2")),
        ("time(0)", Some("0")),
        ("varchar(32)", Some("36")),
        ("character varying(32)", Some("36")),
        ("character(10)", Some("14")),
        // The implicit `(1)` belongs to the `CHARACTER` keyword, not to the
        // type: `bpchar` is the same type under `pg_type`'s own name and
        // carries nothing.
        ("character", Some("5")),
        ("bpchar", Some("-1")),
        ("bpchar(10)", Some("14")),
        ("numeric(10,2)", Some("655366")),
        ("numeric(10)", Some("655364")),
        ("bit", Some("1")),
        ("\"bit\"", Some("-1")),
        ("bit(4)", Some("4")),
        ("varbit", Some("-1")),
        ("varbit(4)", Some("4")),
        ("double precision", Some("-1")),
        // A type nothing declares is the same soft 42704 the cast reports, so
        // the modifier half answers NULL just as `to_regtype` does.
        ("no_such_type(4)", None),
        ("no_such_type", None),
        // The empty string is the one refusal inside `typeStringToTypeName`
        // that is checked before the parser runs, so it stays soft where the
        // grammar's own refusals do not.
        ("", None),
        ("   ", None),
    ] {
        let sql = format!("SELECT to_regtypemod('{written}')");
        assert!(
            client.scalar(&sql).await == expected.map(Into::into),
            "{sql}"
        );
    }
}

/// `format_type(to_regtype(x), to_regtypemod(x))` reads back as `x` did — the
/// round trip the pair exists for.
#[tokio::test]
async fn a_type_name_survives_the_round_trip_through_the_pair() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (written, expected) in [
        ("varchar(32)", "character varying(32)"),
        ("bit", "bit(1)"),
        ("\"bit\"", "\"bit\""),
        ("timestamp(4)", "timestamp(4) without time zone"),
        ("numeric(10,2)", "numeric(10,2)"),
        ("text", "text"),
    ] {
        let sql =
            format!("SELECT format_type(to_regtype('{written}'), to_regtypemod('{written}'))");
        assert!(client.scalar(&sql).await == Some(expected.into()), "{sql}");
    }
}

/// `format_type`'s second argument distinguishes "the modifier is -1" from "no
/// modifier was supplied", and for `bit` and `bpchar` those are different
/// names. Both spell a type whose bare keyword would come back decorated, so
/// the modifier-stated form has to name it in a spelling the parser will not
/// re-decorate.
#[tokio::test]
async fn format_type_distinguishes_a_stated_modifier_from_no_modifier() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (sql, expected) in [
        ("SELECT format_type('bpchar'::regtype, NULL)", "character"),
        ("SELECT format_type('bpchar'::regtype, -1)", "bpchar"),
        ("SELECT format_type('bpchar'::regtype, 14)", "character(10)"),
        ("SELECT format_type('bit'::regtype, NULL)", "bit"),
        ("SELECT format_type('bit'::regtype, -1)", "\"bit\""),
        ("SELECT format_type('bit'::regtype, 4)", "bit(4)"),
        // Every other type names itself the same way either side of the line.
        (
            "SELECT format_type('varchar'::regtype, NULL)",
            "character varying",
        ),
        (
            "SELECT format_type('varchar'::regtype, -1)",
            "character varying",
        ),
        (
            "SELECT format_type('varchar'::regtype, 42)",
            "character varying(38)",
        ),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
}

/// The type grammar's own refusals are `ereport`, not `ereturn`, so they escape
/// `to_reg*` instead of turning into NULL. `PostgreSQL`'s `regproc.sql` files all
/// of these under "Some cases that should be soft errors, but are not yet",
/// and crabka matches it rather than being quietly more forgiving.
#[tokio::test]
async fn a_string_that_is_not_a_type_name_is_a_hard_error() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (written, expected) in [
        (
            "incorrect type name syntax",
            ("42601", "syntax error at or near \"type\""),
        ),
        ("numeric(1,2,3)", ("22023", "invalid NUMERIC type modifier")),
        (
            "numeric(0)",
            ("22023", "NUMERIC precision 0 must be between 1 and 1000"),
        ),
        (
            "varchar(0)",
            ("22023", "length for type varchar must be at least 1"),
        ),
        (
            "bit(0)",
            ("22023", "length for type bit must be at least 1"),
        ),
        (
            "int4(4)",
            ("42601", "type modifier is not allowed for type \"int4\""),
        ),
        (
            "way.too.many.names",
            (
                "42601",
                "improper qualified name (too many dotted names): way.too.many.names",
            ),
        ),
    ] {
        for spelled in [
            format!("SELECT to_regtype('{written}')"),
            format!("SELECT to_regtypemod('{written}')"),
        ] {
            let (code, message) = fails(&mut client, &spelled).await;
            assert!(
                (code.as_str(), message.as_str()) == expected,
                "{spelled}: {code} {message}"
            );
        }
    }
}
