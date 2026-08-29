//! Keyword classes: which of `PostgreSQL`'s 494 keywords may be a name.
//!
//! `PostgreSQL` sorts every keyword into one of four classes in
//! `src/include/parser/kwlist.h`, and the class decides where the word may be
//! written unquoted. An `unreserved_keyword` and a `col_name_keyword` are both
//! `ColId`s: each may name a column, name a table and stand as an alias. A
//! `type_func_name_keyword` and a `reserved_keyword` may do none of those.
//!
//! This lexer promotes about ninety words to keyword tokens so the productions
//! that need them can match on the token kind, and most of those words are
//! `ColId`s in `PostgreSQL`. The cases here read the whole class back through a
//! real path rather than asking the parser what it thinks: the column names
//! come from `information_schema.columns` in `ordinal_position` order and the
//! table names from `information_schema.tables`, both of which the oracle was
//! checked with.
//!
//! Every class list below is written once and shared by every case, so a word
//! cannot be tested in one position and forgotten in another.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// The 330 words `PostgreSQL` 18.4 classifies `unreserved_keyword`. Each may
/// be a column name, a table name and an alias.
///
/// Transcribed from `src/include/parser/kwlist.h`, the one file that gives every
/// keyword its class, and kept in that file's order.
const UNRESERVED_KEYWORDS: &[&str] = &[
    "abort",
    "absent",
    "absolute",
    "access",
    "action",
    "add",
    "admin",
    "after",
    "aggregate",
    "also",
    "alter",
    "always",
    "asensitive",
    "assertion",
    "assignment",
    "at",
    "atomic",
    "attach",
    "attribute",
    "backward",
    "before",
    "begin",
    "breadth",
    "by",
    "cache",
    "call",
    "called",
    "cascade",
    "cascaded",
    "catalog",
    "chain",
    "characteristics",
    "checkpoint",
    "class",
    "close",
    "cluster",
    "columns",
    "comment",
    "comments",
    "commit",
    "committed",
    "compression",
    "conditional",
    "configuration",
    "conflict",
    "connection",
    "constraints",
    "content",
    "continue",
    "conversion",
    "copy",
    "cost",
    "csv",
    "cube",
    "current",
    "cursor",
    "cycle",
    "data",
    "database",
    "day",
    "deallocate",
    "declare",
    "defaults",
    "deferred",
    "definer",
    "delete",
    "delimiter",
    "delimiters",
    "depends",
    "depth",
    "detach",
    "dictionary",
    "disable",
    "discard",
    "document",
    "domain",
    "double",
    "drop",
    "each",
    "empty",
    "enable",
    "encoding",
    "encrypted",
    "enforced",
    "enum",
    "error",
    "escape",
    "event",
    "exclude",
    "excluding",
    "exclusive",
    "execute",
    "explain",
    "expression",
    "extension",
    "external",
    "family",
    "filter",
    "finalize",
    "first",
    "following",
    "force",
    "format",
    "forward",
    "function",
    "functions",
    "generated",
    "global",
    "granted",
    "groups",
    "handler",
    "header",
    "hold",
    "hour",
    "identity",
    "if",
    "immediate",
    "immutable",
    "implicit",
    "import",
    "include",
    "including",
    "increment",
    "indent",
    "index",
    "indexes",
    "inherit",
    "inherits",
    "inline",
    "input",
    "insensitive",
    "insert",
    "instead",
    "invoker",
    "isolation",
    "keep",
    "key",
    "keys",
    "label",
    "language",
    "large",
    "last",
    "leakproof",
    "level",
    "listen",
    "load",
    "local",
    "location",
    "lock",
    "locked",
    "logged",
    "mapping",
    "match",
    "matched",
    "materialized",
    "maxvalue",
    "merge",
    "method",
    "minute",
    "minvalue",
    "mode",
    "month",
    "move",
    "name",
    "names",
    "nested",
    "new",
    "next",
    "nfc",
    "nfd",
    "nfkc",
    "nfkd",
    "no",
    "normalized",
    "nothing",
    "notify",
    "nowait",
    "nulls",
    "object",
    "objects",
    "of",
    "off",
    "oids",
    "old",
    "omit",
    "operator",
    "option",
    "options",
    "ordinality",
    "others",
    "over",
    "overriding",
    "owned",
    "owner",
    "parallel",
    "parameter",
    "parser",
    "partial",
    "partition",
    "passing",
    "password",
    "path",
    "period",
    "plan",
    "plans",
    "policy",
    "preceding",
    "prepare",
    "prepared",
    "preserve",
    "prior",
    "privileges",
    "procedural",
    "procedure",
    "procedures",
    "program",
    "publication",
    "quote",
    "quotes",
    "range",
    "read",
    "reassign",
    "recursive",
    "ref",
    "referencing",
    "refresh",
    "reindex",
    "relative",
    "release",
    "rename",
    "repeatable",
    "replace",
    "replica",
    "reset",
    "restart",
    "restrict",
    "return",
    "returns",
    "revoke",
    "role",
    "rollback",
    "rollup",
    "routine",
    "routines",
    "rows",
    "rule",
    "savepoint",
    "scalar",
    "schema",
    "schemas",
    "scroll",
    "search",
    "second",
    "security",
    "sequence",
    "sequences",
    "serializable",
    "server",
    "session",
    "set",
    "sets",
    "share",
    "show",
    "simple",
    "skip",
    "snapshot",
    "source",
    "sql",
    "stable",
    "standalone",
    "start",
    "statement",
    "statistics",
    "stdin",
    "stdout",
    "storage",
    "stored",
    "strict",
    "string",
    "strip",
    "subscription",
    "support",
    "sysid",
    "system",
    "tables",
    "tablespace",
    "target",
    "temp",
    "template",
    "temporary",
    "text",
    "ties",
    "transaction",
    "transform",
    "trigger",
    "truncate",
    "trusted",
    "type",
    "types",
    "uescape",
    "unbounded",
    "uncommitted",
    "unconditional",
    "unencrypted",
    "unknown",
    "unlisten",
    "unlogged",
    "until",
    "update",
    "vacuum",
    "valid",
    "validate",
    "validator",
    "value",
    "varying",
    "version",
    "view",
    "views",
    "virtual",
    "volatile",
    "whitespace",
    "within",
    "without",
    "work",
    "wrapper",
    "write",
    "xml",
    "year",
    "yes",
    "zone",
];

/// The 63 words `PostgreSQL` 18.4 classifies `col_name_keyword`. Each may be a
/// column name, a table name and an alias — everything an
/// [`UNRESERVED_KEYWORDS`] word may be except a type or function name.
const COL_NAME_KEYWORDS: &[&str] = &[
    "between",
    "bigint",
    "bit",
    "boolean",
    "char",
    "character",
    "coalesce",
    "dec",
    "decimal",
    "exists",
    "extract",
    "float",
    "greatest",
    "grouping",
    "inout",
    "int",
    "integer",
    "interval",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "least",
    "merge_action",
    "national",
    "nchar",
    "none",
    "normalize",
    "nullif",
    "numeric",
    "out",
    "overlay",
    "position",
    "precision",
    "real",
    "row",
    "setof",
    "smallint",
    "substring",
    "time",
    "timestamp",
    "treat",
    "trim",
    "values",
    "varchar",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// The 23 words `PostgreSQL` 18.4 classifies `type_func_name_keyword`. None may
/// be a column name, a table name or an alias.
const TYPE_FUNC_NAME_KEYWORDS: &[&str] = &[
    "authorization",
    "binary",
    "collation",
    "concurrently",
    "cross",
    "current_schema",
    "freeze",
    "full",
    "ilike",
    "inner",
    "is",
    "isnull",
    "join",
    "left",
    "like",
    "natural",
    "notnull",
    "outer",
    "overlaps",
    "right",
    "similar",
    "tablesample",
    "verbose",
];

/// The 78 words `PostgreSQL` 18.4 classifies `reserved_keyword`. None may be a
/// column name, a table name or an alias.
const RESERVED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "system_user",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
];

/// Every word that `PostgreSQL` admits as a `ColId`, which is exactly the
/// union of the two unrestricted classes. Written once here, so the column,
/// table and alias cases cannot drift apart.
fn col_id_keywords() -> Vec<&'static str> {
    UNRESERVED_KEYWORDS
        .iter()
        .chain(COL_NAME_KEYWORDS)
        .copied()
        .collect()
}

struct Client {
    session: SqlSession,
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
            .unwrap_or_else(|error| panic!("{sql} — {}: {}", error.code, error.message))
            .into_iter()
            .next_back()
            .expect("at least one result")
    }

    /// Whether the statement was accepted, and its SQLSTATE when it was not.
    async fn outcome(&mut self, sql: &str) -> Result<(), String> {
        match self.session.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(error) => Err(error.code),
        }
    }

    /// The first column of every row, as text.
    async fn column(&mut self, sql: &str) -> Vec<String> {
        let result = self.run(sql).await;
        let QueryResult::Rows { rows, .. } = &result else {
            panic!("expected rows from {sql}, got {result:?}");
        };
        rows.iter()
            .map(|row| {
                let cell: &Cell = row[0].as_ref().expect("a non-null name");
                String::from_utf8(cell.text.to_vec()).expect("valid text")
            })
            .collect()
    }
}

/// One table with a column per `ColId` keyword, read back by name and in
/// declaration order. `PostgreSQL` 18.4 accepts the same `CREATE TABLE`.
#[tokio::test]
async fn every_col_id_keyword_names_a_column() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    let keywords = col_id_keywords();

    let columns = keywords
        .iter()
        .map(|word| format!("{word} text"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .run(&format!("CREATE TABLE kw_columns ({columns})"))
        .await;

    let read_back = client
        .column(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'kw_columns' ORDER BY ordinal_position",
        )
        .await;
    assert!(read_back == keywords);
}

/// A table per `ColId` keyword, read back from the catalog as a whole set.
#[tokio::test]
async fn every_col_id_keyword_names_a_table() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    let keywords = col_id_keywords();

    for word in &keywords {
        client.run(&format!("CREATE TABLE {word} (n int4)")).await;
    }

    let mut read_back = client
        .column(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' ORDER BY table_name",
        )
        .await;
    read_back.sort();
    let mut expected: Vec<String> = keywords.iter().map(|word| (*word).to_string()).collect();
    expected.sort();
    assert!(read_back == expected);
}

/// A `ColId` keyword as a relation alias, written both with and without `AS`,
/// and used to qualify a column so the alias has to have taken effect.
#[tokio::test]
async fn every_col_id_keyword_is_an_alias() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE kw_alias_base (n int4)").await;
    client.run("INSERT INTO kw_alias_base VALUES (7)").await;

    let mut refused: Vec<(String, String)> = Vec::new();
    for word in col_id_keywords() {
        for sql in [
            format!("SELECT {word}.n FROM kw_alias_base AS {word}"),
            format!("SELECT {word}.n FROM kw_alias_base {word}"),
        ] {
            match client.outcome(&sql).await {
                Ok(()) => {}
                Err(code) => refused.push((sql, code)),
            }
        }
    }
    assert!(refused == Vec::new());
}

/// The alias really binds: the qualified reference reads the aliased relation's
/// row rather than resolving to something else.
#[tokio::test]
async fn an_alias_spelled_as_a_keyword_binds_the_relation() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE kw_alias_base (n int4)").await;
    client.run("INSERT INTO kw_alias_base VALUES (7)").await;

    let read = client
        .column("SELECT set.n FROM kw_alias_base AS set")
        .await;
    assert!(read == vec!["7".to_string()]);
}

/// A reserved or type/function-name keyword is not a name, and this parser must
/// keep refusing every one of them that it lexes as a keyword token — those are
/// the words the widening above could have swept in by mistake.
///
/// The rest of the two classes reach the parser as plain identifiers and are
/// wrongly accepted as names; that is an older and separate gap, not one these
/// cases can pin without asserting the bug.
#[tokio::test]
async fn a_reserved_or_type_function_name_keyword_is_not_a_name() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE kw_alias_base (n int4)").await;

    let mut accepted: Vec<String> = Vec::new();
    for word in RESERVED_KEYWORDS
        .iter()
        .copied()
        .chain(TYPE_FUNC_NAME_KEYWORDS.iter().copied())
    {
        for sql in [
            format!("CREATE TABLE kw_bad (n int4, {word} text)"),
            format!("CREATE TABLE {word} (n int4)"),
            format!("SELECT 1 FROM kw_alias_base AS {word}"),
            format!("CREATE SCHEMA {word}"),
            format!("WITH {word} AS (SELECT 1) SELECT 1 FROM {word}"),
            format!("ALTER TABLE kw_alias_base ADD COLUMN {word} int4"),
        ] {
            if client.outcome(&sql).await.is_ok() {
                accepted.push(sql);
            }
        }
    }
    // `CREATE TABLE kw_bad (n int4, like text)` is the one statement in this set
    // that PostgreSQL and this parser refuse for different reasons: `LIKE text`
    // is a well-formed LIKE clause naming a relation that does not exist, so it
    // is refused at name resolution rather than by the grammar.
    assert!(accepted == Vec::<String>::new());

    // Quoting strips a word of every keyword property, so the same names are
    // reachable when they are written the way PostgreSQL requires.
    client.run(r#"CREATE TABLE kw_quoted ("check" int4)"#).await;
    client
        .run(r#"INSERT INTO kw_quoted ("check") VALUES (1)"#)
        .await;
    assert!(client.column(r#"SELECT "check" FROM kw_quoted"#).await == vec!["1".to_string()]);
}

/// The refusal above is what keeps `PostgreSQL`'s reserved identity words
/// meaning what they say. A column called `session_user` or `current_role` would
/// otherwise sit in the same namespace as the built-in, and a query auditing who
/// it runs as would read whatever the table happened to hold.
#[tokio::test]
async fn an_identity_keyword_cannot_be_given_to_a_column() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    for sql in [
        "CREATE TABLE audit (session_user text, n int4)",
        "CREATE TABLE audit (current_role text, n int4)",
        "CREATE TABLE audit (current_catalog text, n int4)",
        "CREATE TABLE audit (current_user text, n int4)",
    ] {
        assert!(
            client.outcome(sql).await == Err("42601".to_string()),
            "{sql}"
        );
    }
}

/// A role name, a `SET` value and the other positions `PostgreSQL` writes as a
/// `NonReservedWord` are wider than a `ColId`: they admit the type/function-name
/// and column-name classes, so `CREATE ROLE verbose` is legal on 18.4 while
/// `CREATE ROLE session_user` is not.
#[tokio::test]
async fn a_non_reserved_word_position_admits_more_than_a_col_id() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    let mut refused: Vec<String> = Vec::new();
    for word in TYPE_FUNC_NAME_KEYWORDS
        .iter()
        .copied()
        .chain(COL_NAME_KEYWORDS.iter().copied())
    {
        let sql = format!("SET search_path = {word}");
        if client.outcome(&sql).await == Err("42601".to_string()) {
            refused.push(sql);
        }
    }
    assert!(refused == Vec::<String>::new());

    for word in RESERVED_KEYWORDS.iter().copied() {
        // `DEFAULT`, `TRUE`, `FALSE` and `ON` are spellings `SET` gives its own
        // meaning, which is what `opt_boolean_or_string` does in PostgreSQL.
        if matches!(word, "default" | "true" | "false" | "on") {
            continue;
        }
        let sql = format!("SET search_path = {word}");
        assert!(
            client.outcome(&sql).await == Err("42601".to_string()),
            "{sql}"
        );
    }
}

/// A keyword-named column has to read as a column everywhere an expression may
/// go, not only as the whole of one. The right operand of `AND` is the case that
/// was wrong: the lookahead that tells an infix `AND` from a column labelled
/// `and` did not count a keyword as the start of an expression, so
/// `WHERE n = 1 AND between = 3` ended at the `AND`.
#[tokio::test]
async fn a_keyword_named_column_reads_in_every_expression_position() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    let mut refused: Vec<(String, String)> = Vec::new();
    for word in col_id_keywords() {
        client
            .run(&format!("CREATE TABLE kw_expr (n int4, {word} int4)"))
            .await;
        for sql in [
            format!("SELECT {word} FROM kw_expr WHERE {word} = 1"),
            format!("SELECT 1 FROM kw_expr WHERE n = 1 AND {word} = 1"),
            format!("SELECT 1 FROM kw_expr WHERE n = 1 OR {word} = 1"),
            format!("SELECT 1 FROM kw_expr ORDER BY {word}"),
            format!("SELECT {word} FROM kw_expr GROUP BY {word}"),
            format!("UPDATE kw_expr SET {word} = 2 RETURNING {word}"),
        ] {
            if let Err(code) = client.outcome(&sql).await {
                refused.push((sql, code));
            }
        }
        client.run("DROP TABLE kw_expr").await;
    }
    assert!(refused == Vec::new());
}

/// A `SET` parameter takes a `ColId` on both sides: `SET search_path = schema`
/// names a schema, and the words that introduce `SET`'s own special forms —
/// `TRANSACTION`, `SESSION`, `CONSTRAINTS` — are parameter names when `=` or
/// `TO` follows them.
#[tokio::test]
async fn a_keyword_is_a_set_parameter_name_and_a_set_value() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    let mut syntax_errors: Vec<String> = Vec::new();
    for word in col_id_keywords() {
        for sql in [
            format!("SET {word} = 'v'"),
            format!("SET search_path = {word}"),
        ] {
            if client.outcome(&sql).await == Err("42601".to_string()) {
                syntax_errors.push(sql);
            }
        }
    }
    assert!(syntax_errors == Vec::<String>::new());
}

/// Each `ColId` keyword that also begins a statement or a clause. Widening a
/// name position must not cost the word its own syntax, so every one is written
/// here twice: once as the keyword it is, once as the name it may also be.
const STATEMENT_WORDS: &[(&str, &str, &str)] = &[
    ("abort", "ABORT", "CREATE TABLE abort (abort int4)"),
    ("begin", "BEGIN", "CREATE TABLE begin (begin int4)"),
    (
        "by",
        "SELECT n FROM kw_base GROUP BY n",
        "CREATE TABLE by (by int4)",
    ),
    ("commit", "COMMIT", "CREATE TABLE commit (commit int4)"),
    (
        "copy",
        "COPY kw_base TO STDOUT",
        "CREATE TABLE copy (copy int4)",
    ),
    (
        "delete",
        "DELETE FROM kw_base",
        "CREATE TABLE delete (delete int4)",
    ),
    (
        "drop",
        "DROP TABLE kw_dropme",
        "CREATE TABLE drop (drop int4)",
    ),
    (
        "exclude",
        "CREATE TABLE kw_excl (n int4, EXCLUDE (n WITH =))",
        "CREATE TABLE exclude (exclude int4)",
    ),
    (
        "if",
        "DROP TABLE IF EXISTS kw_absent",
        "CREATE TABLE if (if int4)",
    ),
    (
        "import",
        "IMPORT FOREIGN SCHEMA s FROM SERVER kw_srv INTO public",
        "CREATE TABLE import (import int4)",
    ),
    (
        "index",
        "CREATE INDEX kw_ix ON kw_base (n)",
        "CREATE TABLE index (index int4)",
    ),
    (
        "insert",
        "INSERT INTO kw_base VALUES (1)",
        "CREATE TABLE insert (insert int4)",
    ),
    (
        "recursive",
        "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
        "CREATE TABLE recursive (recursive int4)",
    ),
    (
        "rollback",
        "ROLLBACK",
        "CREATE TABLE rollback (rollback int4)",
    ),
    (
        "schema",
        "CREATE SCHEMA kw_schema",
        "CREATE TABLE schema (schema int4)",
    ),
    ("set", "SET timezone = 'UTC'", "CREATE TABLE set (set int4)"),
    (
        "start",
        "START TRANSACTION",
        "CREATE TABLE start (start int4)",
    ),
    (
        "transaction",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "CREATE TABLE transaction (transaction int4)",
    ),
    (
        "update",
        "UPDATE kw_base SET n = 1",
        "CREATE TABLE update (update int4)",
    ),
    (
        "view",
        "CREATE VIEW kw_view AS SELECT 1",
        "CREATE TABLE view (view int4)",
    ),
];

/// Both readings of each word still parse: the keyword's own statement is not a
/// syntax error, and neither is the same word naming a table and a column.
///
/// Parsing is what is at stake, so a statement the grammar accepts and this
/// engine has not implemented (`COPY … TO STDOUT`, `IMPORT FOREIGN SCHEMA`, both
/// `0A000`) counts as parsed. Only `42601` — `syntax_error` — is a failure here.
/// Each statement gets its own session, so the transaction one of them opens
/// cannot decide the outcome of the next.
#[tokio::test]
async fn a_keyword_that_begins_a_statement_keeps_its_own_syntax() {
    let engine = SqlEngine::new();
    let mut setup = Client::new(&engine);
    setup.run("CREATE TABLE kw_base (n int4)").await;
    setup.run("CREATE TABLE kw_dropme (n int4)").await;
    setup.run("CREATE FOREIGN DATA WRAPPER kw_fdw").await;
    setup
        .run("CREATE SERVER kw_srv FOREIGN DATA WRAPPER kw_fdw")
        .await;

    let mut syntax_errors: Vec<(&str, &str)> = Vec::new();
    for (word, keyword_syntax, name_syntax) in STATEMENT_WORDS {
        for sql in [keyword_syntax, name_syntax] {
            let mut client = Client::new(&engine);
            if client.outcome(sql).await == Err("42601".to_string()) {
                syntax_errors.push((word, sql));
            }
        }
    }
    assert!(syntax_errors == Vec::new());
}
