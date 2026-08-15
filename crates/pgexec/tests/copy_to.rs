//! `COPY … TO` end to end: what the session hands the wire layer for a
//! copy-out, byte for byte.
//!
//! Every expectation here was taken from `PostgreSQL` 18.4 running the same
//! statements against the same rows, so a change that "looks right" but moves a
//! quote or an escape fails.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{CopyOutStream, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

async fn copy_out(session: &mut SqlSession, sql: &str) -> CopyOutStream {
    session
        .begin_copy_out(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
        .unwrap_or_else(|| panic!("{sql} should be a copy-out"))
}

/// The whole copy-out payload as one string: what the client receives once the
/// `CopyData` frames are concatenated.
async fn copied(session: &mut SqlSession, sql: &str) -> String {
    let stream = copy_out(session, sql).await;
    let mut out = Vec::new();
    for row in &stream.rows {
        out.extend_from_slice(row);
    }
    String::from_utf8(out).expect("copy-out payload is utf8")
}

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .begin_copy_out(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

/// Three columns holding every byte the two formats treat specially.
const SETUP: &str = "CREATE TABLE t (a int4, b text, c text); \
     INSERT INTO t VALUES \
        (1, E'tab\\there', 'x'), \
        (2, E'nl\\nhere', NULL), \
        (3, E'back\\\\slash', E'cr\\rhere'), \
        (4, E'bs\\b ff\\f vt\\v', 'q\"uote'), \
        (5, 'comma,here', 'plain')";

async fn seeded() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, SETUP).await;
    (engine, session)
}

/// The formats and their options, against `PostgreSQL` 18.4's output for the same
/// rows.
#[tokio::test]
async fn copy_to_stdout_matches_postgres_byte_for_byte() {
    struct Case {
        sql: &'static str,
        expected: &'static str,
    }
    let cases = [
        Case {
            sql: "COPY t TO STDOUT",
            expected: concat!(
                "1\ttab\\there\tx\n",
                "2\tnl\\nhere\t\\N\n",
                "3\tback\\\\slash\tcr\\rhere\n",
                "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                "5\tcomma,here\tplain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV)",
            expected: concat!(
                "1,tab\there,x\n",
                "2,\"nl\nhere\",\n",
                "3,back\\slash,\"cr\rhere\"\n",
                "4,bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                "5,\"comma,here\",plain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, HEADER)",
            expected: concat!(
                "a,b,c\n",
                "1,tab\there,x\n",
                "2,\"nl\nhere\",\n",
                "3,back\\slash,\"cr\rhere\"\n",
                "4,bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                "5,\"comma,here\",plain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (HEADER)",
            expected: concat!(
                "a\tb\tc\n",
                "1\ttab\\there\tx\n",
                "2\tnl\\nhere\t\\N\n",
                "3\tback\\\\slash\tcr\\rhere\n",
                "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                "5\tcomma,here\tplain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (NULL 'NUL')",
            expected: concat!(
                "1\ttab\\there\tx\n",
                "2\tnl\\nhere\tNUL\n",
                "3\tback\\\\slash\tcr\\rhere\n",
                "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                "5\tcomma,here\tplain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (DELIMITER '|')",
            expected: concat!(
                "1|tab\\there|x\n",
                "2|nl\\nhere|\\N\n",
                "3|back\\\\slash|cr\\rhere\n",
                "4|bs\\b ff\\f vt\\v|q\"uote\n",
                "5|comma,here|plain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE (a))",
            expected: concat!(
                "\"1\",tab\there,x\n",
                "\"2\",\"nl\nhere\",\n",
                "\"3\",back\\slash,\"cr\rhere\"\n",
                "\"4\",bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                "\"5\",\"comma,here\",plain\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)",
            expected: concat!(
                "\"1\",\"tab\there\",\"x\"\n",
                "\"2\",\"nl\nhere\",\n",
                "\"3\",\"back\\slash\",\"cr\rhere\"\n",
                "\"4\",\"bs\u{8} ff\u{c} vt\u{b}\",\"q\"\"uote\"\n",
                "\"5\",\"comma,here\",\"plain\"\n",
            ),
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, QUOTE '~', ESCAPE '@')",
            expected: concat!(
                "1,tab\there,x\n",
                "2,~nl\nhere~,\n",
                "3,back\\slash,~cr\rhere~\n",
                "4,bs\u{8} ff\u{c} vt\u{b},q\"uote\n",
                "5,~comma,here~,plain\n",
            ),
        },
        Case {
            sql: "COPY t (c, a) TO STDOUT",
            expected: "x\t1\n\\N\t2\ncr\\rhere\t3\nq\"uote\t4\nplain\t5\n",
        },
        Case {
            sql: "COPY (SELECT a, b FROM t WHERE a < 3 ORDER BY a) TO STDOUT",
            expected: "1\ttab\\there\n2\tnl\\nhere\n",
        },
        // The legacy pre-WITH spellings the grammar still accepts.
        Case {
            sql: "COPY t (a) TO STDOUT WITH CSV HEADER",
            expected: "a\n1\n2\n3\n4\n5\n",
        },
        Case {
            sql: "COPY t (a, c) TO STDOUT USING DELIMITERS '|'",
            expected: "1|x\n2|\\N\n3|cr\\rhere\n4|q\"uote\n5|plain\n",
        },
    ];

    let (_engine, mut session) = seeded().await;
    for case in cases {
        assert!(
            copied(&mut session, case.sql).await == case.expected,
            "{}",
            case.sql
        );
    }
}

/// The `CopyOutResponse` announces the copied width in text format, and the tag
/// counts the rows the header line is not one of.
#[tokio::test]
async fn the_copy_out_response_and_tag_describe_the_result() {
    let (_engine, mut session) = seeded().await;

    let stream = copy_out(&mut session, "COPY t TO STDOUT").await;
    assert!(stream.response.overall_format == 0);
    assert!(stream.response.column_formats == vec![0, 0, 0]);
    assert!(stream.tag == "COPY 5");
    assert!(stream.rows.len() == 5);

    let stream = copy_out(&mut session, "COPY t (a) TO STDOUT WITH (HEADER)").await;
    assert!(stream.response.column_formats == vec![0]);
    assert!(
        stream.tag == "COPY 5",
        "the header line is not a copied row"
    );
    assert!(
        stream.rows.len() == 6,
        "but it is a CopyData frame of its own"
    );

    run(&mut session, "CREATE TABLE empty (x int4)").await;
    let stream = copy_out(&mut session, "COPY empty TO STDOUT WITH (HEADER)").await;
    assert!(stream.tag == "COPY 0");
    assert!(stream.rows.len() == 1, "the header is written for no rows");
}

/// A data-modifying source runs, and the rows it copies are the ones it
/// returned — so its effects are visible afterwards.
#[tokio::test]
async fn a_copy_of_a_data_modifying_statement_runs_it() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "CREATE TABLE moved (id int4)").await;

    assert!(
        copied(
            &mut session,
            "COPY (INSERT INTO moved VALUES (7), (8) RETURNING id) TO STDOUT",
        )
        .await
            == "7\n8\n"
    );
    let QueryResult::Rows { rows, .. } = &session
        .simple_query("SELECT id FROM moved ORDER BY id")
        .await
        .expect("select")[0]
    else {
        panic!("expected rows");
    };
    assert!(rows.len() == 2, "the INSERT the copy ran is durable");
}

/// A statement that is not a copy-out leaves `begin_copy_out` saying so, rather
/// than running it: the wire layer runs it on the ordinary path afterwards.
#[tokio::test]
async fn only_a_lone_copy_to_stdout_is_a_copy_out() {
    let (_engine, mut session) = seeded().await;
    for sql in [
        "SELECT 1",
        "COPY t FROM STDIN",
        // The copy-out block owns the connection's framing until it ends, so a
        // second result in the same query string has nowhere to go.
        "SELECT 1; COPY t TO STDOUT",
        "",
    ] {
        assert!(
            session.begin_copy_out(sql).await.expect("probe") == None,
            "{sql}"
        );
    }
}

/// Every refusal `PostgreSQL` gives a `COPY … TO`, with its SQLSTATE.
#[tokio::test]
async fn copy_to_refusals_match_postgres() {
    struct Case {
        sql: &'static str,
        sqlstate: &'static str,
        message: &'static str,
    }
    let cases = [
        Case {
            sql: "COPY vw TO STDOUT",
            sqlstate: "42809",
            message: "cannot copy from view \"vw\"",
        },
        Case {
            sql: "COPY sq TO STDOUT",
            sqlstate: "42809",
            message: "cannot copy from sequence \"sq\"",
        },
        Case {
            sql: "COPY nosuchtab TO STDOUT",
            sqlstate: "42P01",
            message: "relation \"nosuchtab\" does not exist",
        },
        Case {
            sql: "COPY t (zz) TO STDOUT",
            sqlstate: "42703",
            message: "column \"zz\" of relation \"t\" does not exist",
        },
        Case {
            sql: "COPY t (a, a) TO STDOUT",
            sqlstate: "42701",
            message: "column \"a\" specified more than once",
        },
        Case {
            sql: "COPY t TO STDOUT WITH (DELIMITER 'ab')",
            sqlstate: "0A000",
            message: "COPY delimiter must be a single one-byte character",
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, DELIMITER '\"')",
            sqlstate: "22023",
            message: "COPY delimiter and quote must be different",
        },
        Case {
            sql: "COPY t TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE (zz))",
            sqlstate: "42703",
            message: "column \"zz\" of relation \"t\" does not exist",
        },
        Case {
            sql: "COPY (SELECT 1 AS a) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE (zz))",
            sqlstate: "42703",
            message: "column \"zz\" does not exist",
        },
    ];

    let (_engine, mut session) = seeded().await;
    run(
        &mut session,
        "CREATE VIEW vw AS SELECT 1 AS x; CREATE SEQUENCE sq",
    )
    .await;
    for case in cases {
        assert!(
            error_of(&mut session, case.sql).await
                == (case.sqlstate.to_string(), case.message.to_string()),
            "{}",
            case.sql
        );
    }
}

/// `COPY … TO 'file'` is not a copy-out: it runs on the ordinary statement
/// path, writes the payload server side, and reports the row count.
#[tokio::test]
async fn copy_to_a_file_writes_it_server_side() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("out.txt");
    let path = path.to_str().expect("utf8 path");

    let (_engine, mut session) = seeded().await;
    assert!(
        session
            .begin_copy_out(&format!("COPY t TO '{path}'"))
            .await
            .expect("probe")
            == None,
        "a file destination never enters copy-out mode"
    );

    let results = session
        .simple_query(&format!("COPY t (a, c) TO '{path}'"))
        .await
        .expect("copy to file");
    assert!(
        results
            == vec![QueryResult::Command {
                tag: "COPY 5".into()
            }]
    );
    assert!(
        std::fs::read_to_string(path).expect("written file")
            == "1\tx\n2\t\\N\n3\tcr\\rhere\n4\tq\"uote\n5\tplain\n"
    );

    let error = session
        .simple_query("COPY t TO '/nonexistent-directory/out.txt'")
        .await
        .expect_err("unwritable path");
    assert!(error.code == "58P01");
    assert!(
        error
            .message
            .starts_with("could not open file \"/nonexistent-directory/out.txt\" for writing: ")
    );
}

/// A `COPY … FROM` that names a framing option now honours it. Before the
/// option list was parsed at all these were refused, and ignoring one would
/// load silently wrong rows — which is worse than the refusal it replaced.
#[tokio::test]
async fn copy_from_honours_the_framing_options_it_accepts() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE loaded (s text, n text)").await;

    session
        .copy_in(
            "COPY loaded FROM STDIN WITH (DELIMITER '|', NULL 'NUL', HEADER MATCH)",
            0,
            vec![bytes::Bytes::from_static(b"s|n\na|1\nb|NUL\nc|\\N\n")],
        )
        .await
        .expect("copy in");

    assert!(
        copied(
            &mut session,
            "COPY loaded TO STDOUT WITH (DELIMITER '|', NULL 'NUL')"
        )
        .await
            == "a|1\nb|NUL\nc|N\n",
        "the null string is matched before de-escaping, so `\\N` is the string `N`"
    );
}

/// The options a `COPY … FROM` cannot honour are refused before copy-in mode is
/// announced. Reaching that mode and *then* failing would leave psql feeding the
/// rest of the script to the server as data.
#[tokio::test]
async fn copy_from_refuses_an_option_it_cannot_honour_before_entering_copy_in() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE loaded (s text, n text)").await;

    for sql in [
        "COPY loaded FROM STDIN WITH (DEFAULT 'D')",
        "COPY loaded FROM STDIN WITH (ON_ERROR IGNORE)",
        "COPY loaded FROM STDIN WITH (ENCODING 'LATIN2')",
    ] {
        let error = session
            .begin_copy_in(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} should have been refused"));
        assert!(error.code == "0A000", "{sql}");
    }

    // FREEZE asks for a visibility shortcut, not a different framing: the rows
    // it loads are the same rows, so it is accepted and ignored. `pgbench -i`
    // sends it. CSV is a framing the loader now reads, so it is accepted too.
    for sql in [
        "COPY loaded FROM STDIN WITH (FREEZE)",
        "COPY loaded FROM STDIN WITH (FORMAT CSV)",
    ] {
        assert!(
            session
                .begin_copy_in(sql)
                .await
                .unwrap_or_else(|error| panic!("{sql} is accepted: {error:?}"))
                .is_some()
        );
    }
}

/// The `CONTEXT` a failing `COPY … FROM` row carries.
///
/// `PostgreSQL` writes one of three lines and this pins all three, plus the two
/// rules that are easy to get wrong: a `HEADER` line is counted, so the first
/// data row of a copy with one is line 2; and a field or line longer than 100
/// characters is quoted only that far, with `...` for the rest. Every string
/// here was captured from `PostgreSQL` 18.4 running the same statements.
#[tokio::test]
async fn a_failing_copy_from_row_reports_the_line_it_came_from() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        data: String,
        code: &'static str,
        message: String,
        context: String,
    }

    let long = "x".repeat(200);
    let cases = vec![
        Case {
            name: "a value too long for its type names the column and quotes the field",
            sql: "COPY t (a, c) FROM STDIN",
            data: "1\ttoolong\n".into(),
            code: "22001",
            message: "value too long for type character varying(3)".into(),
            context: "COPY t, line 1, column c: \"toolong\"".into(),
        },
        Case {
            name: "a malformed integer names the column it was being read for",
            sql: "COPY t (a, c) FROM STDIN",
            data: "notanint\tab\n".into(),
            code: "22P02",
            message: "invalid input syntax for type integer: \"notanint\"".into(),
            context: "COPY t, line 1, column a: \"notanint\"".into(),
        },
        Case {
            name: "the line counted is the failing one, not the first",
            sql: "COPY t (a, c) FROM STDIN",
            data: "1\tab\n2\tcd\n3\ttoolong\n".into(),
            code: "22001",
            message: "value too long for type character varying(3)".into(),
            context: "COPY t, line 3, column c: \"toolong\"".into(),
        },
        Case {
            name: "a HEADER line is counted, so the first data row is line 2",
            sql: "COPY t (a, c) FROM STDIN WITH (HEADER true)",
            data: "a\tc\n1\ttoolong\n".into(),
            code: "22001",
            message: "value too long for type character varying(3)".into(),
            context: "COPY t, line 2, column c: \"toolong\"".into(),
        },
        Case {
            name: "too few fields name the first column left unsupplied, and quote the line",
            sql: "COPY t (a, c) FROM STDIN",
            data: "1\n".into(),
            code: "22P04",
            message: "missing data for column \"c\"".into(),
            context: "COPY t, line 1: \"1\"".into(),
        },
        Case {
            name: "too many fields say only that, and quote the line",
            sql: "COPY t (a, c) FROM STDIN",
            data: "1\tab\textra\n".into(),
            code: "22P04",
            message: "extra data after last expected column".into(),
            context: "COPY t, line 1: \"1\tab\textra\"".into(),
        },
        Case {
            name: "a constraint judges the assembled row, so it reports the line alone",
            sql: "COPY nn FROM STDIN",
            data: "\\N\t2\n".into(),
            code: "23502",
            message: "null value in column \"a\" of relation \"nn\" \
                      violates not-null constraint"
                .into(),
            context: "COPY nn, line 1: \"\\N\t2\"".into(),
        },
        Case {
            name: "a CHECK constraint reports the line too",
            sql: "COPY ck FROM STDIN",
            data: "-5\n".into(),
            code: "23514",
            message: "new row for relation \"ck\" violates check constraint \"ck_a_check\"".into(),
            context: "COPY ck, line 1: \"-5\"".into(),
        },
        Case {
            name: "an over-long field is quoted to 100 characters and then elided",
            sql: "COPY big FROM STDIN",
            data: format!("{long}\n"),
            code: "22P02",
            message: format!("invalid input syntax for type integer: \"{long}\""),
            context: (format!("COPY big, line 1, column a: \"{}...\"", "x".repeat(100))),
        },
        Case {
            name: "an over-long line is elided the same way",
            sql: "COPY big FROM STDIN",
            data: format!("1\t{}\n", "y".repeat(115)),
            code: "22P04",
            message: "extra data after last expected column".into(),
            context: (format!("COPY big, line 1: \"1\t{}...\"", "y".repeat(98))),
        },
        Case {
            // PostgreSQL reaches its index at a multi-insert flush, by which
            // point the line buffer no longer describes the row: the counter
            // survives and the line does not.
            name: "a duplicate key reports the line number with no line quoted",
            sql: "COPY u FROM STDIN",
            data: "2\n1\n".into(),
            code: "23505",
            message: "duplicate key value violates unique constraint \"u_pkey\"".into(),
            context: "COPY u, line 2".into(),
        },
    ];

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int, c varchar(3))").await;
    run(&mut session, "CREATE TABLE nn (a int NOT NULL, b int)").await;
    run(&mut session, "CREATE TABLE ck (a int CHECK (a > 0))").await;
    run(&mut session, "CREATE TABLE big (a int)").await;
    run(&mut session, "CREATE TABLE u (a int PRIMARY KEY)").await;
    run(&mut session, "INSERT INTO u VALUES (1)").await;

    for case in cases {
        let error = session
            .copy_in(case.sql, 0, vec![bytes::Bytes::from(case.data.clone())])
            .await
            .err()
            .unwrap_or_else(|| panic!("{} should have failed", case.name));
        let context = error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.context.clone());
        assert!(
            (
                error.code.as_str(),
                error.message.as_str(),
                context.as_deref()
            ) == (
                case.code,
                case.message.as_str(),
                Some(case.context.as_str())
            ),
            "{}",
            case.name
        );
    }
}

/// A `COPY` context is *appended* to whatever context the error already
/// carried, never substituted for it.
///
/// `PostgreSQL` stacks error contexts outward, so a row that fails inside a
/// `plpgsql` `BEFORE` trigger reports the function's frame and the `COPY` line
/// below it:
///
/// ```text
/// ERROR:  boom on 7
/// CONTEXT:  PL/pgSQL function boom() line 3 at RAISE
/// COPY trg, line 1: "7"
/// ```
///
/// That exact case is out of reach here — this engine cannot run a `plpgsql`
/// trigger from inside `COPY` at all — so the rule is exercised through the one
/// error a copied row can raise that already carries a frame: the SQL-language
/// wrapper `numeric + pg_lsn` is implemented as. `PostgreSQL` inlines that
/// wrapper and so prints only the `COPY` frame for this particular expression;
/// the two-frame output below is this engine's, and it is the stacking that is
/// being pinned, not the frame count.
#[tokio::test]
async fn a_copy_context_is_appended_below_one_the_error_already_carried() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE lsnck (a numeric CHECK ((a + '0/0'::pg_lsn) > '0/0'::pg_lsn))",
    )
    .await;

    let error = session
        .copy_in(
            "COPY lsnck FROM STDIN",
            0,
            vec![bytes::Bytes::from_static(b"-5\n")],
        )
        .await
        .expect_err("the check should reject the row");
    let context = error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.context.clone())
        .expect("a context");
    assert!(error.message == "pg_lsn out of range", "{error:?}");
    assert!(
        context
            == "SQL function \"numeric_pl_pg_lsn\" statement 1\n\
                COPY lsnck, line 1: \"-5\"",
        "{context:?}"
    );
}

/// A referential check is a *statement*-level check, so it runs after the row
/// loop and after `PostgreSQL` has popped its per-row error callback: a foreign
/// key violated by a copied row reports no `COPY` context at all.
#[tokio::test]
async fn a_copy_failure_raised_after_the_row_loop_carries_no_context() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE p (a int PRIMARY KEY)").await;
    run(&mut session, "CREATE TABLE c (a int REFERENCES p(a))").await;

    let error = session
        .copy_in(
            "COPY c FROM STDIN",
            0,
            vec![bytes::Bytes::from_static(b"9\n")],
        )
        .await
        .expect_err("the foreign key should reject the row");
    let context = error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.context.as_deref());
    assert!(error.code == "23503", "{error:?}");
    assert!(context.is_none(), "{context:?}");
}

/// A `HEADER MATCH` failure is raised by the decode rather than by a row, and
/// carries the same `CONTEXT` a failing row does: the header is line 1, and it
/// is quoted whole. Captured from `PostgreSQL` 18.4.
#[tokio::test]
async fn a_header_match_failure_reports_the_header_line() {
    struct Case {
        data: &'static [u8],
        message: &'static str,
        context: &'static str,
    }

    let cases = [
        Case {
            data: b"a\tb\td\n1\t2\t3\n",
            message: "column name mismatch in header line field 3: got \"d\", expected \"c\"",
            context: "COPY header_copytest, line 1: \"a\tb\td\"",
        },
        Case {
            data: b"a\tb\n1\t2\n",
            message: "wrong number of fields in header line: got 2, expected 3",
            context: "COPY header_copytest, line 1: \"a\tb\"",
        },
        Case {
            data: b"a\tb\tc\td\n1\t2\t3\t4\n",
            message: "wrong number of fields in header line: got 4, expected 3",
            context: "COPY header_copytest, line 1: \"a\tb\tc\td\"",
        },
    ];

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE header_copytest (a int, b int, c text)",
    )
    .await;

    for case in cases {
        let error = session
            .copy_in(
                "COPY header_copytest FROM STDIN WITH (HEADER match)",
                0,
                vec![bytes::Bytes::from_static(case.data)],
            )
            .await
            .expect_err("the header should not match");
        let context = error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.context.clone());
        assert!(
            (error.message.as_str(), context.as_deref()) == (case.message, Some(case.context)),
            "{error:?}"
        );
    }
}
