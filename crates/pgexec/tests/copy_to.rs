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
        "COPY loaded FROM STDIN WITH (FORMAT CSV)",
        "COPY loaded FROM STDIN WITH (DEFAULT 'D')",
        "COPY loaded FROM STDIN WITH (ON_ERROR IGNORE)",
        "COPY loaded FROM STDIN WITH (ENCODING 'LATIN1')",
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
    // sends it.
    assert!(
        session
            .begin_copy_in("COPY loaded FROM STDIN WITH (FREEZE)")
            .await
            .expect("freeze is accepted")
            .is_some()
    );
}
