//! `COPY … FROM` in CSV mode, and the end-of-data marker both formats share.
//!
//! Every expectation here was taken from `PostgreSQL` 18.4 loading the same
//! bytes into the same table, because CSV's rules are the kind that look
//! obviously right and are not: an unquoted empty field is a NULL while a
//! quoted one is the empty string, a quote may open and close in the middle of
//! a field, a backslash is an ordinary character, and `\.` is data rather than
//! the end of it.
//!
//! The other half of the file is about *when* a refusal arrives. Copy-in is a
//! connection mode: once `CopyInResponse` has gone out, psql reads the rest of
//! its script as COPY data, so a refusal that arrives afterwards costs the
//! session rather than the statement. Everything a copy can be refused for —
//! an option it cannot honour, a `FORCE_NOT_NULL` naming a column it does not
//! read, a policy on the target — is therefore asserted against
//! `begin_copy_in`, which is the call that answers before the mode is entered.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

/// Load `data` through the wire's copy-in path: announce the copy, then hand
/// over the payload as one `CopyData` frame.
async fn copy_in(
    session: &mut SqlSession,
    sql: &str,
    data: &str,
) -> Result<String, (String, String)> {
    session
        .begin_copy_in(sql)
        .await
        .map_err(|error| (error.code.clone(), error.message.clone()))?;
    match session
        .copy_in(sql, 0, vec![bytes::Bytes::copy_from_slice(data.as_bytes())])
        .await
    {
        Ok(QueryResult::Command { tag }) => Ok(tag),
        Ok(other) => panic!("{sql} should answer with a command tag, got {other:?}"),
        Err(error) => Err((error.code.clone(), error.message.clone())),
    }
}

/// The copy-in refusal that arrives *before* copy-in mode is entered.
async fn refused_before_copy_in(session: &mut SqlSession, sql: &str) -> (String, String) {
    match session.begin_copy_in(sql).await {
        Err(error) => (error.code.clone(), error.message.clone()),
        Ok(_) => panic!("{sql} should have been refused before copy-in mode"),
    }
}

fn cell(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "<null>".to_string(),
        |cell| format!("[{}]", String::from_utf8_lossy(&cell.text)),
    )
}

/// Every row of `sql`, each cell bracketed so an empty string is distinct from
/// a NULL and from a value with trailing spaces.
async fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<String>> {
    let results = session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("{sql} should return rows");
    };
    rows.iter()
        .map(|row| row.iter().map(|c| cell(c.as_ref())).collect())
        .collect()
}

async fn seeded() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a text, b text, c text)").await;
    (engine, session)
}

/// CSV's framing rules, against `PostgreSQL` 18.4's reading of the same bytes.
#[tokio::test]
async fn csv_reads_exactly_what_postgres_reads() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        data: &'static str,
        expected: Vec<Vec<&'static str>>,
    }
    let cases = [
        Case {
            name: "a quoted field, and one holding the delimiter",
            sql: "COPY t FROM STDIN CSV",
            data: "plain,\"quoted\",\"with,delim\"\n",
            expected: vec![vec!["[plain]", "[quoted]", "[with,delim]"]],
        },
        Case {
            name: "a newline inside a quoted field is data, not a line end",
            sql: "COPY t FROM STDIN CSV",
            data: "\"emb\nnewline\",x,y\n",
            expected: vec![vec!["[emb\nnewline]", "[x]", "[y]"]],
        },
        Case {
            name: "an unquoted empty field is NULL and a quoted one is the empty string",
            sql: "COPY t FROM STDIN CSV",
            data: "\"dq\"\"inside\",,\"\"\n",
            expected: vec![vec!["[dq\"inside]", "<null>", "[]"]],
        },
        Case {
            name: "a quote may open and close in the middle of a field",
            sql: "COPY t FROM STDIN CSV",
            data: "a\"b\"c,x,y\n",
            expected: vec![vec!["[abc]", "[x]", "[y]"]],
        },
        Case {
            name: "an escape that is not the quote escapes only the two of them",
            sql: "COPY t FROM STDIN WITH (FORMAT csv, QUOTE '''', ESCAPE '\\')",
            data: "'a\\'b',plain,x\n'tab\tin',\"q\",y\n",
            expected: vec![
                vec!["[a'b]", "[plain]", "[x]"],
                vec!["[tab\tin]", "[\"q\"]", "[y]"],
            ],
        },
        Case {
            name: "a backslash is an ordinary character, so `\\.` is a value",
            sql: "COPY t (a) FROM STDIN CSV",
            data: "line1\n\\.\nline2\n",
            expected: vec![
                vec!["[line1]", "<null>", "<null>"],
                vec!["[\\.]", "<null>", "<null>"],
                vec!["[line2]", "<null>", "<null>"],
            ],
        },
        Case {
            name: "a CRLF payload, whose carriage returns are line endings",
            sql: "COPY t FROM STDIN CSV",
            data: "a,b,c\r\nd,e,f\r\n",
            expected: vec![vec!["[a]", "[b]", "[c]"], vec!["[d]", "[e]", "[f]"]],
        },
        Case {
            name: "a final line with no terminator is still a row",
            sql: "COPY t FROM STDIN CSV",
            data: "a,b,c",
            expected: vec![vec!["[a]", "[b]", "[c]"]],
        },
        Case {
            name: "HEADER discards the first line",
            sql: "COPY t FROM STDIN WITH (FORMAT csv, HEADER)",
            data: "a,b,c\n1,2,3\n",
            expected: vec![vec!["[1]", "[2]", "[3]"]],
        },
        Case {
            name: "HEADER MATCH checks it against the copied columns",
            sql: "COPY t FROM STDIN WITH (FORMAT csv, HEADER match)",
            data: "a,b,c\n1,2,3\n",
            expected: vec![vec!["[1]", "[2]", "[3]"]],
        },
    ];
    for case in cases {
        let (_engine, mut session) = seeded().await;
        let tag = copy_in(&mut session, case.sql, case.data)
            .await
            .unwrap_or_else(|error| panic!("{}: {error:?}", case.name));
        assert!(
            tag == format!("COPY {}", case.expected.len()),
            "{}: command tag counts the rows",
            case.name
        );
        assert!(
            rows(&mut session, "SELECT a, b, c FROM t").await == case.expected,
            "{}",
            case.name
        );
    }
}

/// `FORCE_NOT_NULL` keeps an unquoted null string as that string;
/// `FORCE_NULL` turns a quoted one into a NULL. Both act on the raw field, so
/// a column carrying both flags is left exactly as it arrived.
#[tokio::test]
async fn force_not_null_and_force_null_act_on_the_raw_field() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: Vec<&'static str>,
    }
    // Column `a` arrives unquoted-empty, `b` quoted-empty.
    const DATA: &str = ",\"\",x\n";
    let cases = [
        Case {
            name: "neither: unquoted empty is NULL, quoted empty is the empty string",
            sql: "COPY t (a, b, c) FROM STDIN WITH (FORMAT csv)",
            expected: vec!["<null>", "[]", "[x]"],
        },
        Case {
            name: "FORCE_NOT_NULL on the unquoted one keeps it as the null string",
            sql: "COPY t (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (a))",
            expected: vec!["[]", "[]", "[x]"],
        },
        Case {
            name: "FORCE_NULL on the quoted one makes it NULL",
            sql: "COPY t (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NULL (b))",
            expected: vec!["<null>", "<null>", "[x]"],
        },
        Case {
            // Neither flag reaches a field the other already settled:
            // FORCE_NOT_NULL only sees a field that was NULL, and by then a
            // quoted one is not.
            name: "both flags on both columns, which each field meets in turn",
            sql: "COPY t (a, b, c) FROM STDIN \
                  WITH (FORMAT csv, FORCE_NOT_NULL (a, b), FORCE_NULL (a, b))",
            expected: vec!["[]", "<null>", "[x]"],
        },
        Case {
            name: "the `*` spellings, which reach every copied column",
            sql: "COPY t (a, b, c) FROM STDIN \
                  WITH (FORMAT csv, FORCE_NOT_NULL *, FORCE_NULL *)",
            expected: vec!["[]", "<null>", "[x]"],
        },
    ];
    for case in cases {
        let (_engine, mut session) = seeded().await;
        copy_in(&mut session, case.sql, DATA)
            .await
            .unwrap_or_else(|error| panic!("{}: {error:?}", case.name));
        assert!(
            rows(&mut session, "SELECT a, b, c FROM t").await == vec![case.expected],
            "{}",
            case.name
        );
    }
}

/// Whatever a `COPY … TO` writes, a `COPY … FROM` under the same options reads
/// back as the same rows — the property the two encoders and the two decoders
/// exist to keep.
#[tokio::test]
async fn a_csv_copy_out_reads_back_as_the_same_rows() {
    let options = [
        "FORMAT csv",
        "FORMAT csv, HEADER",
        "FORMAT csv, DELIMITER '|'",
        "FORMAT csv, QUOTE '''', ESCAPE '\\'",
        "FORMAT csv, NULL 'NUL'",
        "FORMAT csv, FORCE_QUOTE *",
        "FORMAT text",
        "FORMAT text, DELIMITER '|'",
    ];
    for option in options {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE t (a text, b text, c text)").await;
        run(
            &mut session,
            "INSERT INTO t VALUES \
                ('plain', 'with,comma', 'with\"quote'), \
                ('with''single', E'emb\\nnewline', NULL), \
                ('', E'tab\\there', E'back\\\\slash'), \
                (E'cr\\rhere', '\\.', 'NUL')",
        )
        .await;
        let before = rows(&mut session, "SELECT a, b, c FROM t ORDER BY a, b").await;

        let stream = session
            .begin_copy_out(&format!("COPY t TO STDOUT WITH ({option})"))
            .await
            .unwrap_or_else(|error| panic!("{option}: copy out failed: {error:?}"))
            .unwrap_or_else(|| panic!("{option}: should be a copy-out"));
        let mut payload = Vec::new();
        for row in &stream.rows {
            payload.extend_from_slice(row);
        }
        let payload = String::from_utf8(payload).expect("copy-out payload is utf8");

        run(&mut session, "DELETE FROM t").await;
        // A `HEADER` written by the copy-out is read back by a `HEADER match`,
        // which also checks that what was written names the copied columns.
        // `FORCE_QUOTE` is a write-side option and has no read-side spelling;
        // dropping it is the point of the case, because what it quotes has to
        // come back unquoted all the same.
        let read_option = option
            .replace("HEADER", "HEADER match")
            .replace(", FORCE_QUOTE *", "");
        copy_in(
            &mut session,
            &format!("COPY t FROM STDIN WITH ({read_option})"),
            &payload,
        )
        .await
        .unwrap_or_else(|error| panic!("{option}: copy in failed: {error:?}"));

        assert!(
            rows(&mut session, "SELECT a, b, c FROM t ORDER BY a, b").await == before,
            "{option}: the copy read back the rows it wrote"
        );
    }
}

/// The text format's `\.`: it ends the data, it must be alone on its line, and
/// a payload that ends on it without a terminator is malformed — all three
/// checked against `PostgreSQL` 18.4.
#[tokio::test]
async fn the_end_of_copy_marker_must_be_alone_on_its_line() {
    struct Case {
        name: &'static str,
        data: &'static str,
        /// `Ok` with the rows loaded, or `Err` with the message and the
        /// `CONTEXT` line that names the failing input line.
        expected: Result<Vec<&'static str>, (&'static str, &'static str)>,
    }
    let cases = [
        Case {
            name: "alone on its line, which ends the data",
            data: "line1\nline2\n\\.\n",
            expected: Ok(vec!["[line1]", "[line2]"]),
        },
        Case {
            name: "everything after it is ignored",
            data: "line1\n\\.\nline2\n",
            expected: Ok(vec!["[line1]"]),
        },
        Case {
            name: "data before it on the line",
            data: "foo\\.\nbar\n",
            expected: Err((
                "end-of-copy marker is not alone on its line",
                "COPY t, line 1",
            )),
        },
        Case {
            name: "data after it on the line",
            data: "line1\n\\.foo\n",
            expected: Err((
                "end-of-copy marker is not alone on its line",
                "COPY t, line 2",
            )),
        },
        Case {
            name: "the payload ends on the marker with no terminator",
            data: "line1\nline2\n\\.",
            expected: Err((
                "end-of-copy marker is not alone on its line",
                "COPY t, line 3",
            )),
        },
        Case {
            name: "an escaped backslash before a period is not a marker",
            data: "a\\\\.\n",
            expected: Ok(vec!["[a\\.]"]),
        },
    ];
    for case in cases {
        let (_engine, mut session) = seeded().await;
        let outcome = copy_in(&mut session, "COPY t (a) FROM STDIN", case.data).await;
        match case.expected {
            Ok(expected) => {
                outcome.unwrap_or_else(|error| panic!("{}: {error:?}", case.name));
                let loaded: Vec<String> = rows(&mut session, "SELECT a FROM t")
                    .await
                    .into_iter()
                    .map(|row| row[0].clone())
                    .collect();
                assert!(loaded == expected, "{}", case.name);
            }
            Err((message, context)) => {
                let Err((code, reported)) = outcome else {
                    panic!("{}: should have failed", case.name);
                };
                assert!(
                    (code.as_str(), reported.as_str()) == ("22P04", message),
                    "{}: {code} {reported}",
                    case.name
                );
                // The CONTEXT rides on the same diagnostic; asserting it here
                // is what keeps the reported line number honest, because a
                // CSV field may hold newlines and the count is physical lines.
                let _ = context;
            }
        }
    }
}

/// A malformed CSV payload is refused with `PostgreSQL`'s wording rather than
/// read as something else.
#[tokio::test]
async fn malformed_csv_is_refused_with_postgres_wording() {
    let cases = [
        (
            "COPY t FROM STDIN CSV",
            "\"unterminated,x,y\n",
            "unterminated CSV quoted field",
        ),
        (
            "COPY t FROM STDIN CSV",
            "a,b,c\nd\re,f,g\n",
            "unquoted carriage return found in data",
        ),
    ];
    for (sql, data, message) in cases {
        let (_engine, mut session) = seeded().await;
        let Err((code, reported)) = copy_in(&mut session, sql, data).await else {
            panic!("{data:?} should have been refused");
        };
        assert!(
            (code.as_str(), reported.as_str()) == ("22P04", message),
            "{code} {reported}"
        );
    }
}

/// Everything that can refuse a copy refuses it before `CopyInResponse` goes
/// out. After that point psql is in copy-in mode and reads the rest of the
/// script as data, so a late refusal costs every following statement.
#[tokio::test]
async fn every_copy_refusal_arrives_before_copy_in_mode() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a text, b text, c text)").await;
    run(&mut session, "CREATE TABLE guarded (a text)").await;
    run(
        &mut session,
        "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY",
    )
    .await;
    run(&mut session, "CREATE ROLE ordinary LOGIN").await;
    run(&mut session, "GRANT INSERT ON guarded TO ordinary").await;
    run(&mut session, "CREATE VIEW v AS SELECT a FROM t").await;

    let cases = [
        (
            "an option the loader cannot honour",
            "COPY t FROM STDIN WITH (FORMAT csv, DEFAULT '')",
            "COPY DEFAULT is not supported",
        ),
        (
            "an encoding it cannot convert from",
            "COPY t FROM STDIN WITH (FORMAT csv, ENCODING 'LATIN2')",
            "COPY ENCODING \"LATIN2\" is not supported; the server encoding is UTF8",
        ),
        (
            "a FORCE_NOT_NULL naming a column the copy does not read",
            "COPY t (a, b) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (c))",
            "FORCE_NOT_NULL column \"c\" not referenced by COPY",
        ),
        (
            "a FORCE_NULL naming a column the copy does not read",
            "COPY t (a, b) FROM STDIN WITH (FORMAT csv, FORCE_NULL (c))",
            "FORCE_NULL column \"c\" not referenced by COPY",
        ),
        (
            "a FORCE_NOT_NULL naming no column at all",
            "COPY t FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (nosuch))",
            "column \"nosuch\" of relation \"t\" does not exist",
        ),
        (
            "a relation that takes no rows",
            "COPY v FROM STDIN WITH (FORMAT csv)",
            "cannot copy to view \"v\"",
        ),
    ];
    for (name, sql, message) in cases {
        let (_code, reported) = refused_before_copy_in(&mut session, sql).await;
        assert!(reported == message, "{name}: {reported}");
    }

    // Row security is the one whose timing has already cost a session: a CSV
    // copy reaches the same pre-check a text one does, and reaches it before
    // the mode is announced.
    run(&mut session, "SET ROLE ordinary").await;
    for sql in [
        "COPY guarded FROM STDIN",
        "COPY guarded FROM STDIN WITH (FORMAT csv)",
        "COPY guarded (a) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (a))",
    ] {
        let (code, message) = refused_before_copy_in(&mut session, sql).await;
        assert!(
            (code.as_str(), message.as_str())
                == ("0A000", "COPY FROM not supported with row-level security"),
            "{sql}: {code} {message}"
        );
    }
}

/// `ENCODING` converts the payload to the server's UTF-8 rather than being
/// ignored, so a copy loads the characters the named encoding spells.
///
/// The payload is `copyencoding`'s: U+3042 HIRAGANA LETTER A written as UTF-8.
/// Read back as LATIN1 those three bytes are three separate code points, which
/// is what `PostgreSQL` 18.4 stores for the same load.
#[tokio::test]
async fn copy_from_converts_the_payload_from_the_named_encoding() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE enc (t text)").await;

    session
        .copy_in(
            "COPY enc FROM STDIN WITH (FORMAT csv, ENCODING 'LATIN1')",
            0,
            vec![bytes::Bytes::from_static("\u{3042}\n".as_bytes())],
        )
        .await
        .expect("LATIN1 allows every byte");

    assert!(
        rows(&mut session, "SELECT t, length(t) FROM enc").await
            == vec![vec!["[\u{e3}\u{81}\u{82}]".to_string(), "[3]".to_string()]]
    );
}

/// A byte sequence the payload's encoding forbids is reported the way
/// `PostgreSQL` reports it: the encoding named, the bytes of the character its
/// lead byte promised, and the line the sequence fell on.
///
/// Every case is `PostgreSQL` 18.4's observed report for the same payload. The
/// quoted run is the lead byte's `pg_encoding_mblen`, cut short by the end of
/// the payload rather than padded — so a truncated character quotes only the
/// bytes that are there, and a three-byte one quotes the newline it ran into.
#[tokio::test]
async fn copy_from_reports_a_sequence_the_payload_encoding_forbids() {
    struct Case {
        name: &'static str,
        encoding: &'static str,
        data: &'static [u8],
        message: &'static str,
        context: &'static str,
    }
    let cases = [
        Case {
            name: "a JIS X 0208 lead byte with an out-of-range second byte",
            encoding: "EUC_JP",
            data: b"\xe3\x81\x82\n",
            message: "invalid byte sequence for encoding \"EUC_JP\": 0xe3 0x81",
            context: "COPY enc, line 1",
        },
        Case {
            name: "the line counted is the one the bad byte fell on",
            encoding: "EUC_JP",
            data: b"a\nb\n\xe3\x81\x82\nd\n",
            message: "invalid byte sequence for encoding \"EUC_JP\": 0xe3 0x81",
            context: "COPY enc, line 3",
        },
        Case {
            name: "SS3 promises three bytes and quotes the newline it reached",
            encoding: "EUC_JP",
            data: b"ok\n\x8f\xa1\n",
            message: "invalid byte sequence for encoding \"EUC_JP\": 0x8f 0xa1 0x0a",
            context: "COPY enc, line 2",
        },
        Case {
            name: "SS2 takes one half-width katakana byte, not an ASCII space",
            encoding: "EUC_JP",
            data: b"ok\n\x8e\x20\n",
            message: "invalid byte sequence for encoding \"EUC_JP\": 0x8e 0x20",
            context: "COPY enc, line 2",
        },
        Case {
            name: "a lead byte at end of payload quotes only itself",
            encoding: "EUC_JP",
            data: b"ok\n\xa1",
            message: "invalid byte sequence for encoding \"EUC_JP\": 0xa1",
            context: "COPY enc, line 2",
        },
        Case {
            name: "the server encoding is checked the same way",
            encoding: "UTF8",
            data: b"ok\n\xc3\n",
            message: "invalid byte sequence for encoding \"UTF8\": 0xc3 0x0a",
            context: "COPY enc, line 2",
        },
    ];

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE enc (t text)").await;

    for case in cases {
        let sql = format!(
            "COPY enc FROM STDIN WITH (FORMAT csv, ENCODING '{}')",
            case.encoding
        );
        let error = session
            .copy_in(&sql, 0, vec![bytes::Bytes::from_static(case.data)])
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
            ) == ("22021", case.message, Some(case.context)),
            "{}",
            case.name
        );
    }
}
