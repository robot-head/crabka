//! `ErrorResponse` framing checked field by field, the way a client reads it.
//!
//! Postgres sends the message as a sequence of typed fields terminated by a
//! zero byte, and clients (`tokio-postgres`, `libpq`) surface `D`/`H` as
//! `DbError::detail()` / `hint()`. These tests decode the encoded frame back
//! into `(field type, value)` pairs so the assertions are about what a client
//! would see, not about the order of `put_u8` calls.

use assert2::assert;
use bytes::BytesMut;
use crabka_pgwire::{
    error::{PgError, sqlstate},
    messages::backend,
};

fn encode(err: &PgError) -> BytesMut {
    let mut out = BytesMut::new();
    backend::error_response(&mut out, err);
    out
}

/// Split an encoded `ErrorResponse` into its `(field type, value)` pairs,
/// verifying the frame's self-inclusive length prefix on the way through.
fn decode_error_response(frame: &[u8]) -> Vec<(u8, String)> {
    assert!(frame[0] == b'E');
    let len = i32::from_be_bytes(frame[1..5].try_into().expect("four length bytes"));
    let len = usize::try_from(len).expect("non-negative length");
    assert!(
        len == frame.len() - 1,
        "length prefix covers the whole body"
    );

    let mut fields = Vec::new();
    let mut rest = &frame[5..];
    while rest[0] != 0 {
        let field_type = rest[0];
        let end = rest[1..]
            .iter()
            .position(|b| *b == 0)
            .expect("field value is NUL-terminated");
        let value = str::from_utf8(&rest[1..=end])
            .expect("field value is UTF-8")
            .to_owned();
        fields.push((field_type, value));
        rest = &rest[end + 2..];
    }
    assert!(rest.len() == 1, "terminator is the final byte");
    fields
}

#[test]
fn foreign_key_violation_carries_detail_and_hint_in_postgres_order() {
    let err = PgError::error(
        "23503",
        r#"insert or update on table "c" violates foreign key constraint "c_p_id_fkey""#,
    )
    .with_detail(r#"Key (p_id)=(1) is not present in table "p"."#)
    .with_hint(r#"Truncate table "c" at the same time, or use TRUNCATE ... CASCADE."#);

    let fields = decode_error_response(&encode(&err));

    assert!(
        fields
            == vec![
                (b'S', "ERROR".to_owned()),
                (b'V', "ERROR".to_owned()),
                (b'C', "23503".to_owned()),
                (
                    b'M',
                    r#"insert or update on table "c" violates foreign key constraint "c_p_id_fkey""#
                        .to_owned()
                ),
                (
                    b'D',
                    r#"Key (p_id)=(1) is not present in table "p"."#.to_owned()
                ),
                (
                    b'H',
                    r#"Truncate table "c" at the same time, or use TRUNCATE ... CASCADE."#
                        .to_owned()
                ),
            ]
    );
}

#[test]
fn absent_detail_and_hint_are_omitted_rather_than_sent_empty() {
    let fields = decode_error_response(&encode(&PgError::error(sqlstate::SYNTAX_ERROR, "oops")));

    assert!(
        fields
            == vec![
                (b'S', "ERROR".to_owned()),
                (b'V', "ERROR".to_owned()),
                (b'C', "42601".to_owned()),
                (b'M', "oops".to_owned()),
            ]
    );
}

#[test]
fn detail_alone_and_hint_alone_each_emit_only_their_own_field() {
    let detail_only = decode_error_response(&encode(
        &PgError::error(sqlstate::SYNTAX_ERROR, "oops").with_detail("just detail"),
    ));
    let hint_only = decode_error_response(&encode(
        &PgError::error(sqlstate::SYNTAX_ERROR, "oops").with_hint("just hint"),
    ));

    let types = |fields: &[(u8, String)]| fields.iter().map(|(t, _)| *t).collect::<Vec<_>>();
    assert!(types(&detail_only) == vec![b'S', b'V', b'C', b'M', b'D']);
    assert!(types(&hint_only) == vec![b'S', b'V', b'C', b'M', b'H']);
}

#[test]
fn empty_detail_string_still_produces_a_field() {
    let fields = decode_error_response(&encode(
        &PgError::error(sqlstate::SYNTAX_ERROR, "oops").with_detail(""),
    ));

    assert!(fields.last() == Some(&(b'D', String::new())));
}
