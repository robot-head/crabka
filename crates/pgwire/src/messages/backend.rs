//! Encoding of backend (server → client) messages.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{engine::FieldDescription, error::PgError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TxStatus {
    fn as_byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

/// Writes `tag` + self-inclusive length + body produced by `f`.
fn msg(out: &mut BytesMut, tag: u8, f: impl FnOnce(&mut BytesMut)) {
    out.put_u8(tag);
    let len_at = out.len();
    out.put_i32(0); // patched below
    f(out);
    let body_len = out.len() - len_at;
    let len = i32::try_from(body_len).expect("Postgres message body length fits in i32");
    out[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}

fn count_i16(count: usize) -> i16 {
    i16::try_from(count).expect("Postgres field count fits in i16")
}

fn len_i32(len: usize) -> i32 {
    i32::try_from(len).expect("Postgres byte length fits in i32")
}

fn put_cstr(out: &mut BytesMut, s: &str) {
    out.put_slice(s.as_bytes());
    out.put_u8(0);
}

pub fn authentication_ok(out: &mut BytesMut) {
    msg(out, b'R', |b| b.put_i32(0));
}

pub fn authentication_sasl(out: &mut BytesMut, mechanisms: &[&str]) {
    msg(out, b'R', |b| {
        b.put_i32(10);
        for m in mechanisms {
            put_cstr(b, m);
        }
        b.put_u8(0);
    });
}

pub fn authentication_sasl_continue(out: &mut BytesMut, data: &[u8]) {
    msg(out, b'R', |b| {
        b.put_i32(11);
        b.put_slice(data);
    });
}

pub fn authentication_sasl_final(out: &mut BytesMut, data: &[u8]) {
    msg(out, b'R', |b| {
        b.put_i32(12);
        b.put_slice(data);
    });
}

pub fn parameter_status(out: &mut BytesMut, name: &str, value: &str) {
    msg(out, b'S', |b| {
        put_cstr(b, name);
        put_cstr(b, value);
    });
}

pub fn backend_key_data(out: &mut BytesMut, process_id: i32, secret_key: i32) {
    msg(out, b'K', |b| {
        b.put_i32(process_id);
        b.put_i32(secret_key);
    });
}

pub fn ready_for_query(out: &mut BytesMut, status: TxStatus) {
    msg(out, b'Z', |b| b.put_u8(status.as_byte()));
}

pub fn command_complete(out: &mut BytesMut, tag: &str) {
    msg(out, b'C', |b| put_cstr(b, tag));
}

/// Encode an asynchronous `NotificationResponse` for a `NOTIFY`.
///
/// `process_id` is the notifying backend's process id. For a self-notify this
/// is the receiving connection's own `BackendKeyData` pid. The server delivers
/// the message outside the request/response cycle, so it only ever writes the
/// message between transactions: immediately before `ReadyForQuery`, or while
/// the connection is idle.
pub fn notification_response(out: &mut BytesMut, process_id: i32, channel: &str, payload: &str) {
    msg(out, b'A', |b| {
        b.put_i32(process_id);
        put_cstr(b, channel);
        put_cstr(b, payload);
    });
}

/// Encode a COPY-in response.
///
/// # Panics
///
/// Panics if `overall_format` does not fit in the protocol's one-byte format
/// field or if the number of column formats exceeds the protocol limit.
pub fn copy_in_response(out: &mut BytesMut, overall_format: i16, column_formats: &[i16]) {
    msg(out, b'G', |b| {
        b.put_u8(u8::try_from(overall_format).expect("COPY format code fits in u8"));
        b.put_i16(count_i16(column_formats.len()));
        for format in column_formats {
            b.put_i16(*format);
        }
    });
}

pub fn empty_query_response(out: &mut BytesMut) {
    msg(out, b'I', |_| {});
}

pub fn parse_complete(out: &mut BytesMut) {
    msg(out, b'1', |_| {});
}

pub fn bind_complete(out: &mut BytesMut) {
    msg(out, b'2', |_| {});
}

pub fn portal_suspended(out: &mut BytesMut) {
    msg(out, b's', |_| {});
}

pub fn close_complete(out: &mut BytesMut) {
    msg(out, b'3', |_| {});
}

pub fn no_data(out: &mut BytesMut) {
    msg(out, b'n', |_| {});
}

pub fn parameter_description(out: &mut BytesMut, type_oids: &[u32]) {
    msg(out, b't', |b| {
        b.put_i16(count_i16(type_oids.len()));
        for oid in type_oids {
            b.put_i32(oid.cast_signed());
        }
    });
}

pub fn row_description(out: &mut BytesMut, fields: &[FieldDescription]) {
    msg(out, b'T', |b| {
        b.put_i16(count_i16(fields.len()));
        for f in fields {
            put_cstr(b, &f.name);
            b.put_i32(f.table_oid.cast_signed());
            b.put_i16(f.column_id);
            b.put_i32(f.type_oid.cast_signed());
            b.put_i16(f.type_size);
            b.put_i32(f.type_modifier);
            b.put_i16(f.format);
        }
    });
}

pub fn data_row(out: &mut BytesMut, values: &[Option<Bytes>]) {
    msg(out, b'D', |b| {
        b.put_i16(count_i16(values.len()));
        for v in values {
            match v {
                Some(bytes) => {
                    b.put_i32(len_i32(bytes.len()));
                    b.put_slice(bytes);
                }
                None => b.put_i32(-1),
            }
        }
    });
}

fn diagnostic_response(out: &mut BytesMut, tag: u8, diagnostic: &PgError) {
    msg(out, tag, |b| {
        b.put_u8(b'S');
        put_cstr(b, diagnostic.severity.as_str());
        b.put_u8(b'V');
        put_cstr(b, diagnostic.severity.as_str());
        b.put_u8(b'C');
        put_cstr(b, &diagnostic.code);
        b.put_u8(b'M');
        put_cstr(b, &diagnostic.message);
        let fields = diagnostic.diagnostics.as_deref();
        for (tag, value) in [
            (b'D', fields.and_then(|fields| fields.detail.as_deref())),
            (b'H', fields.and_then(|fields| fields.hint.as_deref())),
            (
                b'P',
                fields
                    .and_then(|fields| fields.position.as_ref())
                    .map(usize::to_string)
                    .as_deref(),
            ),
            (b'W', fields.and_then(|fields| fields.context.as_deref())),
            (b's', fields.and_then(|fields| fields.schema.as_deref())),
            (b't', fields.and_then(|fields| fields.table.as_deref())),
            (b'c', fields.and_then(|fields| fields.column.as_deref())),
            (b'd', fields.and_then(|fields| fields.datatype.as_deref())),
            (b'n', fields.and_then(|fields| fields.constraint.as_deref())),
        ] {
            if let Some(value) = value {
                b.put_u8(tag);
                put_cstr(b, value);
            }
        }
        b.put_u8(0);
    });
}

pub fn error_response(out: &mut BytesMut, error: &PgError) {
    diagnostic_response(out, b'E', error);
}

/// Encode a `PostgreSQL` `NoticeResponse`. Its fields are identical to an
/// `ErrorResponse`. Only the leading message tag differs.
pub fn notice_response(out: &mut BytesMut, notice: &PgError) {
    debug_assert!(notice.severity.is_notice());
    diagnostic_response(out, b'N', notice);
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::*;
    use crate::{
        engine::FieldDescription,
        error::{PgError, sqlstate},
    };

    #[test]
    fn encodes_authentication_ok() {
        let mut out = BytesMut::new();
        authentication_ok(&mut out);
        assert_eq!(&out[..], b"R\x00\x00\x00\x08\x00\x00\x00\x00");
    }

    #[test]
    fn encodes_ready_for_query_idle() {
        let mut out = BytesMut::new();
        ready_for_query(&mut out, TxStatus::Idle);
        assert_eq!(&out[..], b"Z\x00\x00\x00\x05I");
    }

    #[test]
    fn encodes_parameter_status() {
        let mut out = BytesMut::new();
        parameter_status(&mut out, "client_encoding", "UTF8");
        assert_eq!(&out[..], b"S\x00\x00\x00\x19client_encoding\0UTF8\0");
    }

    #[test]
    fn encodes_command_complete() {
        let mut out = BytesMut::new();
        command_complete(&mut out, "SELECT 1");
        assert_eq!(&out[..], b"C\x00\x00\x00\x0dSELECT 1\0");
    }

    #[test]
    fn encodes_error_response_fields() {
        let base = || PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        let cases: [(PgError, &[u8]); 4] = [
            (
                base(),
                b"E\x00\x00\x00\x20SERROR\0VERROR\0C42601\0Moops\0\0",
            ),
            (
                base().with_detail("Key (p_id)=(1)"),
                b"E\x00\x00\x00\x30SERROR\0VERROR\0C42601\0Moops\0DKey (p_id)=(1)\0\0",
            ),
            (
                base().with_hint("use CASCADE"),
                b"E\x00\x00\x00\x2dSERROR\0VERROR\0C42601\0Moops\0Huse CASCADE\0\0",
            ),
            (
                base().with_detail("Key (p_id)=(1)").with_hint("use CASCADE"),
                b"E\x00\x00\x00\x3dSERROR\0VERROR\0C42601\0Moops\0DKey (p_id)=(1)\0Huse CASCADE\0\0",
            ),
        ];

        for (err, expected) in cases {
            let mut out = BytesMut::new();
            error_response(&mut out, &err);
            assert2::assert!(&out[..] == expected);
        }
    }

    #[test]
    fn encodes_fatal_error_response_with_hint_only() {
        let mut out = BytesMut::new();
        error_response(
            &mut out,
            &PgError::protocol("bad frame").with_hint("check the length prefix"),
        );
        assert2::assert!(
            &out[..]
                == &b"E\x00\x00\x00\x3eSFATAL\0VFATAL\0C08P01\0Mbad frame\0Hcheck the length prefix\0\0"[..]
        );
    }

    #[test]
    fn encodes_notice_response_with_structured_fields_exactly() {
        let mut out = BytesMut::new();
        let notice = PgError::warning("careful")
            .with_code("01004")
            .with_detail("shortened")
            .with_hint("widen it")
            .with_context("function f() line 2")
            .with_schema("public")
            .with_table("things")
            .with_column("name")
            .with_datatype("text")
            .with_constraint("things_name_check");
        notice_response(&mut out, &notice);

        assert2::assert!(
            &out[..]
                == b"N\x00\x00\x00\x80SWARNING\0VWARNING\0C01004\0Mcareful\0Dshortened\0Hwiden it\0Wfunction f() line 2\0spublic\0tthings\0cname\0dtext\0nthings_name_check\0\0"
        );
    }

    #[test]
    fn encodes_row_description_and_data_row() {
        let mut out = BytesMut::new();
        let fields = [FieldDescription {
            name: "?column?".into(),
            table_oid: 0,
            column_id: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        }];
        row_description(&mut out, &fields);
        assert_eq!(
            &out[..],
            &b"T\x00\x00\x00\x21\x00\x01?column?\0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00"[..]
        );

        let mut out = BytesMut::new();
        data_row(&mut out, &[Some(Bytes::from_static(b"1")), None]);
        // tag D, len 15: 4(len) + 2(count) + 4+1 (value "1") + 4 (-1 null)
        assert_eq!(
            &out[..],
            b"D\x00\x00\x00\x0f\x00\x02\x00\x00\x00\x011\xff\xff\xff\xff"
        );
    }

    #[test]
    fn encodes_notification_response() {
        let mut out = BytesMut::new();
        notification_response(&mut out, 4242, "chan", "hi");
        // tag A, len 16 = 4(len) + 4(pid) + 5("chan\0") + 3("hi\0")
        assert2::assert!(&out[..] == b"A\x00\x00\x00\x10\x00\x00\x10\x92chan\0hi\0");
    }

    #[test]
    fn encodes_notification_response_with_empty_payload() {
        let mut out = BytesMut::new();
        notification_response(&mut out, 1, "c", "");
        assert2::assert!(&out[..] == b"A\x00\x00\x00\x0b\x00\x00\x00\x01c\0\0");
    }

    #[test]
    fn encodes_backend_key_data() {
        let mut out = BytesMut::new();
        backend_key_data(&mut out, 4242, 777);
        assert_eq!(out[0], b'K');
        assert_eq!(out.len(), 13);
        assert_eq!(&out[5..9], &4242i32.to_be_bytes());
        assert_eq!(&out[9..13], &777i32.to_be_bytes());
    }

    #[test]
    fn encodes_auth_sasl_flow_messages() {
        let mut out = BytesMut::new();
        authentication_sasl(&mut out, &["SCRAM-SHA-256"]);
        assert_eq!(
            &out[..],
            b"R\x00\x00\x00\x17\x00\x00\x00\x0aSCRAM-SHA-256\0\0"
        );

        let mut out = BytesMut::new();
        authentication_sasl_continue(&mut out, b"r=abc");
        assert_eq!(&out[..], b"R\x00\x00\x00\x0d\x00\x00\x00\x0br=abc");

        let mut out = BytesMut::new();
        authentication_sasl_final(&mut out, b"v=xyz");
        assert_eq!(&out[..], b"R\x00\x00\x00\x0d\x00\x00\x00\x0cv=xyz");
    }

    #[test]
    fn encodes_extended_protocol_responses() {
        let mut out = BytesMut::new();
        parse_complete(&mut out);
        bind_complete(&mut out);
        portal_suspended(&mut out);
        close_complete(&mut out);
        no_data(&mut out);
        empty_query_response(&mut out);
        parameter_description(&mut out, &[23, 25]);
        assert_eq!(
            &out[..],
            &b"1\x00\x00\x00\x042\x00\x00\x00\x04s\x00\x00\x00\x043\x00\x00\x00\x04n\x00\x00\x00\x04I\x00\x00\x00\x04t\x00\x00\x00\x0e\x00\x02\x00\x00\x00\x17\x00\x00\x00\x19"[..]
        );
    }
}
