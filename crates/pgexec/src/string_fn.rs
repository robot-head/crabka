//! The remaining PostgreSQL string functions: `format`, the message-digest and
//! binary-encoding families (`md5`, `sha*`, `encode`/`decode`, `to_hex`), SQL
//! quoting (`quote_ident`/`quote_literal`/`quote_nullable`), the search and
//! rewrite utilities (`split_part`, `translate`, `starts_with`, `concat_ws`),
//! and the Unicode surface (`unistr`, `normalize`, `is_normalized`,
//! `parse_ident`, `to_ascii`).
//!
//! Like the other function families here, every entry is a pure transform over
//! one row's already-evaluated Datums. `func::is_scalar` routes the names in, so
//! `eval` needs no new dispatch point.

use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};
use unicode_normalization::{UnicodeNormalization, is_nfc, is_nfd, is_nfkc, is_nfkd};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{
        ambiguous_function, checked_args, int_arg, is_unknown_arg, no_matching_function,
        require_arity, text_render, type_error, undefined_function, undefined_function_spelled,
    },
    scope::Scope,
};

/// The string functions this module owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrFunc {
    Format,
    ConcatWs,
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Encode,
    Decode,
    Convert,
    ConvertFrom,
    ToHex,
    QuoteIdent,
    QuoteLiteral,
    QuoteNullable,
    SplitPart,
    Translate,
    StartsWith,
    Unistr,
    ParseIdent,
    ToAscii,
    Normalize,
    IsNormalized,
    OctetLength,
    BitLength,
}

fn str_func(name: &str) -> Option<StrFunc> {
    Some(match name {
        "format" => StrFunc::Format,
        "concat_ws" => StrFunc::ConcatWs,
        "md5" => StrFunc::Md5,
        "sha224" => StrFunc::Sha224,
        "sha256" => StrFunc::Sha256,
        "sha384" => StrFunc::Sha384,
        "sha512" => StrFunc::Sha512,
        "encode" => StrFunc::Encode,
        "decode" => StrFunc::Decode,
        "convert" => StrFunc::Convert,
        "convert_from" => StrFunc::ConvertFrom,
        "to_hex" => StrFunc::ToHex,
        "quote_ident" => StrFunc::QuoteIdent,
        "quote_literal" => StrFunc::QuoteLiteral,
        "quote_nullable" => StrFunc::QuoteNullable,
        "split_part" => StrFunc::SplitPart,
        "translate" => StrFunc::Translate,
        "starts_with" => StrFunc::StartsWith,
        "unistr" => StrFunc::Unistr,
        "parse_ident" => StrFunc::ParseIdent,
        "to_ascii" => StrFunc::ToAscii,
        "normalize" => StrFunc::Normalize,
        "is_normalized" => StrFunc::IsNormalized,
        "octet_length" => StrFunc::OctetLength,
        "bit_length" => StrFunc::BitLength,
        _ => return None,
    })
}

/// Is `name` one of this module's functions? `func::is_scalar` folds this in.
pub(crate) fn is_string_func(name: &str) -> bool {
    str_func(name).is_some()
}

/// Statically infer a string call's result type, and validate the name and the
/// arity. Argument *types* are mostly unconstrained here. PostgreSQL's `format`,
/// `concat_ws` and `quote_literal` all take `"any"`, and the rest accept any
/// argument the text/bytea output functions can render.
pub(crate) fn string_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = str_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        // `format(fmt, ...)` and `concat_ws(sep, ...)` are variadic over "any".
        StrFunc::Format => {
            require_arity(fc, n >= 1)?;
            Ok(ColumnType::Text)
        }
        // `concat_ws(separator, ...)` is VARIADIC "any" after the separator, so
        // it needs at least one value argument to have a candidate at all.
        StrFunc::ConcatWs => {
            if n < 2 {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(ColumnType::Text)
        }
        // The digests take `text` (md5) or `bytea`; PostgreSQL has no implicit
        // cast onto either from a number, so anything else is 42883.
        StrFunc::Md5 => {
            require_arity(fc, n == 1)?;
            require_string_arg(fc, args, scope)?;
            Ok(ColumnType::Text)
        }
        StrFunc::Sha224 | StrFunc::Sha256 | StrFunc::Sha384 | StrFunc::Sha512 => {
            require_arity(fc, n == 1)?;
            require_string_arg(fc, args, scope)?;
            Ok(ColumnType::Bytea)
        }
        StrFunc::Encode => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Text)
        }
        StrFunc::Decode => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Bytea)
        }
        StrFunc::Convert => {
            require_arity(fc, n == 3)?;
            if !is_unknown_arg(&args[0])
                && crate::eval::infer_type(&args[0], scope)? != ColumnType::Bytea
            {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            for arg in &args[1..] {
                if !is_unknown_arg(arg) && !crate::eval::infer_type(arg, scope)?.is_string() {
                    return Err(undefined_function_spelled(&fc.name, args, scope));
                }
            }
            Ok(ColumnType::Bytea)
        }
        StrFunc::ConvertFrom => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Text)
        }
        // `to_hex` has an int4 and an int8 overload and no preferred one, so a
        // lone `unknown` argument leaves PostgreSQL unable to choose.
        StrFunc::ToHex => {
            require_arity(fc, n == 1)?;
            if is_unknown_arg(&args[0]) {
                return Err(ambiguous_function(&fc.name, 1));
            }
            require_int_or_null(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        StrFunc::QuoteIdent
        | StrFunc::QuoteLiteral
        | StrFunc::QuoteNullable
        | StrFunc::Translate
        | StrFunc::Unistr
        | StrFunc::ToAscii => {
            require_arity(fc, n == if f == StrFunc::Translate { 3 } else { 1 })?;
            Ok(ColumnType::Text)
        }
        StrFunc::Normalize => {
            require_arity(fc, n == 1 || n == 2)?;
            Ok(ColumnType::Text)
        }
        StrFunc::SplitPart => {
            require_arity(fc, n == 3)?;
            Ok(ColumnType::Text)
        }
        StrFunc::StartsWith | StrFunc::IsNormalized => {
            require_arity(fc, n == 1 || n == 2)?;
            Ok(ColumnType::Bool)
        }
        StrFunc::ParseIdent => {
            require_arity(fc, n == 1 || n == 2)?;
            Ok(ColumnType::Array(ElemType::Text))
        }
        StrFunc::OctetLength | StrFunc::BitLength => {
            require_arity(fc, n == 1)?;
            Ok(ColumnType::Int4)
        }
    }
}

/// Require an argument the `text`/`bytea` digest parameters accept: either of
/// those types, or an `unknown` literal PostgreSQL would coerce into one.
fn require_string_arg(fc: &FuncCall, args: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    if is_unknown_arg(&args[0]) {
        return Ok(());
    }
    match crate::eval::infer_type(&args[0], scope)? {
        ColumnType::Text | ColumnType::Bytea => Ok(()),
        _ => Err(undefined_function_spelled(&fc.name, args, scope)),
    }
}

fn require_int_or_null(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if matches!(arg, Expr::NullLiteral) {
        return Ok(());
    }
    match crate::eval::infer_type(arg, scope)? {
        ColumnType::Int4 | ColumnType::Int8 => Ok(()),
        _ => Err(no_matching_function()),
    }
}

/// Evaluate a string call. `format`, `concat_ws` and `quote_nullable` are not
/// strict, and they render or skip NULL arguments. Everything else
/// short-circuits to NULL on a NULL argument.
pub(crate) fn eval_string(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = str_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let vals = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    match f {
        StrFunc::Format => {
            require_arity(fc, !vals.is_empty())?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            format_sql(text_arg(&vals[0])?, &vals[1..], ctx).map(Datum::Text)
        }
        // concat_ws is strict in its SEPARATOR only; NULL values are skipped.
        StrFunc::ConcatWs => {
            require_arity(fc, vals.len() >= 2)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let sep = text_arg(&vals[0])?;
            let parts: Vec<String> = vals[1..]
                .iter()
                .filter(|v| !v.is_null())
                .map(|v| text_render(v, &ctx.time_zone))
                .collect();
            Ok(Datum::Text(parts.join(sep)))
        }
        StrFunc::QuoteNullable if vals.first().is_some_and(Datum::is_null) => {
            Ok(Datum::Text("NULL".into()))
        }
        _ if vals.iter().any(Datum::is_null) => Ok(Datum::Null),
        _ => eval_strict(f, fc, &vals, ctx),
    }
}

fn eval_strict(
    f: StrFunc,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match f {
        StrFunc::Md5 => {
            require_arity(fc, vals.len() == 1)?;
            use md5::Digest;
            Ok(Datum::Text(hex::encode(md5::Md5::digest(bytes_arg(
                &vals[0],
            )?))))
        }
        StrFunc::Sha224 | StrFunc::Sha256 | StrFunc::Sha384 | StrFunc::Sha512 => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Bytea(sha(f, bytes_arg(&vals[0])?)))
        }
        StrFunc::Encode => {
            require_arity(fc, vals.len() == 2)?;
            encode(bytes_of(&vals[0])?, text_arg(&vals[1])?).map(Datum::Text)
        }
        StrFunc::Decode => {
            require_arity(fc, vals.len() == 2)?;
            decode(text_arg(&vals[0])?, text_arg(&vals[1])?).map(Datum::Bytea)
        }
        StrFunc::Convert => {
            require_arity(fc, vals.len() == 3)?;
            let bytes = conversion_bytes(&vals[0], ctx)?;
            convert_encoding(&bytes, text_arg(&vals[1])?, text_arg(&vals[2])?).map(Datum::Bytea)
        }
        StrFunc::ConvertFrom => {
            require_arity(fc, vals.len() == 2)?;
            let bytes = conversion_bytes(&vals[0], ctx)?;
            decode_encoding(&bytes, text_arg(&vals[1])?).map(Datum::Text)
        }
        StrFunc::ToHex => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(match &vals[0] {
                Datum::Int4(n) => format!("{:x}", *n as u32),
                other => format!("{:x}", int_arg(other)? as u64),
            }))
        }
        StrFunc::QuoteIdent => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(quote_ident(text_arg(&vals[0])?)))
        }
        StrFunc::QuoteLiteral | StrFunc::QuoteNullable => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(quote_literal(&text_render(
                &vals[0],
                &ctx.time_zone,
            ))))
        }
        StrFunc::SplitPart => {
            require_arity(fc, vals.len() == 3)?;
            split_part(text_arg(&vals[0])?, text_arg(&vals[1])?, int_arg(&vals[2])?)
                .map(Datum::Text)
        }
        StrFunc::Translate => {
            require_arity(fc, vals.len() == 3)?;
            Ok(Datum::Text(translate(
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
                text_arg(&vals[2])?,
            )))
        }
        StrFunc::StartsWith => {
            require_arity(fc, vals.len() == 2)?;
            Ok(Datum::Bool(
                text_arg(&vals[0])?.starts_with(text_arg(&vals[1])?),
            ))
        }
        StrFunc::Unistr => {
            require_arity(fc, vals.len() == 1)?;
            unistr(text_arg(&vals[0])?).map(Datum::Text)
        }
        StrFunc::ParseIdent => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let strict = match vals.get(1) {
                None => true,
                Some(Datum::Bool(b)) => *b,
                Some(other) => return Err(type_error("parse_ident", other)),
            };
            parse_ident(text_arg(&vals[0])?, strict)
        }
        // PostgreSQL's `to_ascii` only knows how to transliterate from LATIN1,
        // LATIN2, LATIN9 and WIN1250. Crabka is UTF-8 only, so the one-argument
        // form always raises the same 0A000 the oracle raises on a UTF8 database.
        StrFunc::ToAscii => {
            require_arity(fc, vals.len() == 1)?;
            Err(ExecError::FunctionError {
                sqlstate: "0A000",
                message: "encoding conversion from UTF8 to ASCII not supported".into(),
            })
        }
        StrFunc::Normalize => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let form = normalization_form(vals.get(1))?;
            Ok(Datum::Text(normalize(text_arg(&vals[0])?, form)))
        }
        StrFunc::IsNormalized => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let form = normalization_form(vals.get(1))?;
            Ok(Datum::Bool(is_normalized(text_arg(&vals[0])?, form)))
        }
        StrFunc::OctetLength => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Int4(byte_len(bytes_of(&vals[0])?.len())))
        }
        StrFunc::BitLength => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Int4(byte_len(bytes_of(&vals[0])?.len() * 8)))
        }
        StrFunc::Format | StrFunc::ConcatWs => Err(undefined_function(&fc.name)),
    }
}

fn byte_len(n: usize) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

fn conversion_bytes(value: &Datum, ctx: &EvalCtx) -> Result<Vec<u8>, ExecError> {
    match value {
        Datum::Bytea(bytes) => Ok(bytes.clone()),
        Datum::Text(_) => {
            match crabka_pgtypes::cast::cast(value, ColumnType::Bytea, &ctx.time_zone)? {
                Datum::Bytea(bytes) => Ok(bytes),
                _ => unreachable!("text to bytea cast returns bytea"),
            }
        }
        other => Err(type_error("convert", other)),
    }
}

fn convert_encoding(bytes: &[u8], source: &str, target: &str) -> Result<Vec<u8>, ExecError> {
    let Some(source_id) = crate::catalog_fn::encoding_id(source) else {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: format!("invalid source encoding name \"{source}\""),
        });
    };
    let Some(target_id) = crate::catalog_fn::encoding_id(target) else {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: format!("invalid destination encoding name \"{target}\""),
        });
    };
    let supported = source_id == target_id
        || source_id == 0
        || target_id == 0
        || crate::builtin_conversions::BUILTIN_CONVERSIONS.iter().any(
            |&(_, _, _, _, candidate_source, candidate_target, _, default)| {
                default && candidate_source == source_id && candidate_target == target_id
            },
        );
    if !supported {
        return Err(ExecError::FunctionError {
            sqlstate: "42883",
            message: format!("default conversion from {source} to {target} does not exist"),
        });
    }
    // ponytail: ASCII is invariant across PostgreSQL's built-in encodings;
    // add a converter backend when non-ASCII conversion becomes an owning test.
    if bytes.is_ascii() || source_id == target_id || target_id == 0 {
        return Ok(bytes.to_vec());
    }
    Err(ExecError::FunctionError {
        sqlstate: "0A000",
        message: format!("encoding conversion from {source} to {target} is not supported"),
    })
}

fn decode_encoding(bytes: &[u8], source: &str) -> Result<String, ExecError> {
    let Some(source_id) = crate::catalog_fn::encoding_id(source) else {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: format!("invalid source encoding name \"{source}\""),
        });
    };
    if source_id == crate::catalog_fn::UTF8_ENCODING {
        return String::from_utf8(bytes.to_vec()).map_err(|_| ExecError::FunctionError {
            sqlstate: "22021",
            message: "invalid byte sequence for encoding \"UTF8\"".into(),
        });
    }
    if source_id != crate::catalog_fn::EUC_KR_ENCODING {
        return Err(ExecError::FunctionError {
            sqlstate: "0A000",
            message: format!("encoding conversion from {source} to UTF8 is not supported"),
        });
    }
    // PostgreSQL EUC_KR accepts ASCII plus strict KS X 1001 two-byte pairs.
    // encoding_rs implements the wider Windows-949 repertoire, so validate the
    // byte grammar before using its decoder.
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii() {
            index += 1;
            continue;
        }
        if index + 1 >= bytes.len()
            || !(0xa1..=0xfe).contains(&bytes[index])
            || !(0xa1..=0xfe).contains(&bytes[index + 1])
        {
            return Err(ExecError::FunctionError {
                sqlstate: "22021",
                message: format!("invalid byte sequence for encoding \"{source}\""),
            });
        }
        index += 2;
    }
    encoding_rs::EUC_KR
        .decode_without_bom_handling_and_without_replacement(bytes)
        .map(|text| text.into_owned())
        .ok_or_else(|| ExecError::FunctionError {
            sqlstate: "22021",
            message: format!("invalid byte sequence for encoding \"{source}\""),
        })
}

fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// The bytes a `bytea`-parameter function sees. An untyped literal reaches here
/// as text. PostgreSQL coerces it to `bytea` through the input function, and for
/// a plain string that is its UTF-8 encoding.
fn bytes_arg(d: &Datum) -> Result<&[u8], ExecError> {
    match d {
        Datum::Bytea(b) => Ok(b),
        Datum::Text(s) => Ok(s.as_bytes()),
        other => Err(type_error("function", other)),
    }
}

fn bytes_of(d: &Datum) -> Result<&[u8], ExecError> {
    bytes_arg(d)
}

// ---- message digests ----

fn sha(f: StrFunc, input: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    match f {
        StrFunc::Sha224 => sha2::Sha224::digest(input).to_vec(),
        StrFunc::Sha384 => sha2::Sha384::digest(input).to_vec(),
        StrFunc::Sha512 => sha2::Sha512::digest(input).to_vec(),
        _ => sha2::Sha256::digest(input).to_vec(),
    }
}

// ---- binary encodings ----

fn unrecognized(what: &str, name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message: format!("unrecognized {what}: \"{name}\""),
    }
}

/// PostgreSQL wraps base64 output at 76 characters, as MIME does.
const BASE64_LINE: usize = 76;

fn encode(input: &[u8], encoding: &str) -> Result<String, ExecError> {
    match encoding {
        "hex" => Ok(hex::encode(input)),
        "base64" => {
            use base64::Engine;
            let flat = base64::engine::general_purpose::STANDARD.encode(input);
            Ok(flat
                .as_bytes()
                .chunks(BASE64_LINE)
                .map(|c| String::from_utf8_lossy(c).into_owned())
                .collect::<Vec<_>>()
                .join("\n"))
        }
        "escape" => Ok(escape_encode(input)),
        other => Err(unrecognized("encoding", other)),
    }
}

/// `bytea_out`'s traditional escape format: printable ASCII except backslash
/// stays literal, a backslash doubles, and everything else is `\nnn` octal.
fn escape_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input {
        match b {
            b'\\' => out.push_str("\\\\"),
            0x20..=0x7e => out.push(char::from(b)),
            _ => {
                use std::fmt::Write;
                write!(out, "\\{b:03o}").expect("writing to a String cannot fail");
            }
        }
    }
    out
}

fn decode(input: &str, encoding: &str) -> Result<Vec<u8>, ExecError> {
    match encoding {
        "hex" => hex::decode(input).map_err(|_| {
            let bad = input
                .chars()
                .find(|c| !c.is_ascii_hexdigit())
                .map_or_else(|| input.to_string(), |c| c.to_string());
            ExecError::FunctionError {
                sqlstate: "22023",
                message: format!("invalid hexadecimal digit: \"{bad}\""),
            }
        }),
        "base64" => {
            use base64::Engine;
            // PostgreSQL's base64 decoder skips newlines and other whitespace.
            let compact: String = input.chars().filter(|c| !c.is_whitespace()).collect();
            base64::engine::general_purpose::STANDARD
                .decode(compact)
                .map_err(|_| ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "invalid symbol found while decoding base64 sequence".into(),
                })
        }
        "escape" => Ok(escape_decode(input)),
        other => Err(unrecognized("encoding", other)),
    }
}

/// The inverse of [`escape_encode`]: `\\` is one backslash, `\nnn` is one octal
/// byte, and any other byte passes through.
fn escape_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            if bytes[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
                continue;
            }
            if i + 3 < bytes.len()
                && let Some(value) = octal_triple(&bytes[i + 1..i + 4])
            {
                out.push(value);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn octal_triple(digits: &[u8]) -> Option<u8> {
    let mut value: u16 = 0;
    for &d in digits {
        if !(b'0'..=b'7').contains(&d) {
            return None;
        }
        value = value * 8 + u16::from(d - b'0');
    }
    u8::try_from(value).ok()
}

// ---- SQL quoting ----

/// PostgreSQL 18's non-`UNRESERVED` scan keywords: every word
/// `pg_get_keywords()` reports with a category other than `U`. `quote_ident`
/// (and `format`'s `%I`) must quote these even though they are lexically valid
/// identifiers, because a bare re-parse would produce a different parse.
const QUOTED_KEYWORDS: [&str; 164] = [
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
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
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
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

/// `quote_ident`: PostgreSQL's `quote_identifier`. It leaves the string bare
/// only when the string starts with a lowercase letter or `_`, contains nothing
/// but `[a-z0-9_]`, and is not a non-unreserved keyword. Otherwise it
/// double-quotes the string and doubles any embedded quote.
pub(crate) fn quote_ident(s: &str) -> String {
    let safe = s.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && QUOTED_KEYWORDS.binary_search(&s).is_err();
    if safe {
        return s.to_string();
    }
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `quote_literal`: single-quote the string and double any embedded quote. A
/// backslash forces PostgreSQL's `E'…'` escape-string syntax.
fn quote_literal(s: &str) -> String {
    let body = s.replace('\'', "''");
    if s.contains('\\') {
        format!("E'{}'", body.replace('\\', "\\\\"))
    } else {
        format!("'{body}'")
    }
}

// ---- search and rewrite ----

/// `split_part(string, delimiter, n)`: the `n`-th field, counted from 1, or from
/// the end for a negative `n`. An out-of-range `n` gives the empty string, and
/// `n = 0` is an error.
fn split_part(s: &str, delim: &str, n: i64) -> Result<String, ExecError> {
    if n == 0 {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "field position must not be zero".into(),
        });
    }
    if delim.is_empty() {
        // An empty delimiter makes the whole string field 1 (and -1).
        return Ok(if n == 1 || n == -1 {
            s.to_string()
        } else {
            String::new()
        });
    }
    let fields: Vec<&str> = s.split(delim).collect();
    let index = if n > 0 {
        n - 1
    } else {
        fields.len() as i64 + n
    };
    Ok(usize::try_from(index)
        .ok()
        .and_then(|i| fields.get(i))
        .unwrap_or(&"")
        .to_string())
}

/// `translate(string, from, to)`: replace each character of `from` with the
/// character at the same position in `to`, and delete it when `to` is shorter.
fn translate(s: &str, from: &str, to: &str) -> String {
    let from: Vec<char> = from.chars().collect();
    let to: Vec<char> = to.chars().collect();
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match from.iter().position(|f| *f == c) {
            None => out.push(c),
            Some(i) => {
                if let Some(r) = to.get(i) {
                    out.push(*r);
                }
            }
        }
    }
    out
}

// ---- Unicode ----

fn invalid_unicode_escape() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42601",
        message: "invalid Unicode escape".into(),
    }
}

/// `unistr(text)`: expand `\XXXX`, `\+XXXXXX`, `\uXXXX`, `\UXXXXXXXX` and `\\`.
fn unistr(s: &str) -> Result<String, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '\\' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        let next = *chars.get(i + 1).ok_or_else(invalid_unicode_escape)?;
        let (width, start) = match next {
            '\\' => {
                out.push('\\');
                i += 2;
                continue;
            }
            '+' => (6, i + 2),
            'u' => (4, i + 2),
            'U' => (8, i + 2),
            _ => (4, i + 1),
        };
        let end = start + width;
        if end > chars.len() {
            return Err(invalid_unicode_escape());
        }
        let digits: String = chars[start..end].iter().collect();
        let code = u32::from_str_radix(&digits, 16).map_err(|_| invalid_unicode_escape())?;
        out.push(char::from_u32(code).ok_or_else(invalid_unicode_escape)?);
        i = end;
    }
    Ok(out)
}

/// `parse_ident(qualified_name [, strict])`: split a possibly-quoted dotted name
/// into its parts, and lowercase the unquoted ones. `strict`, the default,
/// rejects trailing junk and does not ignore it.
fn parse_ident(s: &str, strict: bool) -> Result<Datum, ExecError> {
    let not_ident = || ExecError::FunctionError {
        sqlstate: "22023",
        message: format!("string is not a valid identifier: \"{s}\""),
    };
    let chars: Vec<char> = s.chars().collect();
    let mut parts: Vec<Datum> = Vec::new();
    let mut i = 0;
    loop {
        while chars.get(i).is_some_and(|c| c.is_whitespace()) {
            i += 1;
        }
        if chars.get(i) == Some(&'"') {
            i += 1;
            let mut part = String::new();
            loop {
                let c = *chars.get(i).ok_or_else(not_ident)?;
                i += 1;
                if c == '"' {
                    if chars.get(i) == Some(&'"') {
                        part.push('"');
                        i += 1;
                        continue;
                    }
                    break;
                }
                part.push(c);
            }
            parts.push(Datum::Text(part));
        } else {
            let start = i;
            while chars
                .get(i)
                .is_some_and(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            {
                i += 1;
            }
            if i == start || chars[start].is_ascii_digit() {
                return Err(not_ident());
            }
            let part: String = chars[start..i].iter().collect();
            parts.push(Datum::Text(part.to_lowercase()));
        }
        while chars.get(i).is_some_and(|c| c.is_whitespace()) {
            i += 1;
        }
        match chars.get(i) {
            Some('.') => i += 1,
            None => break,
            Some(_) if strict => return Err(not_ident()),
            Some(_) => break,
        }
    }
    Ok(Datum::Array(ArrayValue::new(ElemType::Text, parts)))
}

/// The four Unicode normalization forms `normalize`/`is_normalized` accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    Nfc,
    Nfd,
    Nfkc,
    Nfkd,
}

fn normalization_form(arg: Option<&Datum>) -> Result<Form, ExecError> {
    let Some(d) = arg else {
        return Ok(Form::Nfc);
    };
    let name = text_arg(d)?;
    Ok(match name.to_ascii_uppercase().as_str() {
        "NFC" => Form::Nfc,
        "NFD" => Form::Nfd,
        "NFKC" => Form::Nfkc,
        "NFKD" => Form::Nfkd,
        _ => {
            return Err(ExecError::FunctionError {
                sqlstate: "22023",
                message: format!("invalid normalization form: {name}"),
            });
        }
    })
}

fn normalize(s: &str, form: Form) -> String {
    match form {
        Form::Nfc => s.nfc().collect(),
        Form::Nfd => s.nfd().collect(),
        Form::Nfkc => s.nfkc().collect(),
        Form::Nfkd => s.nfkd().collect(),
    }
}

fn is_normalized(s: &str, form: Form) -> bool {
    match form {
        Form::Nfc => is_nfc(s),
        Form::Nfd => is_nfd(s),
        Form::Nfkc => is_nfkc(s),
        Form::Nfkd => is_nfkd(s),
    }
}

// ---- format() ----

fn format_error(sqlstate: &'static str, message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate,
        message: message.into(),
    }
}

/// `format(fmt, ...)` with PostgreSQL's `%s`/`%I`/`%L`/`%%` conversions, the
/// optional `n$` argument selector, and the optional `-`/width field.
fn format_sql(fmt: &str, args: &[Datum], ctx: &EvalCtx) -> Result<String, ExecError> {
    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::with_capacity(fmt.len());
    let mut next_arg = 0usize;
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '%' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        if chars.get(i) == Some(&'%') {
            out.push('%');
            i += 1;
            continue;
        }
        // An `n$` argument selector, if present, precedes the flags.
        let mut explicit: Option<usize> = None;
        let digits_end = scan_digits(&chars, i);
        if digits_end > i && chars.get(digits_end) == Some(&'$') {
            let n: usize = chars[i..digits_end]
                .iter()
                .collect::<String>()
                .parse()
                .map_err(|_| format_error("22023", "number is out of range"))?;
            if n == 0 {
                return Err(format_error(
                    "22023",
                    "format specifies argument 0, but arguments are numbered from 1",
                ));
            }
            explicit = Some(n - 1);
            i = digits_end + 1;
        }
        let left_align = chars.get(i) == Some(&'-');
        if left_align {
            i += 1;
        }
        let width_end = scan_digits(&chars, i);
        let width: usize = if width_end > i {
            chars[i..width_end]
                .iter()
                .collect::<String>()
                .parse()
                .map_err(|_| format_error("22023", "number is out of range"))?
        } else {
            0
        };
        i = width_end;
        let Some(conversion) = chars.get(i).copied() else {
            return Err(format_error(
                "22023",
                "unterminated format() type specifier",
            ));
        };
        i += 1;
        let index = explicit.unwrap_or_else(|| {
            let at = next_arg;
            next_arg += 1;
            at
        });
        let value = args
            .get(index)
            .ok_or_else(|| format_error("22023", "too few arguments for format()"))?;
        let rendered = match conversion {
            's' => {
                if value.is_null() {
                    String::new()
                } else {
                    text_render(value, &ctx.time_zone)
                }
            }
            'I' => {
                if value.is_null() {
                    return Err(format_error(
                        "22004",
                        "null values cannot be formatted as an SQL identifier",
                    ));
                }
                quote_ident(&text_render(value, &ctx.time_zone))
            }
            'L' => {
                if value.is_null() {
                    "NULL".to_string()
                } else {
                    quote_literal(&text_render(value, &ctx.time_zone))
                }
            }
            other => {
                return Err(format_error(
                    "22023",
                    format!("unrecognized format() type specifier \"{other}\""),
                ));
            }
        };
        pad_to(&mut out, &rendered, width, left_align);
    }
    Ok(out)
}

fn scan_digits(chars: &[char], from: usize) -> usize {
    let mut at = from;
    while chars.get(at).is_some_and(char::is_ascii_digit) {
        at += 1;
    }
    at
}

fn pad_to(out: &mut String, value: &str, width: usize, left_align: bool) {
    let len = value.chars().count();
    if len >= width {
        out.push_str(value);
        return;
    }
    let pad = " ".repeat(width - len);
    if left_align {
        out.push_str(value);
        out.push_str(&pad);
    } else {
        out.push_str(&pad);
        out.push_str(value);
    }
}

#[cfg(test)]
mod tests;
