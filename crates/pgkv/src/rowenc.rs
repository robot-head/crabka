//! Versioned row value encoding: a leading version byte then one tagged field
//! per column. It is NOT order-preserving. Values are never sorted by raw
//! bytes.

use crabka_pgtypes::Datum;

use crate::KvError;

/// Current row-value format version.
pub const ROW_VERSION: u8 = 1;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    pub const FLOAT8: u8 = 5;
    pub const NUMERIC: u8 = 6;
    pub const DATE: u8 = 7;
    pub const TIME: u8 = 8;
    pub const TIMESTAMP: u8 = 9;
    pub const TIMESTAMPTZ: u8 = 10;
    pub const INTERVAL: u8 = 11;
    pub const BYTEA: u8 = 12;
    /// `jsonb`, stored as its canonical text (`[13][u32 len][text]`). The
    /// decoder re-parses it. Append-only, with no version bump.
    pub const JSONB: u8 = 13;
    /// A one-dimensional array (`[14][elem code][u32 count][elements...]`).
    /// The same tagged-field format encodes each element. Append-only.
    pub const ARRAY: u8 = 14;
    /// `smallint` (`[15][i16 big-endian]`). Append-only.
    pub const INT2: u8 = 15;
    /// `real` (`[16][f32 big-endian]`). Append-only.
    pub const FLOAT4: u8 = 16;
    /// `time with time zone` (`[17][i64 µs of day][i32 seconds west of UTC]`).
    /// Append-only.
    pub const TIMETZ: u8 = 17;
    /// A composite value (`[18][u32 type oid, 0 = anonymous][u32 field count]`
    /// then per field a `[u32 len][name]`, then the field values in the same
    /// tagged-field format). Append-only.
    pub const RECORD: u8 = 18;
    /// An enum value (`[19][u32 type oid][u32 len][label]`). Append-only.
    pub const ENUM: u8 = 19;
    /// Full-text values, stored as their canonical text representations.
    pub const TSVECTOR: u8 = 20;
    pub const TSQUERY: u8 = 21;
    /// Geometric point (`[22][f64 x][f64 y]`). Append-only.
    pub const POINT: u8 = 22;
    /// Geometric path. Append-only.
    pub const PATH: u8 = 23;
    /// A range (`[24][u32 type oid][u8 flags][tagged finite bounds...]`).
    pub const RANGE: u8 = 24;
    /// A multirange (`[25][u32 type oid][u32 count][tagged ranges...]`).
    pub const MULTIRANGE: u8 = 25;
    /// `jsonpath`, stored as canonical UTF-8 text. Append-only.
    pub const JSONPATH: u8 = 26;
    /// `PostgreSQL` `lseg`. Append-only — no version bump.
    pub const LSEG: u8 = 27;
}

/// Encodes one row in the current storage format.
///
/// # Panics
///
/// Panics when a variable-width datum exceeds the format's 4 GiB field limit.
#[must_use]
pub fn encode_row(cols: &[Datum]) -> Vec<u8> {
    let mut out = vec![ROW_VERSION];
    encode_fields(cols, &mut out);
    out
}

/// Appends `cols` as tagged fields. This is the row body without the version
/// byte, and it is also the payload format for array elements.
fn encode_fields(cols: &[Datum], out: &mut Vec<u8>) {
    for d in cols {
        match d {
            Datum::Null => out.push(tag::NULL),
            Datum::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Datum::Int2(n) => {
                out.push(tag::INT2);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Int4(n) => {
                out.push(tag::INT4);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Int8(n) => {
                out.push(tag::INT8);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Text(s) => {
                out.push(tag::TEXT);
                let len = u32::try_from(s.len()).expect("text column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::JsonPath(s) => {
                out.push(tag::JSONPATH);
                let len = u32::try_from(s.len()).expect("jsonpath column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Float4(f) => {
                out.push(tag::FLOAT4);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Datum::Float8(f) => {
                out.push(tag::FLOAT8);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Datum::Point(point) => {
                out.push(tag::POINT);
                out.extend_from_slice(&point.x.to_be_bytes());
                out.extend_from_slice(&point.y.to_be_bytes());
            }
            Datum::Lseg(lseg) => {
                out.push(tag::LSEG);
                for coordinate in [lseg.start.x, lseg.start.y, lseg.end.x, lseg.end.y] {
                    out.extend_from_slice(&coordinate.to_be_bytes());
                }
            }
            Datum::Path(path) => {
                out.push(tag::PATH);
                out.push(u8::from(path.closed));
                let count = u32::try_from(path.points.len()).expect("path exceeds 2^32 points");
                out.extend_from_slice(&count.to_be_bytes());
                for point in &path.points {
                    out.extend_from_slice(&point.x.to_be_bytes());
                    out.extend_from_slice(&point.y.to_be_bytes());
                }
            }
            Datum::Numeric(d) => {
                out.push(tag::NUMERIC);
                let s = crabka_pgtypes::numeric::to_text(d);
                let len = u32::try_from(s.len()).expect("numeric text exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Date(d) => {
                out.push(tag::DATE);
                out.extend_from_slice(&crabka_pgtypes::datetime::date_to_binary(*d));
            }
            Datum::Time(t) => {
                out.push(tag::TIME);
                out.extend_from_slice(&crabka_pgtypes::datetime::time_to_binary(*t));
            }
            Datum::Timetz(t) => {
                out.push(tag::TIMETZ);
                out.extend_from_slice(&crabka_pgtypes::datetime::timetz_to_binary(*t));
            }
            Datum::Timestamp(ts) => {
                out.push(tag::TIMESTAMP);
                out.extend_from_slice(&crabka_pgtypes::datetime::timestamp_to_binary(*ts));
            }
            Datum::Timestamptz(ts) => {
                out.push(tag::TIMESTAMPTZ);
                out.extend_from_slice(&crabka_pgtypes::datetime::timestamptz_to_binary(*ts));
            }
            Datum::Interval(iv) => {
                out.push(tag::INTERVAL);
                out.extend_from_slice(&crabka_pgtypes::datetime::interval_to_binary(*iv));
            }
            Datum::Bytea(b) => {
                out.push(tag::BYTEA);
                let len = u32::try_from(b.len()).expect("bytea column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(b);
            }
            Datum::Jsonb(j) => {
                out.push(tag::JSONB);
                let text = j.to_text();
                let len = u32::try_from(text.len()).expect("jsonb column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            Datum::Array(a) | Datum::OidVector(a) => {
                out.push(tag::ARRAY);
                a.elem.write_code(out);
                let ndim = u8::try_from(a.dims.len()).expect("array dimensions exceed a byte");
                out.push(ndim);
                for dim in &a.dims {
                    out.extend_from_slice(&dim.lower.to_be_bytes());
                    out.extend_from_slice(&dim.len.to_be_bytes());
                }
                let count = u32::try_from(a.elems.len()).expect("array exceeds 4G elements");
                out.extend_from_slice(&count.to_be_bytes());
                // Elements reuse the same tagged-field encoding, so a `jsonb`
                // inside an array (or a NULL element) needs no special case.
                encode_fields(&a.elems, out);
            }
            Datum::Record(r) => {
                out.push(tag::RECORD);
                out.extend_from_slice(&r.ty.map_or(0, |ty| ty.oid).to_be_bytes());
                let count = u32::try_from(r.values.len()).expect("record exceeds 4G fields");
                out.extend_from_slice(&count.to_be_bytes());
                for name in r.names.iter().take(r.values.len()) {
                    let len = u32::try_from(name.len()).expect("field name exceeds 4 GiB");
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(name.as_bytes());
                }
                encode_fields(&r.values, out);
            }
            // A `regclass` stores as its oid, which is all PostgreSQL keeps on
            // disk too — the name it renders is derived from the catalog at
            // output time, never stored.
            Datum::Regclass(r) => {
                out.push(tag::INT4);
                out.extend_from_slice(&r.oid.to_be_bytes());
            }
            Datum::Enum(e) => {
                out.push(tag::ENUM);
                out.extend_from_slice(&e.ty.oid.to_be_bytes());
                let len = u32::try_from(e.label.len()).expect("enum label exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(e.label.as_bytes());
            }
            Datum::TsVector(vector) => encode_search(tag::TSVECTOR, &vector.to_string(), out),
            Datum::TsQuery(query) => encode_search(tag::TSQUERY, &query.to_string(), out),
            Datum::Range(range) => {
                out.push(tag::RANGE);
                out.extend_from_slice(&range.ty.oid.to_be_bytes());
                let mut flags = u8::from(range.empty);
                flags |= u8::from(range.lower_inclusive) << 1;
                flags |= u8::from(range.upper_inclusive) << 2;
                flags |= u8::from(range.lower.is_none()) << 3;
                flags |= u8::from(range.upper.is_none()) << 4;
                out.push(flags);
                if let Some(lower) = &range.lower {
                    encode_fields(std::slice::from_ref(lower.as_ref()), out);
                }
                if let Some(upper) = &range.upper {
                    encode_fields(std::slice::from_ref(upper.as_ref()), out);
                }
            }
            Datum::Multirange(multirange) => {
                out.push(tag::MULTIRANGE);
                out.extend_from_slice(&multirange.ty.oid.to_be_bytes());
                out.extend_from_slice(
                    &u32::try_from(multirange.ranges.len())
                        .expect("multirange exceeds 4G components")
                        .to_be_bytes(),
                );
                for range in &multirange.ranges {
                    encode_fields(std::slice::from_ref(&Datum::Range(range.clone())), out);
                }
            }
        }
    }
}

fn encode_search(tag: u8, text: &str, out: &mut Vec<u8>) {
    out.push(tag);
    let len = u32::try_from(text.len()).expect("text-search value exceeds 4 GiB");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(text.as_bytes());
}

/// Decodes row bytes into datum values.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when the version, field tag, field length, or
/// encoded value is invalid.
///
/// # Panics
///
/// Panics only if a fixed-width slice that the decoder validated cannot
/// convert to its matching fixed-size array.
pub fn decode_row(bytes: &[u8]) -> Result<Vec<Datum>, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != ROW_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown row version {version}"
        )));
    }
    let mut cols = Vec::new();
    while !cur.is_empty() {
        cols.push(decode_field(&mut cur)?);
    }
    Ok(cols)
}

/// Decode one tagged field, advancing `cur` past it.
#[allow(
    clippy::too_many_lines,
    reason = "one match keeps every row-wire tag and decoder in one auditable map"
)]
fn decode_field(cur: &mut &[u8]) -> Result<Datum, KvError> {
    let t = take_u8(cur)?;
    Ok(match t {
        tag::NULL => Datum::Null,
        tag::BOOL => Datum::Bool(take_bool(cur)?),
        tag::INT2 => {
            let raw = take_n(cur, 2)?;
            Datum::Int2(i16::from_be_bytes(raw.try_into().expect("2 bytes fit i16")))
        }
        tag::INT4 => {
            let raw = take_n(cur, 4)?;
            Datum::Int4(i32::from_be_bytes(raw.try_into().expect("4 bytes fit i32")))
        }
        tag::INT8 => {
            let raw = take_n(cur, 8)?;
            Datum::Int8(i64::from_be_bytes(raw.try_into().expect("8 bytes fit i64")))
        }
        tag::TEXT => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            Datum::Text(
                String::from_utf8(raw.to_vec())
                    .map_err(|_| KvError::CorruptRow("text is not valid UTF-8".into()))?,
            )
        }
        tag::JSONPATH => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            Datum::JsonPath(
                String::from_utf8(raw.to_vec())
                    .map_err(|_| KvError::CorruptRow("jsonpath is not valid UTF-8".into()))?,
            )
        }
        tag::FLOAT4 => {
            let raw = take_n(cur, 4)?;
            Datum::Float4(f32::from_be_bytes(raw.try_into().expect("4 bytes fit f32")))
        }
        tag::FLOAT8 => {
            let raw = take_n(cur, 8)?;
            Datum::Float8(f64::from_be_bytes(raw.try_into().expect("8 bytes fit f64")))
        }
        tag::NUMERIC => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            let s = std::str::from_utf8(raw)
                .map_err(|_| KvError::CorruptRow("numeric text is not valid UTF-8".into()))?;
            Datum::Numeric(
                crabka_pgtypes::numeric::parse(s)
                    .ok_or_else(|| KvError::CorruptRow(format!("invalid numeric {s:?}")))?,
            )
        }
        tag::DATE => {
            let raw = take_n(cur, 4)?;
            Datum::Date(
                crabka_pgtypes::datetime::date_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt date: {e}")))?,
            )
        }
        tag::TIME => {
            let raw = take_n(cur, 8)?;
            Datum::Time(
                crabka_pgtypes::datetime::time_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt time: {e}")))?,
            )
        }
        tag::TIMETZ => {
            let raw = take_n(cur, 12)?;
            Datum::Timetz(
                crabka_pgtypes::datetime::timetz_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt timetz: {e}")))?,
            )
        }
        tag::TIMESTAMP => {
            let raw = take_n(cur, 8)?;
            Datum::Timestamp(
                crabka_pgtypes::datetime::timestamp_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt timestamp: {e}")))?,
            )
        }
        tag::TIMESTAMPTZ => {
            let raw = take_n(cur, 8)?;
            Datum::Timestamptz(
                crabka_pgtypes::datetime::timestamptz_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt timestamptz: {e}")))?,
            )
        }
        tag::INTERVAL => {
            let raw = take_n(cur, 16)?;
            Datum::Interval(
                crabka_pgtypes::datetime::interval_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt interval: {e}")))?,
            )
        }
        tag::POINT => {
            let x = f64::from_be_bytes(
                take_n(cur, 8)?
                    .try_into()
                    .expect("8 bytes fit a point coordinate"),
            );
            let y = f64::from_be_bytes(
                take_n(cur, 8)?
                    .try_into()
                    .expect("8 bytes fit a point coordinate"),
            );
            Datum::Point(crabka_pgtypes::Point { x, y })
        }
        tag::PATH => {
            let closed = match take_u8(cur)? {
                0 => false,
                1 => true,
                flag => {
                    return Err(KvError::CorruptRow(format!(
                        "invalid path closed flag {flag}"
                    )));
                }
            };
            let count = usize::try_from(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")))
                .expect("u32 fits usize");
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                let x = f64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"));
                let y = f64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"));
                points.push(crabka_pgtypes::Point { x, y });
            }
            Datum::Path(crabka_pgtypes::Path { closed, points })
        }
        tag::LSEG => {
            let mut coordinates = [0.0_f64; 4];
            for coordinate in &mut coordinates {
                *coordinate = f64::from_be_bytes(take_n(cur, 8)?.try_into().expect("8"));
            }
            Datum::Lseg(crabka_pgtypes::geometry::Lseg {
                start: crabka_pgtypes::Point {
                    x: coordinates[0],
                    y: coordinates[1],
                },
                end: crabka_pgtypes::Point {
                    x: coordinates[2],
                    y: coordinates[3],
                },
            })
        }
        tag::BYTEA => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            Datum::Bytea(raw.to_vec())
        }
        tag::JSONB => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            let text = std::str::from_utf8(raw)
                .map_err(|_| KvError::CorruptRow("jsonb text is not valid UTF-8".into()))?;
            Datum::Jsonb(
                crabka_pgtypes::jsonb::parse(text)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt jsonb: {e}")))?,
            )
        }
        tag::ARRAY => {
            let elem = crabka_pgtypes::ElemType::read_code(cur)
                .ok_or_else(|| KvError::CorruptRow("unknown array element code".to_string()))?;
            let ndim = take_u8(cur)?;
            let mut dims = Vec::new();
            for _ in 0..ndim {
                let lower = take_i32(cur)?;
                let len = take_i32(cur)?;
                dims.push(crabka_pgtypes::ArrayDim::new(lower, len));
            }
            let count = take_u32_len(cur)?;
            let mut elems = Vec::new();
            for _ in 0..count {
                elems.push(decode_field(cur)?);
            }
            Datum::Array(crabka_pgtypes::ArrayValue::with_dims(elem, elems, dims))
        }
        tag::RECORD => {
            let type_oid = take_u32_len(cur)?;
            let count = take_u32_len(cur)?;
            let mut names = Vec::with_capacity(count);
            for _ in 0..count {
                names.push(take_text(cur, "record field name")?);
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_field(cur)?);
            }
            let ty = u32::try_from(type_oid)
                .ok()
                .filter(|oid| *oid != 0)
                .and_then(|oid| crabka_pgtypes::usertype::lookup_oid(oid).map(|ty| ty.type_ref()));
            Datum::Record(crabka_pgtypes::RecordValue::named(ty, names.into(), values))
        }
        tag::ENUM => {
            let type_oid = u32::try_from(take_u32_len(cur)?)
                .map_err(|_| KvError::CorruptRow("enum type oid out of range".into()))?;
            let label = take_text(cur, "enum label")?;
            let ty = crabka_pgtypes::usertype::lookup_oid(type_oid)
                .ok_or_else(|| {
                    KvError::CorruptRow(format!("enum type {type_oid} is no longer registered"))
                })?
                .type_ref();
            Datum::Enum(crabka_pgtypes::EnumValue { ty, label })
        }
        tag::TSVECTOR => take_text(cur, "tsvector")?
            .parse()
            .map(Datum::TsVector)
            .map_err(|error| KvError::CorruptRow(format!("corrupt tsvector: {error}")))?,
        tag::TSQUERY => take_text(cur, "tsquery")?
            .parse()
            .map(Datum::TsQuery)
            .map_err(|error| KvError::CorruptRow(format!("corrupt tsquery: {error}")))?,
        tag::RANGE => {
            let oid = u32::try_from(take_u32_len(cur)?)
                .map_err(|_| KvError::CorruptRow("range type oid out of range".into()))?;
            let Some(crabka_pgtypes::ColumnType::Range(ty)) =
                crabka_pgtypes::ColumnType::builtin_range(oid)
                    .or_else(|| crabka_pgtypes::usertype::column_type_for_oid(oid))
            else {
                return Err(KvError::CorruptRow(format!(
                    "range type {oid} is not registered"
                )));
            };
            let flags = take_u8(cur)?;
            if flags & !0x1f != 0 {
                return Err(KvError::CorruptRow("invalid range flags".into()));
            }
            let empty = flags & 0x01 != 0;
            let lower = (!empty && flags & 0x08 == 0)
                .then(|| decode_field(cur).map(Box::new))
                .transpose()?;
            let upper = (!empty && flags & 0x10 == 0)
                .then(|| decode_field(cur).map(Box::new))
                .transpose()?;
            Datum::Range(crabka_pgtypes::RangeValue {
                ty,
                lower,
                upper,
                lower_inclusive: !empty && flags & 0x02 != 0,
                upper_inclusive: !empty && flags & 0x04 != 0,
                empty,
            })
        }
        tag::MULTIRANGE => {
            let oid = u32::try_from(take_u32_len(cur)?)
                .map_err(|_| KvError::CorruptRow("multirange type oid out of range".into()))?;
            let Some(crabka_pgtypes::ColumnType::Multirange(ty)) =
                crabka_pgtypes::ColumnType::builtin_multirange(oid)
                    .or_else(|| crabka_pgtypes::usertype::column_type_for_oid(oid))
            else {
                return Err(KvError::CorruptRow(format!(
                    "multirange type {oid} is not registered"
                )));
            };
            let count = take_u32_len(cur)?;
            let mut ranges = Vec::with_capacity(count);
            for _ in 0..count {
                let Datum::Range(range) = decode_field(cur)? else {
                    return Err(KvError::CorruptRow(
                        "multirange component is not a range".into(),
                    ));
                };
                if range.ty != ty.range {
                    return Err(KvError::CorruptRow(
                        "multirange component type does not match".into(),
                    ));
                }
                ranges.push(range);
            }
            Datum::Multirange(crabka_pgtypes::MultirangeValue { ty, ranges })
        }
        other => return Err(KvError::CorruptRow(format!("unknown field tag {other}"))),
    })
}

/// A length-prefixed UTF-8 string, the shape every name and label field uses.
fn take_text(cur: &mut &[u8], what: &str) -> Result<String, KvError> {
    let len = take_u32_len(cur)?;
    let raw = take_n(cur, len)?;
    String::from_utf8(raw.to_vec())
        .map_err(|_| KvError::CorruptRow(format!("{what} is not valid UTF-8")))
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (head, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated".into()))?;
    *cur = rest;
    Ok(*head)
}

fn take_bool(cur: &mut &[u8]) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(KvError::CorruptRow(format!("invalid bool payload {other}"))),
    }
}

fn take_i32(cur: &mut &[u8]) -> Result<i32, KvError> {
    let raw = take_n(cur, 4)?;
    Ok(i32::from_be_bytes(raw.try_into().expect("4 bytes fit i32")))
}

fn take_u32_len(cur: &mut &[u8]) -> Result<usize, KvError> {
    let len_raw = take_n(cur, 4)?;
    let len = u32::from_be_bytes(len_raw.try_into().expect("4 bytes fit u32"));
    usize::try_from(len).map_err(|_| KvError::CorruptRow("length does not fit usize".into()))
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated field".into()));
    }
    let (head, rest) = cur.split_at(n);
    *cur = rest;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::Datum;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn roundtrip_all_datum_kinds_including_null() {
        let crabka_pgtypes::ColumnType::Range(int4range) =
            crabka_pgtypes::ColumnType::builtin_range(crabka_pgtypes::oids::INT4RANGE)
                .expect("built-in range")
        else {
            unreachable!()
        };
        let row = vec![
            Datum::Null,
            Datum::Bool(true),
            Datum::Int2(i16::MIN),
            Datum::Int2(i16::MAX),
            Datum::Int4(i32::MIN),
            Datum::Int8(i64::MIN),
            Datum::Text("héllo".into()),
            Datum::JsonPath("$.\"héllo\"".into()),
            Datum::Float4(-1.5),
            Datum::Float4(f32::NAN),
            Datum::Float4(-0.0),
            Datum::Float8(-1.5),
            Datum::Float8(f64::NAN),
            Datum::Float8(-0.0),
            Datum::Numeric(crabka_pgtypes::numeric::parse("1.50").expect("n")),
            Datum::Numeric(crabka_pgtypes::numeric::parse("-9999999999999999999.0001").expect("n")),
            Datum::Range(
                crabka_pgtypes::range::parse("[1,4)", int4range, &jiff::tz::TimeZone::UTC)
                    .expect("range"),
            ),
        ];
        let bytes = encode_row(&row);
        assert_eq!(decode_row(&bytes).expect("decode"), row);
    }

    #[test]
    fn jsonb_and_array_tags_round_trip() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, ElemType};

        let json = |s: &str| Datum::Jsonb(crabka_pgtypes::jsonb::parse(s).expect("jsonb"));
        let row = vec![
            json(r#"{"b":1,"a":[1,2],"c":"x"}"#),
            json("null"),
            // Empty, NULL elements, and a jsonb inside an array.
            Datum::Array(ArrayValue::new(ElemType::Int4, vec![])),
            Datum::Array(ArrayValue::new(
                ElemType::Int4,
                vec![Datum::Int4(1), Datum::Null, Datum::Int4(3)],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Text,
                vec![Datum::Text("a,b".into()), Datum::Text(String::new())],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Jsonb,
                vec![json(r#"{"z":1}"#), Datum::Null],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::JsonPath,
                vec![Datum::JsonPath("$.\"a\"".into()), Datum::Null],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Numeric,
                vec![Datum::Numeric(
                    crabka_pgtypes::numeric::parse("1.50").expect("n"),
                )],
            )),
            // A scalar after the arrays pins that element counts are honoured.
            Datum::Int4(9),
        ];
        assert!(decode_row(&encode_row(&row)).expect("decode") == row);
    }

    #[test]
    fn jsonb_tag_layout_is_a_length_prefixed_canonical_text() {
        use assert2::assert;

        let value = Datum::Jsonb(crabka_pgtypes::jsonb::parse(r#"{"b":1,"a":2}"#).expect("jsonb"));
        let text = br#"{"a": 2, "b": 1}"#;
        let mut expected = vec![ROW_VERSION, tag::JSONB];
        expected.extend_from_slice(&u32::try_from(text.len()).expect("small").to_be_bytes());
        expected.extend_from_slice(text);
        assert!(encode_row(&[value]) == expected);
    }

    #[test]
    fn corrupt_jsonb_and_array_payloads_error_not_panic() {
        use assert2::assert;

        // An unknown element type code.
        let mut bytes = vec![ROW_VERSION, tag::ARRAY, 200];
        bytes.extend_from_slice(&0u32.to_be_bytes());
        assert!(decode_row(&bytes).is_err());
        // Fewer elements than the declared count.
        let mut bytes = vec![ROW_VERSION, tag::ARRAY, 1];
        bytes.extend_from_slice(&3u32.to_be_bytes());
        assert!(decode_row(&bytes).is_err());
        // Text that is not valid JSON.
        let mut bytes = vec![ROW_VERSION, tag::JSONB];
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(b"{{");
        assert!(decode_row(&bytes).is_err());
    }

    #[test]
    fn version_byte_is_present() {
        assert_eq!(encode_row(&[Datum::Int4(1)])[0], ROW_VERSION);
    }

    #[test]
    fn truncated_value_errors_not_panics() {
        assert!(decode_row(&[ROW_VERSION, 2, 0, 0]).is_err());
    }

    #[test]
    fn invalid_bool_payload_errors() {
        let err = decode_row(&[ROW_VERSION, tag::BOOL, 2]).expect_err("invalid bool payload");
        assert_eq!(err, KvError::CorruptRow("invalid bool payload 2".into()));
    }

    #[test]
    fn numeric_with_an_overflowing_exponent_errors_not_ooms() {
        let text = b"8e88888888";
        let mut bytes = vec![ROW_VERSION, 6];
        bytes.extend_from_slice(&u32::try_from(text.len()).expect("small text").to_be_bytes());
        bytes.extend_from_slice(text);
        assert!(decode_row(&bytes).is_err());
    }

    #[test]
    fn adversarial_datetime_values_error_not_panic() {
        for (tag, width) in [(7u8, 4usize), (8, 8), (9, 8), (10, 8), (11, 16)] {
            for fill in [0xFFu8, 0x7F, 0x80, 0x00] {
                let mut bytes = vec![ROW_VERSION, tag];
                bytes.extend(vec![fill; width]);
                let _ = decode_row(&bytes);
            }
        }
        // `i64::MAX` microseconds is the reserved `timestamptz 'infinity'`
        // sentinel, so it decodes to that value rather than erroring; the
        // largest *finite* count is one below it.
        let mut tstz_max = vec![ROW_VERSION, 10];
        tstz_max.extend_from_slice(&i64::MAX.to_be_bytes());
        assert!(decode_row(&tstz_max).is_ok());
        let mut tstz_over = vec![ROW_VERSION, 10];
        tstz_over.extend_from_slice(&(i64::MAX - 1).to_be_bytes());
        assert!(decode_row(&tstz_over).is_err());
    }

    #[test]
    fn unknown_version_errors() {
        assert!(decode_row(&[99, 1, 1]).is_err());
    }

    fn arb_datum() -> impl Strategy<Value = Datum> {
        prop_oneof![
            Just(Datum::Null),
            any::<bool>().prop_map(Datum::Bool),
            any::<i16>().prop_map(Datum::Int2),
            any::<i32>().prop_map(Datum::Int4),
            any::<i64>().prop_map(Datum::Int8),
            ".*".prop_map(Datum::Text),
            any::<f32>().prop_map(Datum::Float4),
            any::<f64>().prop_map(Datum::Float8),
            (any::<i64>(), 0u32..6).prop_map(|(m, s)| {
                Datum::Numeric(
                    crabka_pgtypes::numeric::parse(&format!("{m}e-{s}")).expect("numeric"),
                )
            }),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_rows(row in prop::collection::vec(arb_datum(), 0..8)) {
            let bytes = encode_row(&row);
            prop_assert_eq!(decode_row(&bytes).expect("decode"), row);
        }
    }

    #[test]
    fn datetime_row_round_trip() {
        use crabka_pgtypes::datetime::Interval;

        let row = vec![
            Datum::Date(crabka_pgtypes::datetime::parse_date("2024-01-15").expect("d")),
            Datum::Time(crabka_pgtypes::datetime::parse_time("13:45:06.5").expect("t")),
            Datum::Timestamp(
                crabka_pgtypes::datetime::parse_timestamp("2024-01-15 13:45:06").expect("ts"),
            ),
            Datum::Timestamptz(
                crabka_pgtypes::datetime::parse_timestamptz(
                    "2024-01-15 13:45:06+00",
                    &jiff::tz::TimeZone::UTC,
                )
                .expect("tstz"),
            ),
            Datum::Interval(Interval {
                months: 14,
                days: -3,
                micros: 4_500_000,
            }),
        ];
        assert_eq!(decode_row(&encode_row(&row)).expect("decode"), row);
    }
}
