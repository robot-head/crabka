use super::*;

pub(crate) fn field(name: &str, ty: ColumnType) -> FieldDescription {
    FieldDescription {
        name: name.to_string(),
        table_oid: 0,
        column_id: 0,
        type_oid: ty.oid(),
        type_size: crate::usertype::declared_base_layout(ty)
            .map_or_else(|| ty.type_size(), |layout| layout.length),
        type_modifier: ty.typmod(),
        format: 0,
    }
}

pub(crate) fn column_type_from_oid(oid: u32) -> Result<ColumnType, ExecError> {
    Ok(match oid {
        crabka_pgtypes::oids::BOOL => ColumnType::Bool,
        crabka_pgtypes::oids::BYTEA => ColumnType::Bytea,
        crabka_pgtypes::oids::INT2 => ColumnType::Int2,
        crabka_pgtypes::oids::INT2VECTOR => ColumnType::Int2Vector,
        crabka_pgtypes::oids::INT4 => ColumnType::Int4,
        crabka_pgtypes::oids::OIDVECTOR => ColumnType::OidVector,
        // The whole `reg*` family, including `regclass` itself: a UNION or a
        // CTE over a column of one of them has to name its type, and an oid
        // this table has no entry for is a hard error rather than a fallback.
        crabka_pgtypes::oids::REGCLASS => ColumnType::Regclass,
        crabka_pgtypes::oids::REGTYPE => ColumnType::Regtype,
        crabka_pgtypes::oids::REGPROCEDURE => ColumnType::Regprocedure,
        crabka_pgtypes::oids::REGNAMESPACE => ColumnType::Regnamespace,
        crabka_pgtypes::oids::REGPROC => ColumnType::Regproc,
        crabka_pgtypes::oids::REGOPER => ColumnType::Regoper,
        crabka_pgtypes::oids::REGOPERATOR => ColumnType::Regoperator,
        crabka_pgtypes::oids::REGCONFIG => ColumnType::Regconfig,
        crabka_pgtypes::oids::REGDICTIONARY => ColumnType::Regdictionary,
        crabka_pgtypes::oids::REGROLE => ColumnType::Regrole,
        crabka_pgtypes::oids::REGCOLLATION => ColumnType::Regcollation,
        crabka_pgtypes::oids::INT8 => ColumnType::Int8,
        crabka_pgtypes::oids::TEXT => ColumnType::Text,
        crabka_pgtypes::oids::NAME => ColumnType::Name,
        crabka_pgtypes::oids::ACLITEM => ColumnType::Aclitem,
        crabka_pgtypes::oids::REFCURSOR => ColumnType::Refcursor,
        crabka_pgtypes::oids::VARCHAR => ColumnType::Varchar(None),
        crabka_pgtypes::oids::BPCHAR => ColumnType::Char(None),
        crabka_pgtypes::oids::CHAR => ColumnType::InternalChar,
        crabka_pgtypes::oids::FLOAT4 => ColumnType::Float4,
        crabka_pgtypes::oids::FLOAT8 => ColumnType::Float8,
        // All seven geometric types. Before the geometric operators landed,
        // only `point` and `path` could reach here, because nothing produced a
        // `box`/`lseg`/`line`/`circle`/`polygon` as a query field type except a
        // bare column reference. `b # b`, `@@ c`, `lseg(b)` and friends now do,
        // so a view over any of them needs its oid to round-trip.
        crabka_pgtypes::oids::POINT => ColumnType::Point,
        crabka_pgtypes::oids::PATH => ColumnType::Path,
        crabka_pgtypes::oids::BOX => ColumnType::Box,
        crabka_pgtypes::oids::LSEG => ColumnType::Lseg,
        crabka_pgtypes::oids::LINE => ColumnType::Line,
        crabka_pgtypes::oids::CIRCLE => ColumnType::Circle,
        crabka_pgtypes::oids::POLYGON => ColumnType::Polygon,
        crabka_pgtypes::oids::NUMERIC => ColumnType::Numeric(None),
        crabka_pgtypes::oids::DATE => ColumnType::Date,
        crabka_pgtypes::oids::TIME => ColumnType::Time,
        crabka_pgtypes::oids::TIMETZ => ColumnType::Timetz,
        crabka_pgtypes::oids::TIMESTAMP => ColumnType::Timestamp,
        crabka_pgtypes::oids::TIMESTAMPTZ => ColumnType::Timestamptz,
        crabka_pgtypes::oids::INTERVAL => ColumnType::Interval,
        crabka_pgtypes::oids::MONEY => ColumnType::Money,
        crabka_pgtypes::oids::BIT => ColumnType::Bit(None),
        crabka_pgtypes::oids::VARBIT => ColumnType::VarBit(None),
        crabka_pgtypes::oids::UUID => ColumnType::Uuid,
        crabka_pgtypes::oids::XML => ColumnType::Xml,
        crabka_pgtypes::oids::JSON => ColumnType::Json,
        crabka_pgtypes::oids::JSONB => ColumnType::Jsonb,
        crabka_pgtypes::oids::JSONPATH => ColumnType::JsonPath,
        crabka_pgtypes::oids::OID => ColumnType::Oid,
        crabka_pgtypes::oids::XID => ColumnType::Xid,
        crabka_pgtypes::oids::XID8 => ColumnType::Xid8,
        crabka_pgtypes::oids::CID => ColumnType::Cid,
        crabka_pgtypes::oids::TID => ColumnType::Tid,
        crabka_pgtypes::oids::PG_LSN => ColumnType::PgLsn,
        crabka_pgtypes::oids::PG_SNAPSHOT => ColumnType::PgSnapshot,
        crabka_pgtypes::oids::TXID_SNAPSHOT => ColumnType::TxidSnapshot,
        crabka_pgtypes::oids::TSVECTOR => ColumnType::TsVector,
        crabka_pgtypes::oids::TSQUERY => ColumnType::TsQuery,
        crabka_pgtypes::oids::INET => ColumnType::Inet,
        crabka_pgtypes::oids::CIDR => ColumnType::Cidr,
        crabka_pgtypes::oids::MACADDR => ColumnType::MacAddr,
        crabka_pgtypes::oids::MACADDR8 => ColumnType::MacAddr8,
        crabka_pgtypes::oids::RECORD => ColumnType::Record(None),
        // Every array oid crabka has an element type for, `_json` included.
        _ => crabka_pgtypes::ColumnType::builtin_range(oid)
            .or_else(|| crabka_pgtypes::ColumnType::builtin_multirange(oid))
            .or_else(|| crabka_pgtypes::ColumnType::information_schema_domain_by_oid(oid))
            .or_else(|| crabka_pgtypes::usertype::column_type_for_oid(oid))
            .or_else(|| crabka_pgtypes::ElemType::from_array_oid(oid).map(ColumnType::Array))
            .ok_or_else(|| ExecError::Unsupported(format!("unknown query field type oid {oid}")))?,
    })
}

/// Resolve a query field type with the current catalog available for relation
/// composite types, whose OIDs are assigned from the catalog's relation set.
pub(crate) fn column_type_from_catalog_oid(
    catalog_kv: &dyn Kv,
    oid: u32,
) -> Result<ColumnType, ExecError> {
    match column_type_from_oid(oid) {
        Ok(ty) => Ok(ty),
        Err(error) => crate::catalog_rel::relation_rowtype_by_oid(catalog_kv, oid)?
            .map(|rowtype| ColumnType::Record(Some(rowtype)))
            .ok_or(error),
    }
}

pub(crate) fn datum_to_cell(
    d: &Datum,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    let text = crabka_pgtypes::encoding::encode_text_in(d, style);
    let binary = crabka_pgtypes::encoding::encode_binary(d);
    Some(Cell {
        text: Bytes::from(text),
        binary: Bytes::from(binary),
    })
}

/// Compare two order-key vectors per the SELECT's ASC/DESC flags, with PG's
/// default null placement (NULLS LAST for ASC, NULLS FIRST for DESC).
pub(crate) fn order_cmp(
    a: &[Datum],
    b: &[Datum],
    order_by: &[crabka_pgparser::ast::OrderItem],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in order_by.iter().enumerate() {
        let (x, y) = (&a[i], &b[i]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            // The parser has already resolved PostgreSQL's defaults into
            // `nulls_first` (NULLS LAST for ASC, NULLS FIRST for DESC), and null
            // placement is independent of the ASC/DESC of the non-null values.
            (true, false) => {
                if item.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if item.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                // SLICE INVARIANT: each ORDER BY key position is type-homogeneous
                // (one column = one declared type; one expression = one static
                // type), so ops::compare never errors here. The Equal fallback is
                // defensive — when CAST / heterogeneous keys arrive in a later SP,
                // this must become a real error path or the sort loses total order.
                let base = crabka_pgtypes::ops::compare(x, y)
                    .ok()
                    .flatten()
                    .unwrap_or(Ordering::Equal);
                if item.asc { base } else { base.reverse() }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// `describe` only resolves the SELECT's row description from the catalog (no
// rows are scanned), so the data store `_kv` is unused here. It is kept in the
// signature for uniformity with the other three executor entry points (all take
