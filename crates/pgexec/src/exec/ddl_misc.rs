use super::*;

/// A `GENERATED … AS IDENTITY` sequence, from the parsed option list.
pub(super) fn sequence_from_options(options: &crabka_pgparser::ast::SequenceOptions) -> Sequence {
    let increment = options.increment.unwrap_or(1);
    Sequence::new(
        options.start.unwrap_or(if increment > 0 { 1 } else { -1 }),
        increment,
        options.min,
        options.max,
        options.cache,
        options.cycle.unwrap_or(false),
    )
}

/// Refuse defaults the catalog serializer cannot preserve.
pub(super) fn ensure_default_can_be_persisted(value: &Datum) -> Result<(), ExecError> {
    if matches!(
        value,
        Datum::Null
            | Datum::Bool(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Text(_)
            | Datum::JsonPath(_)
            | Datum::Float8(_)
            | Datum::Numeric(_)
            | Datum::Json(_)
            | Datum::Xml(_)
            | Datum::Jsonb(_)
            | Datum::TsVector(_)
            | Datum::TsQuery(_)
            | Datum::Regclass(_)
            | Datum::Array(_)
            | Datum::Money(_)
            | Datum::BitString(_)
    ) {
        return Ok(());
    }
    Err(ExecError::Unsupported(
        "defaults for date/time, interval, bytea, composite and enum columns are not persisted yet"
            .into(),
    ))
}

/// Swallow a missing foreign-object error when `IF EXISTS` was given.
pub(super) fn ignore_missing_ops(
    result: Result<Vec<crabka_pgkv::WriteOp>, crabka_pgcatalog::CatalogError>,
    if_exists: bool,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    match result {
        Ok(ops) => Ok(ops),
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if if_exists => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

/// Convert an object-exists error to a no-op when `IF NOT EXISTS` was written.
pub(super) fn ignore_duplicate<T>(
    result: Result<T, crabka_pgcatalog::CatalogError>,
    if_not_exists: bool,
) -> Result<Option<T>, ExecError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(
            crabka_pgcatalog::CatalogError::DuplicateObject(_)
            | crabka_pgcatalog::CatalogError::DuplicateTable(_),
        ) if if_not_exists => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn sequence_from_encoded_options(encoded: &[String]) -> Result<Sequence, ExecError> {
    let mut start = None;
    let mut increment = None;
    let mut min = None;
    let mut max = None;
    let mut cache = None;
    let mut cycle = None;
    for option in encoded {
        let Some((key, value)) = option.split_once('=') else {
            return Err(ExecError::Syntax("invalid encoded sequence option".into()));
        };
        match key {
            "start" => start = Some(parse_sequence_i64(value)?),
            "increment" => increment = Some(parse_sequence_i64(value)?),
            "min" => min = Some(parse_sequence_i64(value)?),
            "max" => max = Some(parse_sequence_i64(value)?),
            "cache" => cache = Some(parse_sequence_i64(value)?),
            "cycle" => cycle = Some(value == "true"),
            _ => return Err(ExecError::Syntax("invalid encoded sequence option".into())),
        }
    }
    let increment = increment.unwrap_or(1);
    let start = start.unwrap_or(if increment > 0 { 1 } else { -1 });
    Ok(Sequence::new(
        start,
        increment,
        min,
        max,
        cache,
        cycle.unwrap_or(false),
    ))
}

fn parse_sequence_i64(value: &str) -> Result<i64, ExecError> {
    value
        .parse::<i64>()
        .map_err(|_| ExecError::Syntax("invalid encoded sequence option".into()))
}
