//! PostgreSQL's statistics import functions.
//!
//! These calls run from scalar expression evaluation, but their catalog writes
//! must be committed by the owning SQL session.  This module therefore parses
//! the variadic name/value input into ordinary catalog operations; `session`
//! supplies the transaction boundary and warning sink.

use crabka_pgcatalog::{CatalogError, RelationName};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgtypes::{ColumnType, Datum, ElemType, encoding::OutputStyle};

use crate::{attrstats, error::ExecError, relstats};

#[derive(Debug, Clone)]
pub(crate) enum StatisticsRequest {
    RestoreRelation(Vec<Datum>),
    ClearRelation(Vec<Datum>),
    RestoreAttribute(Vec<Datum>),
    ClearAttribute(Vec<Datum>),
}

pub(crate) struct StatisticsOutcome {
    pub(crate) value: Datum,
    pub(crate) ops: Vec<WriteOp>,
    pub(crate) warnings: Vec<String>,
    pub(crate) locks: Vec<(RelationName, crate::lockmgr::RelationLockTarget)>,
    pub(crate) error: Option<ExecError>,
}

pub(crate) fn execute(
    kv: &dyn Kv,
    request: StatisticsRequest,
) -> Result<StatisticsOutcome, ExecError> {
    execute_in_scope(
        kv,
        crate::relname::ResolutionScope::default_scope(),
        request,
    )
}

pub(crate) fn execute_in_scope(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    request: StatisticsRequest,
) -> Result<StatisticsOutcome, ExecError> {
    match request {
        StatisticsRequest::RestoreRelation(values) => restore_relation(kv, resolution, &values),
        StatisticsRequest::ClearRelation(values) => clear_relation(kv, resolution, &values),
        StatisticsRequest::RestoreAttribute(values) => restore_attribute(kv, resolution, &values),
        StatisticsRequest::ClearAttribute(values) => clear_attribute(kv, resolution, &values),
    }
}

fn restore_relation(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    values: &[Datum],
) -> Result<StatisticsOutcome, ExecError> {
    if values.len() % 2 != 0 {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "variadic arguments must be name/value pairs".into(),
        });
    }

    let mut schema = None;
    let mut relation = None;
    let mut reltuples = None;
    let mut relpages = None;
    let mut relallvisible = None;
    let mut relallfrozen = None;
    let mut warnings = Vec::new();

    for (index, pair) in values.chunks_exact(2).enumerate() {
        let name = string_argument(&pair[0], "name", index * 2 + 1)?;
        match name.as_str() {
            "schemaname" => schema = text_argument(&pair[1], "schemaname", &mut warnings),
            "relname" => relation = text_argument(&pair[1], "relname", &mut warnings),
            "relpages" => {
                relpages = int4_argument(&pair[1], "relpages", &mut warnings);
            }
            "reltuples" => {
                reltuples = float4_argument(&pair[1], "reltuples", &mut warnings);
            }
            "relallvisible" => {
                relallvisible = int4_argument(&pair[1], "relallvisible", &mut warnings);
            }
            "relallfrozen" => {
                relallfrozen = int4_argument(&pair[1], "relallfrozen", &mut warnings);
            }
            "version" => {
                let _ = int4_argument(&pair[1], "version", &mut warnings);
            }
            _ => warnings.push(format!("unrecognized argument name: \"{name}\"")),
        }
    }

    let relation = match relation_name(kv, resolution, schema, relation) {
        Ok(relation) => relation,
        Err(error) => return return_with_warnings(warnings, error),
    };
    ensure_statistics_relation(kv, &relation)?;
    let locks = statistics_locks(kv, &relation)?;
    let mut ops = Vec::new();
    if let Some(value) = reltuples {
        ops.push(relstats::set_reltuples_op(&relation, value));
    }
    if let Some(value) = relpages {
        ops.push(relstats::set_relpages_op(&relation, value));
    }
    if let Some(value) = relallvisible {
        ops.push(relstats::set_relallvisible_op(&relation, value));
    }
    if let Some(value) = relallfrozen {
        ops.push(relstats::set_relallfrozen_op(&relation, value));
    }
    Ok(StatisticsOutcome {
        value: Datum::Bool(warnings.is_empty()),
        ops,
        warnings,
        locks,
        error: None,
    })
}

fn clear_relation(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    values: &[Datum],
) -> Result<StatisticsOutcome, ExecError> {
    let [schema, relation] = values else {
        return Err(ExecError::UndefinedFunction(
            "function pg_clear_relation_stats(...) does not exist".into(),
        ));
    };
    let relation = relation_name(
        kv,
        resolution,
        required_text(schema, "schemaname")?,
        required_text(relation, "relname")?,
    )?;
    ensure_statistics_relation(kv, &relation)?;
    let locks = statistics_locks(kv, &relation)?;
    Ok(StatisticsOutcome {
        value: Datum::Text(String::new()),
        ops: relstats::clear_ops(&relation),
        warnings: Vec::new(),
        locks,
        error: None,
    })
}

fn restore_attribute(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    values: &[Datum],
) -> Result<StatisticsOutcome, ExecError> {
    if values.len() % 2 != 0 {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "variadic arguments must be name/value pairs".into(),
        });
    }
    let mut schema = None;
    let mut relation = None;
    let mut attname = None;
    let mut attnum = None;
    let mut inherited = None;
    let mut supplied = attrstats::AttributeStats::default();
    let mut warnings = Vec::new();
    for (index, pair) in values.chunks_exact(2).enumerate() {
        let name = string_argument(&pair[0], "name", index * 2 + 1)?;
        match name.as_str() {
            "schemaname" => schema = text_argument(&pair[1], "schemaname", &mut warnings),
            "relname" => relation = text_argument(&pair[1], "relname", &mut warnings),
            "attname" => attname = text_argument(&pair[1], "attname", &mut warnings),
            "attnum" => attnum = int2_argument(&pair[1], "attnum", &mut warnings),
            "inherited" => inherited = bool_argument(&pair[1], "inherited", &mut warnings),
            "null_frac" => {
                supplied.null_frac = float4_argument(&pair[1], "null_frac", &mut warnings)
            }
            "avg_width" => supplied.avg_width = int4_argument(&pair[1], "avg_width", &mut warnings),
            "n_distinct" => {
                supplied.n_distinct = float4_argument(&pair[1], "n_distinct", &mut warnings)
            }
            "most_common_vals" => {
                supplied.most_common_vals =
                    text_argument(&pair[1], "most_common_vals", &mut warnings)
            }
            "most_common_freqs" => {
                supplied.most_common_freqs =
                    real_array_argument(&pair[1], "most_common_freqs", &mut warnings)
            }
            "histogram_bounds" => {
                supplied.histogram_bounds =
                    text_argument(&pair[1], "histogram_bounds", &mut warnings)
            }
            "correlation" => {
                supplied.correlation = float4_argument(&pair[1], "correlation", &mut warnings)
            }
            "most_common_elems" => {
                supplied.most_common_elems =
                    text_argument(&pair[1], "most_common_elems", &mut warnings)
            }
            "most_common_elem_freqs" => {
                supplied.most_common_elem_freqs =
                    real_array_argument(&pair[1], "most_common_elem_freqs", &mut warnings)
            }
            "elem_count_histogram" => {
                supplied.elem_count_histogram =
                    real_array_argument(&pair[1], "elem_count_histogram", &mut warnings)
            }
            "range_length_histogram" => {
                supplied.range_length_histogram =
                    text_argument(&pair[1], "range_length_histogram", &mut warnings)
            }
            "range_empty_frac" => {
                supplied.range_empty_frac =
                    float4_argument(&pair[1], "range_empty_frac", &mut warnings)
            }
            "range_bounds_histogram" => {
                supplied.range_bounds_histogram =
                    text_argument(&pair[1], "range_bounds_histogram", &mut warnings)
            }
            "version" => {
                let _ = int4_argument(&pair[1], "version", &mut warnings);
            }
            _ => warnings.push(format!("unrecognized argument name: \"{name}\"")),
        }
    }
    let relation = match relation_name(kv, resolution, schema, relation) {
        Ok(relation) => relation,
        Err(error) => return return_with_warnings(warnings, error),
    };
    let (table, _) = attribute_statistics_table(kv, &relation)?;
    let locks = statistics_locks(kv, &relation)?;
    let attribute = match (attname, attnum) {
        (Some(_), Some(_)) => {
            return return_with_warnings(
                warnings,
                ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "cannot specify both \"attname\" and \"attnum\"".into(),
                },
            );
        }
        (None, None) => {
            return return_with_warnings(
                warnings,
                ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "must specify either \"attname\" or \"attnum\"".into(),
                },
            );
        }
        (Some(name), None) if crate::scope::SYSTEM_COLUMNS.contains(&name.as_str()) => {
            return return_with_warnings(
                warnings,
                ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("cannot modify statistics on system column \"{name}\""),
                },
            );
        }
        (Some(name), None) => table
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: name,
                table: relation.name.clone(),
            })?,
        (None, Some(number)) => usize::try_from(number.checked_sub(1).unwrap_or(-1))
            .ok()
            .filter(|index| *index < table.columns.len())
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: number.to_string(),
                table: relation.name.clone(),
            })?,
    };
    let column = &table.columns[attribute];
    let inherited = inherited.ok_or_else(|| null_argument("inherited"))?;
    let key = attrstats::AttributeStatsKey {
        relation: relation.clone(),
        attnum: i16::try_from(attribute + 1)
            .map_err(|_| ExecError::Unsupported("attribute number exceeds int2".into()))?,
        inherited,
    };
    let mut merged = attrstats::get(kv, &key)?.unwrap_or_default();
    if supplied.null_frac.is_some() {
        merged.null_frac = supplied.null_frac;
    }
    if supplied.avg_width.is_some() {
        merged.avg_width = supplied.avg_width;
    }
    if supplied.n_distinct.is_some() {
        merged.n_distinct = supplied.n_distinct;
    }
    if let (Some(values), Some(freqs)) = (
        supplied.most_common_vals.as_deref(),
        supplied.most_common_freqs.as_deref(),
    ) {
        match canonical_array(values, column.ty, "most_common_vals") {
            Ok((values, values_len)) => match real_array_len(freqs) {
                Ok(freqs_len) if values_len == freqs_len => {
                    merged.most_common_vals = Some(values);
                    merged.most_common_freqs = Some(freqs.to_owned());
                }
                Ok(_) => warnings.push(
                    "could not parse \"most_common_vals\": incorrect number of elements (same as \"most_common_freqs\" required)".into(),
                ),
                Err(message) => warnings.push(message),
            },
            Err(message) => warnings.push(message),
        }
    } else if supplied.most_common_vals.is_some() {
        warnings.push(
            "argument \"most_common_freqs\" must be specified when argument \"most_common_vals\" is specified".into(),
        );
    } else if supplied.most_common_freqs.is_some() {
        warnings.push(
            "argument \"most_common_vals\" must be specified when argument \"most_common_freqs\" is specified".into(),
        );
    }
    if let Some(bounds) = supplied.histogram_bounds.as_deref() {
        match canonical_array(bounds, column.ty, "histogram_bounds") {
            Ok((bounds, _)) => merged.histogram_bounds = Some(bounds),
            Err(message) => warnings.push(message),
        }
    }
    if supplied.correlation.is_some() {
        merged.correlation = supplied.correlation;
    }
    let element_type = match column.ty {
        ColumnType::Array(element) => Some(element.column_type()),
        _ => None,
    };
    if supplied.most_common_elems.is_some() || supplied.most_common_elem_freqs.is_some() {
        if let Some(element) = element_type {
            if let (Some(values), Some(freqs)) = (
                supplied.most_common_elems.as_deref(),
                supplied.most_common_elem_freqs.as_deref(),
            ) {
                match (
                    canonical_array(values, element, "most_common_elems"),
                    real_array_len(freqs),
                ) {
                    (Ok((values, values_len)), Ok(freqs_len))
                        if freqs_len == values_len + 2 || freqs_len == values_len + 3 =>
                    {
                        merged.most_common_elems = Some(values);
                        merged.most_common_elem_freqs = Some(freqs.to_owned());
                    }
                    (Ok(_), Ok(_)) => warnings.push(
                        "could not parse \"most_common_elems\": incorrect number of elements"
                            .into(),
                    ),
                    (Err(message), _) | (_, Err(message)) => warnings.push(message),
                }
            } else if supplied.most_common_elems.is_some() {
                warnings.push(
                    "argument \"most_common_elem_freqs\" must be specified when argument \"most_common_elems\" is specified".into(),
                );
            } else {
                warnings.push(
                    "argument \"most_common_elems\" must be specified when argument \"most_common_elem_freqs\" is specified".into(),
                );
            }
        } else {
            warnings.push(format!(
                "could not determine element type of column \"{}\"",
                column.name
            ));
        }
    }
    if let Some(histogram) = supplied.elem_count_histogram.as_deref() {
        if element_type.is_some() {
            match real_array_len(histogram) {
                Ok(_) => merged.elem_count_histogram = Some(histogram.into()),
                Err(message) => {
                    warnings.push(message.replace("most_common_freqs", "elem_count_histogram"))
                }
            }
        } else {
            warnings.push(format!(
                "could not determine element type of column \"{}\"",
                column.name
            ));
        }
    }
    let range_stats = supplied.range_length_histogram.is_some()
        || supplied.range_empty_frac.is_some()
        || supplied.range_bounds_histogram.is_some();
    if range_stats {
        if let ColumnType::Range(range) = column.ty {
            if let (Some(histogram), Some(empty_frac)) = (
                supplied.range_length_histogram.as_deref(),
                supplied.range_empty_frac,
            ) {
                match canonical_array(histogram, ColumnType::Float8, "range_length_histogram") {
                    Ok((histogram, _)) => {
                        merged.range_length_histogram = Some(histogram);
                        merged.range_empty_frac = Some(empty_frac);
                    }
                    Err(message) => warnings.push(message),
                }
            } else if supplied.range_length_histogram.is_some() {
                warnings.push(
                    "argument \"range_empty_frac\" must be specified when argument \"range_length_histogram\" is specified".into(),
                );
            } else if supplied.range_empty_frac.is_some() {
                warnings.push(
                    "argument \"range_length_histogram\" must be specified when argument \"range_empty_frac\" is specified".into(),
                );
            }
            if let Some(bounds) = supplied.range_bounds_histogram.as_deref() {
                match canonical_array(bounds, ColumnType::Range(range), "range_bounds_histogram") {
                    Ok((bounds, _)) => merged.range_bounds_histogram = Some(bounds),
                    Err(message) => warnings.push(message),
                }
            }
        } else {
            warnings.push(format!("column \"{}\" is not a range type", column.name));
        }
    }
    Ok(StatisticsOutcome {
        value: Datum::Bool(warnings.is_empty()),
        ops: vec![attrstats::set_op(&key, merged)],
        warnings,
        locks,
        error: None,
    })
}

fn clear_attribute(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    values: &[Datum],
) -> Result<StatisticsOutcome, ExecError> {
    let [schema, relation, attribute, inherited] = values else {
        return Err(ExecError::UndefinedFunction(
            "function pg_clear_attribute_stats(...) does not exist".into(),
        ));
    };
    let relation = relation_name(
        kv,
        resolution,
        required_text(schema, "schemaname")?,
        required_text(relation, "relname")?,
    )?;
    let (table, _) = attribute_statistics_table(kv, &relation)?;
    let locks = statistics_locks(kv, &relation)?;
    let attribute = required_text(attribute, "attname")?.ok_or_else(|| null_argument("attname"))?;
    let attnum = table
        .columns
        .iter()
        .position(|column| column.name == attribute)
        .ok_or_else(|| ExecError::UndefinedTableColumn {
            column: attribute,
            table: relation.name.clone(),
        })?;
    let mut warnings = Vec::new();
    let inherited = bool_argument(inherited, "inherited", &mut warnings)
        .ok_or_else(|| null_argument("inherited"))?;
    let key = attrstats::AttributeStatsKey {
        relation: relation.clone(),
        attnum: i16::try_from(attnum + 1)
            .map_err(|_| ExecError::Unsupported("attribute number exceeds int2".into()))?,
        inherited,
    };
    Ok(StatisticsOutcome {
        value: Datum::Text(String::new()),
        ops: vec![attrstats::clear_op(&key)],
        warnings,
        locks,
        error: None,
    })
}

fn attribute_statistics_table(
    kv: &dyn Kv,
    relation: &RelationName,
) -> Result<(crabka_pgcatalog::Table, crabka_pgcatalog::TableId), ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, relation) {
        return Ok((table.clone(), table.id));
    }
    let index = crabka_pgcatalog::get_index(kv, relation)
        .map_err(|_| attribute_relation_error(relation))?;
    let source = crabka_pgcatalog::get_table(kv, &index.table)?;
    Ok((
        crate::exec::catalog_rows::index_attribute_table(&index, &source)?,
        index.table_id,
    ))
}

fn attribute_relation_error(name: &RelationName) -> ExecError {
    ExecError::FunctionErrorWithMessageDetail {
        sqlstate: "42809",
        message: format!("cannot modify statistics for relation \"{}\"", name.name),
        detail: "This operation is supported only for tables.".into(),
    }
}

fn relation_name(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    schema: Option<String>,
    relation: Option<String>,
) -> Result<RelationName, ExecError> {
    let schema = schema.ok_or_else(|| null_argument("schemaname"))?;
    let relation = relation.ok_or_else(|| null_argument("relname"))?;
    let resolved_schema = if schema == crabka_pgcatalog::PG_TEMP_ALIAS {
        resolution.temp_schema()
    } else {
        schema.clone()
    };
    if !crabka_pgcatalog::schema_exists(kv, &resolved_schema)? {
        return Err(CatalogError::UndefinedSchema(schema).into());
    }
    let name = RelationName::new(resolved_schema, relation.clone());
    if !crabka_pgcatalog::relation_exists(kv, &name)? {
        let reported = RelationName::new(schema, relation);
        return Err(CatalogError::UndefinedTable(reported.to_string()).into());
    }
    Ok(name)
}

fn ensure_statistics_relation(kv: &dyn Kv, name: &RelationName) -> Result<(), ExecError> {
    if crabka_pgcatalog::get_table(kv, name).is_ok()
        || crabka_pgcatalog::get_index(kv, name).is_ok()
    {
        return Ok(());
    }
    let detail = if crabka_pgcatalog::get_sequence(kv, name).is_ok() {
        "This operation is not supported for sequences."
    } else if crabka_pgcatalog::get_view(kv, name).is_ok() {
        "This operation is not supported for views."
    } else {
        "This operation is not supported for this relation type."
    };
    Err(ExecError::FunctionErrorWithMessageDetail {
        sqlstate: "42809",
        message: format!("cannot modify statistics for relation \"{}\"", name.name),
        detail,
    })
}

fn statistics_locks(
    kv: &dyn Kv,
    relation: &RelationName,
) -> Result<Vec<(RelationName, crate::lockmgr::RelationLockTarget)>, ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, relation) {
        return Ok(vec![(
            table.name,
            crate::lockmgr::RelationLockTarget::Table(table.id),
        )]);
    }
    let index = crabka_pgcatalog::get_index(kv, relation)
        .expect("statistics relation was checked as a table or index");
    Ok(vec![
        (
            index.table.clone(),
            crate::lockmgr::RelationLockTarget::Table(index.table_id),
        ),
        (
            index.qualified_name(),
            crate::lockmgr::RelationLockTarget::Index(index.id),
        ),
    ])
}

fn return_with_warnings(
    warnings: Vec<String>,
    error: ExecError,
) -> Result<StatisticsOutcome, ExecError> {
    if warnings.is_empty() {
        Err(error)
    } else {
        Ok(StatisticsOutcome {
            value: Datum::Null,
            ops: Vec::new(),
            warnings,
            locks: Vec::new(),
            error: Some(error),
        })
    }
}

pub(crate) fn warning(message: String) -> crabka_pgwire::error::PgError {
    let detail = if message.starts_with("column ") && message.ends_with(" is not a range type") {
        Some("Cannot set STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM or STATISTIC_KIND_BOUNDS_HISTOGRAM.")
    } else if message.starts_with("could not determine element type of column ") {
        Some("Cannot set STATISTIC_KIND_MCELEM or STATISTIC_KIND_DECHIST.")
    } else {
        None
    };
    match detail {
        Some(detail) => crabka_pgwire::error::PgError::warning(message).with_detail(detail),
        None => crabka_pgwire::error::PgError::warning(message),
    }
}

fn string_argument(value: &Datum, _kind: &str, position: usize) -> Result<String, ExecError> {
    match value {
        Datum::Text(value) => Ok(value.clone()),
        Datum::Null => Err(ExecError::FunctionError {
            sqlstate: "22004",
            message: format!("name at variadic position {position} is null"),
        }),
        _ => Err(ExecError::FunctionError {
            sqlstate: "42804",
            message: "statistics argument names must be text".into(),
        }),
    }
}

fn required_text(value: &Datum, name: &str) -> Result<Option<String>, ExecError> {
    match value {
        Datum::Text(value) => Ok(Some(value.clone())),
        Datum::Null => Ok(None),
        value => Err(ExecError::FunctionError {
            sqlstate: "42804",
            message: format!(
                "argument \"{name}\" has type {}, expected type text",
                type_name(value)
            ),
        }),
    }
}

fn int4_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<i32> {
    match value {
        Datum::Int4(value) => Some(*value),
        Datum::Null => None,
        value => {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type integer",
                type_name(value)
            ));
            None
        }
    }
}

fn int2_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<i16> {
    match value {
        Datum::Int2(value) => Some(*value),
        Datum::Null => None,
        value => {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type smallint",
                type_name(value)
            ));
            None
        }
    }
}

fn float4_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<f32> {
    match value {
        Datum::Float4(value) => Some(*value),
        Datum::Null => None,
        value => {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type real",
                type_name(value)
            ));
            None
        }
    }
}

fn text_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<String> {
    match value {
        Datum::Text(value) => Some(value.clone()),
        Datum::Null => None,
        value => {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type text",
                type_name(value)
            ));
            None
        }
    }
}

fn real_array_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<String> {
    let Datum::Array(array) = value else {
        if !value.is_null() {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type real[]",
                type_name(value)
            ));
        }
        return None;
    };
    if array.elem != ElemType::Float4 {
        warnings.push(format!(
            "argument \"{name}\" has type {}, expected type real[]",
            type_name(value)
        ));
        return None;
    }
    let zone = jiff::tz::TimeZone::UTC;
    let style = OutputStyle::with_zone(&zone);
    Some(
        String::from_utf8(crabka_pgtypes::encoding::encode_text_in(value, style))
            .expect("datum text is UTF-8"),
    )
}

fn canonical_array(
    input: &str,
    element: ColumnType,
    name: &str,
) -> Result<(String, usize), String> {
    let Some(element) = ElemType::from_column_type(element) else {
        return Err(format!(
            "could not parse \"{name}\": column type has no array type"
        ));
    };
    let zone = jiff::tz::TimeZone::UTC;
    let style = OutputStyle::with_zone(&zone);
    let value = crabka_pgtypes::cast::cast_in(
        &Datum::Text(input.into()),
        ColumnType::Array(element),
        style,
    )
    .map_err(|error| error.to_string())?;
    let Datum::Array(array) = &value else {
        unreachable!("array cast returns an array")
    };
    if array.dims.len() > 1 {
        return Err(format!("\"{name}\" must be a one-dimensional array"));
    }
    if array.elems.iter().any(Datum::is_null) {
        return Err(format!("\"{name}\" array must not contain null values"));
    }
    let text = String::from_utf8(crabka_pgtypes::encoding::encode_text_in(&value, style))
        .expect("datum text is UTF-8");
    Ok((text, array.elems.len()))
}

fn real_array_len(input: &str) -> Result<usize, String> {
    let zone = jiff::tz::TimeZone::UTC;
    let style = OutputStyle::with_zone(&zone);
    let value = crabka_pgtypes::cast::cast_in(
        &Datum::Text(input.into()),
        ColumnType::Array(ElemType::Float4),
        style,
    )
    .map_err(|error| error.to_string())?;
    let Datum::Array(array) = value else {
        unreachable!("array cast returns an array")
    };
    if array.dims.len() > 1 {
        return Err("\"most_common_freqs\" must be a one-dimensional array".into());
    }
    if array.elems.iter().any(Datum::is_null) {
        return Err("argument \"most_common_freqs\" array must not contain null values".into());
    }
    Ok(array.elems.len())
}

fn bool_argument(value: &Datum, name: &str, warnings: &mut Vec<String>) -> Option<bool> {
    match value {
        Datum::Bool(value) => Some(*value),
        Datum::Null => None,
        value => {
            warnings.push(format!(
                "argument \"{name}\" has type {}, expected type boolean",
                type_name(value)
            ));
            None
        }
    }
}

fn null_argument(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22004",
        message: format!("argument \"{name}\" must not be null"),
    }
}

fn type_name(value: &Datum) -> &'static str {
    match value {
        Datum::Bool(_) => "boolean",
        Datum::Int2(_) => "smallint",
        Datum::Int4(_) => "integer",
        Datum::Int8(_) => "bigint",
        Datum::Float4(_) => "real",
        Datum::Float8(_) => "double precision",
        Datum::Oid(_) => "oid",
        Datum::Text(_) => "text",
        Datum::Array(array) => array.elem.array_name(),
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, create_schema_ops, create_table};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgtypes::{ColumnType, Datum};

    use super::{
        StatisticsRequest, canonical_array, ensure_statistics_relation, execute,
        real_array_argument, real_array_len, string_argument, type_name,
    };

    fn catalog() -> MemKv {
        let kv = MemKv::new();
        kv.write_batch(&create_schema_ops(&kv, "stats_import", "postgres").expect("schema ops"))
            .expect("schema");
        let relation = RelationName::new("stats_import", "test");
        create_table(
            &kv,
            &relation,
            vec![
                Column::new("id", ColumnType::Int4),
                Column::new("tags", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
                Column::new(
                    "arange",
                    ColumnType::builtin_range(crabka_pgtypes::oids::INT4RANGE).expect("int4range"),
                ),
            ],
        )
        .expect("table");
        kv
    }

    #[test]
    fn relation_restore_and_clear_round_trip_every_stored_statistic() {
        let kv = catalog();
        let outcome = execute(
            &kv,
            StatisticsRequest::RestoreRelation(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("relpages".into()),
                Datum::Int4(18),
                Datum::Text("reltuples".into()),
                Datum::Float4(21.0),
                Datum::Text("relallvisible".into()),
                Datum::Int4(24),
                Datum::Text("relallfrozen".into()),
                Datum::Int4(27),
            ]),
        )
        .expect("restore");
        kv.write_batch(&outcome.ops).expect("persist restore");
        let relation = RelationName::new("stats_import", "test");
        assert!(
            crate::relstats::of(&kv, &relation).expect("stats")
                == crate::relstats::RelStats {
                    reltuples: 21.0,
                    relpages: 18,
                    relallvisible: 24,
                    relallfrozen: 27,
                    has_subclass: false,
                }
        );
        let clear = execute(
            &kv,
            StatisticsRequest::ClearRelation(vec![
                Datum::Text("stats_import".into()),
                Datum::Text("test".into()),
            ]),
        )
        .expect("clear");
        kv.write_batch(&clear.ops).expect("persist clear");
        assert!(
            crate::relstats::of(&kv, &relation).expect("stats")
                == crate::relstats::RelStats::default()
        );
    }

    #[test]
    fn relation_restore_reports_version_and_type_warnings_without_dropping_valid_stats() {
        let kv = catalog();
        let outcome = execute(
            &kv,
            StatisticsRequest::RestoreRelation(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("version".into()),
                Datum::Text("wrong".into()),
                Datum::Text("relpages".into()),
                Datum::Int4(9),
            ]),
        )
        .expect("restore");
        assert!(outcome.value == Datum::Bool(false));
        assert!(
            outcome.warnings == vec!["argument \"version\" has type text, expected type integer"]
        );
        kv.write_batch(&outcome.ops).expect("persist restore");
        let relation = RelationName::new("stats_import", "test");
        assert!(crate::relstats::of(&kv, &relation).expect("stats").relpages == 9);
    }

    #[test]
    fn statistics_warning_details_name_the_rejected_statistic_kinds() {
        let range = super::warning("column \"id\" is not a range type".into());
        assert!(
            range
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.detail.as_deref())
                == Some(
                    "Cannot set STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM or STATISTIC_KIND_BOUNDS_HISTOGRAM."
                )
        );
        let elements = super::warning("could not determine element type of column \"id\"".into());
        assert!(
            elements
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.detail.as_deref())
                == Some("Cannot set STATISTIC_KIND_MCELEM or STATISTIC_KIND_DECHIST.")
        );
    }

    #[test]
    fn statistics_input_diagnostics_keep_variadic_position_and_relation_kind() {
        let kv = catalog();
        let null_name = string_argument(&Datum::Null, "name", 5).expect_err("null name");
        assert!(
            null_name
                == crate::error::ExecError::FunctionError {
                    sqlstate: "22004",
                    message: "name at variadic position 5 is null".into(),
                }
        );
        let missing = RelationName::new("stats_import", "missing");
        assert!(ensure_statistics_relation(&kv, &missing).is_err());
        let later_null_name = match execute(
            &kv,
            StatisticsRequest::RestoreRelation(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Null,
                Datum::Int4(1),
            ]),
        ) {
            Ok(_) => panic!("later null name"),
            Err(error) => error,
        };
        assert!(
            later_null_name
                == crate::error::ExecError::FunctionError {
                    sqlstate: "22004",
                    message: "name at variadic position 3 is null".into(),
                }
        );
        let system_column = match execute(
            &kv,
            StatisticsRequest::RestoreAttribute(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("attname".into()),
                Datum::Text("xmin".into()),
                Datum::Text("inherited".into()),
                Datum::Bool(false),
            ]),
        ) {
            Ok(_) => panic!("system column statistics"),
            Err(error) => error,
        };
        assert!(
            system_column
                == crate::error::ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "cannot modify statistics on system column \"xmin\"".into(),
                }
        );
        let deferred = execute(
            &kv,
            StatisticsRequest::RestoreRelation(vec![
                Datum::Text("schemaname".into()),
                Datum::Float8(3.6),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
            ]),
        )
        .expect("warning before error");
        assert!(
            deferred.warnings
                == vec!["argument \"schemaname\" has type double precision, expected type text"]
        );
        assert!(
            deferred.error
                == Some(crate::error::ExecError::FunctionError {
                    sqlstate: "22004",
                    message: "argument \"schemaname\" must not be null".into(),
                })
        );
        assert!(
            crate::error::ExecError::FunctionError {
                sqlstate: "22023",
                message: "variadic arguments must be name/value pairs".into(),
            }
            .into_pg()
                == crabka_pgwire::error::PgError::error(
                    "22023",
                    "variadic arguments must be name/value pairs",
                )
                .with_hint(
                    "Provide an even number of variadic arguments that can be divided into pairs.",
                )
        );
    }

    #[test]
    fn statistics_type_labels_cover_every_supported_scalar_input() {
        let labels = [
            (Datum::Bool(false), "boolean"),
            (Datum::Int2(0), "smallint"),
            (Datum::Int4(0), "integer"),
            (Datum::Int8(0), "bigint"),
            (Datum::Float4(0.0), "real"),
            (Datum::Float8(0.0), "double precision"),
            (Datum::Oid(0), "oid"),
            (Datum::Text(String::new()), "text"),
            (
                Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Float8,
                    Vec::new(),
                )),
                "double precision[]",
            ),
            (Datum::Null, "unknown"),
        ];
        for (value, expected) in labels {
            assert!(type_name(&value) == expected, "{value:?}");
        }
    }

    #[test]
    fn attribute_restore_merges_fixed_fields_and_clear_removes_one_inheritance_key() {
        let kv = catalog();
        let restore = |pairs| execute(&kv, StatisticsRequest::RestoreAttribute(pairs));
        let first = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("id".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("null_frac".into()),
            Datum::Float4(0.2),
            Datum::Text("avg_width".into()),
            Datum::Int4(5),
        ])
        .expect("first restore");
        kv.write_batch(&first.ops).expect("first write");
        let second = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("id".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("n_distinct".into()),
            Datum::Float4(0.6),
            Datum::Text("version".into()),
            Datum::Text("wrong".into()),
        ])
        .expect("second restore");
        assert!(second.value == Datum::Bool(false));
        assert!(
            second.warnings == vec!["argument \"version\" has type text, expected type integer"]
        );
        kv.write_batch(&second.ops).expect("second write");

        let slotted = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("id".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("most_common_vals".into()),
            Datum::Text("{2,1,3}".into()),
            Datum::Text("most_common_freqs".into()),
            Datum::Array(crabka_pgtypes::ArrayValue::new(
                crabka_pgtypes::ElemType::Float4,
                vec![Datum::Float4(0.3), Datum::Float4(0.25), Datum::Float4(0.05)],
            )),
            Datum::Text("histogram_bounds".into()),
            Datum::Text("{1,2,3,4}".into()),
            Datum::Text("correlation".into()),
            Datum::Float4(-0.5),
        ])
        .expect("slotted restore");
        assert!(slotted.value == Datum::Bool(true));
        kv.write_batch(&slotted.ops).expect("slotted write");

        let mismatch = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("id".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("most_common_vals".into()),
            Datum::Text("{2,1}".into()),
            Datum::Text("most_common_freqs".into()),
            Datum::Array(crabka_pgtypes::ArrayValue::new(
                crabka_pgtypes::ElemType::Float4,
                vec![Datum::Float4(0.3)],
            )),
        ])
        .expect("mismatch restore");
        assert!(mismatch.value == Datum::Bool(false));
        assert!(
            mismatch.warnings
                == vec![
                    "could not parse \"most_common_vals\": incorrect number of elements (same as \"most_common_freqs\" required)"
                ]
        );

        let mut warnings = Vec::new();
        assert!(real_array_argument(&Datum::Null, "most_common_freqs", &mut warnings).is_none());
        assert!(warnings.is_empty());
        assert!(canonical_array("{{1},{2}}", ColumnType::Int4, "histogram_bounds").is_err());
        assert!(real_array_len("{{0.1},{0.2}}").is_err());

        let key = crate::attrstats::AttributeStatsKey {
            relation: RelationName::new("stats_import", "test"),
            attnum: 1,
            inherited: false,
        };
        assert!(
            crate::attrstats::get(&kv, &key).expect("stored stats")
                == Some(crate::attrstats::AttributeStats {
                    null_frac: Some(0.2),
                    avg_width: Some(5),
                    n_distinct: Some(0.6),
                    most_common_vals: Some("{2,1,3}".into()),
                    most_common_freqs: Some("{0.3,0.25,0.05}".into()),
                    histogram_bounds: Some("{1,2,3,4}".into()),
                    correlation: Some(-0.5),
                    ..Default::default()
                })
        );

        let inherited = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("id".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(true),
        ])
        .expect("inherited restore");
        kv.write_batch(&inherited.ops).expect("inherited write");
        let inherited_key = crate::attrstats::AttributeStatsKey {
            inherited: true,
            ..key.clone()
        };
        assert!(
            crate::attrstats::get(&kv, &inherited_key)
                .expect("inherited stats")
                .is_some()
        );

        let later_null_name = match execute(
            &kv,
            StatisticsRequest::RestoreAttribute(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("attname".into()),
                Datum::Text("id".into()),
                Datum::Null,
                Datum::Int4(1),
            ]),
        ) {
            Ok(_) => panic!("later argument name must fail"),
            Err(error) => error,
        };
        assert!(
            later_null_name
                == crate::error::ExecError::FunctionError {
                    sqlstate: "22004",
                    message: "name at variadic position 7 is null".into(),
                }
        );

        let clear = execute(
            &kv,
            StatisticsRequest::ClearAttribute(vec![
                Datum::Text("stats_import".into()),
                Datum::Text("test".into()),
                Datum::Text("id".into()),
                Datum::Bool(false),
            ]),
        )
        .expect("clear");
        kv.write_batch(&clear.ops).expect("clear write");
        assert!(crate::attrstats::get(&kv, &key).expect("cleared stats") == None);
    }

    #[test]
    fn attribute_restore_accepts_attnum_and_all_array_and_range_slots() {
        let kv = catalog();
        let restore = |pairs| execute(&kv, StatisticsRequest::RestoreAttribute(pairs));
        let tags = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attnum".into()),
            Datum::Int2(2),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("most_common_elems".into()),
            Datum::Text("{one,three}".into()),
            Datum::Text("most_common_elem_freqs".into()),
            Datum::Array(crabka_pgtypes::ArrayValue::new(
                crabka_pgtypes::ElemType::Float4,
                vec![
                    Datum::Float4(0.3),
                    Datum::Float4(0.2),
                    Datum::Float4(0.2),
                    Datum::Float4(0.3),
                ],
            )),
            Datum::Text("elem_count_histogram".into()),
            Datum::Array(crabka_pgtypes::ArrayValue::new(
                crabka_pgtypes::ElemType::Float4,
                vec![Datum::Float4(1.0), Datum::Float4(2.0)],
            )),
        ])
        .expect("tags restore");
        assert!(tags.value == Datum::Bool(true));
        kv.write_batch(&tags.ops).expect("tags write");

        assert!(
            restore(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("attnum".into()),
                Datum::Int2(0),
                Datum::Text("inherited".into()),
                Datum::Bool(false),
            ])
            .is_err()
        );
        assert!(
            restore(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("attnum".into()),
                Datum::Int2(i16::MIN),
                Datum::Text("inherited".into()),
                Datum::Bool(false),
            ])
            .is_err()
        );
        assert!(
            restore(vec![
                Datum::Text("schemaname".into()),
                Datum::Text("stats_import".into()),
                Datum::Text("relname".into()),
                Datum::Text("test".into()),
                Datum::Text("attnum".into()),
                Datum::Int2(4),
                Datum::Text("inherited".into()),
                Datum::Bool(false),
            ])
            .is_err()
        );

        let ranges = restore(vec![
            Datum::Text("schemaname".into()),
            Datum::Text("stats_import".into()),
            Datum::Text("relname".into()),
            Datum::Text("test".into()),
            Datum::Text("attname".into()),
            Datum::Text("arange".into()),
            Datum::Text("inherited".into()),
            Datum::Bool(false),
            Datum::Text("range_empty_frac".into()),
            Datum::Float4(0.5),
            Datum::Text("range_length_histogram".into()),
            Datum::Text("{1,2,Infinity}".into()),
            Datum::Text("range_bounds_histogram".into()),
            Datum::Text(r#"{"[1,2)","[3,4)"}"#.into()),
        ])
        .expect("range restore");
        assert!(ranges.value == Datum::Bool(true));
        kv.write_batch(&ranges.ops).expect("range write");

        let tags_key = crate::attrstats::AttributeStatsKey {
            relation: RelationName::new("stats_import", "test"),
            attnum: 2,
            inherited: false,
        };
        assert!(
            crate::attrstats::get(&kv, &tags_key).expect("tags stats")
                == Some(crate::attrstats::AttributeStats {
                    most_common_elems: Some("{one,three}".into()),
                    most_common_elem_freqs: Some("{0.3,0.2,0.2,0.3}".into()),
                    elem_count_histogram: Some("{1,2}".into()),
                    ..Default::default()
                })
        );
        let range_key = crate::attrstats::AttributeStatsKey {
            attnum: 3,
            ..tags_key
        };
        assert!(
            crate::attrstats::get(&kv, &range_key).expect("range stats")
                == Some(crate::attrstats::AttributeStats {
                    range_length_histogram: Some("{1,2,Infinity}".into()),
                    range_empty_frac: Some(0.5),
                    range_bounds_histogram: Some(r#"{"[1,2)","[3,4)"}"#.into()),
                    ..Default::default()
                })
        );
    }
}
