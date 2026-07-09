//! Subscription filter compilation and evaluation.
//!
//! This module accepts a SQL predicate, parses it once at subscription start,
//! and evaluates it against decoded records. Arrow builds compile the SQL
//! predicate to a `DataFusion` physical expression per Arrow schema and evaluate
//! it directly over record batches; the JSON row path remains a compatibility
//! fallback for simple scalar filters when no Arrow batch is available.

use std::collections::BTreeMap;
#[cfg(feature = "arrow")]
use std::{collections::HashMap, sync::Mutex};

use serde_json::Value;
use thiserror::Error;

const RESERVED_ROW_MARKER_FIELD: &str = "__crabka_row_marker";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum FilterCompileError {
    #[error("unsupported SQL subscription filter: {0}")]
    UnsupportedSql(String),
    #[error(
        "DataFusion subscription filter evaluation requires the grpc-gateway arrow feature: {0}"
    )]
    #[allow(
        dead_code,
        reason = "constructed by non-Arrow builds; Arrow all-target checks still compile this enum"
    )]
    DataFusionUnavailable(String),
    #[error("DataFusion subscription filter evaluation failed: {0}")]
    #[allow(
        dead_code,
        reason = "constructed by the Arrow subscription path once schema-routed payloads call it"
    )]
    DataFusion(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodedRecordFilterDecision {
    Deliver,
    Drop,
    #[allow(
        dead_code,
        reason = "constructed by Arrow builds; default cargo check compiles the enum without Arrow IPC routing"
    )]
    ArrowIpcBatch {
        row_count: usize,
        matching_rows: usize,
    },
}

impl DecodedRecordFilterDecision {
    #[must_use]
    pub(crate) const fn should_deliver(&self) -> bool {
        match self {
            Self::Deliver => true,
            Self::Drop => false,
            Self::ArrowIpcBatch { matching_rows, .. } => *matching_rows > 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompiledFilter {
    sql: String,
    predicates: Vec<Predicate>,
    #[cfg(feature = "arrow")]
    arrow_cache: Mutex<HashMap<ArrowFilterCacheKey, ArrowCompiledFilter>>,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArrowFilterCacheKey {
    schema_id: Option<i32>,
    schema_key: String,
}

/// Typed, row-oriented predicate input used by the gateway filter seam.
///
/// JSON decoding, Arrow dictionary decoding, and the future `DataFusion` bridge all
/// meet the filter here: compiled predicates read trusted scalar field values
/// from rows instead of re-parsing transport-specific payloads.
pub(crate) trait FilterRowBatch {
    fn row_count(&self) -> usize;

    fn field_value(&self, row: usize, path: &FieldPath) -> Option<&FieldValue>;
}

#[derive(Debug, Clone, PartialEq)]
struct Predicate {
    field: FieldPath,
    operator: ComparisonOperator,
    expected: FilterValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FieldPath(Vec<String>);

#[derive(Debug, Clone, PartialEq)]
struct RowValue {
    fields: BTreeMap<String, FieldValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TypedRowBatch {
    rows: Vec<RowValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FieldValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

impl CompiledFilter {
    pub(crate) fn compile(sql: &str) -> Result<Self, FilterCompileError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(Self {
                sql: String::new(),
                predicates: Vec::new(),
                #[cfg(feature = "arrow")]
                arrow_cache: Mutex::new(HashMap::new()),
            });
        }
        if sql_references_reserved_row_marker(sql) {
            return Err(FilterCompileError::UnsupportedSql(format!(
                "filter field {RESERVED_ROW_MARKER_FIELD:?} is reserved for internal row-count preservation"
            )));
        }

        #[cfg(not(feature = "arrow"))]
        if sql_requires_datafusion(sql) {
            return Err(FilterCompileError::DataFusionUnavailable(
                "complex SQL predicates require Arrow/DataFusion support".to_string(),
            ));
        }

        let predicates = match parse_legacy_predicates(sql) {
            Ok(predicates) => predicates,
            #[cfg(feature = "arrow")]
            Err(_error) if sql_requires_arrow_datafusion(sql) => Vec::new(),
            #[cfg(feature = "arrow")]
            Err(error) => return Err(error),
            #[cfg(not(feature = "arrow"))]
            Err(error) => return Err(error),
        };

        Ok(Self {
            sql: sql.to_string(),
            predicates,
            #[cfg(feature = "arrow")]
            arrow_cache: Mutex::new(HashMap::new()),
        })
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn compile_for_schema(
        sql: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<ArrowCompiledFilter, FilterCompileError> {
        ArrowCompiledFilter::compile(sql, schema)
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn evaluate_arrow_batch(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<arrow::array::BooleanArray, FilterCompileError> {
        self.evaluate_arrow_batch_for_schema_id(None, batch)
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn evaluate_arrow_batch_for_schema_id(
        &self,
        schema_id: Option<i32>,
        batch: &arrow::array::RecordBatch,
    ) -> Result<arrow::array::BooleanArray, FilterCompileError> {
        if self.sql.is_empty() {
            return Ok(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]));
        }

        let cache_key = ArrowFilterCacheKey {
            schema_id,
            schema_key: arrow_schema_key(batch.schema().as_ref()),
        };
        let mut cache = self.arrow_cache.lock().map_err(|_| {
            FilterCompileError::DataFusion("filter schema cache mutex was poisoned".to_string())
        })?;
        let compiled = if let Some(compiled) = cache.get(&cache_key) {
            compiled
        } else {
            cache.insert(
                cache_key.clone(),
                Self::compile_for_schema(&self.sql, &batch.schema())?,
            );
            cache
                .get(&cache_key)
                .expect("compiled filter cache entry inserted before lookup")
        };
        compiled.evaluate(batch)
    }

    #[must_use]
    pub(crate) fn matches_structured_json(&self, json: Option<&bytes::Bytes>) -> bool {
        if self.sql.is_empty() {
            return true;
        }
        if self.predicates.is_empty() {
            return false;
        }

        let Some(json) = json else {
            return false;
        };
        let Ok(value) = serde_json::from_slice::<Value>(json) else {
            return false;
        };
        let Some(batch) = TypedRowBatch::from_json_for_predicates(&value, &self.predicates) else {
            return false;
        };

        self.matches_row(&batch, 0)
    }

    #[must_use]
    #[allow(
        dead_code,
        reason = "unit tests and non-batch callers use the boolean convenience wrapper"
    )]
    pub(crate) fn matches_decoded_record(
        &self,
        json: Option<&bytes::Bytes>,
        value: &bytes::Bytes,
    ) -> bool {
        #[cfg(feature = "arrow")]
        {
            self.evaluate_decoded_record(json, value)
                .is_ok_and(|decision| decision.should_deliver())
        }

        #[cfg(not(feature = "arrow"))]
        {
            self.evaluate_decoded_record(json, value).should_deliver()
        }
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn evaluate_decoded_record(
        &self,
        json: Option<&bytes::Bytes>,
        value: &bytes::Bytes,
    ) -> Result<DecodedRecordFilterDecision, FilterCompileError> {
        if self.sql.is_empty() {
            return Ok(DecodedRecordFilterDecision::Deliver);
        }

        match self.matches_arrow_ipc_record(value) {
            Ok(Some(decision)) => return Ok(decision),
            Ok(None) if self.predicates.is_empty() && !self.sql.is_empty() => {
                return Ok(DecodedRecordFilterDecision::Drop);
            }
            Ok(None) => {}
            Err(error) => return Err(error),
        }

        Ok(self.evaluate_json_decoded_record(json))
    }

    #[cfg(not(feature = "arrow"))]
    #[must_use]
    pub(crate) fn evaluate_decoded_record(
        &self,
        json: Option<&bytes::Bytes>,
        _value: &bytes::Bytes,
    ) -> DecodedRecordFilterDecision {
        self.evaluate_json_decoded_record(json)
    }

    #[must_use]
    fn evaluate_json_decoded_record(
        &self,
        json: Option<&bytes::Bytes>,
    ) -> DecodedRecordFilterDecision {
        if self.sql.is_empty() {
            return DecodedRecordFilterDecision::Deliver;
        }

        if self.matches_structured_json(json) {
            DecodedRecordFilterDecision::Deliver
        } else {
            DecodedRecordFilterDecision::Drop
        }
    }

    #[cfg(feature = "arrow")]
    fn matches_arrow_ipc_record(
        &self,
        value: &bytes::Bytes,
    ) -> Result<Option<DecodedRecordFilterDecision>, FilterCompileError> {
        use arrow::{array::Array, ipc::reader::StreamReader};

        let Ok(reader) = StreamReader::try_new(&value[..], None) else {
            return Ok(None);
        };
        let mut row_count = 0;
        let mut matching_rows = 0;
        for batch in reader {
            let batch = batch.map_err(datafusion_error)?;
            let mask = self.evaluate_arrow_batch(&batch)?;
            row_count += batch.num_rows();
            matching_rows += (0..mask.len())
                .filter(|row| !mask.is_null(*row) && mask.value(*row))
                .count();
        }
        Ok(Some(DecodedRecordFilterDecision::ArrowIpcBatch {
            row_count,
            matching_rows,
        }))
    }

    #[cfg(feature = "arrow")]
    #[allow(
        dead_code,
        reason = "called when Arrow subscription payload routing is enabled"
    )]
    #[must_use]
    pub(crate) fn matches_arrow_batch(
        &self,
        batch: &arrow::array::RecordBatch,
        row: usize,
    ) -> bool {
        self.try_matches_arrow_batch(batch, row).unwrap_or(false)
    }

    #[cfg(feature = "arrow")]
    #[allow(
        dead_code,
        reason = "tested directly and used by matches_arrow_batch for the Arrow route"
    )]
    fn try_matches_arrow_batch(
        &self,
        batch: &arrow::array::RecordBatch,
        row: usize,
    ) -> Result<bool, FilterCompileError> {
        use arrow::array::Array;

        if self.sql.is_empty() {
            return Ok(row < batch.num_rows());
        }

        if row >= batch.num_rows() {
            return Ok(false);
        }

        let values = self.evaluate_arrow_batch(batch)?;

        Ok(!values.is_null(row) && values.value(row))
    }

    #[must_use]
    pub(crate) fn matches_row<B: FilterRowBatch>(&self, batch: &B, row: usize) -> bool {
        if self.sql.is_empty() {
            return row < batch.row_count();
        }
        if self.predicates.is_empty() {
            return false;
        }

        if row >= batch.row_count() {
            return false;
        }

        self.predicates
            .iter()
            .all(|predicate| predicate.matches(batch, row))
    }
}

impl Predicate {
    fn matches<B: FilterRowBatch>(&self, batch: &B, row: usize) -> bool {
        let Some(actual) = batch.field_value(row, &self.field) else {
            return false;
        };
        self.expected.matches(self.operator, actual)
    }
}

#[cfg(feature = "arrow")]
#[derive(Clone)]
pub(crate) struct ArrowCompiledFilter {
    expr: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    enum_symbol_columns: Vec<ArrowEnumSymbolColumn>,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ArrowEnumSymbolColumn {
    index: usize,
    field_name: String,
    symbols: BTreeMap<i64, String>,
}

#[cfg(feature = "arrow")]
impl std::fmt::Debug for ArrowCompiledFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowCompiledFilter")
            .field("expr", &self.expr.to_string())
            .field("enum_symbol_columns", &self.enum_symbol_columns)
            .finish()
    }
}

#[cfg(feature = "arrow")]
impl ArrowCompiledFilter {
    fn compile(
        sql: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Self, FilterCompileError> {
        use datafusion::{common::DFSchema, prelude::SessionContext};

        let ctx = SessionContext::new();
        let enum_symbol_columns = arrow_enum_symbol_columns(schema.as_ref())?;
        let transformed_schema = arrow_filter_schema(schema.as_ref(), &enum_symbol_columns);
        let df_schema = DFSchema::try_from(transformed_schema).map_err(datafusion_error)?;
        let sql = quote_complex_arrow_field_references(sql, schema.as_ref());
        let logical = ctx
            .parse_sql_expr(&sql, &df_schema)
            .map_err(datafusion_error)?;
        let expr = ctx
            .create_physical_expr(logical, &df_schema)
            .map_err(datafusion_error)?;

        Ok(Self {
            expr,
            enum_symbol_columns,
        })
    }

    fn evaluate(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<arrow::array::BooleanArray, FilterCompileError> {
        use arrow::array::Array;

        let filter_batch = self.enum_symbol_filter_batch(batch)?;
        let values = self
            .expr
            .evaluate(&filter_batch)
            .map_err(datafusion_error)?
            .into_array(filter_batch.num_rows())
            .map_err(datafusion_error)?;
        let mask = values
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                FilterCompileError::DataFusion(format!(
                    "predicate returned {}, expected Boolean",
                    values.data_type()
                ))
            })?;
        Ok(arrow::array::BooleanArray::from(
            (0..mask.len())
                .map(|row| !mask.is_null(row) && mask.value(row))
                .collect::<Vec<_>>(),
        ))
    }

    fn enum_symbol_filter_batch(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<arrow::array::RecordBatch, FilterCompileError> {
        if self.enum_symbol_columns.is_empty() {
            return Ok(batch.clone());
        }

        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                self.enum_symbol_columns
                    .iter()
                    .find(|enum_column| enum_column.index == index)
                    .map_or_else(
                        || Ok(column.clone()),
                        |enum_column| enum_column.to_symbol_array(column),
                    )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let schema = arrow_filter_schema(batch.schema().as_ref(), &self.enum_symbol_columns);
        arrow::array::RecordBatch::try_new(std::sync::Arc::new(schema), columns)
            .map_err(datafusion_error)
    }
}

#[cfg(feature = "arrow")]
impl ArrowEnumSymbolColumn {
    fn to_symbol_array(
        &self,
        column: &arrow::array::ArrayRef,
    ) -> Result<arrow::array::ArrayRef, FilterCompileError> {
        use arrow::{
            array::{Array, DictionaryArray, Int32Array, Int64Array, StringArray},
            datatypes::{DataType, Int32Type},
        };

        let dictionary = column
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| {
                FilterCompileError::DataFusion(format!(
                    "arrow enum field {} expected Dictionary<Int32, Int32|Int64> array values",
                    self.field_name
                ))
            })?;
        let symbols = match dictionary.values().data_type() {
            DataType::Int32 => {
                let values = dictionary
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| unexpected_arrow_enum_values(&self.field_name))?;
                self.symbol_names(dictionary, |index| i64::from(values.value(index)))?
            }
            DataType::Int64 => {
                let values = dictionary
                    .values()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| unexpected_arrow_enum_values(&self.field_name))?;
                self.symbol_names(dictionary, |index| values.value(index))?
            }
            data_type => {
                return Err(FilterCompileError::DataFusion(format!(
                    "arrow enum field {} has unsupported dictionary value type {data_type}",
                    self.field_name
                )));
            }
        };

        Ok(std::sync::Arc::new(StringArray::from(symbols)))
    }

    fn symbol_names(
        &self,
        dictionary: &arrow::array::DictionaryArray<arrow::datatypes::Int32Type>,
        number_at: impl Fn(usize) -> i64,
    ) -> Result<Vec<Option<String>>, FilterCompileError> {
        use arrow::array::Array;

        (0..dictionary.len())
            .map(|row| {
                if dictionary.is_null(row) {
                    return Ok(None);
                }
                let key = dictionary.keys().value(row);
                let value_index = usize::try_from(key).map_err(|_| {
                    FilterCompileError::DataFusion(format!(
                        "arrow enum field {} dictionary key {key} is negative",
                        self.field_name
                    ))
                })?;
                if value_index >= dictionary.values().len() {
                    return Err(FilterCompileError::DataFusion(format!(
                        "arrow enum field {} dictionary key {key} is out of bounds for {} values",
                        self.field_name,
                        dictionary.values().len()
                    )));
                }

                let number = number_at(value_index);
                Ok(Some(self.symbol_name(number)))
            })
            .collect()
    }

    fn symbol_name(&self, number: i64) -> String {
        self.symbols
            .get(&number)
            .cloned()
            .unwrap_or_else(|| format!("UNKNOWN_{number}"))
    }
}

#[cfg(feature = "arrow")]
fn unexpected_arrow_enum_values(field_name: &str) -> FilterCompileError {
    FilterCompileError::DataFusion(format!(
        "arrow enum field {field_name} dictionary values do not match the declared value type"
    ))
}

#[cfg(feature = "arrow")]
fn arrow_enum_symbol_columns(
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<ArrowEnumSymbolColumn>, FilterCompileError> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| arrow_field_needs_enum_symbol_projection(field))
        .map(|(index, field)| {
            Ok(ArrowEnumSymbolColumn {
                index,
                field_name: field.name().clone(),
                symbols: parse_arrow_enum_symbols(field)?,
            })
        })
        .collect()
}

#[cfg(feature = "arrow")]
fn arrow_field_needs_enum_symbol_projection(field: &arrow::datatypes::Field) -> bool {
    use arrow::datatypes::DataType;

    let is_enum_field = field
        .metadata()
        .get("crabka.enum")
        .is_some_and(|value| value == "true")
        || field.metadata().contains_key("crabka.enum.symbols");
    if !is_enum_field {
        return false;
    }

    matches!(
        field.data_type(),
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), DataType::Int32 | DataType::Int64)
    )
}

#[cfg(feature = "arrow")]
fn parse_arrow_enum_symbols(
    field: &arrow::datatypes::Field,
) -> Result<BTreeMap<i64, String>, FilterCompileError> {
    let Some(symbols) = field.metadata().get("crabka.enum.symbols") else {
        return Err(FilterCompileError::DataFusion(format!(
            "arrow enum field {} stores numeric values but has no crabka.enum.symbols descriptor metadata",
            field.name()
        )));
    };
    let parsed: serde_json::Value = serde_json::from_str(symbols).map_err(|_| {
        FilterCompileError::DataFusion(format!(
            "arrow enum field {} has invalid crabka.enum.symbols metadata",
            field.name()
        ))
    })?;
    let Some(object) = parsed.as_object() else {
        return Err(FilterCompileError::DataFusion(format!(
            "arrow enum field {} has invalid crabka.enum.symbols metadata",
            field.name()
        )));
    };

    object
        .iter()
        .map(|(number, name)| {
            let number = number.parse::<i64>().map_err(|_| {
                FilterCompileError::DataFusion(format!(
                    "arrow enum field {} has invalid crabka.enum.symbols metadata",
                    field.name()
                ))
            })?;
            let Some(name) = name.as_str() else {
                return Err(FilterCompileError::DataFusion(format!(
                    "arrow enum field {} has invalid crabka.enum.symbols metadata",
                    field.name()
                )));
            };
            Ok((number, name.to_string()))
        })
        .collect()
}

#[cfg(feature = "arrow")]
fn arrow_filter_schema(
    schema: &arrow::datatypes::Schema,
    enum_symbol_columns: &[ArrowEnumSymbolColumn],
) -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field, Schema};

    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if !enum_symbol_columns
                .iter()
                .any(|enum_column| enum_column.index == index)
            {
                return field.clone();
            }

            let mut projected_field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
            projected_field.set_metadata(field.metadata().clone());
            std::sync::Arc::new(projected_field)
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}

#[cfg(feature = "arrow")]
fn quote_complex_arrow_field_references(sql: &str, schema: &arrow::datatypes::Schema) -> String {
    let mut complex_fields = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| name.chars().any(|ch| matches!(ch, '.' | '[' | ']')))
        .collect::<Vec<_>>();
    if complex_fields.is_empty() {
        return sql.to_string();
    }

    complex_fields.sort_by_key(|field| std::cmp::Reverse(field.len()));
    let mut quoted = String::with_capacity(sql.len());
    let mut index = 0;
    let mut in_string = false;
    while index < sql.len() {
        let rest = &sql[index..];
        let Some(ch) = rest.chars().next() else {
            break;
        };
        if ch == '\'' {
            in_string = !in_string;
            quoted.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if in_string {
            quoted.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if let Some(field) = complex_fields
            .iter()
            .copied()
            .find(|field| starts_with_field_reference(sql, index, field))
        {
            quoted.push('"');
            quoted.push_str(&field.replace('"', "\"\""));
            quoted.push('"');
            index += field.len();
            continue;
        }

        quoted.push(ch);
        index += ch.len_utf8();
    }
    quoted
}

fn starts_with_field_reference(sql: &str, index: usize, field: &str) -> bool {
    let Some(candidate) = sql.get(index..index + field.len()) else {
        return false;
    };
    if candidate != field {
        return false;
    }

    let before = sql[..index].chars().next_back();
    let after = sql[index + field.len()..].chars().next();
    !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
}

fn sql_references_reserved_row_marker(sql: &str) -> bool {
    let mut index = 0;
    let mut in_string = false;
    while index < sql.len() {
        let rest = &sql[index..];
        let Some(ch) = rest.chars().next() else {
            return false;
        };
        if ch == '\'' {
            in_string = !in_string;
            index += ch.len_utf8();
            continue;
        }
        if in_string {
            index += ch.len_utf8();
            continue;
        }
        if starts_with_field_reference(sql, index, RESERVED_ROW_MARKER_FIELD) {
            return true;
        }
        index += ch.len_utf8();
    }

    false
}

impl ComparisonOperator {
    const ORDERED: [Self; 4] = [
        Self::GreaterThan,
        Self::GreaterThanOrEqual,
        Self::LessThan,
        Self::LessThanOrEqual,
    ];

    fn parse(clause: &str) -> Result<(Self, &str, &str), FilterCompileError> {
        for (token, operator) in [
            ("!=", Self::NotEqual),
            (">=", Self::GreaterThanOrEqual),
            ("<=", Self::LessThanOrEqual),
            ("=", Self::Equal),
            (">", Self::GreaterThan),
            ("<", Self::LessThan),
        ] {
            let Some(index) = find_unquoted_token(clause, token)? else {
                continue;
            };
            let field = &clause[..index];
            let literal = &clause[index + token.len()..];
            reject_extra_operator(clause, literal)?;
            return Ok((operator, field, literal));
        }

        Err(FilterCompileError::UnsupportedSql(format!(
            "unsupported filter predicate {clause:?}: expected one comparison operator (=, !=, >, >=, <, <=)"
        )))
    }

    fn accepts_literal(self, literal: &FilterValue) -> bool {
        if !Self::ORDERED.contains(&self) {
            return true;
        }

        matches!(literal, FilterValue::String(_) | FilterValue::Number(_))
    }

    fn compare_order(self, ordering: std::cmp::Ordering) -> bool {
        match self {
            Self::Equal => ordering.is_eq(),
            Self::NotEqual => !ordering.is_eq(),
            Self::GreaterThan => ordering.is_gt(),
            Self::GreaterThanOrEqual => ordering.is_ge(),
            Self::LessThan => ordering.is_lt(),
            Self::LessThanOrEqual => ordering.is_le(),
        }
    }
}

impl FieldPath {
    fn parse(input: &str) -> Result<Self, FilterCompileError> {
        let mut segments = Vec::new();
        for raw_segment in input.split('.') {
            let segment = raw_segment.trim();
            if !is_identifier(segment) {
                return Err(FilterCompileError::UnsupportedSql(format!(
                    "unsupported filter field path {input:?}"
                )));
            }
            segments.push(segment.to_string());
        }

        if segments.is_empty() {
            return Err(FilterCompileError::UnsupportedSql(
                "filter predicate requires a field name".to_string(),
            ));
        }

        #[cfg(not(feature = "arrow"))]
        if segments.len() != 1 {
            return Err(FilterCompileError::DataFusionUnavailable(
                "nested field predicates require the future Arrow/DataFusion predicate compiler"
                    .to_string(),
            ));
        }

        #[cfg(feature = "arrow")]
        if segments.len() != 1 {
            return Err(FilterCompileError::UnsupportedSql(
                "nested field predicates are not supported by the Arrow/DataFusion gateway filter"
                    .to_string(),
            ));
        }

        Ok(Self(segments))
    }

    fn as_top_level_name(&self) -> &str {
        &self.0[0]
    }
}

impl FilterValue {
    fn parse(input: &str) -> Result<Self, FilterCompileError> {
        let literal = input.trim();
        if literal.eq_ignore_ascii_case("true") {
            return Ok(Self::Bool(true));
        }
        if literal.eq_ignore_ascii_case("false") {
            return Ok(Self::Bool(false));
        }
        if literal.eq_ignore_ascii_case("null") {
            return Ok(Self::Null);
        }
        if let Some(value) = parse_quoted_string(literal)? {
            return Ok(Self::String(value));
        }
        if let Ok(value) = literal.parse::<f64>()
            && value.is_finite()
        {
            return Ok(Self::Number(value));
        }

        Err(FilterCompileError::UnsupportedSql(format!(
            "unsupported filter literal {literal:?}"
        )))
    }

    fn matches(&self, operator: ComparisonOperator, actual: &FieldValue) -> bool {
        match (self, actual) {
            (Self::String(expected), FieldValue::String(actual)) => {
                operator.compare_order(actual.as_str().cmp(expected))
            }
            (Self::Number(expected), FieldValue::Number(actual)) => {
                operator.compare_order(actual.total_cmp(expected))
            }
            (Self::Number(expected), FieldValue::String(actual)) => actual
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .is_some_and(|actual| operator.compare_order(actual.total_cmp(expected))),
            (Self::Bool(expected), FieldValue::Bool(actual)) => match operator {
                ComparisonOperator::Equal => *actual == *expected,
                ComparisonOperator::NotEqual => *actual != *expected,
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual => false,
            },
            (Self::Null, FieldValue::Null) => match operator {
                ComparisonOperator::Equal => true,
                ComparisonOperator::NotEqual
                | ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual => false,
            },
            (Self::Null, _) => operator == ComparisonOperator::NotEqual,
            (Self::String(_) | Self::Number(_) | Self::Bool(_), _) => false,
        }
    }
}

impl TypedRowBatch {
    fn from_json_for_predicates(value: &Value, predicates: &[Predicate]) -> Option<Self> {
        Some(Self {
            rows: vec![RowValue::from_json_for_predicates(value, predicates)?],
        })
    }

    #[cfg(test)]
    fn from_rows(rows: Vec<BTreeMap<String, FieldValue>>) -> Self {
        Self {
            rows: rows.into_iter().map(|fields| RowValue { fields }).collect(),
        }
    }
}

impl FilterRowBatch for TypedRowBatch {
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn field_value(&self, row: usize, path: &FieldPath) -> Option<&FieldValue> {
        self.rows.get(row)?.field(path)
    }
}

impl RowValue {
    fn from_json_for_predicates(value: &Value, predicates: &[Predicate]) -> Option<Self> {
        let object = value.as_object()?;
        let mut fields = BTreeMap::new();
        for predicate in predicates {
            let name = predicate.field.as_top_level_name();
            let Some(value) = object.get(name) else {
                continue;
            };
            let field = FieldValue::from_json(value)?;
            fields.insert(name.to_string(), field);
        }
        Some(Self { fields })
    }

    fn field(&self, path: &FieldPath) -> Option<&FieldValue> {
        self.fields.get(path.as_top_level_name())
    }
}

impl FieldValue {
    fn from_json(value: &Value) -> Option<Self> {
        if let Some(scalar) = Self::from_scalar_json(value) {
            return Some(scalar);
        }

        Self::from_dictionary_like_json(value)
    }

    fn from_scalar_json(value: &Value) -> Option<Self> {
        match value {
            Value::String(value) => Some(Self::String(value.clone())),
            Value::Number(value) => value
                .as_f64()
                .filter(|value| value.is_finite())
                .map(Self::Number),
            Value::Bool(value) => Some(Self::Bool(*value)),
            Value::Null => Some(Self::Null),
            Value::Array(_) | Value::Object(_) => None,
        }
    }

    fn from_dictionary_like_json(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        if object.get("$type").and_then(Value::as_str) == Some("dictionary") {
            return object.get("value").and_then(Self::from_scalar_json);
        }
        if object.get("$type").and_then(Value::as_str) == Some("enum") {
            if let Some(name) = object.get("name") {
                return Self::from_scalar_json(name);
            }

            return object.get("number").and_then(Self::unknown_enum_name);
        }

        None
    }

    fn unknown_enum_name(value: &Value) -> Option<Self> {
        let number = value.as_i64()?;
        if number < 0 {
            return None;
        }
        Some(Self::String(format!("UNKNOWN_{number}")))
    }
}

fn parse_legacy_predicates(sql: &str) -> Result<Vec<Predicate>, FilterCompileError> {
    split_and_clauses(sql)?
        .into_iter()
        .map(|clause| parse_predicate(clause.trim()))
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(feature = "arrow")]
fn arrow_schema_key(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field
                .metadata()
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>();
            metadata.sort();
            let metadata = metadata.join(",");
            format!(
                "{}:{:?}:{}:{metadata}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn parse_predicate(clause: &str) -> Result<Predicate, FilterCompileError> {
    if clause.is_empty() {
        return Err(FilterCompileError::UnsupportedSql(
            "filter contains an empty predicate".to_string(),
        ));
    }

    let (operator, field, literal) = ComparisonOperator::parse(clause)?;
    let expected = FilterValue::parse(literal.trim())?;
    if !operator.accepts_literal(&expected) {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "unsupported filter predicate {clause:?}: ordered comparisons require numeric or string literals"
        )));
    }

    Ok(Predicate {
        field: FieldPath::parse(field.trim())?,
        operator,
        expected,
    })
}

fn split_and_clauses(sql: &str) -> Result<Vec<&str>, FilterCompileError> {
    let mut clauses = Vec::new();
    let mut clause_start = 0;
    let mut chars = sql.char_indices().peekable();
    let mut in_string = false;

    while let Some((index, ch)) = chars.next() {
        if ch == '\'' {
            in_string = !in_string;
            continue;
        }
        if ch == '\\' && in_string {
            let _ = chars.next();
            continue;
        }
        if in_string || !starts_with_keyword_at(sql, index, "AND") {
            continue;
        }

        clauses.push(&sql[clause_start..index]);
        clause_start = index + "AND".len();
    }

    if in_string {
        return Err(FilterCompileError::UnsupportedSql(
            "unterminated string literal in filter".to_string(),
        ));
    }

    clauses.push(&sql[clause_start..]);
    Ok(clauses)
}

fn find_unquoted_token(input: &str, token: &str) -> Result<Option<usize>, FilterCompileError> {
    let mut chars = input.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, ch)) = chars.next() {
        if ch == '\'' {
            in_string = !in_string;
            continue;
        }
        if ch == '\\' && in_string {
            let _ = chars.next();
            continue;
        }
        if !in_string && input[index..].starts_with(token) {
            return Ok(Some(index));
        }
    }

    if in_string {
        return Err(FilterCompileError::UnsupportedSql(
            "unterminated string literal in filter".to_string(),
        ));
    }
    Ok(None)
}

fn reject_extra_operator(clause: &str, literal: &str) -> Result<(), FilterCompileError> {
    for token in ["!=", ">=", "<=", "=", ">", "<"] {
        if find_unquoted_token(literal, token)?.is_some() {
            return Err(FilterCompileError::UnsupportedSql(format!(
                "unsupported filter predicate {clause:?}: expected exactly one comparison operator"
            )));
        }
    }
    Ok(())
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_quoted_string(literal: &str) -> Result<Option<String>, FilterCompileError> {
    if !literal.starts_with('\'') {
        return Ok(None);
    }
    if !literal.ends_with('\'') || literal.len() < 2 {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "unterminated string literal {literal:?}"
        )));
    }

    let inner = &literal[1..literal.len() - 1];
    let mut value = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            value.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(FilterCompileError::UnsupportedSql(format!(
                "unterminated escape in string literal {literal:?}"
            )));
        };
        value.push(escaped);
    }
    Ok(Some(value))
}

#[cfg(not(feature = "arrow"))]
fn sql_requires_datafusion(sql: &str) -> bool {
    sql_requires_arrow_datafusion(sql)
}

fn sql_requires_arrow_datafusion(sql: &str) -> bool {
    contains_unquoted_word(sql, "OR")
        || contains_unquoted_word(sql, "IN")
        || contains_unquoted_word(sql, "IS")
        || contains_unquoted_word(sql, "LIKE")
        || contains_unquoted_char(sql, '(')
        || contains_unquoted_char(sql, ')')
        || contains_unquoted_char(sql, '[')
        || contains_unquoted_char(sql, ']')
        || contains_unquoted_field_path_separator(sql)
}

fn contains_unquoted_field_path_separator(input: &str) -> bool {
    let mut chars = input.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, ch)) = chars.next() {
        if ch == '\'' {
            in_string = !in_string;
            continue;
        }
        if ch == '\\' && in_string {
            let _ = chars.next();
            continue;
        }
        if in_string || ch != '.' {
            continue;
        }
        if !is_decimal_literal_point(input, index) {
            return true;
        }
    }

    false
}

fn is_decimal_literal_point(input: &str, index: usize) -> bool {
    let previous = input[..index].chars().next_back();
    let next = input[index + '.'.len_utf8()..].chars().next();

    previous.is_some_and(|ch| ch.is_ascii_digit()) && next.is_some_and(|ch| ch.is_ascii_digit())
}

fn contains_unquoted_word(input: &str, word: &str) -> bool {
    let mut chars = input.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, ch)) = chars.next() {
        if ch == '\'' {
            in_string = !in_string;
            continue;
        }
        if ch == '\\' && in_string {
            let _ = chars.next();
            continue;
        }
        if in_string || !starts_with_keyword_at(input, index, word) {
            continue;
        }
        return true;
    }
    false
}

fn starts_with_keyword_at(input: &str, index: usize, keyword: &str) -> bool {
    let Some(candidate) = input.get(index..index + keyword.len()) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(keyword) {
        return false;
    }

    let before = input[..index].chars().next_back();
    let after = input[index + keyword.len()..].chars().next();
    !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
}

fn contains_unquoted_char(input: &str, target: char) -> bool {
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            in_string = !in_string;
            continue;
        }
        if ch == '\\' && in_string {
            let _ = chars.next();
            continue;
        }
        if !in_string && ch == target {
            return true;
        }
    }
    false
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(feature = "arrow")]
#[allow(
    dead_code,
    reason = "called when Arrow subscription payload routing is enabled"
)]
fn datafusion_predicate_for_schema(
    predicates: &[Predicate],
    schema: &arrow::datatypes::Schema,
) -> Result<std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>, FilterCompileError> {
    use datafusion::physical_expr::expressions::{binary, lit};

    let Some((first, remaining)) = predicates.split_first() else {
        return Ok(lit(true));
    };

    remaining.iter().try_fold(
        first.to_datafusion_expr(schema)?,
        |combined_predicate, predicate| {
            binary(
                combined_predicate,
                datafusion::logical_expr::Operator::And,
                predicate.to_datafusion_expr(schema)?,
                schema,
            )
            .map_err(datafusion_error)
        },
    )
}

#[cfg(feature = "arrow")]
impl Predicate {
    #[allow(
        dead_code,
        reason = "called when Arrow subscription payload routing is enabled"
    )]
    fn to_datafusion_expr(
        &self,
        schema: &arrow::datatypes::Schema,
    ) -> Result<std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>, FilterCompileError>
    {
        use datafusion::physical_expr::expressions::{binary, col, is_not_null, is_null};

        let field_name = self.field.as_top_level_name();
        let column = col(field_name, schema).map_err(datafusion_error)?;
        if self.expected == FilterValue::Null {
            return match self.operator {
                ComparisonOperator::Equal => is_null(column).map_err(datafusion_error),
                ComparisonOperator::NotEqual => is_not_null(column).map_err(datafusion_error),
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual => Err(FilterCompileError::UnsupportedSql(
                    "ordered comparisons against null are not supported".to_string(),
                )),
            };
        }

        let field = schema
            .field_with_name(field_name)
            .map_err(datafusion_error)?;
        binary(
            column,
            self.operator.to_datafusion_operator(),
            self.expected.to_datafusion_literal(field.data_type())?,
            schema,
        )
        .map_err(datafusion_error)
    }
}

#[cfg(feature = "arrow")]
impl ComparisonOperator {
    #[allow(
        dead_code,
        reason = "called when Arrow subscription payload routing is enabled"
    )]
    const fn to_datafusion_operator(self) -> datafusion::logical_expr::Operator {
        match self {
            Self::Equal => datafusion::logical_expr::Operator::Eq,
            Self::NotEqual => datafusion::logical_expr::Operator::NotEq,
            Self::GreaterThan => datafusion::logical_expr::Operator::Gt,
            Self::GreaterThanOrEqual => datafusion::logical_expr::Operator::GtEq,
            Self::LessThan => datafusion::logical_expr::Operator::Lt,
            Self::LessThanOrEqual => datafusion::logical_expr::Operator::LtEq,
        }
    }
}

#[cfg(feature = "arrow")]
impl FilterValue {
    #[allow(
        dead_code,
        reason = "called when Arrow subscription payload routing is enabled"
    )]
    fn to_datafusion_literal(
        &self,
        data_type: &arrow::datatypes::DataType,
    ) -> Result<std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>, FilterCompileError>
    {
        use datafusion::physical_expr::expressions::lit;

        match self {
            Self::String(value) => Ok(lit(value.clone())),
            Self::Number(value) => numeric_datafusion_literal(*value, data_type),
            Self::Bool(value) => Ok(lit(*value)),
            Self::Null => Err(FilterCompileError::UnsupportedSql(
                "null literals must be lowered through IS NULL predicates".to_string(),
            )),
        }
    }
}

#[cfg(feature = "arrow")]
#[allow(
    dead_code,
    reason = "called when Arrow subscription payload routing is enabled"
)]
fn numeric_datafusion_literal(
    value: f64,
    data_type: &arrow::datatypes::DataType,
) -> Result<std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>, FilterCompileError> {
    use arrow::datatypes::DataType;
    use datafusion::physical_expr::expressions::lit;

    match data_type {
        DataType::Int32 => finite_integer_i32(value).map(lit),
        DataType::Int64 => finite_integer_i64(value).map(lit),
        DataType::Dictionary(_, value_type) => numeric_datafusion_literal(value, value_type),
        _ => Ok(lit(value)),
    }
}

#[cfg(feature = "arrow")]
#[allow(
    dead_code,
    reason = "called when Arrow subscription payload routing is enabled"
)]
fn finite_integer_i32(value: f64) -> Result<i32, FilterCompileError> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "numeric literal {value} is not an integer"
        )));
    }
    if !(f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&value) {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "numeric literal {value} is outside Int32 range"
        )));
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "range and integer-ness are checked before casting to the Arrow field type"
    )]
    Ok(value as i32)
}

#[cfg(feature = "arrow")]
#[allow(
    dead_code,
    reason = "called when Arrow subscription payload routing is enabled"
)]
fn finite_integer_i64(value: f64) -> Result<i64, FilterCompileError> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "numeric literal {value} is not an integer"
        )));
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "bounds are used only to validate a user-provided f64 literal before narrowing"
    )]
    let bounds = (i64::MIN as f64)..=(i64::MAX as f64);
    if !bounds.contains(&value) {
        return Err(FilterCompileError::UnsupportedSql(format!(
            "numeric literal {value} is outside Int64 range"
        )));
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "range and integer-ness are checked before casting to the Arrow field type"
    )]
    Ok(value as i64)
}

#[cfg(feature = "arrow")]
#[allow(
    dead_code,
    reason = "called when Arrow subscription payload routing is enabled"
)]
fn datafusion_error(error: impl std::fmt::Display) -> FilterCompileError {
    FilterCompileError::DataFusion(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[cfg(feature = "arrow")]
    fn arrow_parity_batch() -> arrow::array::RecordBatch {
        use std::{collections::HashMap, sync::Arc};

        use arrow::{
            array::{
                BooleanArray, DictionaryArray, Float64Array, Int32Array, StringDictionaryBuilder,
            },
            datatypes::{DataType, Field, Int32Type, Schema},
        };

        let mut enum_field = Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            true,
        );
        enum_field.set_metadata(HashMap::from([
            ("crabka.enum".to_string(), "true".to_string()),
            (
                "crabka.enum.symbols".to_string(),
                r#"{"1":"NETWORK_NODE"}"#.to_string(),
            ),
        ]));
        let schema = Arc::new(Schema::new(vec![
            enum_field,
            Field::new(
                "profile_type",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("priority", DataType::Float64, true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        let statuses = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1)]),
            Arc::new(Int32Array::from(vec![1, 7])),
        )
        .expect("status dictionary batch builds");
        let mut profile_types = StringDictionaryBuilder::<Int32Type>::new();
        profile_types.append_value("cpu");
        profile_types.append_value("disk");

        arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(statuses),
                Arc::new(profile_types.finish()),
                Arc::new(Float64Array::from(vec![Some(9.0), Some(4.0)])),
                Arc::new(BooleanArray::from(vec![Some(false), None])),
            ],
        )
        .expect("parity batch builds")
    }

    fn filter_matches(filter: &str, json: &'static [u8]) -> bool {
        CompiledFilter::compile(filter)
            .expect("filter compiles")
            .matches_structured_json(Some(&Bytes::from_static(json)))
    }

    #[test]
    fn datafusion_heuristic_distinguishes_decimals_from_nested_fields() {
        let cases = [
            ("decimal equality", "load = 1.5", false),
            ("decimal comparison", "load > 0.75", false),
            (
                "quoted dots",
                "domain = 'api.example.com' AND path = 'payload.user.id'",
                false,
            ),
            ("nested field", "payload.user.id = 7", true),
            ("spaced nested field", "payload . user = 'alice'", true),
        ];

        for (name, sql, requires_datafusion) in cases {
            assert_eq!(
                sql_requires_arrow_datafusion(sql),
                requires_datafusion,
                "case {name} must classify SQL correctly"
            );
        }
    }

    #[test]
    fn reserved_row_marker_name_is_rejected_but_string_literal_is_allowed() {
        let marker_error = CompiledFilter::compile("__crabka_row_marker = true")
            .expect_err("internal row marker field is reserved");

        assert!(
            matches!(marker_error, FilterCompileError::UnsupportedSql(message) if message.contains("reserved"))
        );
        assert!(filter_matches(
            "status = '__crabka_row_marker'",
            br#"{"status":"__crabka_row_marker"}"#,
        ));
    }

    #[test]
    #[cfg(not(feature = "arrow"))]
    fn default_build_reserves_nested_field_paths_for_datafusion() {
        let error = CompiledFilter::compile("payload.user.id = 7")
            .expect_err("nested field paths require Arrow/DataFusion");

        assert!(matches!(
            error,
            FilterCompileError::DataFusionUnavailable(_)
        ));
    }

    #[test]
    fn empty_filter_matches_raw_and_structured_records() {
        let filter = CompiledFilter::compile("").expect("empty filter compiles");

        assert!(filter.matches_structured_json(None));
        assert!(filter.matches_structured_json(Some(&Bytes::from_static(br"{}"))));
    }

    #[test]
    fn table_driven_filters_match_expected_structured_records() {
        let cases = [
            (
                "string equality",
                "entity_type = 'NETWORK_NODE'",
                br#"{"entity_type":"NETWORK_NODE"}"# as &'static [u8],
            ),
            (
                "string inequality",
                "entity_type != 'TOPIC'",
                br#"{"entity_type":"NETWORK_NODE"}"#,
            ),
            ("integer equality", "node_id = 7", br#"{"node_id":"7"}"#),
            ("float equality", "load = 1.5", br#"{"load":1.5}"#),
            ("boolean equality", "ready = true", br#"{"ready":true}"#),
            ("null equality", "deleted = null", br#"{"deleted":null}"#),
            (
                "and conjunction",
                "status = 'PAID' AND price >= 150",
                br#"{"status":"PAID","price":150}"#,
            ),
            (
                "lowercase and conjunction",
                "status = 'PAID' and price >= 150",
                br#"{"status":"PAID","price":150}"#,
            ),
            (
                "mixed-case and conjunction",
                "status = 'PAID' AnD price >= 150",
                br#"{"status":"PAID","price":150}"#,
            ),
            ("numeric greater than", "load > 0.75", br#"{"load":0.9}"#),
            (
                "numeric less than or equal",
                "price <= 100",
                br#"{"price":"100"}"#,
            ),
            (
                "enum-like string value",
                "status = 'NETWORK_NODE'",
                br#"{"status":"NETWORK_NODE"}"#,
            ),
        ];

        for (name, filter, json) in cases {
            assert!(filter_matches(filter, json), "case {name} should match");
        }
    }

    #[test]
    fn table_driven_filters_reject_non_matching_records() {
        let cases = [
            (
                "string equality mismatch",
                "entity_type = 'NETWORK_NODE'",
                br#"{"entity_type":"TOPIC"}"# as &'static [u8],
            ),
            (
                "string inequality mismatch",
                "status != 'PAID'",
                br#"{"status":"PAID"}"#,
            ),
            (
                "numeric greater mismatch",
                "price > 100",
                br#"{"price":100}"#,
            ),
            ("numeric less mismatch", "price < 100", br#"{"price":101}"#),
            ("missing field", "status = 'ACTIVE'", br"{}"),
            ("bool mismatch", "ready = true", br#"{"ready":false}"#),
            ("null mismatch", "deleted = null", br#"{"deleted":"no"}"#),
            (
                "and mismatch",
                "status = 'PAID' AND price < 150",
                br#"{"status":"PAID","price":150}"#,
            ),
        ];

        for (name, filter, json) in cases {
            assert!(
                !filter_matches(filter, json),
                "case {name} should not match"
            );
        }
    }

    #[test]
    fn structured_filter_rejects_raw_records() {
        let filter = CompiledFilter::compile("entity_type = 'NETWORK_NODE'")
            .expect("structured filter compiles");

        assert!(!filter.matches_structured_json(None));
    }

    #[test]
    fn typed_filter_matches_enum_and_dictionary_like_values() {
        let cases = [
            (
                "enum symbol name",
                "status = 'PAID'",
                br#"{"status":{"$type":"enum","name":"PAID","number":2}}"# as &'static [u8],
            ),
            (
                "unknown enum sentinel",
                "status = 'UNKNOWN_7'",
                br#"{"status":{"$type":"enum","name":"UNKNOWN_7","number":7}}"#,
            ),
            (
                "unknown enum sentinel synthesized from number",
                "status = 'UNKNOWN_8'",
                br#"{"status":{"$type":"enum","number":8}}"#,
            ),
            (
                "dictionary string value",
                "profile_type = 'cpu'",
                br#"{"profile_type":{"$type":"dictionary","key":1,"value":"cpu"}}"#,
            ),
            (
                "dictionary numeric comparison",
                "priority >= 4",
                br#"{"priority":{"$type":"dictionary","key":9,"value":5}}"#,
            ),
            (
                "dictionary unknown enum sentinel value",
                "status = 'UNKNOWN_12'",
                br#"{"status":{"$type":"dictionary","key":12,"value":"UNKNOWN_12"}}"#,
            ),
        ];

        for (name, filter, json) in cases {
            assert!(filter_matches(filter, json), "case {name} should match");
        }
    }

    #[test]
    fn typed_row_batch_seam_evaluates_arrow_like_enum_dictionary_values() {
        let filter =
            CompiledFilter::compile("status = 'UNKNOWN_7' AND priority >= 3 AND deleted != null")
                .expect("typed row filter compiles");
        let batch = TypedRowBatch::from_rows(vec![
            BTreeMap::from([
                (
                    "status".to_string(),
                    FieldValue::String("NETWORK_NODE".to_string()),
                ),
                ("priority".to_string(), FieldValue::Number(9.0)),
                ("deleted".to_string(), FieldValue::Null),
            ]),
            BTreeMap::from([
                (
                    "status".to_string(),
                    FieldValue::String("UNKNOWN_7".to_string()),
                ),
                ("priority".to_string(), FieldValue::Number(4.0)),
                ("deleted".to_string(), FieldValue::Bool(false)),
            ]),
            BTreeMap::from([
                (
                    "status".to_string(),
                    FieldValue::String("UNKNOWN_7".to_string()),
                ),
                ("priority".to_string(), FieldValue::Number(2.0)),
                ("deleted".to_string(), FieldValue::Bool(false)),
            ]),
        ]);

        assert!(
            !filter.matches_row(&batch, 0),
            "known enum row does not match"
        );
        assert!(
            filter.matches_row(&batch, 1),
            "UNKNOWN_7 dictionary row matches"
        );
        assert!(
            !filter.matches_row(&batch, 2),
            "numeric comparison still gates enum match"
        );
        assert!(!filter.matches_row(&batch, 3), "out-of-bounds row is false");
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn arrow_bridge_json_shape_and_typed_rows_have_filter_parity() {
        use crabka_client_streams::columnar::serde::arrow::arrow_batch_to_filter_json_rows;

        let filter = CompiledFilter::compile(
            "status = 'UNKNOWN_7' AND profile_type = 'disk' AND priority >= 4 AND deleted = null",
        )
        .expect("filter compiles");
        let arrow_batch = arrow_parity_batch();
        let arrow_bridge_rows = arrow_batch_to_filter_json_rows(&arrow_batch)
            .expect("production Arrow bridge converts parity rows");
        let typed_batch = TypedRowBatch::from_rows(vec![
            BTreeMap::from([
                (
                    "status".to_string(),
                    FieldValue::String("NETWORK_NODE".to_string()),
                ),
                (
                    "profile_type".to_string(),
                    FieldValue::String("cpu".to_string()),
                ),
                ("priority".to_string(), FieldValue::Number(9.0)),
                ("deleted".to_string(), FieldValue::Bool(false)),
            ]),
            BTreeMap::from([
                (
                    "status".to_string(),
                    FieldValue::String("UNKNOWN_7".to_string()),
                ),
                (
                    "profile_type".to_string(),
                    FieldValue::String("disk".to_string()),
                ),
                ("priority".to_string(), FieldValue::Number(4.0)),
                ("deleted".to_string(), FieldValue::Null),
            ]),
        ]);

        for (row, arrow_bridge_row) in arrow_bridge_rows.iter().enumerate() {
            let arrow_json = Bytes::from(arrow_bridge_row.to_string());

            assert_eq!(
                filter.matches_structured_json(Some(&arrow_json)),
                filter.matches_row(&typed_batch, row),
                "Arrow bridge row {row} must evaluate like the typed row seam"
            );
        }
        assert!(!filter.matches_row(&typed_batch, 0));
        assert!(filter.matches_row(&typed_batch, 1));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_evaluates_sql_predicates_over_record_batches() {
        use std::sync::Arc;

        use arrow::{
            array::{BooleanArray, Float64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ready", DataType::Boolean, true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["PAID", "PENDING", "PAID"])),
                Arc::new(Float64Array::from(vec![200.0, 50.0, 75.0])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(BooleanArray::from(vec![None, Some(false), None])),
            ],
        )
        .expect("record batch builds");
        let filter = CompiledFilter::compile(
            "status = 'PAID' AND price >= 100 AND ready = true AND deleted IS NULL",
        )
        .expect("filter compiles");

        assert!(filter.matches_arrow_batch(&batch, 0));
        assert!(!filter.matches_arrow_batch(&batch, 1));
        assert!(!filter.matches_arrow_batch(&batch, 2));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_supports_in_like_bool_and_null_sql() {
        use std::sync::Arc;

        use arrow::{
            array::{BooleanArray, Float64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, true),
                Field::new("price", DataType::Float64, false),
                Field::new("ready", DataType::Boolean, true),
                Field::new("deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("PAID"),
                    Some("PENDING"),
                    Some("SHIPPED"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![125.0, 50.0, 200.0, 300.0])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
                Arc::new(BooleanArray::from(vec![None, None, None, None])),
            ],
        )
        .expect("record batch builds");
        let filter = CompiledFilter::compile(
            "status IN ('PAID', 'SHIPPED') AND status LIKE 'P%' AND price > 100 AND ready AND deleted IS NULL",
        )
        .expect("filter compiles");

        let mask = filter
            .evaluate_arrow_batch(&batch)
            .expect("DataFusion evaluates filter");

        assert_eq!(mask, BooleanArray::from(vec![true, false, false, false]));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_compares_dictionary_strings_by_value() {
        use std::sync::Arc;

        use arrow::{
            array::{RecordBatch, StringDictionaryBuilder},
            datatypes::{DataType, Field, Int32Type, Schema},
        };

        let mut statuses = StringDictionaryBuilder::<Int32Type>::new();
        statuses.append_value("PAID");
        statuses.append_value("PENDING");
        statuses.append_value("SHIPPED");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            )])),
            vec![Arc::new(statuses.finish())],
        )
        .expect("dictionary batch builds");
        let filter =
            CompiledFilter::compile("status IN ('PAID', 'SHIPPED')").expect("filter compiles");

        let mask = filter
            .evaluate_arrow_batch(&batch)
            .expect("DataFusion evaluates dictionary filter");

        assert_eq!(
            mask,
            arrow::array::BooleanArray::from(vec![true, false, true])
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_compares_numeric_enum_dictionaries_by_symbol_name() {
        let batch = arrow_parity_batch();
        let filter = CompiledFilter::compile("status = 'NETWORK_NODE'").expect("filter compiles");

        let mask = filter
            .evaluate_arrow_batch(&batch)
            .expect("DataFusion evaluates enum symbol filter");

        assert_eq!(mask, arrow::array::BooleanArray::from(vec![true, false]));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_quotes_nested_and_repeated_row_bridge_field_paths() {
        use crabka_client_streams::columnar::serde::arrow::json_rows_to_arrow_filter_batch;

        let batch = json_rows_to_arrow_filter_batch(&[
            serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 125}],
                "status": "PAID"
            }),
            serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 25}],
                "status": "PAID"
            }),
        ])
        .expect("row bridge builds Arrow batch");
        let filter = CompiledFilter::compile(
            "customer.status = 'ACTIVE' AND items[0].price > 100 AND status = 'PAID'",
        )
        .expect("filter compiles");

        let mask = filter
            .evaluate_arrow_batch_for_schema_id(Some(17), &batch)
            .expect("nested row-bridge filter evaluates");

        assert_eq!(mask, arrow::array::BooleanArray::from(vec![true, false]));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn row_bridge_zero_field_batches_do_not_expose_internal_marker_to_filters() {
        use crabka_client_streams::columnar::serde::arrow::json_rows_to_arrow_filter_batch;

        let batch =
            json_rows_to_arrow_filter_batch(&[serde_json::json!({}), serde_json::json!({})])
                .expect("zero-field row bridge batch builds");
        let empty_filter = CompiledFilter::compile("").expect("empty filter compiles");

        let all_rows = empty_filter
            .evaluate_arrow_batch_for_schema_id(Some(23), &batch)
            .expect("empty filter preserves zero-field row count");
        let missing_field_error = CompiledFilter::compile("status = 'PAID'")
            .expect("missing-field filter compiles")
            .evaluate_arrow_batch_for_schema_id(Some(23), &batch)
            .expect_err("missing user fields fail loudly");
        let marker_error = CompiledFilter::compile("__crabka_row_marker = true")
            .expect_err("internal row marker name is reserved");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(all_rows, arrow::array::BooleanArray::from(vec![true, true]));
        assert!(matches!(
            missing_field_error,
            FilterCompileError::DataFusion(_)
        ));
        assert!(
            matches!(marker_error, FilterCompileError::UnsupportedSql(message) if message.contains("reserved"))
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_recompiles_same_sql_for_each_schema_id() {
        use crabka_client_streams::columnar::serde::arrow::json_rows_to_arrow_filter_batch;

        let filter =
            CompiledFilter::compile("status = 'PAID' AND total > 100").expect("filter compiles");
        let schema_v1 = json_rows_to_arrow_filter_batch(&[serde_json::json!({
            "status": "PAID",
            "total": 125
        })])
        .expect("v1 batch builds");
        let schema_v2 = json_rows_to_arrow_filter_batch(&[serde_json::json!({
            "status": "PENDING",
            "total": 125.5,
            "region": "eu"
        })])
        .expect("v2 batch builds");

        let v1_mask = filter
            .evaluate_arrow_batch_for_schema_id(Some(1), &schema_v1)
            .expect("v1 schema compiles");
        let v2_mask = filter
            .evaluate_arrow_batch_for_schema_id(Some(2), &schema_v2)
            .expect("v2 schema recompiles");

        assert_eq!(v1_mask, arrow::array::BooleanArray::from(vec![true]));
        assert_eq!(v2_mask, arrow::array::BooleanArray::from(vec![false]));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn gateway_subscription_filter_compares_arrow_ipc_numeric_enums_by_symbol_name() {
        use arrow::ipc::writer::StreamWriter;

        let batch = arrow_parity_batch();
        let mut encoded_batch = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded_batch, &batch.schema())
                .expect("Arrow IPC writer builds");
            writer.write(&batch).expect("Arrow IPC batch writes");
            writer.finish().expect("Arrow IPC stream finishes");
        }
        let filter = CompiledFilter::compile("status = 'NETWORK_NODE'").expect("filter compiles");

        let decision = filter
            .evaluate_decoded_record(None, &Bytes::from(encoded_batch))
            .expect("Arrow IPC enum symbol filter evaluates");

        assert_eq!(
            decision,
            DecodedRecordFilterDecision::ArrowIpcBatch {
                row_count: 2,
                matching_rows: 1,
            }
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_rejects_numeric_enum_dictionaries_without_symbols() {
        use std::{collections::HashMap, sync::Arc};

        use arrow::{
            array::{DictionaryArray, Int32Array, RecordBatch},
            datatypes::{DataType, Field, Int32Type, Schema},
        };

        let mut enum_field = Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            false,
        );
        enum_field.set_metadata(HashMap::from([(
            "crabka.enum".to_string(),
            "true".to_string(),
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![enum_field])),
            vec![Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0)]),
                    Arc::new(Int32Array::from(vec![1])),
                )
                .expect("enum dictionary builds"),
            )],
        )
        .expect("record batch builds");
        let filter = CompiledFilter::compile("status = 'NETWORK_NODE'").expect("filter compiles");

        let error = filter
            .evaluate_arrow_batch(&batch)
            .expect_err("numeric enum dictionaries require symbol metadata");

        assert!(
            matches!(error, FilterCompileError::DataFusion(message) if message.contains("no crabka.enum.symbols"))
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_rejects_invalid_enum_symbol_metadata() {
        use std::{collections::HashMap, sync::Arc};

        use arrow::{
            array::{DictionaryArray, Int32Array, RecordBatch},
            datatypes::{DataType, Field, Int32Type, Schema},
        };

        let mut enum_field = Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            false,
        );
        enum_field.set_metadata(HashMap::from([
            ("crabka.enum".to_string(), "true".to_string()),
            ("crabka.enum.symbols".to_string(), "not-json".to_string()),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![enum_field])),
            vec![Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0)]),
                    Arc::new(Int32Array::from(vec![1])),
                )
                .expect("enum dictionary builds"),
            )],
        )
        .expect("record batch builds");
        let filter = CompiledFilter::compile("status = 'NETWORK_NODE'").expect("filter compiles");

        let error = filter
            .evaluate_arrow_batch(&batch)
            .expect_err("invalid enum symbol metadata is rejected");

        assert!(
            matches!(error, FilterCompileError::DataFusion(message) if message.contains("invalid crabka.enum.symbols"))
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn gateway_subscription_filter_matches_arrow_ipc_records_without_json_view() {
        use std::sync::Arc;

        use arrow::{
            array::{BooleanArray, Float64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
            ipc::writer::StreamWriter,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ready", DataType::Boolean, true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["PENDING", "PAID"])),
                Arc::new(Float64Array::from(vec![50.0, 125.0])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
                Arc::new(BooleanArray::from(vec![Some(false), None])),
            ],
        )
        .expect("record batch builds");
        let mut encoded_batch = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded_batch, &batch.schema())
                .expect("Arrow IPC writer builds");
            writer.write(&batch).expect("Arrow IPC batch writes");
            writer.finish().expect("Arrow IPC stream finishes");
        }
        let filter = CompiledFilter::compile(
            "status = 'PAID' AND price > 100 AND ready = true AND deleted IS NULL",
        )
        .expect("filter compiles");

        assert!(filter.matches_decoded_record(None, &Bytes::from(encoded_batch)));
        assert!(!filter.matches_decoded_record(None, &Bytes::from_static(b"not arrow ipc")));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn complex_sql_does_not_fall_back_to_legacy_json_filtering() {
        let filter = CompiledFilter::compile("status IN ('PAID', 'SHIPPED')")
            .expect("complex SQL compiles only for the DataFusion path");

        let decision = filter
            .evaluate_decoded_record(
                Some(&Bytes::from_static(br#"{"status":"PAID"}"#)),
                &Bytes::from_static(b"not arrow ipc"),
            )
            .expect("non-Arrow records remain a compatibility path");

        assert_eq!(decision, DecodedRecordFilterDecision::Drop);
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn arrow_ipc_record_filter_reports_batch_row_semantics_without_splitting_bytes() {
        use std::sync::Arc;

        use arrow::{
            array::{Float64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
            ipc::writer::StreamWriter,
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["PENDING", "PAID", "PAID"])),
                Arc::new(Float64Array::from(vec![50.0, 75.0, 125.0])),
            ],
        )
        .expect("record batch builds");
        let mut encoded_batch = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded_batch, &batch.schema())
                .expect("Arrow IPC writer builds");
            writer.write(&batch).expect("Arrow IPC batch writes");
            writer.finish().expect("Arrow IPC stream finishes");
        }
        let filter =
            CompiledFilter::compile("status = 'PAID' AND price > 100").expect("filter compiles");

        let decision = filter
            .evaluate_decoded_record(None, &Bytes::from(encoded_batch))
            .expect("Arrow IPC filter evaluates");

        assert_eq!(
            decision,
            DecodedRecordFilterDecision::ArrowIpcBatch {
                row_count: 3,
                matching_rows: 1,
            }
        );
        assert!(decision.should_deliver());
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn datafusion_arrow_filter_reports_type_errors() {
        use std::sync::Arc;

        use arrow::{
            array::{BooleanArray, RecordBatch},
            datatypes::{DataType, Field, Schema},
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ready",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .expect("record batch builds");
        let filter = CompiledFilter::compile("ready > 0").expect("filter compiles");

        let error = filter
            .try_matches_arrow_batch(&batch, 0)
            .expect_err("boolean/numeric comparison fails through DataFusion");

        assert!(matches!(error, FilterCompileError::DataFusion(_)));
        assert!(!filter.matches_arrow_batch(&batch, 0));
    }

    #[test]
    fn typed_row_boundary_rejects_nested_and_repeated_values() {
        let filter = CompiledFilter::compile("status = 'ACTIVE'").expect("filter compiles");

        for json in [
            br#"{"status":["ACTIVE"]}"# as &'static [u8],
            br#"{"status":{"value":"ACTIVE"}}"#,
            br#"{"status":{"$type":"dictionary","value":["ACTIVE"]}}"#,
            br#"{"status":{"$type":"enum","name":{"nested":"ACTIVE"},"number":1}}"#,
        ] {
            assert!(!filter.matches_structured_json(Some(&Bytes::from_static(json))));
        }
    }

    #[test]
    fn nested_field_paths_are_reserved_for_datafusion() {
        #[cfg(not(feature = "arrow"))]
        let error = CompiledFilter::compile("customer.status = 'ACTIVE'")
            .expect_err("nested field paths require DataFusion");
        #[cfg(not(feature = "arrow"))]
        assert!(matches!(
            error,
            FilterCompileError::DataFusionUnavailable(_)
        ));
        #[cfg(feature = "arrow")]
        assert!(CompiledFilter::compile("customer.status = 'ACTIVE'").is_ok());
    }

    #[test]
    fn typed_filter_rejects_non_matching_dictionary_like_values() {
        let filter = CompiledFilter::compile("status = 'PAID'").expect("filter compiles");

        assert!(!filter.matches_structured_json(Some(&Bytes::from_static(
            br#"{"status":{"$type":"dictionary","key":3,"value":"SHIPPED"}}"#,
        ))));
        assert!(!filter.matches_structured_json(Some(&Bytes::from_static(
            br#"{"status":{"$type":"dictionary","key":3}}"#,
        ))));
    }

    #[test]
    fn unsupported_sql_and_missing_datafusion_are_explicit_errors() {
        let malformed = CompiledFilter::compile("price === 100").expect_err("malformed SQL fails");
        assert!(matches!(malformed, FilterCompileError::UnsupportedSql(_)));

        #[cfg(not(feature = "arrow"))]
        let needs_datafusion = CompiledFilter::compile("status IN ('PAID')")
            .expect_err("unsupported SQL fails without Arrow/DataFusion");
        #[cfg(not(feature = "arrow"))]
        assert!(matches!(
            needs_datafusion,
            FilterCompileError::DataFusionUnavailable(_)
        ));
        #[cfg(feature = "arrow")]
        assert!(CompiledFilter::compile("status IN ('PAID')").is_ok());
    }

    #[test]
    fn unsupported_keywords_are_ascii_case_insensitive() {
        #[cfg(not(feature = "arrow"))]
        for filter in [
            "status = 'PAID' or price = 100",
            "status = 'PAID' oR price = 100",
            "status in ('PAID')",
            "status In ('PAID')",
            "status like 'PA%'",
            "status LiKe 'PA%'",
        ] {
            let error = CompiledFilter::compile(filter).expect_err("unsupported SQL keyword fails");

            #[cfg(not(feature = "arrow"))]
            assert!(
                matches!(error, FilterCompileError::DataFusionUnavailable(_)),
                "filter {filter:?} should require DataFusion, got {error:?}"
            );
        }
    }

    #[test]
    fn table_driven_invalid_filters_fail_loudly_at_compile_time() {
        let filters = [
            "status ==== 'PAID'",
            "ready > true",
            "deleted <= null",
            "price = 100 AND",
            "AND price = 100",
            "price != 100 != 200",
            "price =",
        ];

        for filter in filters {
            assert!(
                CompiledFilter::compile(filter).is_err(),
                "filter {filter:?} should fail to compile"
            );
        }

        #[cfg(not(feature = "arrow"))]
        for filter in [
            "status IN ('PAID')",
            "items[0].price = 100",
            "status OR price = 100",
            "status = 'PAID' OR price = 100",
        ] {
            assert!(
                CompiledFilter::compile(filter).is_err(),
                "filter {filter:?} should require Arrow/DataFusion"
            );
        }
    }

    #[test]
    fn quoted_literals_can_contain_and_or_operator_tokens() {
        assert!(filter_matches(
            "label = 'A AND B = C'",
            br#"{"label":"A AND B = C"}"#
        ));
    }

    #[test]
    fn quoted_literals_can_contain_case_insensitive_sql_keywords() {
        assert!(filter_matches(
            "label = 'and Or IN like' and status = 'Paid'",
            br#"{"label":"and Or IN like","status":"Paid"}"#
        ));
    }
}
