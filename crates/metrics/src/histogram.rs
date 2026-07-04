//! In-memory native-histogram representation and Arrow codec.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int8Array,
        Int8Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray, ListBuilder,
        StructArray, StructBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Fields},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

use crate::schema::{
    COL_FINGERPRINT, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS,
    COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA,
    COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, COL_TIMESTAMP,
    native_histogram_schema,
};

/// A run of populated buckets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}

/// Counter-reset semantics carried with each histogram sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

impl ResetHint {
    #[must_use]
    pub fn as_i8(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Yes => 1,
            Self::No => 2,
            Self::Gauge => 3,
        }
    }

    #[must_use]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Yes,
            2 => Self::No,
            3 => Self::Gauge,
            _ => Self::Unknown,
        }
    }
}

/// A native histogram sample with absolute bucket counts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NativeHistogram {
    pub schema: i8,
    pub is_float: bool,
    pub reset_hint: ResetHint,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<BucketSpan>,
    pub positive_counts: Vec<f64>,
    pub negative_spans: Vec<BucketSpan>,
    pub negative_counts: Vec<f64>,
    pub custom_values: Option<Vec<f64>>,
    pub start_timestamp_ms: Option<i64>,
}

impl NativeHistogram {
    /// NHCB (native histogram with custom buckets) sentinel schema.
    #[must_use]
    pub fn is_nhcb(&self) -> bool {
        self.schema == -53
    }
}

/// Errors from the native-histogram Arrow codec.
#[derive(Debug, thiserror::Error)]
pub enum HistogramCodecError {
    #[error("span/count mismatch: spans claim {spans} buckets, got {counts} counts")]
    SpanCountMismatch { spans: usize, counts: usize },

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

fn span_bucket_total(spans: &[BucketSpan]) -> usize {
    spans.iter().map(|span| span.length as usize).sum()
}

fn validate_span_count_consistency(
    spans: &[BucketSpan],
    counts: &[f64],
) -> Result<(), HistogramCodecError> {
    let span_total = span_bucket_total(spans);
    if span_total == counts.len() {
        Ok(())
    } else {
        Err(HistogramCodecError::SpanCountMismatch {
            spans: span_total,
            counts: counts.len(),
        })
    }
}

fn span_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ])
}

fn span_list_field() -> Field {
    Field::new("item", DataType::Struct(span_struct_fields()), false)
}

fn f64_list_field() -> Field {
    Field::new("item", DataType::Float64, false)
}

fn new_span_list_builder() -> ListBuilder<StructBuilder> {
    let struct_builder = StructBuilder::new(
        span_struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(UInt32Builder::new()),
        ],
    );
    ListBuilder::new(struct_builder).with_field(span_list_field())
}

fn new_f64_list_builder() -> ListBuilder<Float64Builder> {
    ListBuilder::new(Float64Builder::new()).with_field(f64_list_field())
}

fn append_spans(builder: &mut ListBuilder<StructBuilder>, spans: &[BucketSpan]) {
    let struct_builder = builder.values();
    for span in spans {
        struct_builder
            .field_builder::<Int32Builder>(0)
            .expect("span offset builder")
            .append_value(span.offset);
        struct_builder
            .field_builder::<UInt32Builder>(1)
            .expect("span length builder")
            .append_value(span.length);
        struct_builder.append(true);
    }
    builder.append(true);
}

fn append_f64_list(builder: &mut ListBuilder<Float64Builder>, values: &[f64]) {
    for value in values {
        builder.values().append_value(*value);
    }
    builder.append(true);
}

/// Encode `(fingerprint, timestamp, NativeHistogram)` rows into a `RecordBatch`
/// matching [`native_histogram_schema`].
pub fn encode_native_histograms(
    rows: &[(u64, i64, NativeHistogram)],
) -> Result<RecordBatch, HistogramCodecError> {
    for (_, _, histogram) in rows {
        validate_span_count_consistency(&histogram.positive_spans, &histogram.positive_counts)?;
        validate_span_count_consistency(&histogram.negative_spans, &histogram.negative_counts)?;
    }

    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut schemas = Int8Builder::new();
    let mut is_floats = BooleanBuilder::new();
    let mut reset_hints = Int8Builder::new();
    let mut zero_thresholds = Float64Builder::new();
    let mut zero_counts = Float64Builder::new();
    let mut counts = Float64Builder::new();
    let mut sums = Float64Builder::new();
    let mut positive_spans = new_span_list_builder();
    let mut positive_counts = new_f64_list_builder();
    let mut negative_spans = new_span_list_builder();
    let mut negative_counts = new_f64_list_builder();
    let mut custom_values = new_f64_list_builder();
    let mut start_timestamps = Int64Builder::new();

    for (fingerprint, timestamp, histogram) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp);
        schemas.append_value(histogram.schema);
        is_floats.append_value(histogram.is_float);
        reset_hints.append_value(histogram.reset_hint.as_i8());
        zero_thresholds.append_value(histogram.zero_threshold);
        zero_counts.append_value(histogram.zero_count);
        counts.append_value(histogram.count);
        sums.append_value(histogram.sum);
        append_spans(&mut positive_spans, &histogram.positive_spans);
        append_f64_list(&mut positive_counts, &histogram.positive_counts);
        append_spans(&mut negative_spans, &histogram.negative_spans);
        append_f64_list(&mut negative_counts, &histogram.negative_counts);
        match &histogram.custom_values {
            Some(values) => append_f64_list(&mut custom_values, values),
            None => custom_values.append(false),
        }
        match histogram.start_timestamp_ms {
            Some(start_timestamp) => start_timestamps.append_value(start_timestamp),
            None => start_timestamps.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(schemas.finish()),
        Arc::new(is_floats.finish()),
        Arc::new(reset_hints.finish()),
        Arc::new(zero_thresholds.finish()),
        Arc::new(zero_counts.finish()),
        Arc::new(counts.finish()),
        Arc::new(sums.finish()),
        Arc::new(positive_spans.finish()),
        Arc::new(positive_counts.finish()),
        Arc::new(negative_spans.finish()),
        Arc::new(negative_counts.finish()),
        Arc::new(custom_values.finish()),
        Arc::new(start_timestamps.finish()),
    ];

    Ok(RecordBatch::try_new(native_histogram_schema(), columns)?)
}

fn schema_mismatch(column: &str) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!("column `{column}` missing or wrong type"))
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, HistogramCodecError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| schema_mismatch(name))
}

fn typed_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, HistogramCodecError> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| schema_mismatch(name))
}

fn null_required_column(column: &str, row: usize) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!(
        "column `{column}` contains null for required row {row}"
    ))
}

fn require_non_null(
    array: &dyn Array,
    row: usize,
    column: &str,
) -> Result<(), HistogramCodecError> {
    if array.is_null(row) {
        Err(null_required_column(column, row))
    } else {
        Ok(())
    }
}

fn read_spans(
    list: &ListArray,
    row: usize,
    column: &str,
) -> Result<Vec<BucketSpan>, HistogramCodecError> {
    require_non_null(list, row, column)?;
    let value = list.value(row);
    let struct_array = value
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| schema_mismatch(column))?;
    if struct_array.num_columns() < 2 {
        return Err(schema_mismatch(column));
    }
    let offsets = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| schema_mismatch(column))?;
    let lengths = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| schema_mismatch(column))?;

    (0..struct_array.len())
        .map(|index| {
            require_non_null(struct_array, index, column)?;
            require_non_null(offsets, index, column)?;
            require_non_null(lengths, index, column)?;
            Ok(BucketSpan {
                offset: offsets.value(index),
                length: lengths.value(index),
            })
        })
        .collect()
}

fn read_f64_list(
    list: &ListArray,
    row: usize,
    column: &str,
) -> Result<Vec<f64>, HistogramCodecError> {
    require_non_null(list, row, column)?;
    let value = list.value(row);
    let array = value
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| schema_mismatch(column))?;

    (0..array.len())
        .map(|index| {
            require_non_null(array, index, column)?;
            Ok(array.value(index))
        })
        .collect()
}

/// Decode a `RecordBatch` produced by [`encode_native_histograms`].
pub fn decode_native_histograms(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, NativeHistogram)>, HistogramCodecError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let schemas = typed_column::<Int8Array>(batch, COL_NH_SCHEMA)?;
    let is_floats = typed_column::<BooleanArray>(batch, COL_NH_IS_FLOAT)?;
    let reset_hints = typed_column::<Int8Array>(batch, COL_NH_RESET_HINT)?;
    let zero_thresholds = typed_column::<Float64Array>(batch, COL_NH_ZERO_THRESHOLD)?;
    let zero_counts = typed_column::<Float64Array>(batch, COL_NH_ZERO_COUNT)?;
    let counts = typed_column::<Float64Array>(batch, COL_NH_COUNT)?;
    let sums = typed_column::<Float64Array>(batch, COL_NH_SUM)?;
    let positive_spans = typed_column::<ListArray>(batch, COL_NH_POS_SPANS)?;
    let positive_counts = typed_column::<ListArray>(batch, COL_NH_POS_COUNTS)?;
    let negative_spans = typed_column::<ListArray>(batch, COL_NH_NEG_SPANS)?;
    let negative_counts = typed_column::<ListArray>(batch, COL_NH_NEG_COUNTS)?;
    let custom_values = typed_column::<ListArray>(batch, COL_NH_CUSTOM_VALUES)?;
    let start_timestamps = typed_column::<Int64Array>(batch, COL_NH_START_TS)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        require_non_null(fingerprints, row, COL_FINGERPRINT)?;
        require_non_null(timestamps, row, COL_TIMESTAMP)?;
        require_non_null(schemas, row, COL_NH_SCHEMA)?;
        require_non_null(is_floats, row, COL_NH_IS_FLOAT)?;
        require_non_null(reset_hints, row, COL_NH_RESET_HINT)?;
        require_non_null(zero_thresholds, row, COL_NH_ZERO_THRESHOLD)?;
        require_non_null(zero_counts, row, COL_NH_ZERO_COUNT)?;
        require_non_null(counts, row, COL_NH_COUNT)?;
        require_non_null(sums, row, COL_NH_SUM)?;

        let positive_spans = read_spans(positive_spans, row, COL_NH_POS_SPANS)?;
        let positive_counts = read_f64_list(positive_counts, row, COL_NH_POS_COUNTS)?;
        let negative_spans = read_spans(negative_spans, row, COL_NH_NEG_SPANS)?;
        let negative_counts = read_f64_list(negative_counts, row, COL_NH_NEG_COUNTS)?;

        validate_span_count_consistency(&positive_spans, &positive_counts)?;
        validate_span_count_consistency(&negative_spans, &negative_counts)?;

        rows.push((
            fingerprints.value(row),
            timestamps.value(row),
            NativeHistogram {
                schema: schemas.value(row),
                is_float: is_floats.value(row),
                reset_hint: ResetHint::from_i8(reset_hints.value(row)),
                zero_threshold: zero_thresholds.value(row),
                zero_count: zero_counts.value(row),
                count: counts.value(row),
                sum: sums.value(row),
                positive_spans,
                positive_counts,
                negative_spans,
                negative_counts,
                custom_values: if custom_values.is_null(row) {
                    None
                } else {
                    Some(read_f64_list(custom_values, row, COL_NH_CUSTOM_VALUES)?)
                },
                start_timestamp_ms: if start_timestamps.is_null(row) {
                    None
                } else {
                    Some(start_timestamps.value(row))
                },
            },
        ));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn reset_hint_round_trips_i8() {
        for h in [
            ResetHint::Unknown,
            ResetHint::Yes,
            ResetHint::No,
            ResetHint::Gauge,
        ] {
            assert!(ResetHint::from_i8(h.as_i8()) == h);
        }
    }

    #[test]
    fn nhcb_detected_by_schema() {
        let mut h = sample_histogram();
        assert!(!h.is_nhcb());
        h.schema = -53;
        assert!(h.is_nhcb());
    }

    #[test]
    fn encode_decode_round_trips() {
        let h1 = sample_histogram();
        let mut h2 = sample_histogram();
        h2.is_float = true;
        h2.negative_spans = vec![BucketSpan {
            offset: -1,
            length: 1,
        }];
        h2.negative_counts = vec![2.0];
        h2.custom_values = Some(vec![0.5, 1.0, 2.0]);
        h2.schema = -53;
        h2.start_timestamp_ms = Some(123);
        let mut h3 = sample_histogram();
        h3.custom_values = Some(vec![]);
        h3.schema = -53;

        let rows = vec![
            (10_u64, 1000_i64, h1.clone()),
            (20_u64, 2000_i64, h2.clone()),
            (30_u64, 3000_i64, h3.clone()),
        ];
        let batch = encode_native_histograms(&rows).unwrap();
        assert!(batch.num_rows() == 3);

        let back = decode_native_histograms(&batch).unwrap();
        assert!(back == rows);
        check!(back[0].2.custom_values == None);
        check!(back[1].2.custom_values == Some(vec![0.5, 1.0, 2.0]));
        check!(back[2].2.custom_values == Some(vec![]));
    }

    #[test]
    fn encode_validates_span_count_consistency() {
        let mut bad = sample_histogram();
        bad.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 5,
        }];
        bad.positive_counts = vec![1.0, 2.0];
        let err = encode_native_histograms(&[(1, 1, bad)]);
        assert!(err.is_err());
    }

    #[test]
    fn decode_validates_positive_span_count_consistency() {
        let batch = encoded_sample_batch();
        let mut counts = new_f64_list_builder();
        append_f64_list(&mut counts, &[4.0]);
        let batch = batch_with_column(&batch, COL_NH_POS_COUNTS, Arc::new(counts.finish()), false);

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SpanCountMismatch {
                spans: 2,
                counts: 1
            }
        ));
    }

    #[test]
    fn decode_rejects_null_required_scalar() {
        let batch = encoded_sample_batch();
        let batch = batch_with_column(
            &batch,
            COL_NH_SCHEMA,
            Arc::new(Int8Array::from(vec![None::<i8>])),
            true,
        );

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message)
                if message.contains(COL_NH_SCHEMA) && message.contains("null")
        ));
    }

    #[test]
    fn decode_rejects_null_required_list() {
        let batch = encoded_sample_batch();
        let mut spans = new_span_list_builder();
        spans.append(false);
        let batch = batch_with_column(&batch, COL_NH_POS_SPANS, Arc::new(spans.finish()), true);

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message)
                if message.contains(COL_NH_POS_SPANS) && message.contains("null")
        ));
    }

    #[test]
    fn decode_rejects_span_struct_with_missing_child() {
        let batch = encoded_sample_batch();
        let mut spans = ListBuilder::new(StructBuilder::from_fields(
            vec![Field::new("offset", DataType::Int32, false)],
            1,
        ));
        spans
            .values()
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(0);
        spans.values().append(true);
        spans.append(true);
        let spans = spans.finish();
        let index = batch.schema().index_of(COL_NH_POS_SPANS).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(spans.clone());
        let mut fields = batch.schema().fields().to_vec();
        fields[index] = Arc::new(Field::new(
            COL_NH_POS_SPANS,
            spans.data_type().clone(),
            false,
        ));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message) if message.contains(COL_NH_POS_SPANS)
        ));
    }

    #[test]
    fn decode_tolerates_extra_column() {
        let batch = encoded_sample_batch();
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::UInt64, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from(vec![123_u64])));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let decoded = decode_native_histograms(&batch).unwrap();

        assert!(decoded == sample_rows());
    }

    fn encoded_sample_batch() -> RecordBatch {
        encode_native_histograms(&sample_rows()).unwrap()
    }

    fn sample_rows() -> Vec<(u64, i64, NativeHistogram)> {
        vec![(7_u64, 99_i64, sample_histogram())]
    }

    fn batch_with_column(
        batch: &RecordBatch,
        name: &str,
        column: ArrayRef,
        make_field_nullable: bool,
    ) -> RecordBatch {
        let index = batch.schema().index_of(name).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = column;
        let mut fields = batch.schema().fields().to_vec();
        if make_field_nullable {
            fields[index] = Arc::new(fields[index].as_ref().clone().with_nullable(true));
        }
        let struct_columns = fields.iter().cloned().zip(columns).collect::<Vec<_>>();
        RecordBatch::from(StructArray::from(struct_columns))
    }

    fn sample_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 3.0,
            count: 10.0,
            sum: 42.5,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
}
