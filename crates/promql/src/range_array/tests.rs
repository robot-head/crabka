use std::sync::Arc;

use arrow::{
    array::{ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array, StructArray},
    datatypes::{Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use assert2::{assert, check};
use crabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms,
};

use super::RangeArray;

#[test]
fn windows_slice_the_backing_array() {
    let mut builder = Float64Builder::new();
    for value in [10.0, 11.0, 12.0, 13.0, 14.0] {
        builder.append_value(value);
    }
    let values = Arc::new(builder.finish()) as ArrayRef;

    let range_array = RangeArray::from_ranges(values, [(0_u32, 3_u32), (2, 3)]).unwrap();
    assert!(range_array.len() == 2);

    for (index, want) in [(0, vec![10.0, 11.0, 12.0]), (1, vec![12.0, 13.0, 14.0])] {
        let window = range_array.get(index).unwrap();
        let window = window.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            (0..window.len())
                .map(|i| window.value(i))
                .collect::<Vec<_>>()
                == want,
            "case {index}"
        );
    }
}

#[test]
fn out_of_bounds_window_is_rejected() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
    assert!(RangeArray::from_ranges(values, [(1_u32, 5_u32)]).is_err());
}

#[test]
fn basic_accessors_report_empty_state_and_exact_ranges() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
    let empty = RangeArray::from_ranges(values.clone(), []).unwrap();
    let range_array = RangeArray::from_ranges(values, [(1_u32, 0_u32), (0, 2), (2, 1)]).unwrap();
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
    assert_eq!(empty.ranges(), &[][..]);
    assert_eq!(range_array.len(), 3);
    assert!(!range_array.is_empty());
    assert_eq!(range_array.ranges(), &[(1, 0), (0, 2), (2, 1)][..]);
}

#[test]
fn dict_array_round_trips_through_recordbatch_column() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
    let dict = range_array.clone().into_dict_array().unwrap();
    let back = RangeArray::try_from_dict_array(&dict).unwrap();
    assert!(back.len() == range_array.len());

    for (index, want) in [(0, vec![1.0, 2.0]), (1, vec![2.0, 3.0, 4.0])] {
        let window = back.get(index).unwrap();
        let window = window.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            (0..window.len())
                .map(|i| window.value(i))
                .collect::<Vec<_>>()
                == want,
            "case {index}"
        );
    }
}

#[test]
fn paired_builder_shares_window_offsets_across_value_and_timestamp() {
    let values = Float64Array::from(vec![10.0, 11.0, 12.0, 13.0, 14.0]);
    let timestamps = Int64Array::from(vec![0_i64, 15, 30, 45, 60]);
    let (value_ranges, ts_ranges) =
        RangeArray::from_paired_ranges(values, timestamps, [(0_u32, 3_u32), (2, 3)]).unwrap();

    assert_eq!(value_ranges.ranges(), ts_ranges.ranges());
    assert_eq!(value_ranges.len(), 2);

    for (index, want_values, want_timestamps) in [
        (0, [10.0, 11.0, 12.0], [0_i64, 15, 30]),
        (1, [12.0, 13.0, 14.0], [30, 45, 60]),
    ] {
        assert!(
            value_ranges.value_slice(index).unwrap() == want_values,
            "case {index}"
        );
        assert!(
            ts_ranges.timestamp_slice(index).unwrap() == want_timestamps,
            "case {index}"
        );
    }
}

#[test]
fn paired_builder_rejects_length_mismatch() {
    let values = Float64Array::from(vec![1.0, 2.0, 3.0]);
    let timestamps = Int64Array::from(vec![0_i64, 1]);
    assert!(RangeArray::from_paired_ranges(values, timestamps, [(0_u32, 2_u32)]).is_err());
}

#[test]
fn value_slice_reads_typed_float_cells() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();

    for (index, want) in [
        (0, Some(&[1.0, 2.0][..])),
        (1, Some(&[2.0, 3.0, 4.0][..])),
        (2, None),
    ] {
        assert!(range_array.value_slice(index) == want, "case {index}");
    }
    // A timestamp accessor on a float backing yields None (wrong type).
    assert!(range_array.timestamp_slice(0).is_none());
}

#[test]
fn timestamp_slice_reads_typed_int_cells() {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let range_array = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (2, 2)]).unwrap();

    for (index, want) in [
        (0, Some(&[0_i64, 15][..])),
        (1, Some(&[30, 45][..])),
        (2, None),
    ] {
        assert!(range_array.timestamp_slice(index) == want, "case {index}");
    }
}

#[test]
fn cell_len_and_empty_cells() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 0), (1, 1)]).unwrap();

    for (index, want) in [(0, Some(2)), (1, Some(0)), (2, Some(1)), (3, None)] {
        assert!(range_array.cell_len(index) == want, "case {index}");
    }

    // An empty cell yields an empty slice, not None.
    assert!(range_array.value_slice(1).unwrap().is_empty());
}

#[test]
fn typed_accessor_matches_get_over_a_pre_sliced_backing_array() {
    // Build a RangeArray over an already-sliced backing array; the typed
    // zero-copy accessor must agree with the `get()` re-slice path.
    let full = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])) as ArrayRef;
    let sliced = full.slice(2, 3); // logical [2.0, 3.0, 4.0]
    let range_array = RangeArray::from_ranges(sliced, [(0_u32, 2_u32), (1, 2)]).unwrap();

    let via_get = range_array.get(1).unwrap();
    let via_get = via_get.as_any().downcast_ref::<Float64Array>().unwrap();
    let via_get = (0..via_get.len())
        .map(|index| via_get.value(index))
        .collect::<Vec<_>>();
    assert!(via_get == vec![3.0, 4.0]);
    assert!(range_array.value_slice(1).unwrap() == via_get.as_slice());
}

#[test]
fn iter_float_cells_visits_every_window() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();

    let collected = range_array
        .iter_value_slices()
        .unwrap()
        .map(<[f64]>::to_vec)
        .collect::<Vec<_>>();
    assert!(collected == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]);
}

#[test]
fn iter_int_cells_visits_every_window() {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let range_array = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (2, 2)]).unwrap();

    let collected = range_array
        .iter_timestamp_slices()
        .unwrap()
        .map(<[i64]>::to_vec)
        .collect::<Vec<_>>();
    assert!(collected == vec![vec![0, 15], vec![30, 45]]);
}

#[test]
fn histogram_cell_reads_native_histogram_windows() {
    let rows = native_histogram_rows();
    let batch = encode_native_histograms(&rows).unwrap();
    let histograms = Arc::new(StructArray::from(
        batch
            .schema()
            .fields()
            .iter()
            .cloned()
            .zip(batch.columns().iter().cloned())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let range_array = RangeArray::from_ranges(histograms, [(0_u32, 2_u32), (2, 1)]).unwrap();

    let first_cell = range_array.histogram_cell(0).unwrap();
    check!(first_cell.len() == 2);
    check!(!first_cell.is_empty());
    check!(first_cell.schema_slice() == [2, -53]);
    check!(first_cell.reset_hint_slice() == [ResetHint::No.as_i8(), ResetHint::Gauge.as_i8()]);
    check!(first_cell.zero_threshold_slice() == [1e-128, 0.25]);
    check!(first_cell.zero_count_slice() == [3.0, 0.5]);
    check!(first_cell.count_slice() == [10.0, 4.0]);
    check!(first_cell.sum_slice() == [42.5, 7.5]);
    check!(first_cell.is_float(0) == Some(false));
    check!(first_cell.is_float(1) == Some(true));
    check!(first_cell.is_float(2).is_none());

    let first_positive_spans = first_cell.positive_spans(0).unwrap();
    check!(first_positive_spans.offsets() == [0]);
    check!(first_positive_spans.lengths() == [2]);
    check!(first_cell.positive_counts(0) == Some(&[4.0, 6.0][..]));
    let second_negative_spans = first_cell.negative_spans(1).unwrap();
    check!(second_negative_spans.offsets() == [-1]);
    check!(second_negative_spans.lengths() == [1]);
    check!(first_cell.negative_counts(1) == Some(&[0.75][..]));
    check!(first_cell.custom_values(0).is_none());
    check!(first_cell.custom_values(1) == Some(&[0.5, 1.0, 2.0][..]));
    check!(first_cell.start_timestamp_ms(0).is_none());
    check!(first_cell.start_timestamp_ms(1) == Some(123));

    let second_cell = range_array.histogram_cell(1).unwrap();
    check!(second_cell.len() == 1);
    check!(second_cell.positive_spans(0).unwrap().is_empty());
    check!(second_cell.positive_counts(0) == Some(&[][..]));
    check!(range_array.histogram_cell(2).is_none());
    check!(range_array.value_slice(0).is_none());

    let decoded = decode_native_histograms(&batch).unwrap();
    assert!(decoded == rows);
}

#[test]
fn histogram_cell_matches_get_over_a_pre_sliced_backing_array() {
    let rows = native_histogram_rows();
    let batch = encode_native_histograms(&rows).unwrap();
    let histograms = Arc::new(StructArray::from(
        batch
            .schema()
            .fields()
            .iter()
            .cloned()
            .zip(batch.columns().iter().cloned())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let sliced = histograms.slice(1, 2);
    let range_array = RangeArray::from_ranges(sliced, [(0_u32, 1_u32), (1, 1)]).unwrap();

    let via_get = range_array.get(0).unwrap();
    let via_get = via_get.as_any().downcast_ref::<StructArray>().unwrap();
    let via_get_batch = RecordBatch::from(via_get.clone());
    let decoded = decode_native_histograms(&via_get_batch).unwrap();
    let cell = range_array.histogram_cell(0).unwrap();

    assert!(decoded == vec![rows[1].clone()]);
    check!(cell.schema_slice() == [rows[1].2.schema]);
    check!(cell.count_slice() == [rows[1].2.count]);
    check!(cell.positive_counts(0) == Some(rows[1].2.positive_counts.as_slice()));
    check!(cell.negative_counts(0) == Some(rows[1].2.negative_counts.as_slice()));
}

#[tokio::test]
async fn survives_datafusion_projection_as_a_column() {
    use datafusion::{
        datasource::memory::MemorySourceConfig, physical_plan::collect, prelude::SessionContext,
    };

    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let value_ra = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
    let ts_ra = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (1, 3)]).unwrap();

    let value_col: ArrayRef = Arc::new(value_ra.clone().into_dict_array().unwrap());
    let ts_col: ArrayRef = Arc::new(ts_ra.clone().into_dict_array().unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", value_col.data_type().clone(), false),
        Field::new("timestamps", ts_col.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![value_col, ts_col]).unwrap();

    // Run the batch through a trivial DataFusion projection (identity column scan).
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
    let ctx = SessionContext::new();
    let out = collect(source, ctx.task_ctx()).await.unwrap();
    let merged = &out[0];

    let value_dict = merged
        .column_by_name("values")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .unwrap();
    let ts_dict = merged
        .column_by_name("timestamps")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .unwrap();

    let value_back = RangeArray::try_from_dict_array(value_dict).unwrap();
    let ts_back = RangeArray::try_from_dict_array(ts_dict).unwrap();

    assert!(
        value_back
            .iter_value_slices()
            .unwrap()
            .map(<[f64]>::to_vec)
            .collect::<Vec<_>>()
            == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]
    );
    assert!(
        ts_back
            .iter_timestamp_slices()
            .unwrap()
            .map(<[i64]>::to_vec)
            .collect::<Vec<_>>()
            == vec![vec![0, 15], vec![15, 30, 45]]
    );
}

fn native_histogram_rows() -> Vec<(u64, i64, NativeHistogram)> {
    vec![
        (
            7_u64,
            99_i64,
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
                positive_counts: vec![4.0, 6.0],
                negative_spans: Vec::new(),
                negative_counts: Vec::new(),
                custom_values: None,
                start_timestamp_ms: None,
            },
        ),
        (
            8_u64,
            109_i64,
            NativeHistogram {
                schema: -53,
                is_float: true,
                reset_hint: ResetHint::Gauge,
                zero_threshold: 0.25,
                zero_count: 0.5,
                count: 4.0,
                sum: 7.5,
                positive_spans: vec![BucketSpan {
                    offset: 2,
                    length: 2,
                }],
                positive_counts: vec![1.25, 2.0],
                negative_spans: vec![BucketSpan {
                    offset: -1,
                    length: 1,
                }],
                negative_counts: vec![0.75],
                custom_values: Some(vec![0.5, 1.0, 2.0]),
                start_timestamp_ms: Some(123),
            },
        ),
        (
            9_u64,
            119_i64,
            NativeHistogram {
                schema: 1,
                is_float: false,
                reset_hint: ResetHint::Unknown,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: 0.0,
                sum: 0.0,
                positive_spans: Vec::new(),
                positive_counts: Vec::new(),
                negative_spans: Vec::new(),
                negative_counts: Vec::new(),
                custom_values: Some(Vec::new()),
                start_timestamp_ms: Some(456),
            },
        ),
    ]
}
