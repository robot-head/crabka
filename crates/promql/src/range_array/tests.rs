use std::sync::Arc;

use arrow::{
    array::{ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array},
    datatypes::{Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use assert2::{assert, check};

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
    check!(empty.len() == 0);
    check!(empty.is_empty());
    check!(empty.ranges().is_empty());

    let range_array = RangeArray::from_ranges(values, [(1_u32, 0_u32), (0, 2), (2, 1)]).unwrap();
    check!(range_array.len() == 3);
    check!(!range_array.is_empty());
    check!(range_array.ranges() == [(1, 0), (0, 2), (2, 1)]);
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

    assert!(value_ranges.ranges() == ts_ranges.ranges());
    assert!(value_ranges.len() == 2);

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
