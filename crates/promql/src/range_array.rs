//! `RangeArray`: a list-like view where each cell is a sample window.
//!
//! # Native-histogram range vectors
//!
//! The windowing core ([`from_ranges`](RangeArray::from_ranges), [`get`](RangeArray::get),
//! [`into_dict_array`](RangeArray::into_dict_array) /
//! [`try_from_dict_array`](RangeArray::try_from_dict_array)) is type-agnostic: it
//! windows any backing [`ArrayRef`] by `(offset, len)` and round-trips it as a
//! dictionary-of-lists column. A native-histogram range vector therefore already
//! works *structurally* by backing the `RangeArray` with a `StructArray`
//! (count/sum/schema scalars plus bucket-bound and bucket-count lists) instead of
//! a `Float64Array`; `get(i)` returns the sliced `StructArray` for that window.
//!
//! Only the *typed* fast-path accessors below are scalar-specific
//! ([`value_slice`](RangeArray::value_slice) for `f64`,
//! [`timestamp_slice`](RangeArray::timestamp_slice) for `i64`). The rate-family
//! UDFs that consume histograms will want an equivalent zero-copy view per cell.
//!
//! TODO(histogram-rangearray): add a `histogram_cell(index) -> Option<HistogramView<'_>>`
//! typed accessor that downcasts the backing `StructArray` once and exposes each
//! window's count/sum/buckets without re-slicing. Deferred because it requires
//! pinning the native-histogram `StructArray` field layout (a cross-crate schema
//! decision owned by `crabka-metrics`), which is more than modest effort and not
//! on the slice-2 critical path. The generic `get()` path unblocks histogram
//! columns in the meantime.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Float64Array, Int64Array, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::concat;
use arrow::datatypes::{Field, Int64Type};
use arrow::error::ArrowError;

/// A view over `values` partitioned into `(offset, len)` windows.
#[derive(Clone, Debug)]
pub struct RangeArray {
    values: ArrayRef,
    ranges: Vec<(u32, u32)>,
}

impl RangeArray {
    /// Build from a backing array and windows; validates each window fits.
    pub fn from_ranges(
        values: ArrayRef,
        ranges: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, ArrowError> {
        let ranges = ranges.into_iter().collect::<Vec<_>>();
        let total = values.len();
        for &(offset, len) in &ranges {
            let end = offset as usize + len as usize;
            if end > total {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "range window [{offset}, {end}) overruns backing array of len {total}"
                )));
            }
        }
        Ok(Self { values, ranges })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[must_use]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[must_use]
    pub fn ranges(&self) -> &[(u32, u32)] {
        &self.ranges
    }

    /// The windowed slice for cell `index`, or `None` if out of bounds.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<ArrayRef> {
        let &(offset, len) = self.ranges.get(index)?;
        Some(self.values.slice(offset as usize, len as usize))
    }

    /// Build a paired value/timestamp `RangeArray` from one set of windows.
    ///
    /// `RangeManipulate` emits a value column and a timestamp column whose cells
    /// share identical window offsets/lengths. This constructs both from a single
    /// `(offset, len)` range set so the pairing can never drift. The `values` and
    /// `timestamps` backing arrays must have the same length (each sample has both
    /// a value and a timestamp); every window is validated against that length.
    ///
    /// Returns `(value_ranges, timestamp_ranges)`; both share the same `ranges()`.
    pub fn from_paired_ranges(
        values: Float64Array,
        timestamps: Int64Array,
        ranges: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<(Self, Self), ArrowError> {
        if values.len() != timestamps.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "paired value/timestamp backing arrays differ in length: {} != {}",
                values.len(),
                timestamps.len()
            )));
        }
        let ranges = ranges.into_iter().collect::<Vec<_>>();
        let value_ranges = Self::from_ranges(Arc::new(values) as ArrayRef, ranges.iter().copied())?;
        let timestamp_ranges = Self::from_ranges(Arc::new(timestamps) as ArrayRef, ranges)?;
        Ok((value_ranges, timestamp_ranges))
    }

    /// Number of samples in cell `index`, or `None` if out of bounds.
    #[must_use]
    pub fn cell_len(&self, index: usize) -> Option<usize> {
        self.ranges.get(index).map(|&(_, len)| len as usize)
    }

    /// The cell `index` as a contiguous `&[f64]`, or `None` if `index` is out of
    /// bounds or the backing array is not `Float64`. Empty cells yield `&[]`.
    #[must_use]
    pub fn value_slice(&self, index: usize) -> Option<&[f64]> {
        let &(offset, len) = self.ranges.get(index)?;
        let floats = self.values.as_any().downcast_ref::<Float64Array>()?;
        let start = offset as usize;
        Some(&floats.values()[start..start + len as usize])
    }

    /// The cell `index` as a contiguous `&[i64]`, or `None` if `index` is out of
    /// bounds or the backing array is not `Int64`. Empty cells yield `&[]`.
    #[must_use]
    pub fn timestamp_slice(&self, index: usize) -> Option<&[i64]> {
        let &(offset, len) = self.ranges.get(index)?;
        let ints = self.values.as_any().downcast_ref::<Int64Array>()?;
        let start = offset as usize;
        Some(&ints.values()[start..start + len as usize])
    }

    /// Iterate every cell as a `&[f64]`, or `None` if the backing array is not
    /// `Float64`. The iterator yields one slice per cell in order.
    #[must_use]
    pub fn iter_value_slices(&self) -> Option<impl Iterator<Item = &[f64]>> {
        let floats = self.values.as_any().downcast_ref::<Float64Array>()?;
        let backing = floats.values();
        Some(self.ranges.iter().map(move |&(offset, len)| {
            let start = offset as usize;
            &backing[start..start + len as usize]
        }))
    }

    /// Iterate every cell as a `&[i64]`, or `None` if the backing array is not
    /// `Int64`. The iterator yields one slice per cell in order.
    #[must_use]
    pub fn iter_timestamp_slices(&self) -> Option<impl Iterator<Item = &[i64]>> {
        let ints = self.values.as_any().downcast_ref::<Int64Array>()?;
        let backing = ints.values();
        Some(self.ranges.iter().map(move |&(offset, len)| {
            let start = offset as usize;
            &backing[start..start + len as usize]
        }))
    }

    /// Encode windows as a dictionary whose values are per-cell lists.
    ///
    /// Arrow 59 validates dictionary keys as dictionary indices, so we use keys
    /// `0..len` and store each window as one list value. This is safe to pass as
    /// a `RecordBatch` column and preserves the public `RangeArray` behavior.
    pub fn into_dict_array(self) -> Result<DictionaryArray<Int64Type>, ArrowError> {
        let mut offsets = Vec::with_capacity(self.ranges.len() + 1);
        offsets.push(0_i32);
        let mut slices = Vec::with_capacity(self.ranges.len());
        let mut total = 0_i32;
        for &(offset, len) in &self.ranges {
            let len_i32 = i32::try_from(len).map_err(|_| {
                ArrowError::InvalidArgumentError(format!("range length {len} exceeds i32::MAX"))
            })?;
            total = total.checked_add(len_i32).ok_or_else(|| {
                ArrowError::InvalidArgumentError("range offsets exceed i32::MAX".to_string())
            })?;
            offsets.push(total);
            slices.push(self.values.slice(offset as usize, len as usize));
        }

        // `concat` rejects an empty slice list (an empty input, e.g. a nested
        // subquery whose inner produced zero rows), so fall back to an empty
        // array of the element type.
        let values = if slices.is_empty() {
            arrow::array::new_empty_array(self.values.data_type())
        } else {
            concat(&slices.iter().map(AsRef::as_ref).collect::<Vec<_>>())?
        };
        let field = Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            values.is_nullable(),
        ));
        let list = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            None,
        )?;
        let keys = Int64Array::from_iter_values(
            0..i64::try_from(self.ranges.len()).map_err(|_| {
                ArrowError::InvalidArgumentError("range count exceeds i64::MAX".to_string())
            })?,
        );
        DictionaryArray::try_new(keys, Arc::new(list))
    }

    /// Decode a dictionary-of-lists column back into a `RangeArray`.
    pub fn try_from_dict_array(dict: &DictionaryArray<Int64Type>) -> Result<Self, ArrowError> {
        let lists = dict
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "RangeArray dictionary values must be ListArray".to_string(),
                )
            })?;

        let mut ranges = Vec::with_capacity(dict.len());
        let mut slices = Vec::with_capacity(dict.len());
        let mut next_offset = 0_u32;
        for row in 0..dict.len() {
            if dict.is_null(row) {
                ranges.push((next_offset, 0));
                continue;
            }
            let key = usize::try_from(dict.keys().value(row)).map_err(|_| {
                ArrowError::InvalidArgumentError(format!("negative dictionary key at row {row}"))
            })?;
            if key >= lists.len() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "dictionary key {key} at row {row} exceeds values len {}",
                    lists.len()
                )));
            }
            let list_value = lists.value(key);
            let len = u32::try_from(list_value.len()).map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "range length {} exceeds u32::MAX",
                    list_value.len()
                ))
            })?;
            ranges.push((next_offset, len));
            next_offset = next_offset.checked_add(len).ok_or_else(|| {
                ArrowError::InvalidArgumentError("range offsets exceed u32::MAX".to_string())
            })?;
            slices.push(list_value);
        }

        // `concat` rejects an empty slice list (a dictionary with zero rows, or
        // whose every row is null), so fall back to an empty array of the list's
        // element type.
        let values = if slices.is_empty() {
            arrow::array::new_empty_array(lists.values().data_type())
        } else {
            concat(&slices.iter().map(AsRef::as_ref).collect::<Vec<_>>())?
        };
        Self::from_ranges(values, ranges)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Float64Builder, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::{assert, check};

    use super::*;

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

        let range_array =
            RangeArray::from_ranges(values, [(1_u32, 0_u32), (0, 2), (2, 1)]).unwrap();
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
        let range_array =
            RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 0), (1, 1)]).unwrap();

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
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

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
}
