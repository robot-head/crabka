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
//! The typed fast-path accessors below cover scalar cells
//! ([`value_slice`](RangeArray::value_slice) for `f64`,
//! [`timestamp_slice`](RangeArray::timestamp_slice) for `i64`) and native
//! histogram cells ([`histogram_cell`](RangeArray::histogram_cell)).

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int8Array, Int32Array,
        Int64Array, ListArray, StructArray, UInt32Array,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    compute::concat,
    datatypes::{Field, Int64Type},
    error::ArrowError,
};
use crabka_metrics::{
    COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS, COL_NH_NEG_SPANS,
    COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA, COL_NH_START_TS,
    COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD,
};

/// Zero-copy view of one native-histogram range cell.
#[derive(Clone, Copy, Debug)]
pub struct HistogramView<'a> {
    row_start: usize,
    row_len: usize,
    schemas: &'a [i8],
    is_floats: &'a BooleanArray,
    reset_hints: &'a [i8],
    zero_thresholds: &'a [f64],
    zero_counts: &'a [f64],
    counts: &'a [f64],
    sums: &'a [f64],
    positive_spans: &'a ListArray,
    positive_counts: &'a ListArray,
    negative_spans: &'a ListArray,
    negative_counts: &'a ListArray,
    custom_values: &'a ListArray,
    start_timestamps: &'a Int64Array,
}

impl<'a> HistogramView<'a> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_len == 0
    }

    #[must_use]
    pub fn schema_slice(&self) -> &'a [i8] {
        self.schemas
    }

    #[must_use]
    pub fn reset_hint_slice(&self) -> &'a [i8] {
        self.reset_hints
    }

    #[must_use]
    pub fn zero_threshold_slice(&self) -> &'a [f64] {
        self.zero_thresholds
    }

    #[must_use]
    pub fn zero_count_slice(&self) -> &'a [f64] {
        self.zero_counts
    }

    #[must_use]
    pub fn count_slice(&self) -> &'a [f64] {
        self.counts
    }

    #[must_use]
    pub fn sum_slice(&self) -> &'a [f64] {
        self.sums
    }

    #[must_use]
    pub fn is_float(&self, sample_index: usize) -> Option<bool> {
        let row = self.absolute_row(sample_index)?;
        (!self.is_floats.is_null(row)).then(|| self.is_floats.value(row))
    }

    #[must_use]
    pub fn positive_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.positive_spans, row)
    }

    #[must_use]
    pub fn positive_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.positive_counts, row)
    }

    #[must_use]
    pub fn negative_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.negative_spans, row)
    }

    #[must_use]
    pub fn negative_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.negative_counts, row)
    }

    #[must_use]
    pub fn custom_values(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.custom_values, row)
    }

    #[must_use]
    pub fn start_timestamp_ms(&self, sample_index: usize) -> Option<i64> {
        let row = self.absolute_row(sample_index)?;
        (!self.start_timestamps.is_null(row)).then(|| self.start_timestamps.value(row))
    }

    fn absolute_row(&self, sample_index: usize) -> Option<usize> {
        if sample_index >= self.row_len {
            return None;
        }
        Some(self.row_start + sample_index)
    }
}

/// Zero-copy view of a native-histogram span list.
#[derive(Clone, Copy, Debug)]
pub struct HistogramSpanView<'a> {
    offsets: &'a [i32],
    lengths: &'a [u32],
}

impl<'a> HistogramSpanView<'a> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    #[must_use]
    pub fn offsets(&self) -> &'a [i32] {
        self.offsets
    }

    #[must_use]
    pub fn lengths(&self) -> &'a [u32] {
        self.lengths
    }
}

struct HistogramColumns<'a> {
    schemas: &'a Int8Array,
    is_floats: &'a BooleanArray,
    reset_hints: &'a Int8Array,
    zero_thresholds: &'a Float64Array,
    zero_counts: &'a Float64Array,
    counts: &'a Float64Array,
    sums: &'a Float64Array,
    positive_spans: &'a ListArray,
    positive_counts: &'a ListArray,
    negative_spans: &'a ListArray,
    negative_counts: &'a ListArray,
    custom_values: &'a ListArray,
    start_timestamps: &'a Int64Array,
}

impl<'a> HistogramColumns<'a> {
    fn parse(values: &'a dyn Array) -> Option<Self> {
        let histograms = values.as_any().downcast_ref::<StructArray>()?;
        Some(Self {
            schemas: struct_column(histograms, COL_NH_SCHEMA)?,
            is_floats: struct_column(histograms, COL_NH_IS_FLOAT)?,
            reset_hints: struct_column(histograms, COL_NH_RESET_HINT)?,
            zero_thresholds: struct_column(histograms, COL_NH_ZERO_THRESHOLD)?,
            zero_counts: struct_column(histograms, COL_NH_ZERO_COUNT)?,
            counts: struct_column(histograms, COL_NH_COUNT)?,
            sums: struct_column(histograms, COL_NH_SUM)?,
            positive_spans: struct_column(histograms, COL_NH_POS_SPANS)?,
            positive_counts: struct_column(histograms, COL_NH_POS_COUNTS)?,
            negative_spans: struct_column(histograms, COL_NH_NEG_SPANS)?,
            negative_counts: struct_column(histograms, COL_NH_NEG_COUNTS)?,
            custom_values: struct_column(histograms, COL_NH_CUSTOM_VALUES)?,
            start_timestamps: struct_column(histograms, COL_NH_START_TS)?,
        })
    }

    fn cell(self, offset: u32, len: u32) -> HistogramView<'a> {
        let start = offset as usize;
        let end = start + len as usize;
        HistogramView {
            row_start: start,
            row_len: len as usize,
            schemas: &self.schemas.values()[start..end],
            is_floats: self.is_floats,
            reset_hints: &self.reset_hints.values()[start..end],
            zero_thresholds: &self.zero_thresholds.values()[start..end],
            zero_counts: &self.zero_counts.values()[start..end],
            counts: &self.counts.values()[start..end],
            sums: &self.sums.values()[start..end],
            positive_spans: self.positive_spans,
            positive_counts: self.positive_counts,
            negative_spans: self.negative_spans,
            negative_counts: self.negative_counts,
            custom_values: self.custom_values,
            start_timestamps: self.start_timestamps,
        }
    }
}

fn struct_column<'a, T: Array + 'static>(histograms: &'a StructArray, name: &str) -> Option<&'a T> {
    histograms
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<T>()
}

fn list_offsets(list: &ListArray, row: usize) -> Option<(usize, usize)> {
    if list.is_null(row) {
        return None;
    }
    let offsets = list.value_offsets();
    let start = usize::try_from(*offsets.get(row)?).ok()?;
    let end = usize::try_from(*offsets.get(row + 1)?).ok()?;
    Some((start, end))
}

fn f64_list_value(list: &ListArray, row: usize) -> Option<&[f64]> {
    let (start, end) = list_offsets(list, row)?;
    let values = list.values().as_any().downcast_ref::<Float64Array>()?;
    Some(&values.values()[start..end])
}

fn span_list_value(list: &ListArray, row: usize) -> Option<HistogramSpanView<'_>> {
    let (start, end) = list_offsets(list, row)?;
    let values = list.values().as_any().downcast_ref::<StructArray>()?;
    let offsets = values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()?
        .values();
    let lengths = values
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()?
        .values();
    Some(HistogramSpanView {
        offsets: &offsets[start..end],
        lengths: &lengths[start..end],
    })
}

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

    /// The native-histogram cell `index`, or `None` if `index` is out of bounds
    /// or the backing array does not match `crabka-metrics`' native histogram
    /// `StructArray` layout.
    #[must_use]
    pub fn histogram_cell(&self, index: usize) -> Option<HistogramView<'_>> {
        let &(offset, len) = self.ranges.get(index)?;
        let columns = HistogramColumns::parse(self.values.as_ref())?;
        Some(columns.cell(offset, len))
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
mod tests;
