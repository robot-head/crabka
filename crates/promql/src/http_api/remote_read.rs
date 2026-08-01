use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{Float64Type, Int64Type, UInt64Type},
};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use crabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint};
use crabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms,
    wire::{pb, snappy_block_decode},
};
use num_traits::ToPrimitive;
use prost::Message;

use super::{
    ApiError, PrometheusApiState, enforce_sample_count, enforce_selected_series_limit,
    tenant_from_headers, validate_timestamp_range,
};
use crate::{
    MetricStore, PromqlError,
    store::{ExemplarRecord, ScanResult},
};

pub(super) async fn remote_read<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_remote_read_headers(&headers) {
        return error.into_response();
    }

    let decompressed = match snappy_block_decode(&body, state.remote_read_max_body) {
        Ok(decompressed) => decompressed,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let request = match pb::v1::ReadRequest::decode(decompressed.as_slice()) {
        Ok(request) => request,
        Err(error) => {
            return ApiError::bad_data(format!("protobuf decode failed: {error}")).into_response();
        }
    };
    if let Err(error) = require_remote_read_samples_response(&request) {
        return error.into_response();
    }

    let response = match remote_read_response(state.as_ref(), &tenant, request).await {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    let encoded = response.encode_to_vec();
    let compressed = match snap::raw::Encoder::new().compress_vec(&encoded) {
        Ok(compressed) => compressed,
        Err(error) => {
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_type: "execution",
                message: format!("snappy encode failed: {error}"),
            }
            .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-protobuf"),
            (header::CONTENT_ENCODING, "snappy"),
        ],
        compressed,
    )
        .into_response()
}

async fn remote_read_response<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    request: pb::v1::ReadRequest,
) -> Result<pb::v1::ReadResponse, ApiError> {
    let mut results = Vec::with_capacity(request.queries.len());
    for query in request.queries {
        validate_timestamp_range(query.start_timestamp_ms, query.end_timestamp_ms)?;
        let matchers = remote_read_matchers(&query.matchers)?;
        let labels = state
            .store
            .series(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;
        enforce_selected_series_limit(state, tenant, labels.len())?;
        let mut labels_by_fp = labels
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<SeriesFingerprint, Labels>>();
        let scan = state
            .store
            .scan(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;

        let mut by_fp = BTreeMap::<SeriesFingerprint, pb::v1::TimeSeries>::new();
        let mut returned_samples = 0_u64;

        if let Some(float_table) = scan.float_table.clone() {
            append_remote_read_float_samples(
                state,
                tenant,
                &scan,
                &float_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        if let Some(histogram_table) = scan.histogram_table.clone() {
            append_remote_read_histogram_samples(
                state,
                tenant,
                &scan,
                &histogram_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        append_remote_read_exemplars(
            state.store.as_ref(),
            tenant,
            &matchers,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
            &mut labels_by_fp,
            &mut by_fp,
        )
        .await?;

        results.push(pb::v1::QueryResult {
            timeseries: by_fp.into_values().collect(),
        });
    }
    Ok(pb::v1::ReadResponse { results })
}

async fn append_remote_read_float_samples<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    scan: &ScanResult,
    table: &str,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    returned_samples: &mut u64,
) -> Result<(), ApiError> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    let batches = dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;

    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fps.value(row))?;
            series.samples.push(pb::v1::Sample {
                timestamp: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(())
}

async fn append_remote_read_histogram_samples<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    scan: &ScanResult,
    table: &str,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    returned_samples: &mut u64,
) -> Result<(), ApiError> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    let batches = dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;

    for batch in batches {
        for (fp, timestamp, hist) in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fp)?;
            series
                .histograms
                .push(remote_read_histogram(timestamp, &hist));
        }
    }
    Ok(())
}

async fn append_remote_read_exemplars<S: MetricStore>(
    store: &S,
    tenant: &str,
    matchers: &[LabelMatcher],
    start_ms: i64,
    end_ms: i64,
    labels_by_fp: &mut BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
) -> Result<(), ApiError> {
    for exemplar in store
        .exemplars(tenant, matchers, start_ms, end_ms)
        .await
        .map_err(ApiError::from)?
    {
        let fp = exemplar.series_labels.fingerprint();
        labels_by_fp
            .entry(fp)
            .or_insert_with(|| exemplar.series_labels.clone());
        let series = remote_read_series(by_fp, labels_by_fp, fp)?;
        series.exemplars.push(remote_read_exemplar(&exemplar));
    }
    Ok(())
}

fn remote_read_matchers(matchers: &[pb::v1::LabelMatcher]) -> Result<Vec<LabelMatcher>, ApiError> {
    matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.r#type {
                0 => MatchOp::Eq,
                1 => MatchOp::Neq,
                2 => MatchOp::Re,
                3 => MatchOp::Nre,
                other => {
                    return Err(ApiError::bad_data(format!(
                        "unknown remote_read matcher type {other}"
                    )));
                }
            };
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect()
}

fn remote_read_labels(labels: &Labels) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|(name, value)| pb::v1::Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

fn remote_read_exemplar(exemplar: &ExemplarRecord) -> pb::v1::Exemplar {
    pb::v1::Exemplar {
        labels: remote_read_labels(&exemplar.labels),
        value: exemplar.value,
        timestamp: exemplar.ts_ms,
    }
}

fn remote_read_series<'a>(
    by_fp: &'a mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    fp: SeriesFingerprint,
) -> Result<&'a mut pb::v1::TimeSeries, ApiError> {
    let labels = labels_by_fp
        .get(&fp)
        .ok_or_else(|| ApiError::bad_data("remote_read series labels not found"))?;
    Ok(by_fp.entry(fp).or_insert_with(|| pb::v1::TimeSeries {
        labels: remote_read_labels(labels),
        samples: Vec::new(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }))
}

fn remote_read_histogram(timestamp: i64, hist: &NativeHistogram) -> pb::v1::Histogram {
    pb::v1::Histogram {
        count: Some(remote_read_histogram_count(hist)),
        sum: hist.sum,
        schema: i32::from(hist.schema),
        zero_threshold: hist.zero_threshold,
        zero_count: Some(remote_read_histogram_zero_count(hist)),
        negative_spans: remote_read_bucket_spans(&hist.negative_spans),
        negative_deltas: remote_read_histogram_deltas(hist.is_float, &hist.negative_counts),
        negative_counts: if hist.is_float {
            hist.negative_counts.clone()
        } else {
            Vec::new()
        },
        positive_spans: remote_read_bucket_spans(&hist.positive_spans),
        positive_deltas: remote_read_histogram_deltas(hist.is_float, &hist.positive_counts),
        positive_counts: if hist.is_float {
            hist.positive_counts.clone()
        } else {
            Vec::new()
        },
        reset_hint: remote_read_reset_hint(hist.reset_hint),
        timestamp,
        custom_values: hist.custom_values.clone().unwrap_or_default(),
    }
}

fn remote_read_histogram_count(hist: &NativeHistogram) -> pb::v1::histogram::Count {
    if hist.is_float {
        pb::v1::histogram::Count::CountFloat(hist.count)
    } else {
        pb::v1::histogram::Count::CountInt(hist.count.to_u64().unwrap_or(u64::MAX))
    }
}

fn remote_read_histogram_zero_count(hist: &NativeHistogram) -> pb::v1::histogram::ZeroCount {
    if hist.is_float {
        pb::v1::histogram::ZeroCount::ZeroCountFloat(hist.zero_count)
    } else {
        pb::v1::histogram::ZeroCount::ZeroCountInt(hist.zero_count.to_u64().unwrap_or(u64::MAX))
    }
}

fn remote_read_bucket_spans(spans: &[BucketSpan]) -> Vec<pb::v1::BucketSpan> {
    spans
        .iter()
        .map(|span| pb::v1::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn remote_read_histogram_deltas(is_float: bool, counts: &[f64]) -> Vec<i64> {
    if is_float {
        return Vec::new();
    }
    let mut previous = 0.0;
    counts
        .iter()
        .map(|count| {
            let delta = *count - previous;
            previous = *count;
            delta.to_i64().unwrap_or_else(|| {
                if delta.is_sign_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            })
        })
        .collect()
}

fn remote_read_reset_hint(reset_hint: ResetHint) -> i32 {
    match reset_hint {
        ResetHint::Unknown => pb::v1::histogram::ResetHint::Unknown as i32,
        ResetHint::Yes => pb::v1::histogram::ResetHint::Yes as i32,
        ResetHint::No => pb::v1::histogram::ResetHint::No as i32,
        ResetHint::Gauge => pb::v1::histogram::ResetHint::Gauge as i32,
    }
}

fn require_remote_read_samples_response(request: &pb::v1::ReadRequest) -> Result<(), ApiError> {
    if request.accepted_response_types.is_empty()
        || request
            .accepted_response_types
            .contains(&(pb::v1::ResponseType::Samples as i32))
    {
        return Ok(());
    }
    Err(ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        error_type: "execution",
        message: "remote_read only supports samples responses".into(),
    })
}

fn require_remote_read_headers(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if !content_type.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "remote_read requires application/x-protobuf".into(),
        });
    }

    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim();
    if !header_list_includes(content_encoding, "snappy") {
        return Err(ApiError::bad_data(
            "remote_read requires snappy content encoding",
        ));
    }
    Ok(())
}

fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}
