//! Test in-memory profile store.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Builder, StringDictionaryBuilder, UInt64Builder},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
use datafusion::{catalog::MemTable, prelude::SessionContext};
use regex::Regex;

use crate::{
    error::ProfileError,
    samples::profile_samples_schema,
    store::{ProfileScan, ProfileStats, ProfileStore},
    symbol_db::SymbolDb,
};

#[derive(Clone, Debug)]
struct SampleRow {
    profile_type: String,
    fingerprint: u64,
    labels: Vec<(String, String)>,
    partition: u64,
    stacktrace_id: u32,
    value: i64,
    total_value: i64,
    span_id: Option<u64>,
    trace_id: Option<Vec<u8>>,
    timestamp_ms: i64,
}

/// In-memory `ProfileStore` used by engine tests.
#[derive(Clone, Debug, Default)]
pub struct InMemoryProfileStore {
    samples: HashMap<String, Vec<SampleRow>>,
    symbols: SymbolDb,
}

impl InMemoryProfileStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            samples: HashMap::new(),
            symbols: SymbolDb::new(),
        }
    }

    pub fn symbols_mut(&mut self) -> &mut SymbolDb {
        &mut self.symbols
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_sample(
        &mut self,
        tenant: &str,
        profile_type: &str,
        labels: Vec<(String, String)>,
        partition: u64,
        stacktrace_id: u32,
        value: i64,
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total(
            tenant,
            profile_type,
            labels,
            partition,
            stacktrace_id,
            value,
            value,
            timestamp_ms,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_sample_with_total(
        &mut self,
        tenant: &str,
        profile_type: &str,
        labels: Vec<(String, String)>,
        partition: u64,
        stacktrace_id: u32,
        value: i64,
        total_value: i64,
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total_and_associations(
            tenant,
            profile_type,
            labels,
            partition,
            stacktrace_id,
            value,
            total_value,
            timestamp_ms,
            None,
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_sample_with_total_and_span(
        &mut self,
        tenant: &str,
        profile_type: &str,
        labels: Vec<(String, String)>,
        partition: u64,
        stacktrace_id: u32,
        value: i64,
        total_value: i64,
        timestamp_ms: i64,
        span_id: u64,
    ) {
        self.push_sample_with_total_and_associations(
            tenant,
            profile_type,
            labels,
            partition,
            stacktrace_id,
            value,
            total_value,
            timestamp_ms,
            Some(span_id),
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_sample_with_total_and_associations(
        &mut self,
        tenant: &str,
        profile_type: &str,
        labels: Vec<(String, String)>,
        partition: u64,
        stacktrace_id: u32,
        value: i64,
        total_value: i64,
        timestamp_ms: i64,
        span_id: Option<u64>,
        trace_id: Option<Vec<u8>>,
    ) {
        let fingerprint = fingerprint_labels(&labels);
        self.samples
            .entry(tenant.to_string())
            .or_default()
            .push(SampleRow {
                profile_type: profile_type.to_string(),
                fingerprint,
                labels,
                partition,
                stacktrace_id,
                value,
                total_value,
                span_id,
                trace_id,
                timestamp_ms,
            });
    }
}

#[async_trait::async_trait]
impl ProfileStore for InMemoryProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let rows: Vec<&SampleRow> = self
            .samples
            .get(tenant)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter(|row| row.profile_type == profile_type)
            .filter(|row| row.timestamp_ms >= start_ms && row.timestamp_ms <= end_ms)
            .filter(|row| row_matches(row, &compiled))
            .collect();
        let batch = encode_rows(&rows)?;
        let table = MemTable::try_new(profile_samples_schema(), vec![vec![batch]])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = SessionContext::new();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(self.symbols.clone()),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let mut names = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            names.extend(row.labels.iter().map(|(name, _)| name.clone()));
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let mut values = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            for (label_name, value) in &row.labels {
                if label_name == name {
                    values.insert(value.clone());
                }
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let mut types = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            types.insert(row.profile_type.clone());
        }
        Ok(types.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let mut out = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            // An empty `label_names` means "return the full label set" (the
            // Pyroscope `/series` convention). Projecting onto an empty name
            // list yields an empty vec, which surfaces as a spurious `[{}]`
            // entry — mirror the cold-path fix in `crabka_blockstore`'s index.
            let mut projected: Vec<_> = if label_names.is_empty() {
                row.labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect()
            } else {
                label_names
                    .iter()
                    .filter_map(|want| {
                        row.labels
                            .iter()
                            .find(|(name, _)| name == want)
                            .map(|(name, value)| (name.clone(), value.clone()))
                    })
                    .collect()
            };
            // Pyroscope's `/series` emits each set's labels SORTED by name, in
            // both the projected and full-label-set forms (e.g. `__profile_type__`
            // before `service_name`). `row.labels` is in ingest insertion order and
            // the projection follows the request's `label_names` order, so sort
            // here to match the wire order the Grafana drilldown compares against.
            projected.sort();
            if !projected.is_empty() || label_names.is_empty() {
                out.insert(projected);
            }
        }
        Ok(out.into_iter().collect())
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        let mut oldest = None;
        let mut newest = None;
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            oldest =
                Some(oldest.map_or(row.timestamp_ms, |value: i64| value.min(row.timestamp_ms)));
            newest =
                Some(newest.map_or(row.timestamp_ms, |value: i64| value.max(row.timestamp_ms)));
        }
        Ok(ProfileStats {
            data_ingested: oldest.is_some(),
            oldest_profile_time: oldest,
            newest_profile_time: newest,
        })
    }
}

impl InMemoryProfileStore {
    fn rows_in_range(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> impl Iterator<Item = &SampleRow> {
        self.samples
            .get(tenant)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter(move |row| row.timestamp_ms >= start_ms && row.timestamp_ms <= end_ms)
    }
}

fn encode_rows(rows: &[&SampleRow]) -> Result<RecordBatch, ProfileError> {
    let mut fp = UInt64Builder::new();
    let mut ts = Int64Builder::new();
    let mut profile_type = StringDictionaryBuilder::<Int32Type>::new();
    let mut stacktrace_id = UInt64Builder::new();
    let mut value = Int64Builder::new();
    let mut partition = UInt64Builder::new();
    let mut total_value = Int64Builder::new();
    let mut span_id = UInt64Builder::new();
    let mut trace_id = BinaryBuilder::new();

    for row in rows {
        fp.append_value(row.fingerprint);
        ts.append_value(row.timestamp_ms);
        profile_type
            .append(&row.profile_type)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        stacktrace_id.append_value(u64::from(row.stacktrace_id));
        value.append_value(row.value);
        partition.append_value(row.partition);
        total_value.append_value(row.total_value);
        match row.span_id {
            Some(value) => span_id.append_value(value),
            None => span_id.append_null(),
        }
        match &row.trace_id {
            Some(value) => trace_id.append_value(value),
            None => trace_id.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fp.finish()),
        Arc::new(ts.finish()),
        Arc::new(profile_type.finish()),
        Arc::new(stacktrace_id.finish()),
        Arc::new(value.finish()),
        Arc::new(partition.finish()),
        Arc::new(total_value.finish()),
        Arc::new(span_id.finish()),
        Arc::new(trace_id.finish()),
    ];
    RecordBatch::try_new(profile_samples_schema(), columns)
        .map_err(|err| ProfileError::Store(err.to_string()))
}

fn fingerprint_labels(labels: &[(String, String)]) -> u64 {
    let mut canonical = Labels::new();
    for (name, value) in labels {
        canonical.insert(name.clone(), value.clone());
    }
    canonical.fingerprint()
}

enum CompiledMatcher<'a> {
    Literal(&'a LabelMatcher),
    Regex(&'a LabelMatcher, Regex),
}

fn compile_matchers(matchers: &[LabelMatcher]) -> Result<Vec<CompiledMatcher<'_>>, ProfileError> {
    matchers
        .iter()
        .map(|matcher| match matcher.op {
            MatchOp::Eq | MatchOp::Neq => Ok(CompiledMatcher::Literal(matcher)),
            MatchOp::Re | MatchOp::Nre => Regex::new(&format!("^(?:{})$", matcher.value))
                .map(|regex| CompiledMatcher::Regex(matcher, regex))
                .map_err(|err| ProfileError::Plan(format!("bad matcher regex: {err}"))),
        })
        .collect()
}

fn row_matches(row: &SampleRow, matchers: &[CompiledMatcher<'_>]) -> bool {
    matchers.iter().all(|matcher| match matcher {
        CompiledMatcher::Literal(matcher) => {
            let value = label_value(row, &matcher.name);
            match matcher.op {
                MatchOp::Eq => value.is_some_and(|value| value == matcher.value),
                MatchOp::Neq => value.is_none_or(|value| value != matcher.value),
                MatchOp::Re | MatchOp::Nre => unreachable!("regex matchers are compiled"),
            }
        }
        CompiledMatcher::Regex(matcher, regex) => {
            let regex_matched =
                label_value(row, &matcher.name).is_some_and(|value| regex.is_match(value));
            match matcher.op {
                MatchOp::Re => regex_matched,
                MatchOp::Nre => !regex_matched,
                MatchOp::Eq | MatchOp::Neq => unreachable!("literal matchers are not compiled"),
            }
        }
    })
}

fn label_value<'a>(row: &'a SampleRow, name: &str) -> Option<&'a str> {
    row.labels
        .iter()
        .find(|(label_name, _)| label_name == name)
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests {

    use crabka_blockstore::{LabelMatcher, MatchOp};
    use datafusion::arrow::{
        array::AsArray,
        datatypes::{Int64Type, UInt64Type},
    };

    use super::*;
    use crate::{FunctionRec, LineRec, LocationRec};

    fn store_with_two_samples() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let n_main = store.symbols_mut().intern_string("main");
        let f_main = store.symbols_mut().intern_function(FunctionRec {
            name: n_main,
            system_name: n_main,
            filename: 0,
            start_line: 0,
        });
        let n_work = store.symbols_mut().intern_string("work");
        let f_work = store.symbols_mut().intern_function(FunctionRec {
            name: n_work,
            system_name: n_work,
            filename: 0,
            start_line: 0,
        });
        let l_main = store.symbols_mut().intern_location(LocationRec {
            address: 0x10,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: f_main,
                line: 1,
            }],
        });
        let l_work = store.symbols_mut().intern_location(LocationRec {
            address: 0x20,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: f_work,
                line: 2,
            }],
        });
        let st_work = store.symbols_mut().intern_stacktrace(0, &[l_work, l_main]);
        let st_main = store.symbols_mut().intern_stacktrace(0, &[l_main]);
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let labels = vec![("service_name".to_string(), "checkout".to_string())];
        store.push_sample("t", pt, labels.clone(), 0, st_work, 10, 1000);
        store.push_sample("t", pt, labels.clone(), 0, st_work, 5, 1000);
        store.push_sample("t", pt, labels, 0, st_main, 3, 1000);
        store
    }

    #[tokio::test]
    async fn select_registers_samples_table_and_symbols() {
        let store = store_with_two_samples();
        let scan = store
            .select(
                "t",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                &[],
                0,
                5000,
            )
            .await
            .unwrap();
        let df = scan
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {}", scan.samples_table))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let count = out[0].column(0).as_primitive::<Int64Type>().value(0);
        assert2::assert!(count == 3);
        assert2::assert!(
            !scan.symbols.resolve(0, 0).is_empty() || !scan.symbols.resolve(0, 1).is_empty()
        );
    }

    #[tokio::test]
    async fn profile_types_and_label_values() {
        let store = store_with_two_samples();
        let pts = store.profile_types("t", 0, 5000).await.unwrap();
        assert2::assert!(pts == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string()]);
        let vals = store
            .label_values("t", "service_name", &[], 0, 5000)
            .await
            .unwrap();
        assert2::assert!(vals == vec!["checkout".to_string()]);
        let names = store.label_names("t", &[], 0, 5000).await.unwrap();
        assert2::assert!(names == vec!["service_name".to_string()]);
        let series = store
            .series(
                "t",
                &[] as &[LabelMatcher],
                &["service_name".to_string()],
                0,
                5000,
            )
            .await
            .unwrap();
        assert2::assert!(
            series == vec![vec![("service_name".to_string(), "checkout".to_string())]]
        );

        // Empty `label_names` means "return the full label set" (the Pyroscope
        // `/series` convention), mirroring `crabka_blockstore`'s index. It must
        // NOT collapse to a single empty label set (`[{}]`), which breaks
        // Grafana's Pyroscope label autocomplete. All samples here carry the same
        // single label, so the full sets dedup to one series.
        let unprojected = store
            .series("t", &[] as &[LabelMatcher], &[], 0, 5000)
            .await
            .unwrap();
        assert2::assert!(
            unprojected == vec![vec![("service_name".to_string(), "checkout".to_string())]]
        );
    }

    #[tokio::test]
    async fn range_filter_requires_rows_inside_both_bounds() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        store.push_sample(
            "t",
            pt,
            vec![("service_name".to_string(), "early".to_string())],
            0,
            0,
            1,
            1000,
        );
        store.push_sample(
            "t",
            pt,
            vec![("service_name".to_string(), "inside".to_string())],
            0,
            0,
            1,
            2000,
        );

        let values = store
            .label_values("t", "service_name", &[], 1500, 2500)
            .await
            .unwrap();
        let stats = store.stats("t", 1500, 2500).await.unwrap();

        assert2::assert!(values == vec!["inside".to_string()]);
        assert2::assert!(
            stats
                == crate::ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(2000),
                    newest_profile_time: Some(2000),
                }
        );
    }

    #[tokio::test]
    async fn select_encodes_distinct_fingerprints_for_distinct_label_sets() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        store.push_sample(
            "t",
            pt,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            0,
            1,
            1000,
        );
        store.push_sample(
            "t",
            pt,
            vec![("service_name".to_string(), "worker".to_string())],
            0,
            0,
            1,
            1000,
        );
        let scan = store.select("t", pt, &[], 0, 5000).await.unwrap();
        let df = scan
            .ctx
            .sql(&format!(
                "SELECT {} FROM {} ORDER BY {}",
                crate::samples::COL_FINGERPRINT,
                scan.samples_table,
                crate::samples::COL_FINGERPRINT,
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let fingerprints = out[0].column(0).as_primitive::<UInt64Type>();

        assert2::assert!(fingerprints.len() == 2);
        assert2::assert!(fingerprints.value(0) != fingerprints.value(1));
    }

    #[tokio::test]
    async fn label_matchers_filter_negative_literal_and_regex_cases() {
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        for service in ["checkout", "api"] {
            store.push_sample(
                "t",
                pt,
                vec![("service_name".to_string(), service.to_string())],
                0,
                0,
                1,
                1000,
            );
        }

        let neq = store
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Neq, "checkout")],
                0,
                5000,
            )
            .await
            .unwrap();
        let nre = store
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Nre, "check.*")],
                0,
                5000,
            )
            .await
            .unwrap();

        assert2::assert!(neq == vec!["api".to_string()]);
        assert2::assert!(nre == vec!["api".to_string()]);
    }

    #[tokio::test]
    async fn series_emits_each_label_set_sorted_by_name() {
        // Push a sample whose labels are in ingest insertion order that is NOT
        // sorted by name (`service_name` before `__profile_type__`). Pyroscope's
        // `/series` emits each set's labels SORTED by name, so both the projected
        // and full-label-set forms must come back with `__profile_type__` first
        // (`_` < `s`). This is the exact ordering the Grafana Profiles Drilldown
        // compares against in the pyroscope_differential test.
        let mut store = InMemoryProfileStore::new();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let labels = vec![
            ("service_name".to_string(), "api".to_string()),
            ("__name__".to_string(), "process_cpu".to_string()),
            ("__profile_type__".to_string(), pt.to_string()),
        ];
        store.push_sample("t", pt, labels, 0, 0, 1, 1000);

        // Projected onto the drilldown's exact label list (request order is
        // `service_name, __profile_type__`) — the response must still be sorted.
        let projected = store
            .series(
                "t",
                &[] as &[LabelMatcher],
                &["service_name".to_string(), "__profile_type__".to_string()],
                0,
                5000,
            )
            .await
            .unwrap();
        assert2::assert!(
            projected
                == vec![vec![
                    ("__profile_type__".to_string(), pt.to_string()),
                    ("service_name".to_string(), "api".to_string()),
                ]]
        );

        // Full label set (`label_names=[]`) — also sorted by name.
        let full = store
            .series("t", &[] as &[LabelMatcher], &[], 0, 5000)
            .await
            .unwrap();
        assert2::assert!(
            full == vec![vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("__profile_type__".to_string(), pt.to_string()),
                ("service_name".to_string(), "api".to_string()),
            ]]
        );
    }
}
