//! Prometheus `.test` DSL parser.

use std::collections::BTreeMap;

use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use crate::PromqlError;
use crate::error::Result;

/// Runs parsed `.test` files through the in-memory `PromQL` engine.
pub mod testkit {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crabka_blockstore::Labels;

    use crate::conformance::{
        AnnotationExpect, ExpectLine, RangeExpect, SampleSpec, Statement, TestFile, parse_test_file,
    };
    use crate::{
        Annotations, EngineOpts, InMemoryMetricStore, PromqlEngine, PromqlError, QueryResult,
        SampleValue,
    };

    use super::Result;

    const TENANT: &str = "test";
    /// Prometheus promqltest's default relative error for sample values.
    const FLOAT_TOLERANCE: f64 = 1e-6;
    const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

    /// Per-file result for a Prometheus `.test` corpus run.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct FileResult {
        pub name: String,
        pub passed: bool,
        pub passed_cases: usize,
        pub total_cases: usize,
        pub error: Option<String>,
    }

    /// Per-file coverage report for a Prometheus `.test` corpus run.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct Report {
        pub files: Vec<FileResult>,
    }

    impl Report {
        /// Write a stable text report to `path`.
        ///
        /// # Errors
        ///
        /// Returns any filesystem error from creating the parent directory or file.
        pub fn write_to(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
            let path = path.as_ref();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, self.to_string())
        }
    }

    impl fmt::Display for Report {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            writeln!(f, "PromQL conformance report")?;
            writeln!(f, "files: {}", self.files.len())?;
            for file in &self.files {
                let status = if file.passed { "PASS" } else { "FAIL" };
                writeln!(
                    f,
                    "{status} {} {}/{}",
                    file.name, file.passed_cases, file.total_cases
                )?;
                if let Some(error) = &file.error {
                    writeln!(f, "  {error}")?;
                }
            }
            Ok(())
        }
    }

    /// Run a parsed Prometheus `.test` file through an [`InMemoryMetricStore`].
    ///
    /// # Errors
    ///
    /// Returns the first parse, execution, or assertion mismatch error.
    pub async fn run_test_file(file: &TestFile) -> Result<()> {
        let mut store = InMemoryMetricStore::new();

        for statement in &file.statements {
            match statement {
                Statement::Load { step_ms, series } => {
                    for load_series in series {
                        let labels = metric_to_labels(&load_series.metric);
                        for (index, sample) in load_series.values.iter().enumerate() {
                            match sample {
                                SampleSpec::Value(value) => {
                                    store.push_float(
                                        TENANT,
                                        labels.clone(),
                                        index_to_timestamp(index, *step_ms)?,
                                        *value,
                                    );
                                }
                                SampleSpec::Histogram(histogram) => {
                                    store.push_histogram(
                                        TENANT,
                                        labels.clone(),
                                        index_to_timestamp(index, *step_ms)?,
                                        histogram.clone(),
                                    );
                                }
                                SampleSpec::Stale => {
                                    store.push_float(
                                        TENANT,
                                        labels.clone(),
                                        index_to_timestamp(index, *step_ms)?,
                                        stale_nan(),
                                    );
                                }
                                SampleSpec::Missing => {}
                                SampleSpec::String(_) => {
                                    return Err(PromqlError::Parse(
                                        "string expectations are not valid load samples"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
                Statement::EvalInstant {
                    at_ms,
                    expr,
                    expect,
                    annotations,
                    range_expect,
                    fail_message,
                } => {
                    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
                    let result = engine
                        .query_instant_with_annotations(TENANT, expr, *at_ms)
                        .await;
                    handle_instant_eval_result(
                        result,
                        expect,
                        annotations,
                        range_expect.as_ref(),
                        fail_message.as_deref(),
                    )
                    .map_err(|error| add_eval_context(error, "instant", expr))?;
                }
                Statement::EvalRange {
                    start_ms,
                    end_ms,
                    step_ms,
                    expr,
                    expect,
                    annotations,
                    fail_message,
                } => {
                    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
                    let result = engine
                        .query_range_with_annotations(TENANT, expr, *start_ms, *end_ms, *step_ms)
                        .await;
                    handle_range_eval_result(
                        result,
                        expect,
                        annotations,
                        fail_message.as_deref(),
                        *start_ms,
                        *step_ms,
                    )
                    .map_err(|error| add_eval_context(error, "range", expr))?;
                }
                Statement::Clear => store = InMemoryMetricStore::new(),
            }
        }

        Ok(())
    }

    fn add_eval_context(error: PromqlError, kind: &str, expr: &str) -> PromqlError {
        match error {
            PromqlError::Parse(message) => {
                PromqlError::Parse(format!("{kind} eval `{expr}`: {message}"))
            }
            PromqlError::Plan(message) => {
                PromqlError::Plan(format!("{kind} eval `{expr}`: {message}"))
            }
            PromqlError::Exec(message) => {
                PromqlError::Exec(format!("{kind} eval `{expr}`: {message}"))
            }
            PromqlError::Store(message) => {
                PromqlError::Store(format!("{kind} eval `{expr}`: {message}"))
            }
            PromqlError::Unsupported(message) => {
                PromqlError::Unsupported(format!("{kind} eval `{expr}`: {message}"))
            }
        }
    }

    /// Read, parse, and run a Prometheus `.test` file.
    ///
    /// # Errors
    ///
    /// Returns the first file read, parse, execution, or assertion mismatch error.
    pub async fn run_test_path(path: &str) -> Result<()> {
        let src = std::fs::read_to_string(path)
            .map_err(|error| PromqlError::Exec(format!("read `{path}`: {error}")))?;
        let file = parse_test_file(&src)?;
        run_test_file(&file).await
    }

    /// Run every `.test` file in `dir` and return a per-file report.
    pub async fn run_corpus_dir(dir: impl AsRef<Path>) -> Report {
        let dir = dir.as_ref();
        let mut paths = match corpus_paths(dir) {
            Ok(paths) => paths,
            Err(error) => {
                return Report {
                    files: vec![FileResult {
                        name: dir.display().to_string(),
                        passed: false,
                        passed_cases: 0,
                        total_cases: 0,
                        error: Some(error.to_string()),
                    }],
                };
            }
        };
        paths.sort();

        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            files.push(run_corpus_path(dir, &path).await);
        }
        Report { files }
    }

    fn corpus_paths(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        std::fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .filter_map(|path| match path {
                Ok(path)
                    if path.extension().is_some_and(|ext| ext == "test")
                        && corpus_path_enabled(&path) =>
                {
                    Some(Ok(path))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn corpus_path_enabled(path: &Path) -> bool {
        if cfg!(feature = "experimental-functions") {
            return true;
        }

        path.file_name().and_then(|name| name.to_str()) != Some("limit.test")
    }

    async fn run_corpus_path(dir: &Path, path: &Path) -> FileResult {
        let name = path.strip_prefix(dir).unwrap_or(path).display().to_string();
        let src = match std::fs::read_to_string(path) {
            Ok(src) => src,
            Err(error) => {
                return FileResult {
                    name,
                    passed: false,
                    passed_cases: 0,
                    total_cases: 0,
                    error: Some(format!("read `{}`: {error}", path.display())),
                };
            }
        };
        let file = match parse_test_file(&src) {
            Ok(file) => file,
            Err(error) => {
                return FileResult {
                    name,
                    passed: false,
                    passed_cases: 0,
                    total_cases: 0,
                    error: Some(error.to_string()),
                };
            }
        };
        let total_cases = count_eval_cases(&file);
        match run_test_file(&file).await {
            Ok(()) => FileResult {
                name,
                passed: true,
                passed_cases: total_cases,
                total_cases,
                error: None,
            },
            Err(error) => FileResult {
                name,
                passed: false,
                passed_cases: 0,
                total_cases,
                error: Some(error.to_string()),
            },
        }
    }

    fn count_eval_cases(file: &TestFile) -> usize {
        file.statements
            .iter()
            .filter(|statement| {
                matches!(
                    statement,
                    Statement::EvalInstant { .. } | Statement::EvalRange { .. }
                )
            })
            .count()
    }

    pub(crate) fn metric_to_labels(metric: &str) -> Labels {
        let mut labels = Labels::new();
        let Some(open) = metric.find('{') else {
            labels.insert("__name__", metric);
            return labels;
        };
        let name = &metric[..open];
        if !name.is_empty() {
            labels.insert("__name__", name);
        }
        let inside = metric[open + 1..].strip_suffix('}').unwrap_or_default();
        for pair in split_label_pairs(inside) {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            labels.insert(key.trim(), unquote_label_value(value.trim()));
        }
        labels
    }

    fn handle_instant_eval_result(
        result: Result<(QueryResult, Annotations)>,
        expect: &[ExpectLine],
        annotations: &[AnnotationExpect],
        range_expect: Option<&RangeExpect>,
        fail_message: Option<&str>,
    ) -> Result<()> {
        match (result, fail_message) {
            (Ok(_), Some(_)) => Err(PromqlError::Exec(
                "query succeeded but test expected failure".to_string(),
            )),
            (Err(error), Some(expected)) => compare_expected_failure(&error, expected),
            (Err(error), None) => Err(error),
            (Ok((result, raised)), None) => {
                compare_annotations(annotations, &raised)?;
                match range_expect {
                    Some(range_expect) => compare_range_result(
                        result,
                        expect,
                        range_expect.start_ms,
                        range_expect.step_ms,
                    ),
                    None => compare_instant_result(result, expect),
                }
            }
        }
    }

    fn handle_range_eval_result(
        result: Result<(QueryResult, Annotations)>,
        expect: &[ExpectLine],
        annotations: &[AnnotationExpect],
        fail_message: Option<&str>,
        start_ms: i64,
        step_ms: i64,
    ) -> Result<()> {
        match (result, fail_message) {
            (Ok(_), Some(_)) => Err(PromqlError::Exec(
                "query succeeded but test expected failure".to_string(),
            )),
            (Err(error), Some(expected)) => compare_expected_failure(&error, expected),
            (Err(error), None) => Err(error),
            (Ok((result, raised)), None) => {
                compare_annotations(annotations, &raised)?;
                compare_range_result(result, expect, start_ms, step_ms)
            }
        }
    }

    /// Assert the raised annotations satisfy every directive.
    ///
    /// Mirrors Prometheus promqltest: `warn`/`info` require at least one of that
    /// kind, `no_warn`/`no_info` require none, and `msg:` directives require an
    /// exact-text match. A mismatch is a hard test failure naming the expected
    /// and actual annotation sets.
    fn compare_annotations(expects: &[AnnotationExpect], raised: &Annotations) -> Result<()> {
        for expect in expects {
            let ok = match expect {
                AnnotationExpect::AnyWarn => !raised.warnings.is_empty(),
                AnnotationExpect::AnyInfo => !raised.infos.is_empty(),
                AnnotationExpect::NoWarn => raised.warnings.is_empty(),
                AnnotationExpect::NoInfo => raised.infos.is_empty(),
                AnnotationExpect::WarnMsg(text) => raised.warnings.iter().any(|w| w == text),
                AnnotationExpect::InfoMsg(text) => raised.infos.iter().any(|i| i == text),
                AnnotationExpect::Ordered => true,
            };
            if !ok {
                return Err(PromqlError::Exec(format!(
                    "annotation expectation `{}` not satisfied; warnings={:?} infos={:?}",
                    describe_annotation_expect(expect),
                    raised.warnings,
                    raised.infos,
                )));
            }
        }
        Ok(())
    }

    fn describe_annotation_expect(expect: &AnnotationExpect) -> String {
        match expect {
            AnnotationExpect::AnyWarn => "expect warn".to_string(),
            AnnotationExpect::AnyInfo => "expect info".to_string(),
            AnnotationExpect::NoWarn => "expect no_warn".to_string(),
            AnnotationExpect::NoInfo => "expect no_info".to_string(),
            AnnotationExpect::WarnMsg(text) => format!("expect warn msg:{text}"),
            AnnotationExpect::InfoMsg(text) => format!("expect info msg:{text}"),
            AnnotationExpect::Ordered => "expect ordered".to_string(),
        }
    }

    fn compare_expected_failure(error: &PromqlError, expected: &str) -> Result<()> {
        if expected.is_empty() {
            return Ok(());
        }
        let actual = error.to_string();
        if actual.contains(expected) {
            Ok(())
        } else {
            Err(PromqlError::Exec(format!(
                "expected failure containing `{expected}`, got `{actual}`"
            )))
        }
    }

    fn compare_instant_result(result: QueryResult, expect: &[ExpectLine]) -> Result<()> {
        if let QueryResult::Str { value, .. } = result {
            let expected = expect_single_string(expect)?;
            if value == expected {
                return Ok(());
            }
            return Err(PromqlError::Exec(format!(
                "string mismatch: expected `{expected}`, got `{value}`"
            )));
        }
        if let QueryResult::Scalar { value, .. } = result {
            let expected = expect_single_scalar(expect)?;
            if floats_equal(value, expected) {
                return Ok(());
            }
            return Err(PromqlError::Exec(format!(
                "scalar mismatch: expected {expected}, got {value}"
            )));
        }

        let actual = match result {
            QueryResult::InstantVector(samples) => samples
                .into_iter()
                .map(|sample| Ok((labels_key(&sample.labels), sample.value)))
                .collect::<Result<BTreeMap<_, _>>>()?,
            other => {
                return Err(PromqlError::Exec(format!(
                    "expected instant vector result, got {}",
                    other.result_type()
                )));
            }
        };
        let mut expected = BTreeMap::new();
        for line in expect {
            let key = labels_key(&metric_to_labels(&line.metric));
            let value = expect_single_instant_value(line)?;
            // Two `expect` lines collapsing to the same labelset would silently
            // overwrite in the map and weaken the count check below; reject the
            // duplicate instead of deduping it away.
            if expected.insert(key.clone(), value).is_some() {
                return Err(PromqlError::Parse(format!(
                    "duplicate expected series {key}"
                )));
            }
        }

        if actual.len() != expected.len() {
            return Err(PromqlError::Exec(format!(
                "expected {} samples, got {}",
                expected.len(),
                actual.len()
            )));
        }
        for (labels, expected_value) in expected {
            let Some(actual_value) = actual.get(&labels) else {
                return Err(PromqlError::Exec(format!("missing sample for {labels}")));
            };
            if !instant_values_equal(actual_value, &expected_value) {
                return Err(PromqlError::Exec(format!(
                    "value mismatch for {labels}: expected {expected_value:?}, got {actual_value:?}"
                )));
            }
        }

        Ok(())
    }

    fn compare_range_result(
        result: QueryResult,
        expect: &[ExpectLine],
        start_ms: i64,
        step_ms: i64,
    ) -> Result<()> {
        let actual = match result {
            QueryResult::RangeMatrix(series) => series
                .into_iter()
                .map(|series| {
                    let samples = series
                        .samples
                        .into_iter()
                        .map(|(timestamp, value)| Ok((timestamp, value)))
                        .collect::<Result<Vec<_>>>()?;
                    Ok((labels_key(&series.labels), samples))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
            other => {
                return Err(PromqlError::Exec(format!(
                    "expected range matrix result, got {}",
                    other.result_type()
                )));
            }
        };
        let expected = expect
            .iter()
            .map(|line| {
                Ok((
                    labels_key(&metric_to_labels(&line.metric)),
                    expected_range_samples(line, start_ms, step_ms)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;

        if actual.len() != expected.len() {
            return Err(PromqlError::Exec(format!(
                "expected {} series, got {}",
                expected.len(),
                actual.len()
            )));
        }
        for (labels, expected_samples) in expected {
            let Some(actual_samples) = actual.get(&labels) else {
                return Err(PromqlError::Exec(format!("missing series for {labels}")));
            };
            if actual_samples.len() != expected_samples.len() {
                return Err(PromqlError::Exec(format!(
                    "expected {} samples for {labels}, got {}",
                    expected_samples.len(),
                    actual_samples.len()
                )));
            }
            for ((actual_ts, actual_value), (expected_ts, expected_value)) in
                actual_samples.iter().zip(expected_samples)
            {
                if *actual_ts != expected_ts {
                    return Err(PromqlError::Exec(format!(
                        "timestamp mismatch for {labels}: expected {expected_ts}, got {actual_ts}"
                    )));
                }
                if !instant_values_equal(actual_value, &expected_value) {
                    return Err(PromqlError::Exec(format!(
                        "value mismatch for {labels} at {expected_ts}: expected {expected_value:?}, got {actual_value:?}"
                    )));
                }
            }
        }

        Ok(())
    }

    fn expect_single_instant_value(line: &ExpectLine) -> Result<SampleValue> {
        match line.values.as_slice() {
            [SampleSpec::Value(value)] => Ok(SampleValue::Float(*value)),
            [SampleSpec::Histogram(histogram)] => Ok(SampleValue::Histogram(histogram.clone())),
            [SampleSpec::Missing | SampleSpec::Stale | SampleSpec::String(_)] | [] => {
                Err(PromqlError::Exec(format!(
                    "expected one value for instant expectation `{}`",
                    line.metric
                )))
            }
            _ => Err(PromqlError::Exec(format!(
                "expected one value for instant expectation `{}`, got {}",
                line.metric,
                line.values.len()
            ))),
        }
    }

    fn expect_single_string(expect: &[ExpectLine]) -> Result<String> {
        match expect {
            [line] => match line.values.as_slice() {
                [SampleSpec::String(value)] => Ok(value.clone()),
                _ => Err(PromqlError::Exec(
                    "expected one string assertion".to_string(),
                )),
            },
            _ => Err(PromqlError::Exec(format!(
                "expected one string assertion, got {}",
                expect.len()
            ))),
        }
    }

    fn expect_single_scalar(expect: &[ExpectLine]) -> Result<f64> {
        match expect {
            [line] => match line.values.as_slice() {
                [SampleSpec::Value(value)] => Ok(*value),
                _ => Err(PromqlError::Exec(
                    "expected one scalar assertion".to_string(),
                )),
            },
            _ => Err(PromqlError::Exec(format!(
                "expected one scalar assertion, got {}",
                expect.len()
            ))),
        }
    }

    fn instant_values_equal(actual: &SampleValue, expected: &SampleValue) -> bool {
        match (actual, expected) {
            (SampleValue::Float(actual), SampleValue::Float(expected)) => {
                floats_equal(*actual, *expected)
            }
            (SampleValue::Histogram(actual), SampleValue::Histogram(expected)) => {
                actual == expected
            }
            _ => false,
        }
    }

    fn floats_equal(actual: f64, expected: f64) -> bool {
        (actual.is_infinite()
            && expected.is_infinite()
            && actual.is_sign_positive() == expected.is_sign_positive())
            || (actual.is_nan() && expected.is_nan())
            || relative_floats_equal(actual, expected)
    }

    fn relative_floats_equal(actual: f64, expected: f64) -> bool {
        let abs_sum = actual.abs() + expected.abs();
        let diff = (actual - expected).abs();
        if matches!(actual.classify(), std::num::FpCategory::Zero)
            || matches!(expected.classify(), std::num::FpCategory::Zero)
            || abs_sum < f64::MIN_POSITIVE
        {
            return diff < FLOAT_TOLERANCE * f64::MIN_POSITIVE;
        }
        diff / abs_sum.min(f64::MAX) < FLOAT_TOLERANCE
    }

    fn expected_range_samples(
        line: &ExpectLine,
        start_ms: i64,
        step_ms: i64,
    ) -> Result<Vec<(i64, SampleValue)>> {
        line.values
            .iter()
            .enumerate()
            .filter_map(|(index, sample)| match sample {
                SampleSpec::Value(value) => Some(Ok((
                    start_ms + i64::try_from(index).expect("sample index fits i64") * step_ms,
                    SampleValue::Float(*value),
                ))),
                SampleSpec::Histogram(histogram) => Some(Ok((
                    start_ms + i64::try_from(index).expect("sample index fits i64") * step_ms,
                    SampleValue::Histogram(histogram.clone()),
                ))),
                SampleSpec::Missing | SampleSpec::Stale => None,
                SampleSpec::String(_) => Some(Err(PromqlError::Exec(
                    "string assertions are only valid for instant string results".to_string(),
                ))),
            })
            .collect()
    }

    fn index_to_timestamp(index: usize, step_ms: i64) -> Result<i64> {
        let index = i64::try_from(index)
            .map_err(|error| PromqlError::Exec(format!("sample index too large: {error}")))?;
        index
            .checked_mul(step_ms)
            .ok_or_else(|| PromqlError::Exec("sample timestamp overflow".to_string()))
    }

    fn labels_key(labels: &Labels) -> String {
        let mut key = String::new();
        for (name, value) in labels.iter() {
            key.push_str(name);
            key.push('=');
            key.push_str(value);
            key.push('\n');
        }
        key
    }

    fn stale_nan() -> f64 {
        f64::from_bits(STALE_NAN_BITS)
    }

    fn split_label_pairs(inside: &str) -> Vec<&str> {
        let mut pairs = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;
        let mut escaped = false;
        for (index, ch) in inside.char_indices() {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' if in_quotes => escaped = true,
                '"' => in_quotes = !in_quotes,
                ',' if !in_quotes => {
                    pairs.push(inside[start..index].trim());
                    start = index + 1;
                }
                _ => {}
            }
        }
        if start < inside.len() {
            pairs.push(inside[start..].trim());
        }
        pairs
    }

    fn unquote_label_value(value: &str) -> String {
        value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or(value)
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
    }
}

/// Parsed Prometheus `.test` file.
#[derive(Clone, Debug, PartialEq)]
pub struct TestFile {
    /// Top-level statements in file order.
    pub statements: Vec<Statement>,
}

/// A top-level Prometheus `.test` statement.
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    /// Load one or more series at a fixed step.
    Load {
        /// Step between loaded samples in milliseconds.
        step_ms: i64,
        /// Series loaded by this statement.
        series: Vec<LoadSeries>,
    },
    /// Evaluate an instant query.
    EvalInstant {
        /// Evaluation timestamp in milliseconds.
        at_ms: i64,
        /// `PromQL` expression.
        expr: String,
        /// Expected output lines.
        expect: Vec<ExpectLine>,
        /// Expected annotation directives (`warn`/`info`/`no_warn`/`no_info`).
        annotations: Vec<AnnotationExpect>,
        /// Optional matrix expectation metadata for instant range-vector results.
        range_expect: Option<RangeExpect>,
        /// Expected failure message; empty means any failure.
        fail_message: Option<String>,
    },
    /// Evaluate a range query.
    EvalRange {
        /// Range start timestamp in milliseconds.
        start_ms: i64,
        /// Range end timestamp in milliseconds.
        end_ms: i64,
        /// Query step in milliseconds.
        step_ms: i64,
        /// `PromQL` expression.
        expr: String,
        /// Expected output lines.
        expect: Vec<ExpectLine>,
        /// Expected annotation directives (`warn`/`info`/`no_warn`/`no_info`).
        annotations: Vec<AnnotationExpect>,
        /// Expected failure message; empty means any failure.
        fail_message: Option<String>,
    },
    /// Clear loaded series.
    Clear,
}

/// One series in a `load` statement.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSeries {
    /// Metric selector text, including labels when present.
    pub metric: String,
    /// Expanded sample values.
    pub values: Vec<SampleSpec>,
}

/// One loaded sample slot.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleSpec {
    /// A concrete float sample.
    Value(f64),
    /// A concrete native histogram sample.
    Histogram(NativeHistogram),
    /// A concrete string result.
    String(String),
    /// Missing sample (`_`).
    Missing,
    /// Prometheus stale marker (`stale`).
    Stale,
}

/// One expected output line.
#[derive(Clone, Debug, PartialEq)]
pub struct ExpectLine {
    /// Metric selector text, including labels when present.
    pub metric: String,
    /// Expected sample slots. Instant evaluations must contain one float value;
    /// range evaluations use one slot per step.
    pub values: Vec<SampleSpec>,
}

/// An expected (or forbidden) annotation on an eval result.
///
/// Mirrors Prometheus promqltest `expect warn`/`expect info`/`expect no_warn`/
/// `expect no_info` directives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnnotationExpect {
    /// `expect warn`: at least one warning must be raised.
    AnyWarn,
    /// `expect info`: at least one info must be raised.
    AnyInfo,
    /// `expect no_warn`: no warnings may be raised.
    NoWarn,
    /// `expect no_info`: no infos may be raised.
    NoInfo,
    /// `expect warn msg:<text>`: a warning exactly equal to `<text>` must exist.
    WarnMsg(String),
    /// `expect info msg:<text>`: an info exactly equal to `<text>` must exist.
    InfoMsg(String),
    /// `expect ordered`: result ordering directive, no annotation semantics.
    Ordered,
}

/// Expected sample timestamps for an instant query that returns a range vector.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeExpect {
    /// Expected first sample timestamp in milliseconds.
    pub start_ms: i64,
    /// Expected step between samples in milliseconds.
    pub step_ms: i64,
}

/// Parse the legacy Prometheus `.test` DSL subset used by the conformance harness.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when the input is not valid legacy `.test` DSL.
pub fn parse_test_file(src: &str) -> Result<TestFile> {
    let mut parser = TestParser::new(src);
    parser.parse_file()
}

struct TestParser<'a> {
    lines: Vec<Line<'a>>,
    index: usize,
}

#[derive(Clone, Copy)]
struct Line<'a> {
    number: usize,
    raw: &'a str,
    trimmed: &'a str,
}

impl<'a> TestParser<'a> {
    fn new(src: &'a str) -> Self {
        let lines = src
            .lines()
            .enumerate()
            .filter_map(|(index, raw)| {
                let trimmed = raw.trim();
                (!trimmed.is_empty() && !trimmed.starts_with('#')).then_some(Line {
                    number: index + 1,
                    raw,
                    trimmed,
                })
            })
            .collect();
        Self { lines, index: 0 }
    }

    fn parse_file(&mut self) -> Result<TestFile> {
        let mut statements = Vec::new();

        while let Some(line) = self.peek() {
            if line.trimmed.starts_with("load ") {
                statements.push(self.parse_load(false)?);
            } else if line.trimmed.starts_with("load_with_nhcb ") {
                statements.push(self.parse_load(true)?);
            } else if line.trimmed == "clear" {
                self.index += 1;
                statements.push(Statement::Clear);
            } else if line.trimmed.starts_with("eval instant at ")
                || line.trimmed.starts_with("eval_fail instant at ")
            {
                statements.push(self.parse_eval_instant()?);
            } else if line.trimmed.starts_with("eval range from ")
                || line.trimmed.starts_with("eval_fail range from ")
            {
                statements.push(self.parse_eval_range()?);
            } else {
                return Err(parse_error(
                    line,
                    "expected load, eval, eval_fail, or clear",
                ));
            }
        }

        Ok(TestFile { statements })
    }

    fn parse_load(&mut self, with_nhcb: bool) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let step = if with_nhcb {
            header
                .trimmed
                .strip_prefix("load_with_nhcb ")
                .ok_or_else(|| parse_error(header, "expected load_with_nhcb statement"))?
        } else {
            header
                .trimmed
                .strip_prefix("load ")
                .ok_or_else(|| parse_error(header, "expected load statement"))?
        };
        let step_ms = parse_duration_ms(step.trim(), header)?;
        let mut series = Vec::new();

        while let Some(line) = self.peek() {
            if !is_block_line(line) {
                break;
            }
            let line = self.next().expect("peeked block line");
            let (metric, values) = split_metric_and_tail(line.trimmed, line)?;
            let values = split_sample_tokens(values, line)?
                .into_iter()
                .map(|token| parse_sample_token(token, line))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            series.push(LoadSeries {
                metric: metric.to_string(),
                values,
            });
        }

        if with_nhcb {
            series.extend(load_with_nhcb_series(&series, header)?);
        }

        Ok(Statement::Load { step_ms, series })
    }

    fn parse_eval_instant(&mut self) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let (fail, rest) = if let Some(rest) = header.trimmed.strip_prefix("eval_fail instant at ")
        {
            (true, rest)
        } else {
            let rest = header
                .trimmed
                .strip_prefix("eval instant at ")
                .ok_or_else(|| parse_error(header, "expected instant eval statement"))?;
            (false, rest)
        };
        let (at, expr) = split_once_whitespace(rest.trim(), header)?;
        let ExpectBlock {
            lines: expect,
            annotations,
            fail_message: expect_fail_message,
            range,
        } = self.parse_expect_block()?;

        Ok(Statement::EvalInstant {
            at_ms: parse_duration_ms(at, header)?,
            expr: expr.to_string(),
            expect,
            annotations,
            range_expect: range,
            fail_message: failure_message(fail, expect_fail_message),
        })
    }

    fn parse_eval_range(&mut self) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let (fail, rest) = if let Some(rest) = header.trimmed.strip_prefix("eval_fail range from ")
        {
            (true, rest)
        } else {
            let rest = header
                .trimmed
                .strip_prefix("eval range from ")
                .ok_or_else(|| parse_error(header, "expected range eval statement"))?;
            (false, rest)
        };
        let (start, rest) = split_once_whitespace(rest.trim(), header)?;
        let rest = rest
            .strip_prefix("to ")
            .ok_or_else(|| parse_error(header, "expected `to` in range eval"))?;
        let (end, rest) = split_once_whitespace(rest.trim(), header)?;
        let rest = rest
            .strip_prefix("step ")
            .ok_or_else(|| parse_error(header, "expected `step` in range eval"))?;
        let (step, expr) = split_once_whitespace(rest.trim(), header)?;
        let ExpectBlock {
            lines: expect,
            annotations,
            fail_message: expect_fail_message,
            range,
        } = self.parse_expect_block()?;
        if range.is_some() {
            return Err(parse_error(
                header,
                "expect range vector is only valid for instant evals",
            ));
        }

        Ok(Statement::EvalRange {
            start_ms: parse_duration_ms(start, header)?,
            end_ms: parse_duration_ms(end, header)?,
            step_ms: parse_duration_ms(step, header)?,
            expr: expr.to_string(),
            expect,
            annotations,
            fail_message: failure_message(fail, expect_fail_message),
        })
    }

    fn parse_expect_block(&mut self) -> Result<ExpectBlock> {
        let mut expect = Vec::new();
        let mut annotations = Vec::new();
        let mut fail_message = None;
        let mut range = None;

        while let Some(line) = self.peek() {
            if !is_block_line(line) {
                break;
            }
            let line = self.next().expect("peeked block line");
            if line.trimmed == "fail" {
                fail_message = Some(String::new());
                continue;
            }
            if let Some(directive) = line.trimmed.strip_prefix("expect ") {
                if directive == "fail" {
                    fail_message = Some(String::new());
                    continue;
                }
                if let Some(message) = directive.strip_prefix("fail msg:") {
                    fail_message = Some(message.trim().to_string());
                    continue;
                }
                if let Some(value) = directive.trim().strip_prefix("string ") {
                    expect.push(ExpectLine {
                        metric: String::new(),
                        values: vec![SampleSpec::String(parse_expect_string(value, line)?)],
                    });
                    continue;
                }
                if let Some(range_expect) = parse_range_vector_directive(directive, line)? {
                    if range.replace(range_expect).is_some() {
                        return Err(parse_error(line, "duplicate expect range vector directive"));
                    }
                    continue;
                }
                annotations.push(parse_expect_directive(directive, line)?);
                continue;
            }
            if !line.trimmed.contains(char::is_whitespace) {
                let values = parse_sample_token(line.trimmed, line)?
                    .into_iter()
                    .collect::<Vec<_>>();
                if !values.is_empty() {
                    expect.push(ExpectLine {
                        metric: String::new(),
                        values,
                    });
                    continue;
                }
            }
            let (metric, value) = split_metric_and_tail(line.trimmed, line)?;
            let values = split_sample_tokens(value, line)?
                .into_iter()
                .map(|token| parse_sample_token(token, line))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            if values.is_empty() {
                return Err(parse_error(line, "expected at least one expected value"));
            }
            expect.push(ExpectLine {
                metric: metric.to_string(),
                values,
            });
        }

        Ok(ExpectBlock {
            lines: expect,
            annotations,
            fail_message,
            range,
        })
    }

    fn peek(&self) -> Option<Line<'a>> {
        self.lines.get(self.index).copied()
    }

    fn next(&mut self) -> Option<Line<'a>> {
        let line = self.peek()?;
        self.index += 1;
        Some(line)
    }
}

struct ExpectBlock {
    lines: Vec<ExpectLine>,
    annotations: Vec<AnnotationExpect>,
    fail_message: Option<String>,
    range: Option<RangeExpect>,
}

fn failure_message(header_fail: bool, expect_fail_message: Option<String>) -> Option<String> {
    match (header_fail, expect_fail_message) {
        (_, Some(message)) => Some(message),
        (true, None) => Some(String::new()),
        (false, None) => None,
    }
}

fn parse_range_vector_directive(directive: &str, line: Line<'_>) -> Result<Option<RangeExpect>> {
    let Some(rest) = directive.trim().strip_prefix("range vector from ") else {
        return Ok(None);
    };
    let (start, rest) = split_once_whitespace(rest.trim(), line)?;
    let rest = rest
        .strip_prefix("to ")
        .ok_or_else(|| parse_error(line, "expected `to` in expect range vector"))?;
    let (_end, rest) = split_once_whitespace(rest.trim(), line)?;
    let rest = rest
        .strip_prefix("step ")
        .ok_or_else(|| parse_error(line, "expected `step` in expect range vector"))?;
    let step = rest.trim();
    Ok(Some(RangeExpect {
        start_ms: parse_duration_ms(start, line)?,
        step_ms: parse_duration_ms(step, line)?,
    }))
}

fn parse_expect_directive(directive: &str, line: Line<'_>) -> Result<AnnotationExpect> {
    let directive = directive.trim();
    match directive {
        "no_warn" => return Ok(AnnotationExpect::NoWarn),
        "no_info" => return Ok(AnnotationExpect::NoInfo),
        "warn" => return Ok(AnnotationExpect::AnyWarn),
        "info" => return Ok(AnnotationExpect::AnyInfo),
        _ => {}
    }
    if let Some(message) = directive.strip_prefix("warn msg:") {
        return Ok(AnnotationExpect::WarnMsg(message.trim().to_string()));
    }
    if let Some(message) = directive.strip_prefix("info msg:") {
        return Ok(AnnotationExpect::InfoMsg(message.trim().to_string()));
    }
    if directive == "ordered" {
        return Ok(AnnotationExpect::Ordered);
    }
    Err(parse_error(
        line,
        format!("unsupported expect directive `{directive}`"),
    ))
}

fn parse_expect_string(src: &str, line: Line<'_>) -> Result<String> {
    let src = src.trim();
    if let Some(value) = src
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
    {
        return Ok(value.to_string());
    }
    let value = src
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| parse_error(line, "expected quoted string assertion"))?;
    Ok(value.replace("\\\"", "\"").replace("\\\\", "\\"))
}

fn is_block_line(line: Line<'_>) -> bool {
    line.raw.starts_with(' ') || line.raw.starts_with('\t')
}

fn split_once_whitespace<'a>(src: &'a str, line: Line<'_>) -> Result<(&'a str, &'a str)> {
    let Some(index) = src.find(char::is_whitespace) else {
        return Err(parse_error(line, "expected whitespace-separated fields"));
    };
    let (head, tail) = src.split_at(index);
    Ok((head, tail.trim()))
}

fn split_metric_and_tail<'a>(src: &'a str, line: Line<'_>) -> Result<(&'a str, &'a str)> {
    let mut brace_depth = 0_u32;
    let mut in_quotes = false;
    let mut escaped = false;

    for (index, ch) in src.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            '{' if !in_quotes => brace_depth += 1,
            '}' if !in_quotes => {
                brace_depth = brace_depth.saturating_sub(1);
            }
            _ if ch.is_whitespace() && !in_quotes && brace_depth == 0 => {
                let (metric, tail) = src.split_at(index);
                return Ok((metric, tail.trim()));
            }
            _ => {}
        }
    }

    Err(parse_error(
        line,
        "expected metric followed by whitespace-separated fields",
    ))
}

fn parse_sample_token(token: &str, line: Line<'_>) -> Result<Vec<SampleSpec>> {
    if token == "_" {
        return Ok(vec![SampleSpec::Missing]);
    }
    if token == "stale" {
        return Ok(vec![SampleSpec::Stale]);
    }
    if let Some((start, step, count)) = parse_histogram_expansion(token, line)? {
        return Ok((0..=count)
            .map(|offset| SampleSpec::Histogram(add_histogram_step(&start, &step, offset)))
            .collect());
    }
    if let Some((histogram, count)) = parse_histogram_repetition(token, line)? {
        return Ok((0..=count)
            .map(|_| SampleSpec::Histogram(histogram.clone()))
            .collect());
    }
    if token.starts_with("{{") {
        return Ok(vec![SampleSpec::Histogram(parse_histogram_literal(
            token, line,
        )?)]);
    }

    if let Some((base, count)) = token.rsplit_once('x') {
        let count = count.parse::<u32>().map_err(|err| {
            parse_error(
                line,
                format!("invalid expanding-point count `{count}`: {err}"),
            )
        })?;
        let step_index = base
            .char_indices()
            .skip(usize::from(base.starts_with('+') || base.starts_with('-')))
            .find_map(|(index, ch)| matches!(ch, '+' | '-').then_some((index, ch)));
        let (start, step) = match step_index {
            Some(index) => {
                let (index, sign) = index;
                let (start, step) = base.split_at(index);
                let step = parse_float(&step[1..], line)?;
                let step = if sign == '-' { -step } else { step };
                (parse_float(start, line)?, step)
            }
            None => (parse_float(base, line)?, 0.0),
        };
        return Ok((0..=count)
            .map(|offset| SampleSpec::Value(start + step * f64::from(offset)))
            .collect());
    }

    Ok(vec![SampleSpec::Value(parse_float(token, line)?)])
}

fn load_with_nhcb_series(series: &[LoadSeries], line: Line<'_>) -> Result<Vec<LoadSeries>> {
    let mut groups: BTreeMap<String, NhcbGroup> = BTreeMap::new();
    for load_series in series {
        let labels = testkit::metric_to_labels(&load_series.metric);
        let Some(name) = labels.get("__name__") else {
            continue;
        };
        if let Some(native_name) = name.strip_suffix("_sum") {
            let mut native_labels = labels.clone();
            native_labels.insert("__name__", native_name);
            let key = conformance_labels_key(&native_labels);
            groups
                .entry(key)
                .or_insert_with(|| NhcbGroup {
                    labels: native_labels,
                    buckets: Vec::new(),
                    sum_values: None,
                })
                .sum_values = Some(load_series.values.clone());
            continue;
        }
        let Some(native_name) = name.strip_suffix("_bucket") else {
            continue;
        };
        let Some(le) = labels.get("le") else {
            continue;
        };
        let upper_bound = parse_bucket_bound(le, line)?;
        let mut native_labels = labels_without_label(&labels, "le");
        native_labels.insert("__name__", native_name);
        let key = conformance_labels_key(&native_labels);
        groups
            .entry(key)
            .or_insert_with(|| NhcbGroup {
                labels: native_labels,
                buckets: Vec::new(),
                sum_values: None,
            })
            .buckets
            .push(NhcbBucketSeries {
                upper_bound,
                values: load_series.values.clone(),
            });
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group
            .buckets
            .sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
        if group.buckets.is_empty() {
            continue;
        }
        let sample_count = group
            .buckets
            .iter()
            .map(|bucket| bucket.values.len())
            .max()
            .unwrap_or(0);
        let custom_values = group
            .buckets
            .iter()
            .filter_map(|bucket| bucket.upper_bound.is_finite().then_some(bucket.upper_bound))
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(sample_count);
        for index in 0..sample_count {
            values.push(nhcb_sample_at(
                &group.buckets,
                group.sum_values.as_deref(),
                &custom_values,
                index,
                line,
            )?);
        }
        out.push(LoadSeries {
            metric: labels_to_metric(&group.labels),
            values,
        });
    }
    Ok(out)
}

#[derive(Clone)]
struct NhcbGroup {
    labels: Labels,
    buckets: Vec<NhcbBucketSeries>,
    sum_values: Option<Vec<SampleSpec>>,
}

#[derive(Clone)]
struct NhcbBucketSeries {
    upper_bound: f64,
    values: Vec<SampleSpec>,
}

fn nhcb_sample_at(
    buckets: &[NhcbBucketSeries],
    sum_values: Option<&[SampleSpec]>,
    custom_values: &[f64],
    index: usize,
    line: Line<'_>,
) -> Result<SampleSpec> {
    let mut cumulative = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        match bucket.values.get(index) {
            Some(SampleSpec::Value(value)) => cumulative.push(*value),
            Some(SampleSpec::Missing) | None => return Ok(SampleSpec::Missing),
            Some(SampleSpec::Stale) => return Ok(SampleSpec::Stale),
            Some(SampleSpec::Histogram(_) | SampleSpec::String(_)) => {
                return Err(parse_error(
                    line,
                    "load_with_nhcb bucket samples must be float values",
                ));
            }
        }
    }
    let counts = cumulative_to_bucket_counts(&cumulative);
    let total = cumulative.last().copied().unwrap_or(0.0);
    let sum = match sum_values.and_then(|values| values.get(index)) {
        Some(SampleSpec::Value(value)) => *value,
        Some(SampleSpec::Missing) => return Ok(SampleSpec::Missing),
        Some(SampleSpec::Stale) => return Ok(SampleSpec::Stale),
        Some(SampleSpec::Histogram(_) | SampleSpec::String(_)) => {
            return Err(parse_error(
                line,
                "load_with_nhcb sum samples must be float values",
            ));
        }
        None => 0.0,
    };
    Ok(SampleSpec::Histogram(native_custom_bucket_histogram(
        custom_values,
        &counts,
        total,
        sum,
        line,
    )?))
}

fn cumulative_to_bucket_counts(cumulative: &[f64]) -> Vec<f64> {
    let mut previous = 0.0;
    cumulative
        .iter()
        .map(|value| {
            let count = *value - previous;
            previous = *value;
            count
        })
        .collect()
}

fn native_custom_bucket_histogram(
    custom_values: &[f64],
    counts: &[f64],
    total: f64,
    sum: f64,
    line: Line<'_>,
) -> Result<NativeHistogram> {
    let positive_counts = if total == 0.0 {
        Vec::new()
    } else {
        counts.to_vec()
    };
    let positive_spans = histogram_span(0, positive_counts.len(), line)?;
    Ok(NativeHistogram {
        schema: -53,
        is_float: true,
        reset_hint: ResetHint::Unknown,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: total,
        sum,
        positive_spans: positive_spans.into_iter().collect(),
        positive_counts,
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: Some(custom_values.to_vec()),
        start_timestamp_ms: None,
    })
}

fn parse_bucket_bound(value: &str, line: Line<'_>) -> Result<f64> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => parse_float(value, line),
    }
}

fn labels_without_label(labels: &Labels, drop: &str) -> Labels {
    let mut out = Labels::new();
    for (name, value) in labels.iter() {
        if name != drop {
            out.insert(name, value);
        }
    }
    out
}

fn labels_to_metric(labels: &Labels) -> String {
    let name = labels.get("__name__").unwrap_or_default();
    let pairs = labels
        .iter()
        .filter(|(label, _)| *label != "__name__")
        .map(|(label, value)| format!(r#"{label}="{}""#, escape_label_value(value)))
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return name.to_string();
    }
    format!("{name}{{{}}}", pairs.join(","))
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn conformance_labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for (name, value) in labels.iter() {
        key.push_str(name);
        key.push('=');
        key.push_str(value);
        key.push('\n');
    }
    key
}

fn split_sample_tokens<'a>(src: &'a str, line: Line<'_>) -> Result<Vec<&'a str>> {
    let mut tokens = Vec::new();
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < src.len() {
        while index < src.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == src.len() {
            break;
        }
        let start = index;
        if src[start..].starts_with("{{") {
            let Some(relative_end) = src[start + 2..].find("}}") else {
                return Err(parse_error(line, "unterminated native histogram literal"));
            };
            index = start + 2 + relative_end + 2;
            if src[index..].starts_with("+{{") {
                let second_start = index + 1;
                let Some(relative_end) = src[second_start + 2..].find("}}") else {
                    return Err(parse_error(line, "unterminated native histogram literal"));
                };
                index = second_start + 2 + relative_end + 2;
            }
            if index < src.len() && bytes[index] == b'x' {
                index += 1;
                while index < src.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
            }
        } else {
            while index < src.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
        }
        tokens.push(&src[start..index]);
    }

    Ok(tokens)
}

fn parse_histogram_repetition(
    token: &str,
    line: Line<'_>,
) -> Result<Option<(NativeHistogram, u32)>> {
    let Some((literal, count)) = token.rsplit_once('x') else {
        return Ok(None);
    };
    if !literal.starts_with("{{") || !literal.ends_with("}}") {
        return Ok(None);
    }
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    Ok(Some((parse_histogram_literal(literal, line)?, count)))
}

fn parse_histogram_expansion(
    token: &str,
    line: Line<'_>,
) -> Result<Option<(NativeHistogram, NativeHistogram, u32)>> {
    let Some((base, count)) = token.rsplit_once('x') else {
        return Ok(None);
    };
    let Some((start, step)) = base.split_once("}}+{{") else {
        return Ok(None);
    };
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    let mut start_literal = start.to_string();
    start_literal.push_str("}}");
    let mut step_literal = "{{".to_string();
    step_literal.push_str(step);
    let start = parse_histogram_literal(&start_literal, line)?;
    let step = parse_histogram_literal(&step_literal, line)?;
    Ok(Some((start, step, count)))
}

fn add_histogram_step(
    start: &NativeHistogram,
    step: &NativeHistogram,
    offset: u32,
) -> NativeHistogram {
    let multiplier = f64::from(offset);
    let mut histogram = start.clone();
    histogram.sum += step.sum * multiplier;
    histogram.count += step.count * multiplier;
    histogram.zero_count += step.zero_count * multiplier;
    (histogram.positive_spans, histogram.positive_counts) = add_histogram_counts(
        &start.positive_spans,
        &start.positive_counts,
        &step.positive_spans,
        &step.positive_counts,
        multiplier,
    );
    (histogram.negative_spans, histogram.negative_counts) = add_histogram_counts(
        &start.negative_spans,
        &start.negative_counts,
        &step.negative_spans,
        &step.negative_counts,
        multiplier,
    );
    histogram
}

fn add_histogram_counts(
    start_spans: &[BucketSpan],
    start_counts: &[f64],
    step_spans: &[BucketSpan],
    step_counts: &[f64],
    multiplier: f64,
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(start_spans, start_counts);
    for (index, count) in spanned_histogram_counts(step_spans, step_counts) {
        *buckets.entry(index).or_insert(0.0) += count * multiplier;
    }
    compact_spanned_histogram_counts(buckets)
}

fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}

fn compact_spanned_histogram_counts(buckets: BTreeMap<i32, f64>) -> (Vec<BucketSpan>, Vec<f64>) {
    let buckets = buckets
        .into_iter()
        .filter(|(_, count)| *count != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for (index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (spans, counts)
}

fn parse_histogram_literal(src: &str, line: Line<'_>) -> Result<NativeHistogram> {
    let content = src
        .strip_prefix("{{")
        .and_then(|src| src.strip_suffix("}}"))
        .ok_or_else(|| parse_error(line, "invalid native histogram literal"))?;
    let fields = histogram_fields(content, line)?;
    let schema = parse_optional_histogram_i8(&fields, "schema", line)?.unwrap_or(0);
    let sum = parse_optional_histogram_f64(&fields, "sum", line)?.unwrap_or(0.0);
    let count = parse_optional_histogram_f64(&fields, "count", line)?.unwrap_or(0.0);
    let zero_count = parse_optional_histogram_f64(&fields, "z_bucket", line)?.unwrap_or(0.0);
    let zero_threshold = parse_optional_histogram_f64(&fields, "z_bucket_w", line)?.unwrap_or(0.0);
    let positive_counts =
        parse_optional_histogram_buckets(&fields, "buckets", line)?.unwrap_or_else(Vec::new);
    let positive_offset = parse_optional_histogram_i32(&fields, "offset", line)?.unwrap_or(0);
    let negative_counts =
        parse_optional_histogram_buckets(&fields, "n_buckets", line)?.unwrap_or_else(Vec::new);
    let negative_offset = parse_optional_histogram_i32(&fields, "n_offset", line)?.unwrap_or(0);
    let custom_values = parse_optional_histogram_buckets(&fields, "custom_values", line)?;
    let reset_hint = parse_optional_histogram_reset_hint(&fields, line)?;
    let positive_spans = histogram_span(positive_offset, positive_counts.len(), line)?;
    let negative_spans = histogram_span(negative_offset, negative_counts.len(), line)?;

    Ok(NativeHistogram {
        schema,
        is_float: true,
        reset_hint,
        zero_threshold,
        zero_count,
        count,
        sum,
        positive_spans: positive_spans.into_iter().collect(),
        positive_counts,
        negative_spans: negative_spans.into_iter().collect(),
        negative_counts,
        custom_values,
        start_timestamp_ms: None,
    })
}

fn histogram_fields<'a>(content: &'a str, line: Line<'_>) -> Result<BTreeMap<&'a str, &'a str>> {
    let mut fields = BTreeMap::new();
    let mut index = 0;
    let bytes = content.as_bytes();
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == bytes.len() {
            break;
        }
        let name_start = index;
        while index < bytes.len() && bytes[index] != b':' {
            if bytes[index].is_ascii_whitespace() {
                return Err(parse_error(
                    line,
                    "histogram field name must be followed by `:`",
                ));
            }
            index += 1;
        }
        if index == bytes.len() {
            return Err(parse_error(line, "histogram field missing `:`"));
        }
        let name = &content[name_start..index];
        index += 1;
        let value_start = index;
        if index < bytes.len() && bytes[index] == b'[' {
            let Some(end) = content[index..].find(']') else {
                return Err(parse_error(line, "unterminated histogram bucket list"));
            };
            index += end + 1;
        } else {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
        }
        fields.insert(name, &content[value_start..index]);
    }
    Ok(fields)
}

fn parse_optional_histogram_i8(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<i8>> {
    fields
        .get(name)
        .map(|value| {
            value.parse::<i8>().map_err(|error| {
                parse_error(line, format!("invalid histogram {name} `{value}`: {error}"))
            })
        })
        .transpose()
}

fn parse_optional_histogram_i32(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<i32>> {
    fields
        .get(name)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                parse_error(line, format!("invalid histogram {name} `{value}`: {error}"))
            })
        })
        .transpose()
}

fn parse_optional_histogram_f64(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<f64>> {
    fields
        .get(name)
        .map(|value| parse_float(value, line))
        .transpose()
}

fn parse_optional_histogram_buckets(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<Vec<f64>>> {
    let Some(value) = fields.get(name) else {
        return Ok(None);
    };
    let bucket_values = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| {
            parse_error(
                line,
                format!("histogram {name} must be enclosed in `[` and `]`"),
            )
        })?;
    if bucket_values.trim().is_empty() {
        return Ok(Some(Vec::new()));
    }
    let values = bucket_values
        .split_whitespace()
        .map(|bucket| parse_float(bucket, line))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(values))
}

fn parse_optional_histogram_reset_hint(
    fields: &BTreeMap<&str, &str>,
    line: Line<'_>,
) -> Result<ResetHint> {
    match fields.get("counter_reset_hint").copied() {
        None | Some("unknown") => Ok(ResetHint::Unknown),
        Some("reset") => Ok(ResetHint::Yes),
        Some("not_reset") => Ok(ResetHint::No),
        Some("gauge") => Ok(ResetHint::Gauge),
        Some(value) => Err(parse_error(
            line,
            format!("invalid histogram counter_reset_hint `{value}`"),
        )),
    }
}

fn histogram_span(offset: i32, len: usize, line: Line<'_>) -> Result<Option<BucketSpan>> {
    if len == 0 {
        return Ok(None);
    }
    Ok(Some(BucketSpan {
        offset,
        length: u32::try_from(len)
            .map_err(|error| parse_error(line, format!("too many histogram buckets: {error}")))?,
    }))
}

fn parse_duration_ms(src: &str, line: Line<'_>) -> Result<i64> {
    let src = src.trim();
    if src == "0" {
        return Ok(0);
    }

    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < bytes.len() {
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(parse_error(line, format!("invalid duration `{src}`")));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| parse_error(line, format!("invalid duration amount `{src}`: {err}")))?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &src[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(parse_error(line, format!("invalid duration unit `{unit}`"))),
        };
        total_ms += amount * multiplier;
    }

    Ok(total_ms)
}

fn parse_float(src: &str, line: Line<'_>) -> Result<f64> {
    src.parse::<f64>()
        .map_err(|err| parse_error(line, format!("invalid float `{src}`: {err}")))
}

fn parse_error(line: Line<'_>, message: impl Into<String>) -> PromqlError {
    PromqlError::Parse(format!("line {}: {}", line.number, message.into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_legacy_test_file_load_eval_and_clear() {
        let file = parse_test_file(
            r#"
load 1m
  metric{a="b"} 0+1x4

eval instant at 3m metric{a="b"}
  metric{a="b"} 3

clear
"#,
        )
        .unwrap();

        assert!(file.statements.len() == 3);

        assert!(let
            Statement::Load { step_ms, series } = &file.statements[0]
        );
        assert!(*step_ms == 60_000);
        assert!(series.len() == 1);
        assert!(series[0].metric == r#"metric{a="b"}"#);
        assert!(series[0].values.len() == 5);
        assert_sample_value(&series[0].values[0], 0.0);
        assert_sample_value(&series[0].values[1], 1.0);
        assert_sample_value(&series[0].values[2], 2.0);
        assert_sample_value(&series[0].values[3], 3.0);
        assert_sample_value(&series[0].values[4], 4.0);

        assert!(let
            Statement::EvalInstant {
                at_ms,
                expr,
                expect,
                annotations: _,
                range_expect,
                fail_message,
            } = &file.statements[1]
        );
        assert!(*at_ms == 180_000);
        assert!(expr == r#"metric{a="b"}"#);
        assert!(range_expect.is_none());
        assert!(fail_message.is_none());
        assert!(expect.len() == 1);
        assert!(expect[0].metric == r#"metric{a="b"}"#);
        assert!(expect[0].values.len() == 1);
        assert_sample_value(&expect[0].values[0], 3.0);

        assert!(matches!(file.statements[2], Statement::Clear));
    }

    fn assert_sample_value(sample: &SampleSpec, expected: f64) {
        assert!(let SampleSpec::Value(actual) = sample);
        assert!((actual - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_descending_expanding_point_notation() {
        let file = parse_test_file(
            r"
load 1m
  metric 99-1x2
",
        )
        .unwrap();

        let Statement::Load { series, .. } = &file.statements[0] else {
            panic!("expected load statement");
        };
        assert!(series[0].values.len() == 3);
        assert_sample_value(&series[0].values[0], 99.0);
        assert_sample_value(&series[0].values[1], 98.0);
        assert_sample_value(&series[0].values[2], 97.0);
    }

    #[tokio::test]
    async fn testkit_runs_inline_instant_eval() {
        let file = parse_test_file(
            r#"
load 1m
  up{job="api"} 0+1x1

eval instant at 1m up{job="api"}
  up{job="api"} 1
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_rejects_duplicate_expected_series() {
        // Two `expect` lines collapsing to the same labelset must be rejected,
        // not silently deduped (which would weaken the result count check).
        let file = parse_test_file(
            r#"
load 1m
  up{job="api"} 1

eval instant at 0 up{job="api"}
  up{job="api"} 1
  up{job="api"} 1
"#,
        )
        .unwrap();

        let err = testkit::run_test_file(&file).await.unwrap_err();
        assert!(let PromqlError::Parse(_) = &err);
        if let PromqlError::Parse(message) = &err {
            assert!(message.contains("duplicate expected series"));
        }
    }

    #[tokio::test]
    async fn testkit_accepts_expect_directives() {
        let file = parse_test_file(
            r#"
load 1m
  up{job="api"} 1

eval instant at 0 up{job="api"}
  expect no_warn
  expect no_info
  expect ordered
  up{job="api"} 1
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_runs_expect_string_assertion() {
        let file = parse_test_file(
            r#"
eval instant at 50m " Foo "
  expect string " Foo "
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_runs_inline_instant_range_vector_eval() {
        let file = parse_test_file(
            r#"
load 10s
  some_metric{env="a"} 1+1x5

eval instant at 40s some_metric[30s]
  expect range vector from 20s to 40s step 10s
  some_metric{env="a"} 3 4 5
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_runs_inline_native_histogram_instant_eval() {
        let file = parse_test_file(
            r#"
load 1m
  latency{job="api"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}

eval instant at 0 latency{job="api"}
  latency{job="api"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[test]
    fn parses_prometheus_native_histogram_optional_fields() {
        let file = parse_test_file(
            r#"
load 1m
  latency{job="api"} {{schema:1 sum:-0.3 count:3.1 z_bucket:7.1 z_bucket_w:0.05 buckets:[5.1 10 7] offset:-3 n_buckets:[4.1 5] n_offset:-5 counter_reset_hint:gauge}}
"#,
        )
        .unwrap();

        let Statement::Load { series, .. } = &file.statements[0] else {
            panic!("expected load statement");
        };
        let SampleSpec::Histogram(histogram) = &series[0].values[0] else {
            panic!("expected native histogram sample");
        };

        assert!(histogram.schema == 1);
        assert!(histogram.reset_hint == ResetHint::Gauge);
        assert_float(histogram.zero_threshold, 0.05);
        assert_float(histogram.zero_count, 7.1);
        assert_float(histogram.count, 3.1);
        assert_float(histogram.sum, -0.3);
        assert!(
            histogram.positive_spans
                == vec![BucketSpan {
                    offset: -3,
                    length: 3,
                }]
        );
        assert!(histogram.positive_counts == vec![5.1, 10.0, 7.0]);
        assert!(
            histogram.negative_spans
                == vec![BucketSpan {
                    offset: -5,
                    length: 2,
                }]
        );
        assert!(histogram.negative_counts == vec![4.1, 5.0]);
    }

    #[test]
    fn parses_prometheus_native_histogram_repetition() {
        let file = parse_test_file(
            r#"
load 1m
  latency{job="api"} {{schema:1 sum:15 count:10 buckets:[3 2 5 7 9]}}x2
"#,
        )
        .unwrap();

        let Statement::Load { series, .. } = &file.statements[0] else {
            panic!("expected load statement");
        };
        assert!(series[0].values.len() == 3);
        for value in &series[0].values {
            let SampleSpec::Histogram(histogram) = value else {
                panic!("expected native histogram sample");
            };
            assert!(histogram.schema == 1);
            assert_float(histogram.sum, 15.0);
            assert_float(histogram.count, 10.0);
            assert!(histogram.positive_counts == vec![3.0, 2.0, 5.0, 7.0, 9.0]);
        }
    }

    #[test]
    fn parses_prometheus_native_histogram_expansion_with_offset_step() {
        let file = parse_test_file(
            r#"
load 1m
  latency{job="api"} {{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:2 count:1 buckets:[1] offset:1}}x1
"#,
        )
        .unwrap();

        let Statement::Load { series, .. } = &file.statements[0] else {
            panic!("expected load statement");
        };
        assert!(series[0].values.len() == 2);
        let SampleSpec::Histogram(histogram) = &series[0].values[1] else {
            panic!("expected native histogram sample");
        };
        assert_float(histogram.sum, 6.0);
        assert_float(histogram.count, 5.0);
        assert!(
            histogram.positive_spans
                == vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }]
        );
        assert!(histogram.positive_counts == vec![1.0, 3.0, 1.0]);
        assert!(histogram.reset_hint == ResetHint::Unknown);
    }

    fn assert_float(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn testkit_runs_inline_range_eval() {
        let file = parse_test_file(
            r#"
load 1m
  up{job="api"} 0+1x2

eval range from 0m to 2m step 1m up{job="api"}
  up{job="api"} 0 1 2
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_runs_inline_native_histogram_range_eval() {
        let file = parse_test_file(
            r#"
load 1m
  latency{job="api"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:8 count:6 buckets:[1 4 1]}}

eval range from 0m to 1m step 1m latency{job="api"}
  latency{job="api"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:8 count:6 buckets:[1 4 1]}}
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }

    #[tokio::test]
    async fn testkit_loads_stale_markers() {
        let file = parse_test_file(
            r#"
load 1m
  up{job="api"} 1 stale

eval instant at 1m up{job="api"}
"#,
        )
        .unwrap();

        testkit::run_test_file(&file).await.unwrap();
    }
}
