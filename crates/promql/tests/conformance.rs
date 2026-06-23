//! Runs the vendored Prometheus `.test` subsets through the engine via the
//! in-memory store. The headline conformance signal for Slice 2.

use assert2::assert;
use crabka_promql::testkit::{run_corpus_dir, run_test_path};
use std::path::PathBuf;

const CORPUS_DIR: &str = "tests/testdata";

async fn run_file(path: &str) {
    run_test_path(path).await.expect("conformance");
}

fn report_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/promql-conformance-report.txt")
}

#[tokio::test]
async fn full_corpus_gate_writes_per_file_report() {
    let report = run_corpus_dir(CORPUS_DIR).await;

    report.write_to(report_path()).expect("write report");
    let failing = report
        .files
        .iter()
        .filter(|file| !file.passed)
        .collect::<Vec<_>>();
    println!("{report}");

    assert!(!report.files.is_empty());
    assert!(failing.is_empty(), "promql .test regressions: {failing:?}");
}

#[tokio::test]
async fn aggregators_subset_conforms() {
    run_file("tests/testdata/aggregators.test").await;
}

#[tokio::test]
async fn at_modifier_subset_conforms() {
    run_file("tests/testdata/at_modifier.test").await;
}

#[tokio::test]
async fn collision_subset_conforms() {
    run_file("tests/testdata/collision.test").await;
}

#[tokio::test]
async fn duration_expression_subset_conforms() {
    run_file("tests/testdata/duration_expression.test").await;
}

#[tokio::test]
async fn extended_vectors_subset_conforms() {
    run_file("tests/testdata/extended_vectors.test").await;
}

#[tokio::test]
async fn functions_subset_conforms() {
    run_file("tests/testdata/functions.test").await;
}

#[tokio::test]
async fn histograms_subset_conforms() {
    run_file("tests/testdata/histograms.test").await;
}

#[tokio::test]
async fn info_subset_conforms() {
    run_file("tests/testdata/info.test").await;
}

#[tokio::test]
async fn literals_subset_conforms() {
    run_file("tests/testdata/literals.test").await;
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn limit_subset_conforms() {
    run_file("tests/testdata/limit.test").await;
}

#[tokio::test]
async fn name_label_dropping_subset_conforms() {
    run_file("tests/testdata/name_label_dropping.test").await;
}

#[tokio::test]
async fn native_histograms_subset_conforms() {
    run_file("tests/testdata/native_histograms.test").await;
}

#[tokio::test]
async fn operators_subset_conforms() {
    run_file("tests/testdata/operators.test").await;
}

#[tokio::test]
async fn ranges_subset_conforms() {
    run_file("tests/testdata/ranges.test").await;
}

#[tokio::test]
async fn range_queries_subset_conforms() {
    run_file("tests/testdata/range_queries.test").await;
}

#[tokio::test]
async fn selectors_subset_conforms() {
    run_file("tests/testdata/selectors.test").await;
}

#[tokio::test]
async fn staleness_subset_conforms() {
    run_file("tests/testdata/staleness.test").await;
}

#[tokio::test]
async fn subquery_subset_conforms() {
    run_file("tests/testdata/subquery.test").await;
}

#[tokio::test]
async fn trig_functions_subset_conforms() {
    run_file("tests/testdata/trig_functions.test").await;
}

#[tokio::test]
async fn type_and_unit_subset_conforms() {
    run_file("tests/testdata/type_and_unit.test").await;
}
