//! Checked-in `PromQL` corpus gate that also emits the CI artifact.

use crabka_promql::testkit::run_corpus_dir;

#[tokio::test]
async fn checked_in_corpus_is_green_and_writes_report() {
    let report = run_corpus_dir("tests/testdata").await;
    report
        .write_to("../../target/promql-conformance-report.txt")
        .expect("write PromQL conformance report");
    assert!(!report.files.is_empty(), "the PromQL corpus did not run");
    assert!(
        report.files.iter().all(|file| file.passed),
        "PromQL conformance failures; see ../../target/promql-conformance-report.txt"
    );
}
