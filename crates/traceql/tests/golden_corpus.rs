use assert2::assert;
use std::path::Path;

#[allow(clippy::unnecessary_wraps)]
fn golden_case_file(path: &Path) -> datatest_stable::Result<()> {
    let report = crabka_traceql::testkit::run_corpus_file(path);
    println!("{}", report.to_text());

    let failing = report
        .cases
        .iter()
        .filter(|case| !case.passed)
        .collect::<Vec<_>>();

    assert!(
        failing.is_empty(),
        "traceql golden corpus failures: {failing:?}"
    );
    Ok(())
}

datatest_stable::harness! {
    { test = golden_case_file, root = "tests/testdata/traceql", pattern = r".*\.case$" },
}
