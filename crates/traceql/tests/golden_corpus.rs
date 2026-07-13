use std::path::Path;

use assert2::assert;

fn golden_case_file(path: &Path) -> datatest_stable::Result<()> {
    std::fs::metadata(path)?;
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
