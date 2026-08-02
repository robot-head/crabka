use std::path::Path;

use assert2::assert;

fn corpus_root() -> String {
    let root = std::env::var_os("TEST_SRCDIR")
        .map(|root| {
            std::path::PathBuf::from(root).join("_main/crates/traceql/tests/testdata/traceql")
        })
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/traceql")
        });
    std::fs::canonicalize(root.join("metrics.case"))
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

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
    { test = golden_case_file, root = corpus_root(), pattern = r".*\.case$" },
}
