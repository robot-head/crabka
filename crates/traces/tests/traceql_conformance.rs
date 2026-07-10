use std::path::Path;

const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[];

#[allow(clippy::unnecessary_wraps)]
fn traceql_case_file(path: &Path) -> datatest_stable::Result<()> {
    let report = crabka_traceql::testkit::run_corpus_file(path);
    println!("{}", report.to_text());

    let failing = report
        .cases
        .iter()
        .filter(|case| {
            !case.passed
                && !KNOWN_UNSUPPORTED
                    .iter()
                    .any(|(name, _)| case.name.ends_with(name))
        })
        .collect::<Vec<_>>();

    assert2::assert!(failing.is_empty());
    Ok(())
}

datatest_stable::harness! {
    { test = traceql_case_file, root = "../traceql/tests/testdata/traceql", pattern = r".*\.case$" },
}
