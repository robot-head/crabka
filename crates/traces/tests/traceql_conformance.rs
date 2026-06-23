use assert2::assert;

const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[];

#[test]
fn full_traceql_golden_corpus_passes() {
    let corpus_dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../traceql/tests/testdata/traceql"
    );
    let report = crabka_traceql::testkit::run_corpus_dir(corpus_dir);
    let report_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/traceql-conformance-report.txt"
    );
    report.write_to(report_path).unwrap();
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

    assert!(
        failing.is_empty(),
        "traceql golden regressions: {failing:?}"
    );
}
