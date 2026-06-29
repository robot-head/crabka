//! Runs the vendored Prometheus `.test` subsets through the engine via the
//! in-memory store. The headline conformance signal for Slice 2.

use std::path::Path;

use crabka_promql::testkit::run_test_path;

fn corpus_pattern() -> &'static str {
    if cfg!(feature = "experimental-functions") {
        r".*\.test$"
    } else {
        r"^(?!limit\.test$).*\.test$"
    }
}

fn conformance_file(path: &Path) -> datatest_stable::Result<()> {
    let path = path
        .to_str()
        .expect("PromQL conformance corpus paths must be UTF-8");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("PromQL conformance runtime");
    rt.block_on(run_test_path(path))?;
    Ok(())
}

datatest_stable::harness! {
    { test = conformance_file, root = "tests/testdata", pattern = corpus_pattern() },
}
