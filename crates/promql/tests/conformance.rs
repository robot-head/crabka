//! Runs the vendored Prometheus `.test` subsets through the engine via the
//! in-memory store. The headline conformance signal for Slice 2.

use std::path::{Path, PathBuf};

use crabka_promql::testkit::run_test_path;

fn corpus_pattern() -> &'static str {
    if cfg!(feature = "experimental-functions") {
        r".*\.test$"
    } else {
        r"^(?!limit\.test$).*\.test$"
    }
}

fn corpus_root() -> String {
    let root = std::env::var_os("TEST_SRCDIR")
        .map(|root| PathBuf::from(root).join("_main/crates/promql/tests/testdata"))
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/testdata"));
    std::fs::canonicalize(root.join("aggregators.test"))
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned()
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
    { test = conformance_file, root = corpus_root(), pattern = corpus_pattern() },
}
