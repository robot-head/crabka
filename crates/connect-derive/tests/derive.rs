use std::path::Path;

fn runfiles(path: &str, marker: &str) -> String {
    let root = std::env::var_os("TEST_SRCDIR")
        .map(|root| {
            std::path::PathBuf::from(root)
                .join("_main/crates/connect-derive")
                .join(path)
        })
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path));
    std::fs::canonicalize(root.join(marker))
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn ui_compile_failure(path: &Path) -> datatest_stable::Result<()> {
    std::fs::metadata(path)?;
    let cases = trybuild::TestCases::new();
    cases.compile_fail(path);
    Ok(())
}

fn pass_case(path: &Path) -> datatest_stable::Result<()> {
    std::fs::metadata(path)?;
    let cases = trybuild::TestCases::new();
    cases.pass(path);
    Ok(())
}

datatest_stable::harness! {
    { test = ui_compile_failure, root = runfiles("tests/ui", "unsupported_type.stderr"), pattern = r".*\.rs$" },
    { test = pass_case, root = runfiles("tests/pass", "crate_override.rs"), pattern = r".*\.rs$" },
}
