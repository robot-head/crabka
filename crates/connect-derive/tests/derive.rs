use std::path::Path;

#[allow(clippy::unnecessary_wraps)]
fn ui_compile_failure(path: &Path) -> datatest_stable::Result<()> {
    let cases = trybuild::TestCases::new();
    cases.compile_fail(path);
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn pass_case(path: &Path) -> datatest_stable::Result<()> {
    let cases = trybuild::TestCases::new();
    cases.pass(path);
    Ok(())
}

datatest_stable::harness! {
    { test = ui_compile_failure, root = "tests/ui", pattern = r".*\.rs$" },
    { test = pass_case, root = "tests/pass", pattern = r".*\.rs$" },
}
