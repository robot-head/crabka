use std::path::Path;

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
    { test = ui_compile_failure, root = "tests/ui", pattern = r".*\.rs$" },
    { test = pass_case, root = "tests/pass", pattern = r".*\.rs$" },
}
