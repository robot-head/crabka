#[test]
fn ui_compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

#[test]
fn pass_cases() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/pass/*.rs");
}
