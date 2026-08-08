//! Format generated Rust source: pretty-print the AST with `prettyplease`,
//! then refine through `rustfmt`.
//!
//! The quote-based emitters build a `proc_macro2::TokenStream` and render it
//! with `TokenStream::to_string()`, which is valid but unformatted Rust (no
//! line breaks, ` :: ` / ` . ` spacing). rustfmt alone is not enough. When a
//! line exceeds `max_width` and cannot be broken, for example the long
//! fully-qualified paths in `to_owned` such as
//! `crate::owned::common::<msg>::<struct>::TypeName`, rustfmt *gives up on
//! that item and emits it verbatim*. That leaves the raw `quote!` ` :: `
//! spacing.
//!
//! `prettyplease` pretty-prints the `syn` AST and always renders canonical,
//! correctly-spaced source whatever the line length, so this module runs it
//! first. The rustfmt pass then applies the project's canonical style to
//! everything it *can* fit, and leaves the unbreakable long lines clean, as
//! prettyplease rendered them. Both passes also work as validation. Malformed
//! generated code fails to parse or exits non-zero here, so codegen bugs
//! surface at regeneration time rather than three crates downstream.

use std::{
    io::Write,
    process::{Command, Stdio},
};

#[derive(Debug, thiserror::Error)]
pub enum FmtError {
    #[error("spawning rustfmt (is it on PATH? `rustup component add rustfmt`): {0}")]
    Spawn(#[source] std::io::Error),
    #[error("rustfmt I/O: {0}")]
    Io(#[source] std::io::Error),
    #[error("rustfmt rejected generated source (status {status}):\n{stderr}")]
    Rejected { status: String, stderr: String },
}

/// Pretty-print `src` with prettyplease, refine it through `rustfmt` at
/// edition 2024, and return the formatted output.
///
/// `src` must be a complete Rust source file, which every generated file is.
/// This function keeps the leading `//`-style banner comment. `syn` discards
/// line comments, so the function removes the banner before it pretty-prints
/// and re-attaches the banner afterwards.
/// # Errors
/// Returns an error when the schema model is invalid or generated Rust cannot be formatted or written.
pub fn rustfmt(src: &str) -> Result<String, FmtError> {
    let (banner, body) = split_banner(src);
    // prettyplease normalizes spacing and breaks lines rustfmt would otherwise
    // give up on. If the body does not parse (a real codegen bug), fall back to
    // the raw body so the rustfmt pass surfaces the precise error.
    let body = match syn::parse_file(body) {
        Ok(file) => prettyplease::unparse(&file),
        Err(_) => body.to_string(),
    };
    run_rustfmt(&format!("{banner}{body}"))
}

/// Split the leading banner block off from the code body. The banner is the
/// contiguous run of blank and `//`-comment lines at the top of a generated
/// file.
fn split_banner(src: &str) -> (&str, &str) {
    let mut idx = 0;
    for line in src.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            idx += line.len();
        } else {
            break;
        }
    }
    src.split_at(idx)
}

fn run_rustfmt(src: &str) -> Result<String, FmtError> {
    let mut child = Command::new("rustfmt")
        .args([
            "--edition",
            "2024",
            "--emit",
            "stdout",
            "--quiet",
            "--config",
            "max_width=240",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(FmtError::Spawn)?;
    child
        .stdin
        .take()
        .expect("stdin was piped")
        .write_all(src.as_bytes())
        .map_err(FmtError::Io)?;
    let out = child.wait_with_output().map_err(FmtError::Io)?;
    if !out.status.success() {
        return Err(FmtError::Rejected {
            status: out.status.to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn split_banner_separates_leading_comment_block() {
        let src = "// AUTO-GENERATED foo\n// line two\n\npub struct X;\n";
        let (banner, body) = split_banner(src);
        assert_eq!(banner, "// AUTO-GENERATED foo\n// line two\n\n");
        assert_eq!(body, "pub struct X;\n");
    }

    #[test]
    fn split_banner_handles_no_banner() {
        let src = "pub struct X;\n";
        let (banner, body) = split_banner(src);
        assert_eq!(banner, "");
        assert_eq!(body, src);
    }

    /// Regression guard for the ` :: ` / ` . ` spacing bug: a `to_owned` whose
    /// fully-qualified return path is too long for rustfmt to fit in `max_width`.
    /// rustfmt gives up and emits the raw `quote!` token spacing verbatim.
    /// prettyplease must render it cleanly, and the banner must survive.
    #[test]
    fn formats_long_to_owned_path_without_spaced_tokens() {
        let banner = "// AUTO-GENERATED against deadbeef. Do not edit.\n\n";
        let body = "impl Foo { pub fn to_owned (& self) -> crate :: owned :: common :: \
            some_very_long_owning_message_name :: some_very_long_common_struct_name :: \
            SomeVeryLongCommonStructName { crate :: owned :: common :: \
            some_very_long_owning_message_name :: some_very_long_common_struct_name :: \
            SomeVeryLongCommonStructName { field_one : (self . field_one) , } } }";
        let out = rustfmt(&format!("{banner}{body}")).expect("rustfmt+prettyplease");
        check!(
            out.starts_with("// AUTO-GENERATED against deadbeef."),
            "banner must be preserved, got:\n{out}"
        );
        check!(!out.contains(" :: "), "spaced `::` survived:\n{out}");
        check!(!out.contains(" . "), "spaced `.` survived:\n{out}");
    }
}
