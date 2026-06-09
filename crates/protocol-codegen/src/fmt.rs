//! Format generated Rust source with `prettyplease`.
//!
//! The quote-based emitters build a `proc_macro2::TokenStream` and render it
//! with `TokenStream::to_string()`, which is valid but unformatted Rust (no
//! line breaks or indentation). `prettyplease` is the secondary processing step
//! that turns that into the canonical, human-readable form committed under
//! `crates/protocol/generated`.
//!
//! Parsing doubles as validation: malformed generated code fails before write,
//! surfacing codegen bugs at regeneration time rather than three crates
//! downstream when the generated file is compiled.

#[derive(Debug, thiserror::Error)]
pub enum FmtError {
    #[error("generated source is not valid Rust: {0}")]
    Parse(#[from] syn::Error),
}

/// Run `src` through `prettyplease` and return the formatted output.
///
/// `src` must be a complete Rust source file (the generated files all are). A
/// leading `//`-style banner comment is preserved.
pub fn prettyplease(src: &str) -> Result<String, FmtError> {
    let (banner, body) = split_leading_line_comments(src);
    let file = syn::parse_file(body)?;
    Ok(format!("{banner}{}", prettyplease::unparse(&file)))
}

fn split_leading_line_comments(src: &str) -> (&str, &str) {
    let mut split = 0;
    let mut saw_comment = false;
    for line in src.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            saw_comment = true;
            split += line.len();
        } else if saw_comment && trimmed.is_empty() {
            split += line.len();
        } else {
            break;
        }
    }
    src.split_at(split)
}
