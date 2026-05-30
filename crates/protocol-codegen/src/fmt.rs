//! Format generated Rust source by shelling out to `rustfmt`.
//!
//! The quote-based emitters build a `proc_macro2::TokenStream` and render it
//! with `TokenStream::to_string()`, which is valid but unformatted Rust (no
//! line breaks or indentation). `rustfmt` is the secondary processing step that
//! turns that into the canonical, human-readable form committed under
//! `crates/protocol/generated`.
//!
//! rustfmt doubles as validation: malformed generated code makes it exit
//! non-zero, surfacing codegen bugs at regeneration time rather than three
//! crates downstream when the generated file is compiled.

use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum FmtError {
    #[error("spawning rustfmt (is it on PATH? `rustup component add rustfmt`): {0}")]
    Spawn(#[source] std::io::Error),
    #[error("rustfmt I/O: {0}")]
    Io(#[source] std::io::Error),
    #[error("rustfmt rejected generated source (status {status}):\n{stderr}")]
    Rejected { status: String, stderr: String },
}

/// Run `src` through `rustfmt` (edition 2024) and return the formatted output.
///
/// `src` must be a complete Rust source file (the generated files all are). A
/// leading `//`-style banner comment is preserved.
pub fn rustfmt(src: &str) -> Result<String, FmtError> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout", "--quiet"])
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
