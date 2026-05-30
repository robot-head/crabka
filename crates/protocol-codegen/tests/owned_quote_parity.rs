//! Validates the quote-based `owned_quote` emitter against the proven string
//! `owned` emitter for every vendored schema by comparing their **token
//! streams** (parsed with syn, so comments and whitespace are normalized away).
//!
//! Equal tokens ⇒ wire-identical by construction: the leaf codecs are literally
//! reused from `owned`, and the surrounding scaffolding parses to the same AST.
//! The two emitters differ only cosmetically — the string emitter writes a few
//! explanatory `//` comments and dense one-line bodies — which carry no tokens
//! and so drop out of this comparison.
//!
//! Run: `cargo test -p crabka-protocol-codegen --test owned_quote_parity`

use assert2::assert;
use std::path::PathBuf;

use crabka_protocol_codegen::emit::{owned, owned_quote};
use crabka_protocol_codegen::ir;
use quote::ToTokens;

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
}

/// Canonical token text for a Rust source file (comments stripped, spacing
/// normalized by the parser + pretty-printer).
fn tokens(src: &str, who: &str, name: &str) -> String {
    syn::parse_str::<syn::File>(src)
        .unwrap_or_else(|e| panic!("{who} output for {name} did not parse: {e}"))
        .into_token_stream()
        .to_string()
}

#[test]
fn owned_quote_matches_string_emitter() {
    let specs = ir::load_dir(&schemas_dir()).expect("schemas load");
    let mut mismatches = Vec::new();
    let mut checked = 0;
    for spec in &specs {
        if spec.valid_versions.is_empty() {
            continue;
        }
        let s = owned::emit(spec, "test").expect("string emit").primary;
        let q = owned_quote::emit(spec, "test").expect("quote emit").primary;
        checked += 1;
        if tokens(&s, "string", &spec.name) != tokens(&q, "quote", &spec.name) {
            mismatches.push(spec.name.clone());
        }
    }
    eprintln!(
        "checked {checked} schemas, {} mismatch(es)",
        mismatches.len()
    );
    assert!(
        mismatches.is_empty(),
        "owned_quote token streams diverged from owned for {} schema(s): {}",
        mismatches.len(),
        mismatches.join(", ")
    );
}
