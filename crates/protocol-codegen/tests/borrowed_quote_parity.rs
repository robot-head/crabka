//! Token-stream equality between the quote-based `borrowed_quote` emitter and
//! the proven string `borrowed` emitter, for every vendored schema. Equal tokens
//! ⇒ wire-identical (the leaf codecs are reused; the scaffolding parses to the
//! same AST). See `owned_quote_parity` for the rationale.
//!
//! Run: `cargo test -p crabka-protocol-codegen --test borrowed_quote_parity`

use std::path::PathBuf;

use crabka_protocol_codegen::emit::{borrowed, borrowed_quote};
use crabka_protocol_codegen::ir;
use quote::ToTokens;

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
}

fn tokens(src: &str, who: &str, name: &str) -> String {
    syn::parse_str::<syn::File>(src)
        .unwrap_or_else(|e| panic!("{who} output for {name} did not parse: {e}"))
        .into_token_stream()
        .to_string()
}

#[test]
fn borrowed_quote_matches_string_emitter() {
    let specs = ir::load_dir(&schemas_dir()).expect("schemas load");
    let mut mismatches = Vec::new();
    let mut checked = 0;
    for spec in &specs {
        if spec.valid_versions.is_empty() {
            continue;
        }
        let s = borrowed::emit(spec, "test", None)
            .expect("string emit")
            .primary;
        let q = borrowed_quote::emit(spec, "test", None)
            .expect("quote emit")
            .primary;
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
        "borrowed_quote token streams diverged from borrowed for {} schema(s): {}",
        mismatches.len(),
        mismatches.join(", ")
    );
}
