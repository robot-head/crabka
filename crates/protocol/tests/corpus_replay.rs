use std::{fs, path::Path};

use assert2::assert;
use serde::Deserialize;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

#[derive(Debug, Deserialize)]
struct Meta {
    api_key: i16,
    version: i16,
    direction: String,
    #[allow(dead_code)]
    source_kafka_version: String,
    #[allow(dead_code)]
    synthetic: bool,
    #[allow(dead_code)]
    description: String,
}

fn load_pair(stem: &Path) -> (Meta, Vec<u8>) {
    let hex_raw: String = fs::read_to_string(stem.with_extension("hex"))
        .unwrap()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let bytes = hex::decode(hex_raw).unwrap();
    let meta: Meta =
        toml::from_str(&fs::read_to_string(stem.with_extension("toml")).unwrap()).unwrap();
    (meta, bytes)
}

/// Map (`api_key`, direction) to the message name via the generated `CASES` table.
fn name_for(api_key: i16, is_request: bool) -> Option<&'static str> {
    CASES
        .iter()
        .find(|c| {
            c.api_key == api_key
                && matches!(
                    (c.kind, is_request),
                    (Kind::Request, true) | (Kind::Response, false)
                )
        })
        .map(|c| c.name)
}

#[allow(clippy::unnecessary_wraps)]
fn corpus_entry_round_trips(path: &Path) -> datatest_stable::Result<()> {
    let stem = path.with_extension("");
    let (meta, bytes) = load_pair(&stem);
    let is_request = match meta.direction.as_str() {
        "request" => true,
        "response" => false,
        other => panic!("bad direction {other} in {}", stem.display()),
    };
    let name = name_for(meta.api_key, is_request).unwrap_or_else(|| {
        panic!(
            "no CASES name for api_key {} in {}",
            meta.api_key,
            stem.display()
        )
    });
    let re = roundtrip(name, meta.version, &bytes);
    assert!(
        re == bytes,
        "byte mismatch in {} ({name} v{})",
        stem.display(),
        meta.version
    );
    Ok(())
}

datatest_stable::harness! {
    { test = corpus_entry_round_trips, root = "tests/corpus", pattern = r".*\.hex$" },
}
