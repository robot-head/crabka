use assert2::assert;
use std::fs;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::{Decode, Encode};
use serde::Deserialize;

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

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn load_pair(stem: &Path) -> (Meta, Vec<u8>) {
    let hex_path = stem.with_extension("hex");
    let toml_path = stem.with_extension("toml");
    let hex_raw: String = fs::read_to_string(hex_path)
        .unwrap()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let bytes = hex::decode(hex_raw).unwrap();
    let meta: Meta = toml::from_str(&fs::read_to_string(toml_path).unwrap()).unwrap();
    (meta, bytes)
}

#[test]
fn corpus_round_trips() {
    let dir = corpus_dir();
    let mut entries = 0;
    for e in fs::read_dir(&dir).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") {
            continue;
        }
        let stem = path.with_extension("");
        let (meta, bytes) = load_pair(&stem);
        entries += 1;

        match (meta.api_key, meta.direction.as_str()) {
            (18, "request") => {
                let mut cur = &bytes[..];
                let decoded = ApiVersionsRequest::decode(&mut cur, meta.version).unwrap();
                assert!(cur.is_empty(), "trailing bytes in {}", stem.display());
                let mut re = BytesMut::new();
                decoded.encode(&mut re, meta.version).unwrap();
                assert!(re.as_ref() == bytes, "byte mismatch in {}", stem.display());
            }
            _ => panic!("unhandled corpus entry: {}", stem.display()),
        }
    }
    assert!(entries > 0, "corpus is empty");
}
