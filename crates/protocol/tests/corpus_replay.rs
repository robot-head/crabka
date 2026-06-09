use assert2::assert;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

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

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
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

#[test]
fn corpus_round_trips() {
    let dir = corpus_dir();
    let mut seen: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    let mut entries = 0;
    for e in fs::read_dir(&dir).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") {
            continue;
        }
        let stem = path.with_extension("");
        let (meta, bytes) = load_pair(&stem);
        entries += 1;
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
        assert!(
            seen.insert((meta.api_key, meta.version, is_request)),
            "duplicate corpus entry for {} v{} {}",
            meta.api_key,
            meta.version,
            meta.direction
        );
    }
    assert!(entries > 0, "corpus is empty");
}

/// The corpus must cover every Request/Response (`api_key`, version) pair in `CASES`.
#[test]
fn corpus_covers_all_pairs() {
    let mut want: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    for c in CASES {
        match c.kind {
            Kind::Request => {
                want.insert((c.api_key, c.version, true));
            }
            Kind::Response => {
                want.insert((c.api_key, c.version, false));
            }
            Kind::RequestHeader | Kind::ResponseHeader => {}
        }
    }
    let mut have: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    for e in fs::read_dir(corpus_dir()).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") {
            continue;
        }
        let (meta, _) = load_pair(&path.with_extension(""));
        have.insert((meta.api_key, meta.version, meta.direction == "request"));
    }
    let missing: Vec<_> = want.difference(&have).collect();
    assert!(
        missing.is_empty(),
        "corpus missing {} pair(s): {:?}",
        missing.len(),
        missing
    );
    // Reverse: no stale entry for a pair that is no longer a valid CASES
    // request/response (e.g. an out-of-range version left behind after a
    // schema-pin bump).
    let stale: Vec<_> = have.difference(&want).collect();
    assert!(
        stale.is_empty(),
        "corpus has {} stale pair(s) not in CASES: {:?}",
        stale.len(),
        stale
    );
}
