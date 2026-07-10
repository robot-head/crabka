use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

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

fn load_meta(stem: &Path) -> Meta {
    toml::from_str(&fs::read_to_string(stem.with_extension("toml")).unwrap()).unwrap()
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
        let meta = load_meta(&path.with_extension(""));
        let is_request = meta.direction == "request";
        assert2::assert!(have.insert((meta.api_key, meta.version, is_request)));
    }
    let missing: Vec<_> = want.difference(&have).collect();
    assert2::assert!(missing.is_empty());
    // Reverse: no stale entry for a pair that is no longer a valid CASES
    // request/response (e.g. an out-of-range version left behind after a
    // schema-pin bump).
    let stale: Vec<_> = have.difference(&want).collect();
    assert2::assert!(stale.is_empty());
}
