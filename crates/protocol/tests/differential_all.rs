//! Parameterised differential sweep over every active (api_key, version) pair.
//!
//! For each case in the generated CASES table, encodes the Rust default fixture,
//! sends the equivalent JSON to the JVM oracle, and asserts byte equality.
//! All failures are collected and reported at the end so a single run reveals
//! every divergence (not just the first).

mod support;
use support::oracle;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

#[test]
#[ignore = "requires JVM oracle"]
fn every_pair_byte_equal() {
    let mut o = oracle::shared();
    let mut failures: Vec<String> = Vec::new();
    for case in CASES {
        let rust_bytes = encode_default(case.name, case.version);
        let json = default_json_for(case.name);
        let jvm_bytes = match case.kind {
            Kind::Request => o.encode(case.api_key, case.version, true, &json),
            Kind::Response => o.encode(case.api_key, case.version, false, &json),
            Kind::RequestHeader => o.header_encode("request", case.version, &json),
            Kind::ResponseHeader => o.header_encode("response", case.version, &json),
        };
        if rust_bytes != jvm_bytes {
            failures.push(format!(
                "{}[{}] v{}: rust={} ({} bytes), jvm={} ({} bytes), first_diff_at={}",
                case.name,
                match case.kind {
                    Kind::Request => "req",
                    Kind::Response => "resp",
                    Kind::RequestHeader => "rhdr",
                    Kind::ResponseHeader => "shdr",
                },
                case.version,
                hex::encode(&rust_bytes),
                rust_bytes.len(),
                hex::encode(&jvm_bytes),
                jvm_bytes.len(),
                first_diff(&rust_bytes, &jvm_bytes),
            ));
        }
    }
    if !failures.is_empty() {
        panic!(
            "{} pair(s) failed differential:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

fn first_diff(a: &[u8], b: &[u8]) -> usize {
    let min = a.len().min(b.len());
    for i in 0..min {
        if a[i] != b[i] {
            return i;
        }
    }
    min
}
