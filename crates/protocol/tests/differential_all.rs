//! Parameterised differential sweep over every active `(api_key, version)` pair.
//!
//! For each case in the generated CASES table, encodes the Rust default fixture,
//! sends the equivalent JSON to the JVM oracle, and asserts byte equality.
//! The sweep collects all failures and reports them at the end, so a single run
//! reveals every divergence and not only the first one.

mod support;
use serde_json::json;
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
        let jval = default_json_for(case.name, case.version);

        let req = match case.kind {
            Kind::Request => json!({
                "op": "encode",
                "apiKey": case.api_key,
                "messageName": case.name,
                "version": case.version,
                "isRequest": true,
                "value": jval,
            }),
            Kind::Response => json!({
                "op": "encode",
                "apiKey": case.api_key,
                "messageName": case.name,
                "version": case.version,
                "isRequest": false,
                "value": jval,
            }),
            Kind::RequestHeader => json!({
                "op": "header_encode",
                "kind": "request",
                "version": case.version,
                "value": jval,
            }),
            Kind::ResponseHeader => json!({
                "op": "header_encode",
                "kind": "response",
                "version": case.version,
                "value": jval,
            }),
        };

        let result = o.try_call(&req);
        match result {
            Err(e) => {
                failures.push(format!(
                    "{}[{}] v{}: ORACLE_ERROR: {}",
                    case.name,
                    kind_str(case.kind),
                    case.version,
                    e,
                ));
            }
            Ok(resp) => {
                let jvm_bytes = hex::decode(resp["hex"].as_str().unwrap()).unwrap();
                if rust_bytes != jvm_bytes {
                    failures.push(format!(
                        "{}[{}] v{}: rust={} ({} bytes), jvm={} ({} bytes), first_diff_at={}",
                        case.name,
                        kind_str(case.kind),
                        case.version,
                        hex::encode(&rust_bytes),
                        rust_bytes.len(),
                        hex::encode(&jvm_bytes),
                        jvm_bytes.len(),
                        first_diff(&rust_bytes, &jvm_bytes),
                    ));
                }
            }
        }
    }
    assert2::assert!(failures.is_empty());
}

fn kind_str(k: Kind) -> &'static str {
    match k {
        Kind::Request => "req",
        Kind::Response => "resp",
        Kind::RequestHeader => "rhdr",
        Kind::ResponseHeader => "shdr",
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
