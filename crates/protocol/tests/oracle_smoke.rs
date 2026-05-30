use assert2::assert;
mod support;
use support::oracle;

use serde_json::json;

#[test]
#[ignore = "requires JVM oracle built; see CONTRIBUTING"]
fn encode_apiversions_v0_empty() {
    let mut o = oracle::shared();
    let bytes = o.encode(18, 0, true, &json!({}));
    assert!(bytes.is_empty(), "v0 ApiVersionsRequest has empty body");
}
