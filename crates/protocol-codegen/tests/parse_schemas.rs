use std::path::PathBuf;

#[test]
fn every_vendored_schema_parses() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas");

    let specs = crabka_protocol_codegen::ir::load_dir(&dir).expect("schemas must parse");

    assert2::assert!(specs.len() > 50);

    // Sanity: ApiVersionsRequest is present.
    let api_versions = specs
        .iter()
        .find(|s| s.name == "ApiVersionsRequest")
        .unwrap();
    assert2::assert!(api_versions.valid_versions.contains(0));
    assert2::assert!(matches!(
        api_versions.message_type,
        crabka_protocol_codegen::ir::MessageType::Request
    ));
}
