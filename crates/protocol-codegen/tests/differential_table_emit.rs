//! Smoke test: the differential-table emitter produces the new dispatch fns.
use assert2::check;
use crabka_protocol_codegen::{
    emit::differential_table,
    ir::{FlexibleVersions, MessageSpec, MessageType, VersionRange},
};

fn req(name: &str, api_key: i16, min: i16, max: i16, flex_min: i16) -> MessageSpec {
    MessageSpec {
        name: name.to_string(),
        message_type: MessageType::Request,
        api_key: Some(api_key),
        valid_versions: VersionRange { min, max },
        flexible_versions: FlexibleVersions::Range(VersionRange {
            min: flex_min,
            max: i16::MAX,
        }),
        fields: vec![],
        common_structs: vec![],
        internal: false,
    }
}

#[test]
fn emits_roundtrip_and_header_helpers() {
    let specs = vec![req("ApiVersionsRequest", 18, 0, 3, 3)];
    let out = differential_table::emit(&specs, "testsha");
    // quote! token output uses spaces between punctuation tokens; match on
    // function name fragments that are stable regardless of exact token spacing.
    check!(out.contains("pub fn roundtrip"));
    check!(
        out.contains("name : & str , version : i16 , bytes : & [u8]")
            || out.contains("name: &str, version: i16, bytes: &[u8]")
    );
    check!(out.contains("pub fn request_header_version"));
    check!(out.contains("pub fn response_header_version"));
    check!(out.contains("pub fn strip_frame_header"));
    check!(out.contains("\"ApiVersionsResponse\" => 0"));
}
