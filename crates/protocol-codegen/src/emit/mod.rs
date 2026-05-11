pub mod api_key_enum;
pub mod borrowed;
pub mod common;
pub mod default_json;
pub mod differential_table;
pub mod mod_rs;
pub mod owned;
pub mod wrappers;
pub use crate::emit::owned::EmitError;

/// The output of a single emitter run for one `MessageSpec`.
///
/// `primary` is the body of the main generated `.rs` file.
/// `commons` contains one entry per top-level `commonStruct` in the schema;
/// each entry is `(struct_name, file_body)`. For the current curated set,
/// `commons` is always empty (`DescribeGroups` uses inline nested structs, not
/// top-level commonStructs). The field is included so future schemas with real
/// commonStructs can be wired up without changing the API again.
pub struct EmittedMessage {
    pub primary: String,
    pub commons: Vec<(String, String)>,
}
