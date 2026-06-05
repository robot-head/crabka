//! Protobuf: parse a single `.proto` source into a `FileDescriptorProto`; dedup
//! key = deterministic prost encoding of the descriptor (source-info cleared so
//! formatting doesn't change the bytes). Confluent-exact canonical form is slice 2+.

use crate::error::SrError;
use super::ParsedSchema;

use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorProto;

pub struct ProtobufSchema(FileDescriptorProto);

pub fn parse(schema: &str) -> Result<ProtobufSchema, SrError> {
    protox_parse::parse("schema.proto", schema)
        .map(ProtobufSchema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))
}

impl ParsedSchema for ProtobufSchema {
    fn canonical_form(&self) -> String {
        // Clone the descriptor, clear source_code_info (formatting/comments)
        // and the file name so neither whitespace nor the synthetic filename
        // affects the dedup key. Then prost-encode to bytes and hex-encode.
        // NOTE: this is a descriptor-bytes key, not Confluent canonical form
        // (which is slice 2+).
        tracing::debug!("protobuf canonical_form: using descriptor-bytes key (not Confluent canonical form; slice 2+)");
        let mut d = self.0.clone();
        d.source_code_info = None;
        d.name = None;
        // hex-encode for a printable, stable string (lowercase, like `{:02x}`)
        hex::encode(d.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    const P: &str = "syntax = \"proto3\"; message User { int32 id = 1; }";

    #[test]
    fn parses_and_is_stable() {
        let a = parse(P).unwrap();
        let b = parse("syntax = \"proto3\";\nmessage User {\n  int32 id = 1;\n}\n").unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
    }

    #[test]
    fn rejects_invalid_proto() {
        assert!(parse("this is not protobuf").is_err());
    }
}
