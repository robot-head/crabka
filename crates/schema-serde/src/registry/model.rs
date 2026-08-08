//! Confluent Schema Registry REST DTOs.

use serde::{Deserialize, Serialize};

/// POST body for register and lookup.
///
/// An omitted `schema_type` means AVRO.
#[derive(Debug, Serialize)]
pub struct SchemaPayload<'a> {
    pub schema: &'a str,
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<&'a str>,
    #[serde(rename = "messageType", skip_serializing_if = "Option::is_none")]
    pub message_type: Option<&'a str>,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub references: &'a [SchemaReference],
}

/// A reference to another registered schema.
///
/// The register and lookup payloads send an empty reference list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// Response of register: `POST /subjects/{s}/versions`.
#[derive(Debug, Deserialize)]
pub struct RegisterResponse {
    pub id: u32,
}

/// Response of lookup `POST /subjects/{s}` and of the version GETs.
#[derive(Debug, Deserialize)]
pub struct SubjectVersionResponse {
    pub id: u32,
    #[serde(default)]
    pub version: i32,
    #[serde(default)]
    pub schema: String,
    #[serde(rename = "schemaType", default)]
    pub schema_type: Option<String>,
    #[serde(rename = "messageType", default)]
    pub message_type: Option<String>,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Response of `GET /schemas/ids/{id}`.
#[derive(Debug, Deserialize)]
pub struct SchemaByIdResponse {
    pub schema: String,
    #[serde(rename = "schemaType", default)]
    pub schema_type: Option<String>,
    #[serde(rename = "messageType", default)]
    pub message_type: Option<String>,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}
