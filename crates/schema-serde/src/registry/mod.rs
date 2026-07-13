//! Async Confluent Schema Registry REST client.

pub mod model;

use std::collections::{HashMap, HashSet};

use model::{
    RegisterResponse, SchemaByIdResponse, SchemaPayload, SchemaReference, SubjectVersionResponse,
};
use reqwest::Client;

use crate::{error::SchemaSerdeError, subject::SchemaKind};

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
const MAX_REFERENCE_DEPTH: usize = 64;

/// Thin async client over the registry REST API. Cloneable (shares the
/// underlying `reqwest::Client` connection pool).
#[derive(Debug, Clone)]
pub struct RegistryClient {
    base_url: String,
    http: Client,
}

/// Schema text plus optional Crabka extension metadata fetched by global id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedSchema {
    pub schema: String,
    pub message_type: Option<String>,
    pub references: Vec<SchemaReference>,
}

impl RegistryClient {
    /// Build a client for a registry at `base_url` (e.g. `http://localhost:8081`).
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: Client::new(),
        }
    }

    /// Register `schema` under `subject`, returning its global id
    /// (`auto.register.schemas=true`).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn register(
        &self,
        subject: &str,
        kind: SchemaKind,
        schema: &str,
        message_type: Option<&str>,
    ) -> Result<u32, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions", self.base_url);
        let body = SchemaPayload {
            schema,
            schema_type: kind.wire_name(),
            message_type,
            references: &[] as &[SchemaReference],
        };
        let resp: RegisterResponse = self.post_json(&url, &body).await?;
        Ok(resp.id)
    }

    /// Look up the id of an already-registered `schema` under `subject`
    /// (`auto.register.schemas=false`).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn lookup(
        &self,
        subject: &str,
        kind: SchemaKind,
        schema: &str,
        message_type: Option<&str>,
    ) -> Result<u32, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}", self.base_url);
        let body = SchemaPayload {
            schema,
            schema_type: kind.wire_name(),
            message_type,
            references: &[] as &[SchemaReference],
        };
        let resp: SubjectVersionResponse = self.post_json(&url, &body).await?;
        Ok(resp.id)
    }

    /// Fetch the latest registered version's id under `subject`
    /// (`use.latest.version=true`).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn latest(&self, subject: &str) -> Result<SubjectVersionResponse, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions/latest", self.base_url);
        self.get_json(&url).await
    }

    /// Fetch the latest registered version's id under `subject`
    /// (`use.latest.version=true`).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn latest_id(&self, subject: &str) -> Result<u32, SchemaSerdeError> {
        Ok(self.latest(subject).await?.id)
    }

    /// Fetch a schema's text and optional metadata by global id (deserialize path).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn schema_by_id(&self, id: u32) -> Result<FetchedSchema, SchemaSerdeError> {
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let resp: SchemaByIdResponse = self.get_json(&url).await?;
        Ok(FetchedSchema {
            schema: resp.schema,
            message_type: resp.message_type,
            references: resp.references,
        })
    }

    /// Fetch a schema source by the subject and version named in a reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry request fails or its response cannot
    /// be decoded.
    pub async fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<FetchedSchema, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions/{version}", self.base_url);
        let resp: SubjectVersionResponse = self.get_json(&url).await?;
        Ok(FetchedSchema {
            schema: resp.schema,
            message_type: resp.message_type,
            references: resp.references,
        })
    }

    /// Resolve registry references to their exact, name-keyed source text.
    ///
    /// Resolution is bounded and detects subject-version cycles. It never uses
    /// the filesystem or any import mechanism beyond Schema Registry.
    ///
    /// # Errors
    ///
    /// Returns an error when a reference is cyclic, exceeds the depth limit,
    /// or cannot be fetched or decoded from the registry.
    pub async fn reference_sources(
        &self,
        references: &[SchemaReference],
    ) -> Result<HashMap<String, String>, SchemaSerdeError> {
        let mut sources = HashMap::new();
        let mut resolving = HashSet::new();
        self.resolve_reference_sources(references, &mut sources, &mut resolving, 0)
            .await?;
        Ok(sources)
    }

    async fn resolve_reference_sources(
        &self,
        references: &[SchemaReference],
        sources: &mut HashMap<String, String>,
        resolving: &mut HashSet<(String, i32)>,
        depth: usize,
    ) -> Result<(), SchemaSerdeError> {
        if depth >= MAX_REFERENCE_DEPTH {
            return Err(SchemaSerdeError::Schema(format!(
                "schema reference depth exceeds {MAX_REFERENCE_DEPTH}"
            )));
        }

        for reference in references {
            if sources.contains_key(&reference.name) {
                continue;
            }
            let reference_key = (reference.subject.clone(), reference.version);
            if !resolving.insert(reference_key.clone()) {
                return Err(SchemaSerdeError::Schema(format!(
                    "cyclic schema reference at {} version {}",
                    reference.subject, reference.version
                )));
            }
            let referenced_schema = self
                .schema_by_subject_version(&reference.subject, reference.version)
                .await?;
            Box::pin(self.resolve_reference_sources(
                &referenced_schema.references,
                sources,
                resolving,
                depth + 1,
            ))
            .await?;
            resolving.remove(&reference_key);
            sources.insert(reference.name.clone(), referenced_schema.schema);
        }
        Ok(())
    }

    async fn post_json<B: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<R, SchemaSerdeError> {
        let resp = self
            .http
            .post(url)
            .header("Content-Type", CONTENT_TYPE)
            .json(body)
            .send()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        Self::parse(resp).await
    }

    async fn get_json<R: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<R, SchemaSerdeError> {
        let resp = self
            .http
            .get(url)
            .header("Accept", CONTENT_TYPE)
            .send()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        Self::parse(resp).await
    }

    async fn parse<R: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<R, SchemaSerdeError> {
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        if !status.is_success() {
            return Err(SchemaSerdeError::RegistryStatus {
                status: status.as_u16(),
                body: text,
            });
        }
        serde_json::from_str(&text).map_err(|e| SchemaSerdeError::RegistryDecode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::{model::SchemaPayload, *};

    #[test]
    fn base_url_trims_trailing_slash() {
        let c = RegistryClient::new("http://localhost:8081/");
        check!(c.base_url == "http://localhost:8081");
    }

    #[test]
    fn payload_omits_avro_type_and_empty_refs() {
        let p = SchemaPayload {
            schema: "\"string\"",
            schema_type: SchemaKind::Avro.wire_name(),
            message_type: None,
            references: &[],
        };
        let j = serde_json::to_string(&p).unwrap();
        check!(j == r#"{"schema":"\"string\""}"#);
    }

    #[test]
    fn payload_includes_protobuf_type() {
        let p = SchemaPayload {
            schema: "syntax = \"proto3\";",
            schema_type: SchemaKind::Protobuf.wire_name(),
            message_type: Some("demo.Order"),
            references: &[],
        };
        let j = serde_json::to_string(&p).unwrap();
        check!(
            (
                j.contains(r#""schemaType":"PROTOBUF""#),
                j.contains(r#""messageType":"demo.Order""#),
            ) == (true, true)
        );
    }

    #[tokio::test]
    async fn register_posts_schema_payload_and_returns_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .and(body_json(serde_json::json!({
                "schema": "syntax = \"proto3\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Order"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 42
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let id = client
            .register(
                "orders-value",
                SchemaKind::Protobuf,
                "syntax = \"proto3\";",
                Some("demo.Order"),
            )
            .await
            .unwrap();

        check!(id == 42);
    }

    #[tokio::test]
    async fn lookup_posts_schema_payload_and_returns_existing_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value"))
            .and(body_json(serde_json::json!({
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 43,
                "version": 7,
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let id = client
            .lookup(
                "orders-value",
                SchemaKind::Json,
                r#"{"type":"object"}"#,
                None,
            )
            .await
            .unwrap();

        check!(id == 43);
    }

    #[tokio::test]
    async fn latest_id_fetches_latest_subject_version() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 44,
                "version": 9,
                "schema": "syntax = \"proto3\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Order"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        check!(client.latest_id("orders-value").await.unwrap() == 44);
    }
}
