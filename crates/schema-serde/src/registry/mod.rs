//! Async Confluent Schema Registry REST client.

pub mod model;

use model::{
    RegisterResponse, SchemaByIdResponse, SchemaPayload, SchemaReference, SubjectVersionResponse,
};
use reqwest::Client;

use crate::{error::SchemaSerdeError, subject::SchemaKind};

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

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
    pub async fn latest(&self, subject: &str) -> Result<SubjectVersionResponse, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions/latest", self.base_url);
        self.get_json(&url).await
    }

    /// Fetch the latest registered version's id under `subject`
    /// (`use.latest.version=true`).
    pub async fn latest_id(&self, subject: &str) -> Result<u32, SchemaSerdeError> {
        Ok(self.latest(subject).await?.id)
    }

    /// Fetch a schema's text and optional metadata by global id (deserialize path).
    pub async fn schema_by_id(&self, id: u32) -> Result<FetchedSchema, SchemaSerdeError> {
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let resp: SchemaByIdResponse = self.get_json(&url).await?;
        Ok(FetchedSchema {
            schema: resp.schema,
            message_type: resp.message_type,
        })
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
            .map_err(|e| SchemaSerdeError::Registry(e.to_string()))?;
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
            .map_err(|e| SchemaSerdeError::Registry(e.to_string()))?;
        Self::parse(resp).await
    }

    async fn parse<R: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<R, SchemaSerdeError> {
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SchemaSerdeError::Registry(e.to_string()))?;
        if !status.is_success() {
            return Err(SchemaSerdeError::RegistryStatus {
                status: status.as_u16(),
                body: text,
            });
        }
        serde_json::from_str(&text).map_err(|e| SchemaSerdeError::Registry(e.to_string()))
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
        check!(j.contains(r#""schemaType":"PROTOBUF""#));
        check!(j.contains(r#""messageType":"demo.Order""#));
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
