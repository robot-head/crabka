//! HTTP client for a Confluent Schema Registry REST API.
//!
//! [`SchemaRegistryClient`] talks to the REST endpoints (`/subjects`,
//! `/schemas/ids`) and keeps two in-process caches:
//! - `by_id` — schema string + format, keyed on schema-id (immutable once
//!   registered; never evicted).
//! - `by_subject_latest` — schema-id of the latest version per subject, with
//!   a TTL so topology changes are picked up within a bounded window.

use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::codec::{CodecError, SchemaFormat};

/// TTL for `by_subject_latest` cache entries.
const LATEST_TTL: Duration = Duration::from_secs(5);

/// A caching HTTP client for a Confluent-compatible Schema Registry.
#[derive(Debug)]
pub struct SchemaRegistryClient {
    /// Underlying HTTP client (shared, connection-pooled).
    pub http: reqwest::Client,
    /// Base URL of the Schema Registry (e.g. `http://localhost:8081`).
    pub base: Url,
    /// Cache: schema-id → (schema string, format). Immutable once registered.
    pub by_id: DashMap<i32, (String, SchemaFormat)>,
    /// Cache: subject → (latest schema-id, fetched-at timestamp for TTL).
    pub by_subject_latest: DashMap<String, (i32, Instant)>,
}

// ── Confluent wire shapes ────────────────────────────────────────────────────

#[derive(Serialize)]
struct RegisterBody<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType")]
    schema_type: &'a str,
}

#[derive(Deserialize)]
struct RegisterResponse {
    id: i32,
}

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Deserialize)]
struct LatestResponse {
    id: i32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

// ── Format helpers ───────────────────────────────────────────────────────────

/// Map [`SchemaFormat`] to the Confluent `schemaType` string.
#[must_use]
pub fn fmt_to_str(fmt: SchemaFormat) -> &'static str {
    match fmt {
        SchemaFormat::Avro => "AVRO",
        SchemaFormat::Json => "JSON",
        SchemaFormat::Protobuf => "PROTOBUF",
    }
}

/// Parse a Confluent `schemaType` string (or absent value) into [`SchemaFormat`].
/// An absent or empty `schemaType` defaults to Avro (Confluent default).
#[must_use]
pub fn str_to_fmt(s: Option<&str>) -> SchemaFormat {
    match s {
        Some("JSON") => SchemaFormat::Json,
        Some("PROTOBUF") => SchemaFormat::Protobuf,
        _ => SchemaFormat::Avro, // "AVRO", absent, or empty all → Avro
    }
}

/// Percent-encode a subject name for safe inclusion in a URL path segment.
///
/// Uses `url`'s `form_urlencoded` [`byte_serialize`](url::form_urlencoded::byte_serialize)
/// so that `/` and other special characters in subject names are percent-encoded.
fn encode_subject(subject: &str) -> String {
    url::form_urlencoded::byte_serialize(subject.as_bytes()).collect()
}

// ── Client ───────────────────────────────────────────────────────────────────

impl SchemaRegistryClient {
    /// Construct a new client pointing at `base_url`.
    ///
    /// Returns [`CodecError::Registry`] if `base_url` is not a valid URL or
    /// the underlying HTTP client cannot be built.
    pub fn new(base_url: &str) -> Result<Self, CodecError> {
        let base = Url::parse(base_url)
            .map_err(|e| CodecError::Registry(format!("invalid schema registry URL: {e}")))?;
        let http = reqwest::Client::new();
        Ok(Self {
            http,
            base,
            by_id: DashMap::new(),
            by_subject_latest: DashMap::new(),
        })
    }

    /// Register `schema` (expressed as `fmt`) under `subject`.
    ///
    /// Returns the assigned schema id.  If the schema is already registered
    /// the registry returns the existing id.
    pub async fn register(
        &self,
        subject: &str,
        schema: &str,
        fmt: SchemaFormat,
    ) -> Result<i32, CodecError> {
        let url = self
            .base
            .join(&format!("subjects/{}/versions", encode_subject(subject)))
            .map_err(|e| CodecError::Registry(format!("URL build error: {e}")))?;

        let body = RegisterBody {
            schema,
            schema_type: fmt_to_str(fmt),
        };

        let resp = self
            .http
            .post(url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/vnd.schemaregistry.v1+json",
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| CodecError::Registry(format!("registry transport error: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            let parsed: RegisterResponse = resp
                .json()
                .await
                .map_err(|e| CodecError::Registry(format!("registry response parse error: {e}")))?;
            // Populate by_id cache on successful registration.
            self.by_id.insert(parsed.id, (schema.to_owned(), fmt));
            Ok(parsed.id)
        } else if status.is_client_error() {
            let text = resp.text().await.unwrap_or_default();
            Err(CodecError::Validate(format!(
                "registry rejected schema (HTTP {status}): {text}"
            )))
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(CodecError::Registry(format!(
                "registry error (HTTP {status}): {text}"
            )))
        }
    }

    /// Resolve a schema string and its format by numeric id.
    ///
    /// Results are cached in [`Self::by_id`] indefinitely (schema ids are
    /// immutable once assigned).
    pub async fn schema_by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
        // Cache hit — return clone without network.
        if let Some(entry) = self.by_id.get(&id) {
            return Ok(entry.clone());
        }

        let url = self
            .base
            .join(&format!("schemas/ids/{id}"))
            .map_err(|e| CodecError::Registry(format!("URL build error: {e}")))?;

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| CodecError::Registry(format!("registry transport error: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 404 {
            return Err(CodecError::Registry(format!("schema id {id} not found")));
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(CodecError::Registry(format!(
                "registry error (HTTP {status}): {text}"
            )));
        }

        let parsed: SchemaByIdResponse = resp
            .json()
            .await
            .map_err(|e| CodecError::Registry(format!("registry response parse error: {e}")))?;

        let fmt = str_to_fmt(parsed.schema_type.as_deref());
        let entry = (parsed.schema, fmt);
        self.by_id.insert(id, entry.clone());
        Ok(entry)
    }

    /// Return the latest `(id, schema, format)` tuple for `subject`.
    ///
    /// Results are cached in [`Self::by_subject_latest`] and re-fetched after
    /// a TTL to pick up new versions.
    pub async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
        // Check cache: if the subject entry is fresh AND by_id has the schema, return it.
        if let Some(entry) = self.by_subject_latest.get(subject) {
            let (cached_id, fetched_at) = *entry;
            if fetched_at.elapsed() < LATEST_TTL
                && let Some(schema_entry) = self.by_id.get(&cached_id)
            {
                let (schema, fmt) = schema_entry.clone();
                return Ok((cached_id, schema, fmt));
            }
        }

        let url = self
            .base
            .join(&format!(
                "subjects/{}/versions/latest",
                encode_subject(subject)
            ))
            .map_err(|e| CodecError::Registry(format!("URL build error: {e}")))?;

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| CodecError::Registry(format!("registry transport error: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 404 {
            return Err(CodecError::Registry(format!(
                "subject '{subject}' not found"
            )));
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(CodecError::Registry(format!(
                "registry error (HTTP {status}): {text}"
            )));
        }

        let parsed: LatestResponse = resp
            .json()
            .await
            .map_err(|e| CodecError::Registry(format!("registry response parse error: {e}")))?;

        let fmt = str_to_fmt(parsed.schema_type.as_deref());
        // Populate both caches.
        self.by_id.insert(parsed.id, (parsed.schema.clone(), fmt));
        self.by_subject_latest
            .insert(subject.to_owned(), (parsed.id, Instant::now()));

        Ok((parsed.id, parsed.schema, fmt))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── fmt helpers ──────────────────────────────────────────────────────────

    #[test]
    fn fmt_roundtrip_cases() {
        for (_name, format, wire_name) in [
            ("avro", SchemaFormat::Avro, "AVRO"),
            ("json", SchemaFormat::Json, "JSON"),
            ("protobuf", SchemaFormat::Protobuf, "PROTOBUF"),
        ] {
            assert2::assert!(fmt_to_str(format) == wire_name);
            assert2::assert!(str_to_fmt(Some(wire_name)) == format);
        }
    }

    #[test]
    fn str_to_fmt_default_cases() {
        for (_name, input) in [("absent", None), ("empty", Some(""))] {
            assert2::assert!(str_to_fmt(input) == SchemaFormat::Avro);
        }
    }

    // ── new() URL parsing ────────────────────────────────────────────────────

    #[test]
    fn new_valid_url_ok() {
        let client = SchemaRegistryClient::new("http://localhost:8081").unwrap();
        assert2::assert!(client.base.host_str() == Some("localhost"));
        assert2::assert!(client.base.port() == Some(8081));
    }

    #[test]
    fn new_invalid_url_err() {
        let err = SchemaRegistryClient::new("not-a-url").unwrap_err();
        let msg = err.to_string();
        assert2::assert!(msg.contains("invalid schema registry URL"));
    }

    // ── by_id cache hit (no network) ────────────────────────────────────────

    #[test]
    fn schema_by_id_cache_hit() {
        // Pre-populate the cache and assert schema_by_id returns it without
        // making a network call.  We drive this synchronously since there is
        // no async path taken on a cache hit.
        let client = SchemaRegistryClient::new("http://localhost:8081").unwrap();
        client
            .by_id
            .insert(42, ("{}".to_owned(), SchemaFormat::Json));

        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(client.schema_by_id(42));

        assert2::assert!(result.unwrap() == ("{}".to_string(), SchemaFormat::Json));
    }

    // ── latest cache hit (no network) ───────────────────────────────────────

    #[test]
    fn latest_cache_hit() {
        let client = SchemaRegistryClient::new("http://localhost:8081").unwrap();
        // Pre-populate both caches with a fresh entry.
        client
            .by_id
            .insert(7, ("{\"type\":\"null\"}".to_owned(), SchemaFormat::Avro));
        client
            .by_subject_latest
            .insert("test-value".to_owned(), (7, Instant::now()));

        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(client.latest("test-value"));

        assert2::assert!(
            result.unwrap() == (7, "{\"type\":\"null\"}".to_string(), SchemaFormat::Avro)
        );
    }

    // ── mock server tests (axum + tokio) ────────────────────────────────────

    /// Spin up a tiny axum server (axum 0.8 `{param}` syntax) handling:
    ///   POST /subjects/{subject}/versions  → `{"id":1}`
    ///   GET  /schemas/ids/{id}             → `{"schema":"...","schemaType":"PROTOBUF"}`
    ///   GET  /subjects/{subject}/versions/latest → `{"id":2,"schema":"...","schemaType":"JSON"}`
    fn start_mock_server() -> (tokio::runtime::Runtime, u16, String) {
        use std::net::SocketAddr;

        use axum::{
            Json, Router,
            extract::Path,
            routing::{get, post},
        };
        use serde_json::json;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let app = Router::new()
            .route(
                "/subjects/{subject}/versions",
                post(|Path(subject): Path<String>, body: String| async move {
                    let _ = subject;
                    let _ = body;
                    Json(json!({"id": 1}))
                }),
            )
            .route(
                "/schemas/ids/{id}",
                get(|Path(id): Path<String>| async move {
                    if id == "1" {
                        Json(json!({"schema": "{\"type\":\"string\"}", "schemaType": "PROTOBUF"}))
                    } else {
                        Json(json!({"schema": "{}", "schemaType": "AVRO"}))
                    }
                }),
            )
            .route(
                "/subjects/{subject}/versions/latest",
                get(|Path(_subject): Path<String>| async move {
                    Json(json!({
                        "id": 2,
                        "schema": "{\"type\":\"record\"}",
                        "schemaType": "JSON"
                    }))
                }),
            );

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = rt.block_on(tokio::net::TcpListener::bind(addr)).unwrap();
        let port = listener.local_addr().unwrap().port();
        rt.spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let base_url = format!("http://127.0.0.1:{port}/");
        (rt, port, base_url)
    }

    #[test]
    fn register_posts_correct_body_and_populates_by_id() {
        let (rt, _port, base_url) = start_mock_server();

        rt.block_on(async {
            let client = SchemaRegistryClient::new(&base_url).unwrap();
            let schema = "{\"type\":\"string\"}";
            let id = client
                .register("my-subject", schema, SchemaFormat::Protobuf)
                .await
                .unwrap();
            // by_id should now have the entry.
            let cached = client.by_id.get(&1).unwrap();
            assert2::assert!(id == 1);
            assert2::assert!(cached.0.as_str() == schema);
            assert2::assert!(cached.1 == SchemaFormat::Protobuf);
        });
    }

    #[test]
    fn schema_by_id_fetches_from_server_and_caches() {
        let (rt, _port, base_url) = start_mock_server();

        rt.block_on(async {
            let client = SchemaRegistryClient::new(&base_url).unwrap();
            // First call — cache miss, goes to server.
            let first = client.schema_by_id(1).await.unwrap();
            // Confirm entry is in by_id.
            assert2::assert!(first.0.as_str() == "{\"type\":\"string\"}");
            assert2::assert!(first.1 == SchemaFormat::Protobuf);
            assert2::assert!(client.by_id.contains_key(&1));
            // Second call — should be a cache hit.
            let second = client.schema_by_id(1).await.unwrap();
            assert2::assert!(second == first);
        });
    }

    #[test]
    fn latest_fetches_from_server_and_caches() {
        let (rt, _port, base_url) = start_mock_server();

        rt.block_on(async {
            let client = SchemaRegistryClient::new(&base_url).unwrap();
            let (id, schema, fmt) = client.latest("my-subject-value").await.unwrap();
            assert2::assert!(id == 2);
            assert2::assert!(schema.as_str() == "{\"type\":\"record\"}");
            assert2::assert!(fmt == SchemaFormat::Json);
            assert2::assert!(client.by_id.contains_key(&2));
            assert2::assert!(client.by_subject_latest.contains_key("my-subject-value"));
        });
    }
}
