//! HTTP client for a Confluent Schema Registry REST API.
//!
//! [`SchemaRegistryClient`] talks to the REST endpoints (`/subjects`,
//! `/schemas/ids`) and keeps two in-process caches:
//! - `by_id` — schema string + format, keyed on schema-id (immutable once
//!   registered; never evicted).
//! - `by_subject_latest` — schema-id of the latest version per subject, with
//!   a TTL so topology changes are picked up within a bounded window.

#![allow(clippy::todo, unused_variables)]

use std::time::Instant;

use dashmap::DashMap;
use url::Url;

use crate::codec::{CodecError, SchemaFormat};

/// A caching HTTP client for a Confluent-compatible Schema Registry.
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

#[allow(clippy::unused_async)] // stubs — implementations will await
impl SchemaRegistryClient {
    /// Construct a new client pointing at `base_url`.
    ///
    /// Returns [`CodecError::Registry`] if `base_url` is not a valid URL or
    /// the underlying HTTP client cannot be built.
    pub fn new(_base_url: &str) -> Result<Self, CodecError> {
        todo!()
    }

    /// Register `schema` (expressed as `fmt`) under `subject`.
    ///
    /// Returns the assigned schema id.  If the schema is already registered
    /// the registry returns the existing id.
    pub async fn register(
        &self,
        _subject: &str,
        _schema: &str,
        _fmt: SchemaFormat,
    ) -> Result<i32, CodecError> {
        todo!()
    }

    /// Resolve a schema string and its format by numeric id.
    ///
    /// Results are cached in [`Self::by_id`] indefinitely (schema ids are
    /// immutable once assigned).
    pub async fn schema_by_id(&self, _id: i32) -> Result<(String, SchemaFormat), CodecError> {
        todo!()
    }

    /// Return the latest `(id, schema, format)` tuple for `subject`.
    ///
    /// Results are cached in [`Self::by_subject_latest`] and re-fetched after
    /// a TTL to pick up new versions.
    pub async fn latest(&self, _subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
        todo!()
    }
}
