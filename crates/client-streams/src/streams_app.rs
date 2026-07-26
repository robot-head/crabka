//! [`StreamsApp`] — one component that owns the schema-registry lifecycle and
//! the managed [`KafkaStreams`] runtime, so applications don't hand-wire the
//! cache, `set_default_registry`, pre-warm, and `KafkaStreams::builder()`.
//!
//! Build it **first** — that installs the process default registry — then build a
//! topology against it and `run` it. The registry must be installed before the
//! topology is constructed, because the default Avro/Protobuf/JSON serdes read it
//! when the DSL (or [`Topology`](crate::Topology)) builds them.
//!
//! ```no_run
//! use apache_avro::AvroSchema;
//! use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsApp};
//! use crabka_schema_serde::format::avro::AvroSerde;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     id: String,
//!     total: f64,
//! }
//! impl DefaultSerde for Order {
//!     type Serde = SchemaSerde<Order, AvroSerde<Order>>;
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let app = StreamsApp::builder()
//!     .bootstrap("127.0.0.1:9092")
//!     .application_id("orders")
//!     .schema_registry("http://127.0.0.1:8081")
//!     .build();
//!
//! let topology = app.streams_builder();
//! topology
//!     .stream::<String, Order>(["orders"])
//!     .map_values(|o: &Order| Order {
//!         id: o.id.clone(),
//!         total: o.total * 2.0,
//!     })
//!     .to("orders-doubled");
//!
//! let streams = app.run(topology).await?;
//! streams.close().await?;
//! # Ok(()) }
//! ```

use std::sync::Arc;

use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    set_default_registry,
};

use crate::{
    dsl::StreamsBuilder,
    error::StreamsClientError,
    runtime::{KafkaStreams, eos::ProcessingGuarantee},
    store::StoreBackend,
    topology::BuiltTopology,
};

/// Owns the schema-registry lifecycle (cache + process default + pre-warm) and
/// the [`KafkaStreams`] wiring for a streams application. Construct via
/// [`StreamsApp::builder`].
pub struct StreamsApp {
    bootstrap: String,
    application_id: String,
    cache: Arc<SchemaCache>,
    store_backend: StoreBackend,
    processing_guarantee: ProcessingGuarantee,
    broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
    cache_max_bytes: i64,
}

#[bon::bon]
impl StreamsApp {
    /// Configure the app and **install the process default schema registry**.
    ///
    /// Installs the registry as a side effect of `build`, so it is ready before
    /// the topology (and its default serdes) are constructed.
    #[must_use]
    #[builder(start_fn = builder, finish_fn = build)]
    pub fn new(
        #[builder(into)] bootstrap: String,
        #[builder(into)] application_id: String,
        /// Schema Registry base URL, e.g. `http://localhost:8081`.
        #[builder(into)]
        schema_registry: String,
        /// Registry behavior (auto-register vs lookup); defaults to auto-register.
        cache_config: Option<CacheConfig>,
        #[builder(default)] store_backend: StoreBackend,
        #[builder(default)] processing_guarantee: ProcessingGuarantee,
        /// Deadline for each Kafka broker DNS lookup owned by this process.
        #[builder(default)]
        broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
        /// Record-cache budget (JVM `statestore.cache.max.bytes`); `0` disables
        /// caching. Defaults to 10 MiB, matching the JVM default.
        #[builder(default = 10_485_760)]
        cache_max_bytes: i64,
    ) -> Self {
        let cache = SchemaCache::new(
            RegistryClient::new(schema_registry),
            cache_config.unwrap_or_default(),
        );
        set_default_registry(cache.clone());
        Self {
            bootstrap,
            application_id,
            cache,
            store_backend,
            processing_guarantee,
            broker_dns_timeout,
            cache_max_bytes,
        }
    }
}

impl StreamsApp {
    /// A fresh DSL builder; the schema registry is already installed.
    #[must_use]
    // cargo-mutants: trivial builder accessor.
    #[cfg_attr(test, mutants::skip)]
    pub fn streams_builder(&self) -> StreamsBuilder {
        StreamsBuilder::new()
    }

    /// The configured application id — pass it to [`Topology::build`] if you wire
    /// a Processor-API topology by hand and feed it to [`run_built`](Self::run_built).
    ///
    /// [`Topology::build`]: crate::Topology::build
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    /// Lower the DSL topology, pre-warm its schema subjects, and start the app.
    #[tracing::instrument(
        name = "streams.app.run",
        level = "info",
        skip_all,
        fields(application_id = %self.application_id),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn run(self, builder: StreamsBuilder) -> Result<KafkaStreams, StreamsClientError> {
        let built = builder.build(&self.application_id)?;
        self.run_built(built).await
    }

    /// Like [`run`](Self::run), but for an already-built topology (Processor API).
    #[tracing::instrument(
        name = "streams.app.run_built",
        level = "info",
        skip_all,
        fields(application_id = %self.application_id, guarantee = ?self.processing_guarantee),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn run_built(
        self,
        topology: BuiltTopology,
    ) -> Result<KafkaStreams, StreamsClientError> {
        self.cache
            .prewarm()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        KafkaStreams::builder()
            .bootstrap(self.bootstrap)
            .application_id(self.application_id)
            .topology(topology)
            .store_backend(self.store_backend)
            .processing_guarantee(self.processing_guarantee)
            .broker_dns_timeout(self.broker_dns_timeout)
            .cache_max_bytes(self.cache_max_bytes)
            .build()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_id_returns_configured_value() {
        let app = StreamsApp::builder()
            .bootstrap("127.0.0.1:9092")
            .application_id("orders-app")
            .schema_registry("http://127.0.0.1:8081")
            .build();

        assert_eq!(app.application_id(), "orders-app");
    }

    #[test]
    fn broker_dns_timeout_uses_typed_default_and_override() {
        let defaults = StreamsApp::builder()
            .bootstrap("127.0.0.1:9092")
            .application_id("default")
            .schema_registry("http://127.0.0.1:8081")
            .build();
        assert_eq!(
            defaults.broker_dns_timeout,
            crabka_client_core::ClientDnsTimeout::default()
        );

        let timeout =
            crabka_client_core::ClientDnsTimeout::new(std::time::Duration::from_millis(43))
                .expect("positive timeout");
        let overridden = StreamsApp::builder()
            .bootstrap("127.0.0.1:9092")
            .application_id("override")
            .schema_registry("http://127.0.0.1:8081")
            .broker_dns_timeout(timeout)
            .build();
        assert_eq!(overridden.broker_dns_timeout, timeout);
    }
}
