//! Kafka wire-protocol compression codecs.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-compression-1b-design.md`.
//!
//! Kafka uses four codecs on the wire — gzip, snappy, lz4, zstd — each
//! with a specific framing convention. `crabka-compression` wraps the
//! third-party Rust crates for those codecs and adds the Kafka-specific
//! framing where needed (notably xerial-snappy for snappy and the LZ4
//! frame format with independent blocks for lz4).
#![doc(html_root_url = "https://docs.rs/crabka-compression/0.0.0")]
