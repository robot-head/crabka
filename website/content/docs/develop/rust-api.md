+++
title = "Rust API"
weight = 20
template = "docs/page.html"
+++

The website publishes generated rustdoc for the workspace under `/api/rust/`.
Use it when you are building against Crabka crates directly instead of operating
the broker through Kafka-compatible tools.

Common entry points:

- [`crabka_broker`](/api/rust/crabka_broker/index.html) for the broker runtime.
- [`crabka_protocol`](/api/rust/crabka_protocol/index.html) for Kafka wire types
  and codecs.
- [`crabka_client_core`](/api/rust/crabka_client_core/index.html) for connection
  management.
- [`crabka_client_producer`](/api/rust/crabka_client_producer/index.html) for
  producers.
- [`crabka_client_consumer`](/api/rust/crabka_client_consumer/index.html) for
  consumers.
- [`crabka_client_admin`](/api/rust/crabka_client_admin/index.html) for admin
  operations.
- [`crabka_client_streams`](/api/rust/crabka_client_streams/index.html) for
  stream processing.
- [`crabka_operator`](/api/rust/crabka_operator/index.html) for operator
  internals.

These are built with `cargo doc --no-deps --workspace` and published under
`/api/rust/`.
