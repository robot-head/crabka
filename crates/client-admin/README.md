# crabka-client-admin

[![Crates.io](https://img.shields.io/crates/v/crabka-client-admin.svg)](https://crates.io/crates/crabka-client-admin)
[![Docs.rs](https://docs.rs/crabka-client-admin/badge.svg)](https://docs.rs/crabka-client-admin)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Operator-side admin client for Crabka and Kafka-compatible clusters.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-client-admin` is the control-plane client used by operators and
automation. It builds on `crabka-client-core` for typed Kafka protocol dispatch,
tracks the active controller for controller-routed RPCs, and retries selected
requests when a broker returns `NOT_CONTROLLER`.

Use this crate when a program needs to create topics, change topic config,
manage users or ACLs, inspect log directories, or issue other Kafka admin RPCs
without depending on broker internals.

## Capabilities

- Topic metadata, topic creation/deletion, and partition expansion.
- Dynamic topic config describe and incremental alter operations.
- SCRAM user credential upsert/delete for SHA-256 and SHA-512.
- ACL create, delete, and describe operations.
- Per-user client quota describe/alter helpers.
- Delegation-token create, renew, expire, and describe operations.
- Per-broker log-directory alter/describe calls.
- Consumer-group listing and offset inspection.

## Kafka Scope

The public API wraps Kafka admin RPCs that Crabka operators currently need,
including SCRAM credentials (KIP-554), delegation tokens (KIP-48), dynamic topic
configuration, ACLs, client quotas, and log-directory inspection. It is not a
complete clone of the JVM `AdminClient` surface.

Log-directory calls target the connected broker and do not perform controller
retry. Quota helpers currently expose the per-user entity shape used by Crabka's
operator controllers.

## Install

```sh
cargo add crabka-client-admin
```

For workspace development, use the path dependency from this repository.

## Usage

Create a topic and fetch its metadata:

```rust,no_run
use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut admin = AdminClient::connect(&["127.0.0.1:9092".to_string()]).await?;

admin
    .create_topics(
        &[CreateTopicSpec {
            name: "orders".into(),
            partitions: 3,
            replicas: 1,
            configs: BTreeMap::new(),
        }],
        30_000,
    )
    .await?;

let metadata = admin.metadata(&["orders"]).await?;
println!("topics: {:?}", metadata.topics);
# Ok(())
# }
```

## Documentation

- [API documentation](https://docs.rs/crabka-client-admin)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
