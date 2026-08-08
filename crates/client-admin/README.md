# crabka-client-admin

[![Crates.io](https://img.shields.io/crates/v/crabka-client-admin.svg)](https://crates.io/crates/crabka-client-admin)
[![Docs.rs](https://docs.rs/crabka-client-admin/badge.svg)](https://docs.rs/crabka-client-admin)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Operator-side admin client for Crabka and Kafka-compatible clusters.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

Operators and automation use `crabka-client-admin` as their control-plane
client. It builds on `crabka-client-core` for typed Kafka protocol dispatch and
tracks the active controller for controller-routed RPCs. It retries selected
requests when a broker returns `NOT_CONTROLLER`.

Use this crate when a program must create topics, change topic config, manage
users or ACLs, or inspect log directories. It sends these Kafka admin RPCs
without a dependency on broker internals.

## Capabilities

- Read topic metadata, create and delete topics, and expand partitions.
- Describe dynamic topic config and change it with incremental alter operations.
- Upsert and delete SCRAM user credentials for SHA-256 and SHA-512.
- Create, delete, and describe ACLs.
- Describe and alter client quotas for each user.
- Create, renew, expire, and describe delegation tokens.
- Alter and describe the log directories of each broker.
- List consumer groups and inspect their offsets.

## Kafka Scope

The public API wraps the Kafka admin RPCs that Crabka operators need now. These
RPCs cover SCRAM credentials (KIP-554), delegation tokens (KIP-48), dynamic
topic configuration, ACLs, client quotas, and log-directory inspection. The API
is not a complete clone of the JVM `AdminClient` surface.

Log-directory calls target the connected broker. They do not retry against the
controller. Quota helpers expose the per-user entity shape that Crabka's
operator controllers use.

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
