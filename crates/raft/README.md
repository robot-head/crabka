# crabka-raft

[![Crates.io](https://img.shields.io/crates/v/crabka-raft.svg)](https://crates.io/crates/crabka-raft)
[![Docs.rs](https://docs.rs/crabka-raft/badge.svg)](https://docs.rs/crabka-raft)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Metadata KRaft quorum (KIP-595 KraftController) for Crabka.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-raft
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Exercise the KRaft quorum state machine in a deterministic unit test:

```rust
use crabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
use crabka_raft::kraft::{QuorumState, QuorumStateMachine};
use uuid::Uuid;

let voters = VoterSet::new(vec![Voter {
    id: 1,
    directory_id: Uuid::new_v4(),
    endpoints: vec![VoterEndpoint { name: "CONTROLLER".into(), host: "127.0.0.1".into(), port: 9093 }],
    kraft_version: KRaftVersionRange { min: 0, max: 1 },
}]).unwrap();
let state = QuorumState::bootstrap(Uuid::new_v4(), voters);
let machine = QuorumStateMachine::new(1, state, 1_000);

assert!(machine.is_voter());
```

## Documentation

Read the API documentation at [docs.rs/crabka-raft](https://docs.rs/crabka-raft). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
