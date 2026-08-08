# crabka-operator

[![Crates.io](https://img.shields.io/crates/v/crabka-operator.svg)](https://crates.io/crates/crabka-operator)
[![Docs.rs](https://docs.rs/crabka-operator/badge.svg)](https://docs.rs/crabka-operator)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kubernetes operator for Crabka clusters.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-operator
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Generate CRDs locally, then run the operator in a Kubernetes cluster:

```bash
crabka-operator gen-crds ./target/crds
kubectl apply -f ./target/crds

WATCH_NAMESPACE=crabka-system crabka-operator run
```

## Gres CRDs

The operator includes two `crabka.io/v1alpha1` Gres resources:

- `Gres` (`gg`) defines one PgDog-backed front door for a Kafka cluster. Its spec
  names the Kafka cluster, the PgDog image/replica/listen settings, and optional
  TLS and admin secret references. The spec also holds the tenant defaults for
  WAL replication, checkpoint thresholds, and idle timeout.
- `GresTenant` (`gt`) defines one tenant in a `Gres` fleet. Its spec names the
  fleet, SQL user, password Secret key, optional suspension state, resources, and
  default overrides.

The Gres tenant reconciler creates the tenant WAL topic, the compacted
`__gres_cfg.<tenant>` topic, and the compacted `__gres_tenants` registry topic
when they are missing. It hashes the Kubernetes Secret password into SCRAM
material and writes the tenant record to both control-plane topics. It manages
the Kafka SCRAM credentials and the tenant-scoped ACLs, and it deploys a single
`crabka-gres` compute for active tenants. Plaintext SQL passwords stay in the
referenced Kubernetes Secret, and the operator does not write them to the Gres
registry topics.

The operator supports only the single-range (r0) topology. The operator does not
provision a `GresTenant` with more than one range. That tenant reports
`Ready=False` with reason `MultiRangeUnsupported`. Remote range-0 replication
and WAL-writer fencing must exist before distributed range placement is safe.

The Gres fleet reconciler renders PgDog config from `GresTenant` objects, stores
`pgdog.toml` and `users.toml` in a Secret, and manages the PgDog Service and
Deployment. Suspended tenants do not receive a backend route unless the caller
supplies an activator to the renderer.

## Documentation

The API documentation is on [docs.rs/crabka-operator](https://docs.rs/crabka-operator). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
