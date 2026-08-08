# crabka-security

[![Crates.io](https://img.shields.io/crates/v/crabka-security.svg)](https://crates.io/crates/crabka-security)
[![Docs.rs](https://docs.rs/crabka-security/badge.svg)](https://docs.rs/crabka-security)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

TLS, SASL, SCRAM, OAuth, Kerberos, and certificate utilities for Crabka.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-security` collects the reusable security primitives that Crabka brokers,
clients, operators, and tests share. It builds rustls configs, implements SASL
mechanism helpers, and runs SCRAM exchanges. It also validates OAuth bearer
tokens, extracts Kafka principals from certificates, and issues local
test/cluster certificates.

The crate supplies security building blocks. It does not open network sockets
and it does not own OAuth/JWKS refresh loops. Callers integrate these
primitives with their own transport and runtime.

## Capabilities

- SASL mechanism names and auth method types for PLAIN, SCRAM-SHA-256,
  SCRAM-SHA-512, OAUTHBEARER, and GSSAPI.
- PLAIN verification helpers.
- SCRAM credential hashing and client/server exchange state machines.
- OAuth bearer initial-response parsing, unsecured JWS validation, signed JWS
  validation through JWKS, and introspection traits.
- TLS client/server config builders and dynamic server config reload support.
- mTLS principal extraction from X.509 subject distinguished names.
- Cluster and client CA generation plus broker/user certificate issuance.
- Delegation-token HMAC helpers and redacted secret wrappers.
- Kerberos/GSSAPI trait surfaces, keytab parsing, and provider scaffolding.

## Kafka Security Scope

SASL mechanism wire names match Kafka mechanism strings. SCRAM follows RFC 5802
and includes KIP-554-style broker credential derivation. OAuth bearer parsing
matches the RFC 7628/KIP-255 client initial response shape. mTLS principal
extraction follows Kafka's default subject-DN principal style.

GSSAPI support is a de-risking surface. Review it before you use it in
production.

## Install

```sh
cargo add crabka-security
```

For workspace development, use the path dependency from this repository.

## Usage

Build the client-first message for a SCRAM-SHA-256 exchange:

```rust
use crabka_security::{SaslMechanism, ScramClientExchange};

let exchange = ScramClientExchange::new(
    "alice".to_string(),
    b"correct horse battery staple".to_vec(),
    SaslMechanism::ScramSha256,
);

let (client_first, _exchange) = exchange.client_first()?;
println!("send SCRAM client-first-message: {}", String::from_utf8_lossy(&client_first));
# Ok::<(), crabka_security::AuthError>(())
```

## Documentation

- [API documentation](https://docs.rs/crabka-security)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
