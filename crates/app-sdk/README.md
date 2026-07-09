# crabka-app-sdk

`crabka-app-sdk` is the Rust application SDK surface for Crabka serverless apps.

It is private while the contract is still being validated by the in-repository conformance suite. The SDK is intentionally separate from the native Kafka-compatible client crates: use this crate for the cross-language application contract, and use the native `crabka-client-*` crates when you need Kafka-shaped administration, produce, or consume APIs.
