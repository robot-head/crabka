# crabka-voters

KIP-853 voter-set value types for [Crabka](https://github.com/robot-head/crabka).
A voter is `(id, directory-id, endpoints, kraft.version range)`.

This is a pure value-type leaf crate with no IO, no async, and no crypto, so it
builds for `wasm32-unknown-unknown`. `crabka-metadata` re-exports it as the
`voters` module. The deterministic consensus core `crabka-kraft-core` embeds it
through `QuorumState`.
