# crabka-voters

KIP-853 voter-set value types for [Crabka](https://github.com/robot-head/crabka):
a voter is `(id, directory-id, endpoints, kraft.version range)`.

This is a pure value-type leaf crate — no IO, no async, no crypto — so it builds
for `wasm32-unknown-unknown`. It is re-exported by `crabka-metadata` as its
`voters` module and embedded in the deterministic consensus core
(`crabka-kraft-core`) via `QuorumState`.
