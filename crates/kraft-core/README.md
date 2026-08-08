# crabka-kraft-core

The deterministic, sans-IO `KRaft` consensus core (KIP-595 + KIP-996) for
[Crabka](https://github.com/robot-head/crabka).

A synchronous `on_event(event, log, now) -> Vec<Action>` state machine over the
`QuorumState`/`Role` model. It never reads the clock, opens a socket, or writes
to disk. The caller injects the time. The core reads the log through the
`LogView` seam and returns every effect as an `Action` for the caller to run.

`crabka-raft` wraps this with the async engine, the real KIP-595 wire, and the
on-disk log. The core is a clean leaf with no tokio, no filesystem, and no
crypto, so it builds for `wasm32-unknown-unknown`. The `sim` feature uses that
build for the in-browser consensus playground.
