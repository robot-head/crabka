# crabka-kraft-core

The deterministic, sans-IO `KRaft` consensus core (KIP-595 + KIP-996) for
[Crabka](https://github.com/robot-head/crabka).

A synchronous `on_event(event, log, now) -> Vec<Action>` state machine over the
`QuorumState`/`Role` model. It never reads the clock, opens a socket, or writes
to disk: time is injected, the log is read through the `LogView` seam, and every
effect is returned as an `Action` for the caller to run.

`crabka-raft` wraps this with the async engine, the real KIP-595 wire, and the
on-disk log. Because the core is a clean leaf (no tokio, no filesystem, no
crypto), it builds for `wasm32-unknown-unknown` — which is what powers the
in-browser consensus playground via the `sim` feature.
