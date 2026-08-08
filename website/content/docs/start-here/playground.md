+++
title = "Playground"
description = "Simulate KRaft consensus in your browser. Crabka's deterministic Rust consensus core compiles to WebAssembly so you can drive elections and replication live."
weight = 30
template = "docs/page.html"

[extra]
mermaid = true
+++

## Simulate consensus in your browser

Crabka's metadata quorum is a native KRaft implementation. Its core is
deterministic: it reaches the same state from the same sequence of events. This
makes the consensus engine testable, and it also lets the engine run in your
browser. The engine on this page is the engine that runs in the broker.

The playground below compiles the deterministic consensus core to
**WebAssembly** and drives it from JavaScript through custom in-memory
transports. The peers do not use TCP. They exchange messages over an in-memory
bus that the page controls. You can **inject partitions, drop, reorder, or
duplicate messages, and produce records**. You then watch the cluster elect a
leader, lose the leader, and recover. There is no backend.

{{ playground() }}

Press **Play** to watch a fresh cluster hold its first election. Then cut off the
leader with **Partition leader**. The majority elects a new leader at a higher
epoch. To heal the partition, click the dimmed node. The stale leader then steps
down, because the engine never allows two leaders in one epoch.

Every action drives the *real* KIP-595/996 state machine, not a scripted
animation.

### How it works

{% mermaid() %}
flowchart LR
  UI[Browser UI] -->|trigger faults| Core[WASM consensus core]
  Core <-->|messages| Transport[JS-controlled in-memory bus]
  Transport --> Trace[Event trace]
  Trace --> SVG[SVG render]
  SVG --> UI
{% end %}

The UI drives the consensus core that is compiled to WASM. The core sends and
receives peer messages over a JS-controlled in-memory bus. Every step appends to
an event trace. The page renders the trace to SVG and shows it in the UI as an
interactive timeline.

The engine already had the necessary boundary. The consensus state machine is a
pure, sans-IO `on_event(event, log, now) -> [Action]` function. It never reads
the clock, opens a socket, or writes to disk. This page drives the same
deterministic multi-node simulator that the integration tests and the
[generated failure diagrams](/docs/reference/concepts/failure-scenarios/) use.
Here *you* supply the events.

The core is in a dependency-light leaf crate, `crabka-kraft-core`, with the
voter-set value types in `crabka-voters`. The crate carries no `tokio`, no
filesystem, and no crypto, so it compiles cleanly for
`wasm32-unknown-unknown`. A thin `wasm-bindgen` shim, `crabka-playground`, gives
JavaScript access to the simulator. The page supplies the clock and the
transports.

The async engine, the real KIP-595 wire, and the on-disk log are in
`crabka-raft`. That crate re-exports the same core, so the algorithm on this
page is byte-for-byte the algorithm that the broker runs.

The [failure-scenario reference](/docs/reference/concepts/failure-scenarios/)
shows the same partition, reordering, and duplication scenarios as generated
sequence diagrams.
