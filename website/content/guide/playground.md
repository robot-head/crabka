+++
title = "Playground"
weight = 40
template = "docs/page.html"

[extra]
mermaid = true
+++

## Simulate consensus in your browser

Crabka's metadata quorum is a native KRaft (Raft, KIP-595) implementation, and
its core is **deterministic** — given the same sequence of events it always
reaches the same state. That property makes it testable, and it also makes it
*demonstrable*: the same engine that runs in production runs right here in your
browser.

The playground below compiles the deterministic consensus core to
**WebAssembly** and drives it from JavaScript through custom in-memory
transports. Instead of real TCP, peers exchange messages over an in-memory bus
that the page controls. That lets you interactively **inject partitions, drop,
reorder, or duplicate messages, and produce records**, then watch the cluster
elect a leader, lose it, and recover — live, with no backend.

{{ playground() }}

Press **Play** to watch a fresh cluster hold its first election, then cut off
the leader with **Partition leader** and watch the majority elect a new one at a
higher epoch. Heal it (click the dimmed node) and the stale leader steps down —
the engine never allows two leaders in one epoch. Every action you take is the
*real* KIP-595/996 state machine reacting, not a scripted animation.

### How it works

{% mermaid() %}
flowchart LR
  UI[Browser UI] -->|trigger faults| Core[WASM consensus core]
  Core <-->|messages| Transport[JS-controlled in-memory bus]
  Transport --> Trace[Event trace]
  Trace --> SVG[SVG render]
  SVG --> UI
{% end %}

The UI drives the consensus core compiled to WASM; the core sends and receives
peer messages over a JS-controlled in-memory bus; every step appends to an event
trace; the trace is rendered to SVG and fed back to the UI as an interactive
timeline.

The seam that makes this possible already existed in the engine: the consensus
state machine is a pure, sans-IO `on_event(event, log, now) -> [Action]`
function — it never reads the clock, opens a socket, or writes to disk. The same
deterministic multi-node simulator the integration tests and the
[generated failure diagrams](/reference/concepts/failure-scenarios/) use is what
this page drives, only here *you* supply the events.

To reach the browser, that core was lifted out of the async engine into a
dependency-light leaf crate (`crabka-kraft-core`, with the voter-set value types
in `crabka-voters`) that carries no `tokio`, no filesystem, and no crypto, so it
compiles cleanly for `wasm32-unknown-unknown`. A thin `wasm-bindgen` shim
(`crabka-playground`) exposes the simulator to JavaScript; the clock and the
transports are supplied by the page. The async engine, the real KIP-595 wire,
and the on-disk log still live in `crabka-raft`, which re-exports the same core —
so the algorithm you poke at above is byte-for-byte the one the broker runs in
production.

For the same partition, reordering, and duplication scenarios rendered as
curated, generated sequence diagrams, see the
[failure-scenario reference](/reference/concepts/failure-scenarios/).
