+++
title = "Playground (Roadmap)"
weight = 40
template = "docs/page.html"

[extra]
mermaid = true
+++

## Roadmap: simulate consensus in your browser

Crabka's metadata quorum is a native KRaft (Raft, KIP-595) implementation, and
its core is **deterministic** — given the same sequence of events it always
reaches the same state. That property makes it testable, and it also makes it
*demonstrable*: the same engine that runs in production can run in your browser.

The goal of the playground is to compile the deterministic consensus core to
**WebAssembly** and drive it from JavaScript through custom in-memory
transports. Instead of real TCP, peers exchange messages over an in-memory bus
that the page controls. That lets you interactively **inject partitions, drop
or reorder messages, and delay delivery**, then watch the cluster elect a
leader, lose it, and recover — live, with no backend.

### How it would work

{% mermaid() %}
flowchart LR
  UI[Browser UI] -->|trigger faults| Core[WASM consensus core]
  Core <-->|messages| Transport[JS in-memory transports]
  Transport --> Trace[Event trace]
  Trace --> SVG[SVG render]
  SVG --> UI
{% end %}

The UI drives the consensus core compiled to WASM; the core sends and receives
peer messages over JS-controlled in-memory transports; every step appends to an
event trace; the trace is rendered to SVG and fed back to the UI as an
interactive timeline.

### Why this is a roadmap item

The seam already exists. The engine's transport is abstracted behind
`PeerSender`, and the simulation harness already swaps in an in-memory log via
`SimNodeLog` — that is exactly the boundary a WASM build would target, and it is
how the deterministic-simulation tests already exercise partitions and
reordering today.

What still blocks an in-browser build is runtime coupling: parts of the stack
reach for `tokio` and the filesystem, neither of which exists in a browser WASM
target. Decoupling the deterministic core from that I/O — so it can be built for
`wasm32` with the transports and clock supplied by JavaScript — is the work this
page is tracking.

> ▶ **Simulate in browser — coming soon**
>
> *(This panel is a placeholder. The interactive WASM simulator is not yet
> wired up.)*

In the meantime, the same partition and reordering scenarios are documented as
generated diagrams in the
[failure-scenario reference](/reference/concepts/failure-scenarios/).
