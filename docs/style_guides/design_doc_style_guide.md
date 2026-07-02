# Design Document Style Guide

This guide defines the style and content expectations for design documents in Crabka.

## Purpose

Design documents capture **architectural decisions and rationale** — the "why" behind the code. They are for engineers who need to understand a subsystem conceptually before diving into implementation details.

Crabka's design specs live under [`docs/superpowers/specs/`](../superpowers/specs/), named `YYYY-MM-DD-<topic>-design.md`. A subsystem may also carry a `docs/design.md` inside its crate when a durable, per-crate design reference is warranted; link it from the crate README.

## What Belongs in Design Docs

- **Design goals and constraints** that shaped the implementation.
- **Key architectural decisions** and the alternatives considered.
- **Conceptual models** — how components interact, how data and control flow, where the trust boundaries are.
- **Trade-offs** — what was sacrificed for what benefit.
- **Integration points** — how this subsystem relates to others in the system (broker, KRaft quorum, log, clients).
- **KIP / specification interpretation** — how a KIP's requirements, or an observed Kafka behaviour, influenced the design.

## What Does NOT Belong in Design Docs

- **API reference material** — struct fields, enum variants, method signatures (these belong in rustdoc).
- **Usage examples** — code snippets showing how to call APIs (rustdoc).
- **Exhaustive lists** — every error type, every flag bit, every field (rustdoc).
- **Implementation details** that don't reflect an architectural choice.

## Document Structure

```markdown
# <subsystem> Design

<One-line description of purpose>

## Design Goals

What properties/qualities was this subsystem designed to achieve?
Why does it exist in this shape rather than a simpler or off-the-shelf one?

## Architecture Overview

High-level conceptual model. How do the pieces fit together?
Diagrams welcome if they clarify relationships.

## Key Design Decisions

### <Decision 1 Title>

What was decided, why, and what alternatives were rejected.

### <Decision 2 Title>

...

## Integration

How does this subsystem interact with other Crabka components
(broker, KRaft controller, log storage, clients)? What are the
contracts / interfaces?

## Kafka / KIP Compliance

Which KIPs or Kafka wire behaviours does this implement?
Any notable interpretation decisions, or places Crabka deliberately
diverges (and why)?

## Testing

Link to the coverage report and any relevant differential-test suites
(don't duplicate their content).
```

## Research and Verification

When writing or updating design documents:

- **Consult the KIP and the Kafka wire schemas** to ensure accurate terminology and correct descriptions of compatibility. Use the precise language of the KIP and the schema field names when describing message formats or protocol behaviour, and cross-check against the [KIP matrix](../KIP_MATRIX.md).
- **Where Kafka's behaviour is undocumented or version-dependent, verify it empirically** against the latest released `cp-kafka` / `apache/kafka` image rather than relying on the wiki (see [`CLAUDE.md`](../../CLAUDE.md)). Document what you observed and against which version.
- **Ask clarifying questions** if the design intent is unclear from examining the code. It is better to ask the maintainer than to guess or document assumptions that may be wrong.

## Writing Style

- **Be concise but not terse** — brevity is good, but not at the expense of readability. Write in complete sentences with natural flow.
- **Explain the "why"** — decisions without rationale are not useful.
- **Use concrete examples** when they clarify a concept, but not as API documentation.
- **Link to KIPs** (and to RFCs where relevant, e.g. SASL/TLS) with section anchors for traceability.
- **Bullets are fine when appropriate** — use them for lists of items, options, or requirements. But each bullet should be a complete thought, not a terse fragment. Design rationale and explanations typically read better as paragraphs.
- **One line per paragraph** in the Markdown source; let the renderer wrap (see the [code style guide](code_style_guide.md#markdown-and-prose-for-docs-you-write)).

## Assumed Reader Background

- **Familiar with Kafka concepts** — topics, partitions, offsets, ISR, consumer groups, the KRaft metadata quorum, transactions/EOS, tiered storage.
- **Comfortable with distributed-systems fundamentals** — consensus, replication, leader election, linearizability.
- **May have limited Rust experience** — explain Rust-specific idioms (ownership, traits, async, `Arc`/lock choices) when they are central to a design decision.
- **Likely background in Java, Go, C, or C++** — comparisons to patterns in those languages, or to how the JVM Kafka broker does something, can help clarify.

When a Rust concept is integral to the design (e.g., "a single writer task owns the log so we never need a lock across an await"), briefly explain what the concept achieves rather than assuming the reader knows the idiom.

## Questions to Ask When Writing

1. If I deleted this paragraph, would someone misunderstand the design?
2. Is this explaining a decision, or just describing what the code does?
3. Could this information be found by reading the rustdoc or source?
4. Would a new team member understand *why* things are this way?
5. Is every Kafka-compatibility claim consistent with the KIP matrix and the differential tests?
