# Design Document Style Guide

This guide defines the style and content expectations for design documents in Crabka. The [prose style guide](prose_style_guide.md) defines the wording rules that apply to everything you write here.

## Purpose

Design documents capture **architectural decisions and rationale**, the "why" behind the code. They are for engineers who need to understand a subsystem conceptually before they read the implementation details.

Crabka's design specs live under [`docs/superpowers/specs/`](../superpowers/specs/), named `YYYY-MM-DD-<topic>-design.md`. A subsystem may also carry a `docs/design.md` inside its crate when it needs a durable, per-crate design reference. Link that file from the crate README.

## What Belongs in Design Docs

- **Design goals and constraints** that shaped the implementation.
- **Key architectural decisions** and the alternatives considered.
- **Conceptual models** — how components interact, how data and control flow, where the trust boundaries are.
- **Trade-offs** — what the design gave up, and for what benefit.
- **Integration points** — how this subsystem relates to others in the system (broker, KRaft quorum, log, clients).
- **KIP and specification interpretation** — how a KIP's requirements, or an observed Kafka behaviour, influenced the design.

## What Does NOT Belong in Design Docs

- **API reference material** — struct fields, enum variants, method signatures (these belong in rustdoc).
- **Usage examples** — code snippets that show how to call an API (rustdoc).
- **Exhaustive lists** — every error type, every flag bit, every field (rustdoc).
- **Implementation details** that do not reflect an architectural choice.

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

When you write or update a design document:

- **Consult the KIP and the Kafka wire schemas** to make sure the terminology is accurate and the compatibility descriptions are correct. Use the precise language of the KIP and the schema field names when you describe message formats or protocol behaviour. Cross-check against the [KIP matrix](../KIP_MATRIX.md).
- **Where Kafka's behaviour is undocumented or version-dependent, verify it empirically** against the latest released `cp-kafka` or `apache/kafka` image. Do not rely on the wiki. See [`CLAUDE.md`](../../CLAUDE.md). Document what you observed and the version you observed it against.
- **Ask clarifying questions** if the code does not make the design intent clear. It is better to ask the maintainer than to guess or to document assumptions that may be wrong.

## Writing Style

- **Be concise but not terse** — brevity is good, but not at the expense of readability. Write in complete sentences with natural flow.
- **Explain the "why"** — decisions without rationale are not useful.
- **Use concrete examples** when they clarify a concept, but not as API documentation.
- **Link to KIPs** with section anchors for traceability. Link to RFCs the same way where they are relevant, for example SASL and TLS.
- **Bullets are fine when appropriate** — use them for lists of items, options, or requirements. But each bullet should be a complete thought, not a terse fragment. Design rationale and explanations usually read better as paragraphs.
- **One line per paragraph** in the Markdown source. Let the renderer wrap the text. See the [code style guide](code_style_guide.md#markdown-and-prose-for-docs-you-write).

## Assumed Reader Background

- **Familiar with Kafka concepts** — topics, partitions, offsets, ISR, consumer groups, the KRaft metadata quorum, transactions and EOS, tiered storage.
- **Comfortable with distributed-systems fundamentals** — consensus, replication, leader election, linearizability.
- **May have limited Rust experience** — explain Rust-specific idioms (ownership, traits, async, `Arc` and lock choices) when they are central to a design decision.
- **Likely background in Java, Go, C, or C++** — comparisons to patterns in those languages, or to how the JVM Kafka broker does something, can help clarify.

When a Rust concept is integral to the design, briefly explain what the concept achieves. Do not assume that the reader knows the idiom. One example is "a single writer task owns the log so we never need a lock across an await".

## Questions to Ask When Writing

1. If I deleted this paragraph, would someone misunderstand the design?
2. Does this explain a decision, or does it only describe what the code does?
3. Could a reader find this information in the rustdoc or the source?
4. Would a new team member understand *why* things are this way?
5. Is every Kafka-compatibility claim consistent with the KIP matrix and the differential tests?
