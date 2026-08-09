# Crate README Style Guide

This guide defines the style and content expectations for per-crate `README.md` files in Crabka. The [prose style guide](prose_style_guide.md) defines the wording rules that apply to everything you write here.

## Purpose

Each crate README is the **entry point for someone who sees the crate for the first time**. It answers: "what is this, why does it exist, and how do I use it?" Crabka publishes its crates to crates.io with release-plz, so READMEs serve two audiences:

- **crates.io and docs.rs readers** — people who evaluate whether to use the crate.
- **Internal developers** — people who need to understand a crate's role in the Crabka workspace.

## What Belongs in READMEs

- **One-line description** — what the crate does.
- **Role in Crabka** — how it fits into the larger system.
- **Key features and capabilities**, including which Kafka KIPs or wire APIs it covers.
- **Quick start or usage example** (for binaries and public API crates).
- **Configuration reference** (for server binaries).
- **Links** to design docs, the [KIP matrix](../KIP_MATRIX.md), test coverage reports, and API documentation.

## What Does NOT Belong in READMEs

- **Exhaustive API reference** — that belongs in rustdoc.
- **Design rationale** — that belongs in the design doc.
- **Test coverage details** — that belongs in the coverage report.
- **TODO lists or known issues** — those belong in the repo-level tracking docs, for example `KNOWN_ISSUES.md`, not per-crate READMEs.

## Document Structure

### Library Crates

```markdown
# crabka-<name>

[![Crates.io](https://img.shields.io/crates/v/crabka-<name>.svg)](https://crates.io/crates/crabka-<name>)
[![Docs.rs](https://docs.rs/crabka-<name>/badge.svg)](https://docs.rs/crabka-<name>)

<One-line description of what this crate does.>

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

<2-3 sentences explaining the crate's role in the system, which Kafka
standard(s) / KIP(s) it implements, and its relationship to other Crabka crates.>

## Features

- Feature 1
- Feature 2
- Cargo feature: `feature-name` — what it enables

## Usage

```rust
// Minimal example showing the primary API
```

## Documentation

- [Design](docs/design.md)
- [Test Coverage](docs/test_coverage_report.md)
- [API Documentation](https://docs.rs/crabka-<name>)
- [KIP Matrix](../../docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
```

### Server / Binary Crates

```markdown
# crabka-<name>

<One-line description of what this binary does.>

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Quick Start

<3-5 steps to get running, including minimal config and run command.>

## Configuration

<Table of configuration options with defaults and descriptions.>

Configuration is read from TOML files and environment variables
(`<PREFIX>_` prefix).

| Option | Default | Description |
|--------|---------|-------------|
| ... | ... | ... |

## Container Image

```bash
docker pull ghcr.io/robot-head/crabka-<name>:latest
```

## Documentation

- [Design](docs/design.md)
- [Test Coverage](docs/test_coverage_report.md)
- [KIP Matrix](../../docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
```

### Small / Internal Library Crates

For crates under about 200 lines with a single responsibility:

```markdown
# crabka-<name>

<One-line description.>

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.
<1-2 sentences on what it does and which crate(s) use it.>

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
```

## Writing Style

- **Be concise** — READMEs should be scannable. If a section exceeds a screenful, it probably belongs in a separate doc.
- **Lead with the most useful information** — what it does, not how it is built.
- **Use concrete examples** — a 5-line code snippet is worth a paragraph of description.
- **Link, do not duplicate** — point to docs.rs for API details, design docs for rationale, the KIP matrix for compatibility scope, and the coverage report for what is tested.
- **State Kafka-compatibility scope honestly** — if the crate implements a KIP partially, say so and link the KIP matrix rather than imply full support.

## Badges

The standard badge set is the one form of image Crabka READMEs use, because the badges carry real information for crates.io readers. The set is the crates.io version badge, the docs.rs badge, and optionally a CI badge. Avoid other, decorative images. Prefer text descriptions.

## Naming Conventions

- **Title**: use the crate name as-is (for example, `# crabka-protocol`, not `# Kafka Protocol Library`).
- **Links**: use relative paths within the repo (for example, `../../NOTICE`, `../../docs/KIP_MATRIX.md`), not absolute URLs, except for external sites (crates.io, docs.rs, kafka.apache.org, KIP pages).
- **License**: American spelling (`## License`), Apache-2.0, and the Kafka derivative-work line that points at `NOTICE`. Every crate is a derivative work of Apache Kafka.

## Questions to Ask When Writing

1. Could someone understand what this crate does from the first two sentences?
2. Is there enough information to use the crate without the source code?
3. Do I duplicate content that lives in another document, such as rustdoc, a design doc, a coverage report, or the KIP matrix?
4. Would this be useful on crates.io and docs.rs?
5. Is the Kafka-compatibility scope stated accurately, and does it match the KIP matrix?
