# Crabka Style Guides

Conventions for writing code and documentation in Crabka. They are adapted from the [Hardy project's style guides](https://github.com/ricktaylor/hardy/tree/main/docs/style_guides) and tailored to Crabka's toolchain (pinned stable Rust, edition 2024, `unsafe` forbidden, `clippy::pedantic`, `[workspace.dependencies]` / `[workspace.lints]`) and its domain (Apache Kafka wire compatibility, KRaft consensus, and the KIPs Crabka implements).

The guides describe what reviewers expect. They are **not** a mandate to reformat existing code — see the [code style guide](code_style_guide.md#applying-these-conventions): bring a file into line only when you are already changing it, and keep the tidy-up proportionate.

| Guide | Covers |
| :--- | :--- |
| [Code Style](code_style_guide.md) | General Rust conventions: toolchain, formatting, linting, naming, imports, error handling, wire-format safety, async, tests. |
| [Rustdoc](rustdoc_style_guide.md) | Doc-comment conventions for the public API. |
| [README](readme_style_guide.md) | Per-crate `README.md` structure and content. |
| [Design Docs](design_doc_style_guide.md) | Architectural design documents — the "why". |
| [Coverage Reports](coverage_report_style_guide.md) | Per-crate `test_coverage_report.md` — what's tested, how, and what remains. |

See also [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for build/test commands and [`CLAUDE.md`](../../CLAUDE.md) for project-specific guidance (greenfield stance, Kafka compatibility constraints, execution workflow).
