# Crabka Style Guides

Conventions for writing code and documentation in Crabka. They are adapted from the [Hardy project's style guides](https://github.com/ricktaylor/hardy/tree/main/docs/style_guides). Crabka tailors them to its toolchain and to its domain. The toolchain is pinned stable Rust, edition 2024, `unsafe` forbidden, `clippy::pedantic`, and `[workspace.dependencies]` with `[workspace.lints]`. The domain is Apache Kafka wire compatibility, KRaft consensus, and the KIPs Crabka implements.

The guides describe what reviewers expect. They are **not** a mandate to reformat existing code. See the [code style guide](code_style_guide.md#applying-these-conventions). Bring a file into line only when you are already changing it, and keep the tidy-up proportionate.

Crabka writes all prose in **ASD-STE100 Simplified Technical English**. The [prose style guide](prose_style_guide.md) defines the wording rules that every other guide here leaves open. It governs doc comments, READMEs, design docs, coverage reports, and commit and pull request text.

| Guide | Covers |
| :--- | :--- |
| [Prose Style](prose_style_guide.md) | ASD-STE100 Simplified Technical English: words, sentence length, active voice, `must` against `should`, and what is exempt. |
| [Code Style](code_style_guide.md) | General Rust conventions: toolchain, formatting, linting, naming, imports, error handling, wire-format safety, async, tests. |
| [Rustdoc](rustdoc_style_guide.md) | Doc-comment conventions for the public API. |
| [README](readme_style_guide.md) | Per-crate `README.md` structure and content. |
| [Design Docs](design_doc_style_guide.md) | Architectural design documents: the "why". |
| [Coverage Reports](coverage_report_style_guide.md) | Per-crate `test_coverage_report.md`: what is tested, how, and what remains. |

See also [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for build and test commands. See [`CLAUDE.md`](../../CLAUDE.md) for project-specific guidance: the greenfield stance, the Kafka compatibility constraints, and the execution workflow.
