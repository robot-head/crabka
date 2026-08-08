# Prose Style Guide

This guide defines how to write prose in Crabka. It applies to every guide in this directory, and it governs the wording rules that the others leave open.

Crabka writes prose in **ASD-STE100 Simplified Technical English (STE)**. STE is a controlled-language specification for technical documentation. It gives each word one meaning, keeps sentences short, and puts the actor in front of the verb. The result is easy to read for a reviewer, for a new contributor, and for a reader whose first language is not English.

## Scope

STE applies to all prose that Crabka writes:

- Rustdoc comments (`///` and `//!`) and ordinary `//` comments
- Crate `README.md` files, design docs, and coverage reports
- Commit messages, pull request titles, and pull request bodies
- Every document under `docs/`

STE does **not** apply to:

- Rust code, attributes, and the contents of doctest fences
- Identifiers of any kind: type names, function names, crate names, feature flags, CLI flags, environment variables, config keys, and file paths
- Protocol vocabulary: Kafka API and record-field names, `KIP-###` references, error codes, PostgreSQL keywords, `pg_*` names, and `SQLSTATE` codes
- Quoted output, captured logs, and table data
- Text copied from an external specification

Technical names are exempt from the word rules. Write `ApiVersions`, `RowDescription`, and `producer_byte_rate` as they are.

## Words

- Use one word for one meaning. Use the same word every time for the same thing. Do not change words for variety.
- Use each word as one part of speech only.
- Prefer the short, common word.
- Do not use slang, idioms, or metaphors. Write the plain fact. "Blast radius", "front door", and "footgun" are not STE.

Frequent substitutions:

| Do not write | Write |
| :--- | :--- |
| utilize, leverage | use |
| via | through, with |
| commence, initiate | start |
| terminate | stop, end |
| prior to | before |
| subsequent to | after |
| in order to | to |
| ensure | make sure |
| perform | do |
| provide | give, supply |
| obtain | get |
| require | need |
| approximately | about |
| sufficient | enough |
| however | but |
| therefore | so |
| due to | because of |
| in the event that | if |

## `must` and `should`

This rule matters more than any other in this guide, because it changes what an API promises.

**Write `must` only when the code enforces the rule.** A rule is enforced when breaking it produces an observable failure from the code you are documenting: an `Err` return, a `panic!`, an `assert!`, an `expect`, or a protocol-level rejection.

**Write `should` for a recommendation.** Advice to a caller, to an operator, to an external client, or to a person reading a debugging note is a recommendation. Crabka cannot make a foreign Kafka client retry, and it cannot make a caller's runtime back off.

Do not write `shall`. Keep `must not` and `do not` for prohibitions.

An STE rewrite must never convert a `should` into a `must`. That is a change of contract, not a change of wording.

```rust
/// Panics if `size` is under one millisecond.        // must-class: an assert enforces it
pub fn of_size(size: Time) -> Self {
    assert!(size >= MIN_RESOLUTION, "window size must be >= 1ms");
```

```rust
/// Returns `None` when the source is momentarily caught up. The runtime should
/// back off and poll again.                          // should-class: nothing enforces it
```

## Sentences and paragraphs

- Keep procedural sentences to 20 words or fewer. Keep descriptive sentences to 25 words or fewer.
- Write one instruction per sentence. Keep one topic per sentence.
- Start with the main point, not with a qualifier.
- Do not remove articles (`a`, `an`, `the`) or other words that complete the sentence.
- Keep paragraphs to six sentences or fewer. Put the topic sentence first.

## Verbs

- Use the active voice and name the actor. Write "the broker rejects the request", not "the request is rejected".
- Use simple tenses only: present, past, and future. Use the past participle as an adjective.
- Do not use the `-ing` form as a noun or as a trailing analysis clause. Write a new sentence instead.
- Use the imperative for instructions.

## Noun clusters

Use a maximum of three nouns in a row. Break a longer cluster with a preposition or a hyphen. A technical name counts as one unit.

## Punctuation

- Do not use an em dash to join two clauses. Use a period or a comma. A dash that separates a list term from its definition is acceptable, and the rustdoc guide uses that form.
- Do not put essential information in parentheses. Give it its own clause.
- Do not use `->`, `=>`, `+`, `/`, or `iff` as words in prose. Write them out.

## Safety and rules

Keep every warning, prohibition, and absolute exactly as forceful as it is. Words such as `never`, `must`, `all`, `only`, and `do not` define rules. Do not soften one, do not narrow its scope, and do not drop a negation when you split a sentence.

## Rustdoc specifics

The [rustdoc guide](rustdoc_style_guide.md) defines structure. These rules govern its prose:

- The first line is the rustdoc summary. Keep it to one short sentence on the first line. Do not split it into two sentences.
- Never rename a heading in a Markdown file. Other files link to it by anchor.
- **`# Errors` and `# Panics` must state the real conditions for the item they sit on.** Do not paste a generic sentence across a crate. A prose audit of this workspace found eight such boilerplate strings, and many were false: a `# Panics` body that named a mutex on a file with no mutex, a `# Errors` body that named Kubernetes in a crate with no Kubernetes dependency, and encode functions documented as failing on truncated input. A section that does not describe the item is worse than no section, because a reader trusts it.
- If you cannot state the real condition, delete the section. Do not leave a placeholder.

## Applying these conventions

The same rule applies here as in the [code style guide](code_style_guide.md#applying-these-conventions). This guide is not a mandate to rewrite prose you are not otherwise touching. Bring a file into line when you already edit it, and keep the tidy-up proportionate to the change.
