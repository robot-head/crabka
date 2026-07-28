# Admin UI Topic Mutation Timeout Design

## Goal

Replace the three fixed 30,000-millisecond admin UI topic-mutation timeouts
with one validated runtime setting while preserving the existing default and
broker request behavior.

## Scope

This slice changes only the timeout passed by `crabka-admin-ui` to
`AdminClient::create_topics`, `AdminClient::delete_topics`, and
`AdminClient::create_partitions`.

It does not change client transport deadlines, alter other admin operations,
split the shared policy into per-operation settings, migrate unrelated admin
UI settings, add an operator deployment, or add a CRD field. No operator or
checked-in Kubernetes deployment currently owns this binary.

## Public Configuration

The compiled default remains exactly 30,000 milliseconds.

The binary accepts:

- `--topic-mutation-timeout-ms`
- `CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS`

The command-line value wins when both sources are present. When neither is
present, the compiled default is used.

Zero, malformed, negative, and values outside the Kafka request field's `i32`
range fail during argument parsing, before the listener is bound or broker I/O
begins.

## Validated Type

Add a public `TopicMutationTimeoutMs` newtype. Its constructor and `FromStr`
implementation validate with `refined_type::rule::GreaterI32<0>`.
`AdminUiConfig` stores this newtype instead of a raw integer, and the type
exposes its validated `i32` only at the three `AdminClient` call sites.

The default is a named `DEFAULT_TOPIC_MUTATION_TIMEOUT_MS` constant. No new
dependency or lockfile change is needed because `crabka-admin-ui` already
directly depends on the workspace-pinned `refined_type`.

## Input Resolution

Add the field to the existing `AdminUiRuntimeArgs` Clap parser. Clap's
explicit long name, `env` support, and typed default provide
command-line-over-environment precedence without custom resolution code.

The binary parses runtime arguments before calling `AdminUiConfig::from_env()`,
then copies all resolved typed runtime settings into the configuration.
Existing admin UI configuration behavior remains unchanged.

## Runtime Flow

The exact value flow is:

```text
CLI / environment / typed default
  -> TopicMutationTimeoutMs
  -> AdminUiConfig::topic_mutation_timeout_ms
  -> BrokerAdminMutationSeam
  -> AdminClient::create_topics
     AdminClient::delete_topics
     AdminClient::create_partitions
```

Each client method continues to put the timeout into the Kafka request and to
reuse it on its existing `NOT_CONTROLLER` retry. Request validation, outcome
mapping, authentication, and error behavior remain unchanged.

## Tests

Test-first coverage will prove:

1. the configured default remains 30,000 milliseconds;
2. one millisecond is accepted;
3. zero, malformed, negative, and `i32` overflow values are rejected;
4. an environment value is accepted and a command-line value overrides it.

The three production call sites are trivial forwarding expressions rather
than new branching logic. Focused source inspection must prove that all three
old `30_000` literals are gone and all three calls consume the typed config
value. The crate's all-target tests, strict Clippy, nightly formatting,
single-help-entry check, diff hygiene, and unchanged `Cargo.lock` remain
required completion gates.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, the complete value flow, verification evidence,
and the next real unresolved admin UI owner. Invariants, static UI data, test
inputs, and already-configured defaults remain fixed rather than becoming
additional settings.
