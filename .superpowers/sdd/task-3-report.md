# Task 3 Report: Schema Registry CRD Runtime Policy

## RED

Added reconciliation coverage before production changes:

- `runtime_policy_renders_exact_flags_and_probe_timings`
- `runtime_invalid_policy_is_rejected_before_deployment`

`cargo test -p crabka-operator --test reconcile_schema_registry runtime_`
failed with the expected 12 compile errors: missing
`SchemaRegistryRuntime`, `SchemaRegistryHealthChecks`, the three new spec
fields, and `ReconcileError::SchemaRegistryConfigInvalid`.

## GREEN

- Added optional camel-case `runtime`, `clientId`, and `healthChecks` CRD
  fields with OpenAPI scalar bounds.
- Validated every scalar with `refined_type`, checked effective election
  relationships and string domains, and rejected invalid policy before any
  child resource render.
- Rendered all ten runtime flags, `--client-id`, and configured probe timings.
- Regenerated `crabka.io_schemaregistries.yaml`.

Fresh verification:

- `cargo test -p crabka-operator --test reconcile_schema_registry`: 13 passed.
- `cargo test -p crabka-operator --lib crd::schema_registry`: compiled clean;
  675 tests filtered out.
- Generated SchemaRegistry CRD diff: empty.
- Strict all-target operator Clippy: passed.
- Nightly workspace format check: passed.
- `git diff --check`: passed.

## Self-review

Validation is operator-local and uses no Schema Registry production dependency.
Partial election overrides are checked against the direct process defaults.
No unrelated dirty files are included in the Task 3 commit.
