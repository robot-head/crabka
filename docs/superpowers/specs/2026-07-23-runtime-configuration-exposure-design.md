# Runtime Configuration Exposure Design

Crabka exposes operator-tunable runtime policy without turning compatibility rules or implementation invariants into configuration.

## Design Goals

The codebase contains thousands of named constants and runtime literals, but only a subset represents choices a deployer could reasonably tune.

This work will expose every operational tuning value through the nearest existing configuration path while preserving current behavior as the default.

Operator-managed settings will be available in the relevant CRD and direct deployments will receive equivalent command-line arguments backed by environment variables.

Invalid configuration must be rejected before it reaches runtime behavior.

## Scope

A value is configurable when changing it can reasonably tune availability, latency, throughput, resource use, retry behavior, retention, scheduling, batching, concurrency, network binding, or another deployment policy without changing Crabka's protocol meaning.

The following values remain fixed:

- Kafka and PostgreSQL protocol constants, wire identifiers, error and status codes, serialization markers, and compatibility-mandated defaults.
- Mathematical, calendar, unit-conversion, encoding, and parsing semantics.
- Compile-time safety bounds, array dimensions, algorithmic sentinels, and values whose modification would violate an invariant.
- Test fixtures, benchmark inputs, example data, and deliberately selected differential-test values.
- Dependency versions, schema versions, and other build-time metadata.

The final audit records why every candidate value is configured or excluded. A value is not excluded merely because it is inconvenient to expose.

## Architecture Overview

Each deployable binary continues to own its typed runtime configuration. This avoids a global configuration crate that would couple unrelated services and duplicate existing config structures.

The configuration flow is:

```text
CRD field -> operator validation -> pod argument or environment
                                      |
direct CLI or environment ------------+
                                      v
                              command-line parsing
                                      v
                         refined runtime config type
                                      v
                              service behavior
```

Library callers construct the same refined runtime config types directly. Existing values remain the defaults so an unset field behaves exactly as it does before this work.

## Key Design Decisions

### Use Existing Typed Configuration Paths

The implementation proceeds one deployable service at a time and adds fields to the nearest existing config struct.

Operator-managed workloads receive optional, camel-case CRD tuning structs grouped by component. The operator validates and renders them into the service's supported command-line arguments or environment variables. Process-only operator settings use command-line arguments with environment-variable fallbacks.

A central configuration crate was rejected because it would add a second ownership layer over service-specific configuration. Generic string maps were rejected for new settings because they defer type errors until runtime and provide weak CRD schemas.

### Validate Newtypes with `refined_type`

New validated scalar types use `refined_type::Refined<Rule>`. Reusable constraints cover positive counts, bounded percentages, ports, durations, capacities, and similar values.

Built-in rules are composed where possible. A small named rule is added only when the constraint cannot be expressed by composition. Defaults and parsed values use checked construction; `unsafe_new` is forbidden.

CRDs retain schema-friendly primitive fields with Kubernetes range constraints. Reconciliation immediately converts those primitives into the same refined types used by direct configuration. Command-line and environment parsing likewise constructs refined types before building service state.

Relationships involving multiple fields, such as heartbeat interval being less than heartbeat timeout, remain config-level validation because no scalar newtype can express them.

### Preserve One Source of Runtime Truth

A configurable value is read once into the service config and passed to every consumer. Call sites do not independently read environment variables or apply fallback literals.

When sibling callers share a hardcoded policy, the value moves to their common config owner rather than adding separate overrides to each caller.

### Keep an Auditable Completion Ledger

A checked-in audit document classifies production candidates as configurable or excluded and records the reason for exclusions.

The ledger is reviewed after each service slice and reconciled with a fresh source scan at completion. It is evidence for the repo-wide requirement, not a runtime registry or code-generation input.

## Error Handling

Invalid direct configuration fails command-line parsing or startup with the option name, rejected value, and constraint.

Invalid CRD configuration fails reconciliation before workload rendering and surfaces a specific status condition identifying the field and constraint.

Cross-field validation reports the complete relationship rather than allowing arithmetic saturation or silently rewriting the supplied values.

No invalid value is clamped unless the setting's documented semantics explicitly define clamping.

## Testing

Each configuration slice includes focused behavior tests proving:

- The default produces the pre-change runtime behavior.
- Command-line and environment overrides reach the service config.
- CRD fields render the expected workload argument, environment variable, or config-file value.
- Invalid scalar and cross-field values produce specific boundary errors.

CRD slices regenerate and check manifests and schemas.

Completion requires formatting, Clippy, targeted crate tests, workspace tests where practical, and a fresh production-literal audit reconciled against the decision ledger.
