# Traces Scan Concatenation Cap Design

## Goal

Replace the fixed traces scan-concatenation memory cap with a validated runtime
setting while preserving the existing 1,500,000,000-byte default and Arrow
safety ceiling.

## Scope

This slice covers `CrabkaSpanStore` merging cold and live scan batches before
nested-set recomputation. It does not change block-read limits, query limits,
Arrow schemas, filtering, or concatenation behavior below the cap.

## Public Configuration

The traces binary accepts:

```text
--scan-concat-max-bytes
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES
```

Command-line values win over environment values. The default remains
1,500,000,000 bytes.

Values must be between 1 and 1,500,000,000 bytes inclusive. Malformed, zero,
negative, primitive-overflow, and above-ceiling values are rejected by Clap
before object-store or network I/O.

## Validated Type and Safety Ceiling

Add `ScanConcatMaxBytes(u64)` to the traces querier store module. It uses
`refined_type::rule::MinMaxU64<1, 1_500_000_000>` and implements the parsing
and display traits needed by Clap. It exposes a `crabka_units::ByteSize` for
the runtime comparison.

The 1,500,000,000-byte upper bound is not configurable. Arrow variable-length
columns use signed 32-bit offsets, so raising the cap beyond the existing safe
headroom has no sensible operational use. Operators may lower the setting to
bound memory more tightly.

The traces crate adds the existing workspace-pinned `refined_type` dependency.
No new external dependency or abstraction is introduced.

## Runtime Flow

`CrabkaSpanStore` stores the validated cap. Its existing constructor remains a
default-preserving compatibility wrapper, and a configurable constructor
accepts the validated value.

The traces querier and live-store construction paths pass the parsed setting to
the configurable constructor. `recompute_scan_nested_sets` receives the stored
cap and keeps its existing behavior: an exact-cap result is accepted, and a
larger result returns the actionable store error before `concat_batches`.

## Deployment Wiring

The observability Docker Compose deployment exposes
`CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES` only on `traces-querier`, preserving the
1,500,000,000-byte default. The demo does not run the separate live-store role.

No CRD or operator field is added because traces is not managed by an existing
repository CRD.

## Tests

Test-first coverage will prove:

1. the validated type accepts 1 and 1,500,000,000;
2. it rejects zero, malformed, negative, primitive-overflow, and values above
   the fixed safety ceiling;
3. the binary preserves the default, reads the environment, and prefers the
   command line;
4. `CrabkaSpanStore::new` preserves the default;
5. an exact-cap merge is accepted and an over-configured-cap merge is rejected;
   and
6. Docker Compose preserves the default and accepts an override.

Completion gates are focused tests, affected all-target tests, strict Clippy,
nightly formatting, one help entry, Compose validation and rendering, diff
hygiene, scanner stability, and lockfile inspection.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts plus verification evidence.
