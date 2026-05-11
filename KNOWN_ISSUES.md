# Known issues

## Captured-traffic corpus deviation from coverage acceptance criterion #9

The coverage meta-spec
(`docs/superpowers/specs/2026-05-11-crabka-protocol-coverage-design.md`)
acceptance criterion #9 requires a captured-traffic corpus entry per
`(api_key, version)` pair. Sub-plan 1d explicitly does not build the
corpus. Differential testing (default-fixture per pair on PR CI;
256 proptest per pair nightly) is the substitute.

Rationale: building ~1000 corpus entries via real broker captures
(high setup cost) or oracle-synthetic generation (which proves
nothing differential testing doesn't) is not worth the work for the
validation value it adds. The corpus remains useful for regression
reproduction; growth is deferred to a future maintenance task.

Status: open. Tracked here pending a future maintenance pass.
