# Shared Record Decompression Policy

## Scope

Replace the duplicated decompression budgets in Kafka v2 `RecordBatch` and
legacy v0/v1 `MessageSet` decoding with one validated policy. Expose that
policy through the broker's existing CLI/environment/file configuration and
Kafka `BrokerTuning` CRD surface. Preserve all current defaults and public
decode behavior.

Kafka wire masks, format identifiers, and codec framing remain fixed protocol
invariants.

## Policy

`crabka-compression` owns `RecordDecompressionPolicy` because both
`crabka-protocol` and `crabka-records-legacy` already depend on that crate.
The policy contains:

- `max_ratio: Ratio`, default and immutable upper bound `100`;
- `output_floor: ByteSize`, default `16MiB`;
- `output_ceiling: ByteSize`, default and immutable upper bound `1GiB`.

Construction rejects non-finite or non-positive ratios, non-positive or
fractional byte sizes, a floor above the ceiling, a ratio above 100, or a
ceiling above 1 GiB. The whole-byte validation uses repository
`refined_type` rules at the scalar boundary.

The output budget remains:

```text
clamp(compressed_size * max_ratio, output_floor, output_ceiling)
```

## Decode API

The shared policy owns the budget calculation so legacy and v2 decoding cannot
drift.

Existing decode entry points continue using `RecordDecompressionPolicy::default`
for compatibility. Policy-aware variants are added only where the broker must
pass deployment configuration:

- v2 `RecordBatch` and `RecordsPayload` decoding;
- legacy `decode_message_set` and `legacy_to_v2`.

The broker Produce fallback path passes its configured policy to both formats.
Verbatim v2 passthrough remains unchanged because it does not decompress.
Other library callers retain the current default behavior.

## Configuration

`BrokerConfig` carries the three UOM values and validates them by constructing
the shared policy. The existing runtime overlay exposes:

| CLI | Environment | CRD `brokerTuning` |
|---|---|---|
| `--record-decompression-max-ratio` | `CRABKA_RECORD_DECOMPRESSION_MAX_RATIO` | `recordDecompressionMaxRatio` |
| `--record-decompression-output-floor` | `CRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR` | `recordDecompressionOutputFloor` |
| `--record-decompression-output-ceiling` | `CRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING` | `recordDecompressionOutputCeiling` |

File configuration uses the matching snake-case names in `[runtime]`.
The operator validates the same bounds, renders the runtime TOML, includes it
in the config hash, and regenerates the Kafka CRD.

## Errors

Invalid configuration fails before broker startup and invalid CRD policy fails
reconciliation without rolling pods. A compressed record exceeding the
effective budget continues returning the existing decompression-too-large
error, which Produce maps to its existing invalid-record response.

## Verification

- shared policy unit tests cover defaults, the linear range, both clamps, and
  every invalid relation/security boundary;
- legacy and v2 policy-aware decode tests prove identical budget behavior;
- broker CLI, environment, file overlay, and Produce tests prove configured
  limits reach both decode formats;
- operator tests round-trip rendered TOML through the broker parser and reject
  over-ceiling CRD values;
- regenerate CRDs;
- run focused tests, workspace strict Clippy, nightly formatting, and
  `git diff --check`.
