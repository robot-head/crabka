# G-7 slice review fix report

Date: 2026-07-11 UTC

## Result

Both Important findings in `.superpowers/sdd/g7-slice-review.md` are addressed.

1. Registry endpoints now name per-range Kubernetes Services. The operator applies those Services before committing their DNS names to the registry, each Service selects exactly one `crabka.io/gres-range` label, and the aggregate PostgreSQL Service has a distinct multi-range name. Reconciliation observes every range Deployment after apply and exposes `Ready=True` only when its current generation has all desired replicas available; otherwise it reports `Ready=False / ComputeProgressing`.
2. SQL row results no longer need to fit a single 1 MiB frame. The framed server moves rows through bounded chunks, sends field metadata only once, marks the final command tag explicitly, and the client validates ordering while reconstructing the original result list. Nulls, text and binary encodings, result boundaries, empty/command results, and tags are preserved. An indivisible oversized row/description/tag returns SQLSTATE `54000`; partial client accumulation is discarded and a subsequent connection remains usable.

## Strict TDD evidence

Transport RED:

```text
cargo test -p crabka-gres-ranges remote_query_pages_results_larger_than_one_transport_frame --no-fail-fast
remote_query_pages_results_larger_than_one_transport_frame ... FAILED
Transport(Io(Kind(UnexpectedEof)))
```

Transport GREEN:

```text
cargo test -p crabka-gres-ranges remote_query_pages_results_larger_than_one_transport_frame --no-fail-fast
1 passed; 0 failed

cargo test -p crabka-gres-ranges oversized_single_row_returns_bounded_error_and_does_not_poison_server --no-fail-fast
1 passed; 0 failed
```

Operator renderer/readiness RED failures were missing `render_range_service` and `deployment_is_ready`, respectively. GREEN:

```text
timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator range_service_has_stable_registry_name_and_selects_only_its_range --no-fail-fast
1 passed; 0 failed

timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator deployment_readiness_requires_observed_generation_and_available_replicas --no-fail-fast
1 passed; 0 failed
```

The three-range mock initially failed because its obsolete rejection path expected a five-minute requeue; after replacing that path with the deployable topology assertion and correcting the live reconcile interval:

```text
timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator --test reconcile_gres_tenant multi_range_tenant_publishes_range_services_and_becomes_ready_after_all_deployments --no-fail-fast
1 passed; 0 failed
```

`rustfmt --edition 2024` and `git diff --check` passed for changed source files. Stable rustfmt printed only the repository's existing nightly-option warnings.

## Commits

- `e0c38ebd fix(gres): stream bounded remote query result frames`
- `f7d19a85 fix(operator): publish stable range services truthfully`
- `903ced0f test(operator): prove multi-range service readiness`

## Broader-test note

The full `reconcile_gres_tenant` integration target was run and reported 9 passing / 9 failing. The focused new multi-range test passed. The remaining failures are pre-existing lifecycle/sticky-legacy expectations (parking, resume, WAL deletion, and legacy `MultiRangeUnsupported`) outside these two review findings; they are not represented as green evidence here.

The first cold operator build in `/tmp/crabka-g7-slice-target` stopped with `Disk quota exceeded`; that task-local target was removed. This is recorded only as an environment event, not test evidence.
