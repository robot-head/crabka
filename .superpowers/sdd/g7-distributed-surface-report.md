# G-7 distributed surface closure report

Date: 2026-07-11 UTC

## Result

This run made two bounded advances toward G-7 Tasks 5-6, but it does **not** close the binding brief and makes no Task 7/8 or G-8+ claim.

Implemented:

- Remote simple-query forwarding now returns complete row descriptions, text/binary cells, nulls, command results, and empty results over the framed range protocol. The gateway consumes row-returning responses rather than rejecting remote SELECT.
- The operator no longer rejects multi-range layouts before reconciliation. Each rendered Deployment hosts only its own range (`--host-ranges rN`) instead of every Deployment writing r0.
- Follow-up review fixes now publish one stable, range-selective Service per range before recording its DNS name, retain a separate multi-range PostgreSQL front door, and report `Ready=True` only after every desired Deployment generation has the required available replicas.
- Remote simple-query results are emitted as an ordered sequence of sub-1 MiB frames. Field metadata is sent once, row chunks preserve nulls and both cell encodings, and the client reconstructs result boundaries and command tags. A single indivisible row that exceeds the limit fails deterministically with SQLSTATE `54000` without poisoning later connections.

Still open:

- Engine-owned extended parse/bind/describe/execute/close/sync forwarding and typed parameter/portal state.
- Stateful remote explicit transactions, remote multi-range 2PC, and remote r0 decision/barrier/prologue ownership.
- Separate child-process network kill/recovery and silence-sweeper proof.
- Operator certificate delivery for the range transport and end-to-end tenant-principal validation.
- Actual bounded Kind two/three-range smoke, outside-tenant refusal, and single-range conformance baseline.
- Full workspace check/clippy and independent review.

## TDD evidence

RED:

```text
cargo test -p crabka-gres-ranges forward::tests::remote_query_returns_fields_and_cells --no-fail-fast
error[E0599]: no method named `forward_query` found for struct `RegistryRemoteForward`
```

GREEN:

```text
cargo test -p crabka-gres-ranges forward::tests::remote_query_returns_fields_and_cells --no-fail-fast
test forward::tests::remote_query_returns_fields_and_cells ... ok
test result: ok. 1 passed; 0 failed
```

RED:

```text
cargo test -p crabka-operator each_multi_range_deployment_hosts_only_its_own_range --no-fail-fast
assertion failed: args.windows(2).any(|pair| pair == ["--host-ranges", "r1"])
```

GREEN:

```text
cargo test -p crabka-operator each_multi_range_deployment_hosts_only_its_own_range --no-fail-fast
exit 0
```

Broader range suite exposed one stale assertion after the intentional capability change:

```text
cargo test -p crabka-gres-ranges --tests --no-fail-fast
98 library tests passed; 1 failed:
tenant::tests::unhosted_remote_query_never_falls_back_to_range_zero
```

The assertion expected the removed generic "remote range queries unsupported" error. It was updated to pin the actual no-registry/no-owner failure (`range r1 is not hosted`) while retaining the no-fallback guarantee. A clean rerun remains required.

Formatting was applied with `cargo fmt --all`. Stable rustfmt emitted the repository's existing warnings for nightly-only configuration keys.

Follow-up focused evidence:

```text
cargo test -p crabka-gres-ranges remote_query_pages_results_larger_than_one_transport_frame --no-fail-fast
1 passed; 0 failed

cargo test -p crabka-gres-ranges oversized_single_row_returns_bounded_error_and_does_not_poison_server --no-fail-fast
1 passed; 0 failed

timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator --test reconcile_gres_tenant multi_range_tenant_publishes_range_services_and_becomes_ready_after_all_deployments --no-fail-fast
1 passed; 0 failed

timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator range_service_has_stable_registry_name_and_selects_only_its_range --no-fail-fast
1 passed; 0 failed

timeout 180s env CARGO_BUILD_JOBS=1 cargo test -p crabka-operator deployment_readiness_requires_observed_generation_and_available_replicas --no-fail-fast
1 passed; 0 failed
```

## Commits

- `364c4362 feat(gres): return remote query rows over range transport`
- `ec47588e feat(gres): place one compute per tenant range`
- `e0c38ebd fix(gres): stream bounded remote query result frames`
- `f7d19a85 fix(operator): publish stable range services truthfully`
- `903ced0f test(operator): prove multi-range service readiness`

## Limitations

No broker or Kind cluster was provisioned in this run, so no live-cluster, kill/recovery, NetworkPolicy-negative, or conformance evidence exists. The current operator placement is not deployable as a distributed topology until per-range Services and the remote r0/stateful protocol are completed.
