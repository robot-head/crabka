# G-7 distributed surface closure report

Date: 2026-07-11 UTC

## Result

This run made two bounded advances toward G-7 Tasks 5-6, but it does **not** close the binding brief and makes no Task 7/8 or G-8+ claim.

Implemented:

- Remote simple-query forwarding now returns complete row descriptions, text/binary cells, nulls, command results, and empty results over the framed range protocol. The gateway consumes row-returning responses rather than rejecting remote SELECT.
- The operator no longer rejects multi-range layouts before reconciliation. Each rendered Deployment hosts only its own range (`--host-ranges rN`) instead of every Deployment writing r0.

Still open:

- Engine-owned extended parse/bind/describe/execute/close/sync forwarding and typed parameter/portal state.
- Stateful remote explicit transactions, remote multi-range 2PC, and remote r0 decision/barrier/prologue ownership.
- Separate child-process network kill/recovery and silence-sweeper proof.
- Per-range Kubernetes Services (the registry endpoints currently name Deployments), operator certificate delivery for the range transport, and end-to-end tenant-principal validation.
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

## Commits

- `364c4362 feat(gres): return remote query rows over range transport`
- `ec47588e feat(gres): place one compute per tenant range`

## Limitations

No broker or Kind cluster was provisioned in this run, so no live-cluster, kill/recovery, NetworkPolicy-negative, or conformance evidence exists. The current operator placement is not deployable as a distributed topology until per-range Services and the remote r0/stateful protocol are completed.
