# Gres Early TSO Activation Design

Serve range-0 timestamp grants as soon as range 0 itself recovers, instead of after every hosted range recovers and the tenant assembles.

**Type:** Startup-sequencing change scoped to the live multirange boot path. Follows the G-9a TSO reliability work (successor grace period, gateway grant retry) and closes the "TSO as a liveness dependency" gap named in [G-9](2026-07-09-crabka-gres-g9-distributed-maturity-design.md): every sharded-table transaction in the fleet stalls while the range-0 host is booting, and today that stall spans the recovery of *all* ranges on the host plus tenant assembly, even though the oracle needs only range 0.

## Design Goals

- Shrink the fleet-wide timestamp-grant outage during a range-0 host's startup/failover to approximately range 0's own fence + replay time.
- Change nothing about grant semantics: exactly one live oracle instance per epoch, monotone grants, the successor grace period, and the freshness invariant ("no granted read_ts precedes a commit acknowledged before the grant") all hold unchanged.
- Keep SQL and 2PC serving gated exactly as today: the prologue invariants (fence-before-replay, serving gate after settle) are untouched.

## Architecture Overview

Today the live boot is strictly sequential: `recover_live_multirange_engines` recovers every hosted range, `start_live_multirange_tenant` builds the TSO and the tenant, `open_live_multirange_tenant` builds the `HostedRangeService`, and only then does `start_range_service` bind the range transport listener. Grants are unreachable until the last step.

The change splits "the node can grant timestamps" from "the node serves its ranges":

1. **Bind the listener first.** When the boot is identifiably live-multirange (substrate config present, no `--ranges` dev boundaries, not in-memory bootstrap) and `--range-listen` is set, `serve_listener_with_tenant_config_loader` creates the `DynamicLiveRangeService` up front — wrapping an engine-less **warming** `HostedRangeService` — and binds the range listener before recovery begins. An engine-less `HostedRangeService` already answers every SQL/range request with re-resolvable `StaleEndpoint` errors, so remote gateways treat the warming node exactly like a not-yet-owning host.
2. **Activate the TSO after range 0 recovers.** Range 0 is recovered first (the recovery-config list orders the coordinator ahead of all other ranges). Immediately after its engine and `SubstrateTsoHorizon` are built — fence and replay already done, so `MAX_TS_KEY` is authoritative — the boot constructs the TSO rpc and swaps a warming service carrying it into the dynamic service. From this moment `TsoReq::Grant` succeeds; every other request keeps answering `StaleEndpoint`.
3. **Late-bind the rest.** The remaining ranges recover, the tenant assembles, and the existing `dynamic_service.replace(...)` swaps in the full service — carrying the *same* TSO rpc instance, which `start_live_multirange_tenant` now reuses instead of building anew.

## Key Design Decisions

### One oracle instance per epoch, threaded — not rebuilt

The early-activated TSO rpc is stored on `LiveMultirangeEngines` and reused by `start_live_multirange_tenant`. Building a second `TsoOracle` over the same horizon and epoch was rejected: the second instance would resume past the durable stride while grant RPCs in flight against the first could still complete below it, reproducing the live-zombie freshness hazard *inside one process* — with no fence between the instances to stop it. The single-instance rule is the in-process analogue of "one monotone authority per epoch."

### Warming service = engine-less `HostedRangeService`

A dedicated "recovering" service type was rejected. `HostedRangeService::new(BTreeMap::new())` already produces the exact desired behavior — `StaleEndpoint` ("range rN is not hosted here") for everything, TSO grants once `.with_tso(...)` is attached — and `DynamicLiveRangeService` already wraps `HostedRangeService` concretely. Zero new service surface.

### Early activation only on receipt-free boots

When a split-activation receipt is discovered, `reconcile_before_readiness` may rewrite the range map and topology between recovery and assembly, and the activation flow builds its own TSO under fault injection. Early activation is skipped on those boots; they keep today's conservative sequencing. Normal restarts and failovers — the common case — get the fast path.

### Grants before settle are safe; SQL is not

A timestamp grant reads and writes nothing but the oracle's counter and the durable `max_ts` stride through the already-recovered range-0 committer. It cannot observe intents, locks, or unsettled 2PC state, so serving grants before the in-doubt/settle prologue steps (and before other ranges exist) does not weaken the serving-gate invariant. SQL and timestamp-transaction execution against this host's ranges remain gated behind the full prologue exactly as before — the warming service refuses them. The pre-existing successor grace period inside `TsoOracle` (first grant waits out the predecessor's liveness certificate) is what makes arbitrarily fast activation safe against a fenced-but-alive predecessor.

## Integration

- **Gateways:** unchanged. `RegistryTsoRpc` already retries once with an authoritative registry refresh on re-resolvable and transport errors, so gateways converge on the warming node's grants as soon as they are served.
- **Range transfer / failover (`LiveMultiRangeTransfer`):** unchanged. The successor-staging path is already range-0-scoped; it fences a new generation, so it correctly builds a fresh oracle for the new epoch.
- **Prologue contract (`prologue.rs`):** untouched. Fence-before-replay still holds per range; the serving gate still guards SQL serving. TSO grant availability is deliberately *not* part of that gate.

## Testing

- Recovery-config ordering is a pure function: unit-tested that the coordinator sorts first.
- Early-installation ordering is proven without timing games: recovering a config list whose post-range-0 entry fails still leaves the dynamic service granting timestamps — grants precede full-recovery completion by construction.
- The warming service's behavior (grants once TSO attached, `StaleEndpoint` for SQL before and during) is covered by `HostedRangeService`'s existing unit tests plus a warming-specific test.
- The existing kill/fence process-harness suites (`range0_leader_kill_drain`, cascade kills, jepsen bank/Elle) must pass unchanged — they pin the SQL-side readiness invariants this change must not move.
