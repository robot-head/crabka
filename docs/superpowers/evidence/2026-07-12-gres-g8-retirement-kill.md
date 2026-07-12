# GRES G8 Move retirement SIGKILL evidence

Date: 2026-07-12

## Contract

The process nemesis kills and replaces the real GRES child at four exact durable Move retirement boundaries. Every run maintains a continuously acknowledged SQL ledger, injects one deterministic response loss, preserves an unrelated sentinel topic, and completes with the predecessor WAL absent and replacement owner `r2/g1`.

| Kill point | Durable journal | Retirement sidecar | Predecessor topic at kill | Delete calls at kill | Replay deletes | Injected post-delete errors |
|---|---|---|---:|---:|---:|---:|
| `retiring_before_delete` | `Retiring` | `Parking` | yes | 0 | 1 | 0 |
| `retiring_after_delete` | `Retiring` | `Parking` | no | 1 | 0 | 1 |
| `retiring_parked` | `Retiring` | `Parked` | no | 1 | 0 | 0 |
| `resuming` | `Resuming` | `Parked` | no | 1 | 0 | 0 |

Every run issued exactly one delete request, and the request named only the exact predecessor topic. No replay except `BeforeDelete` reissued deletion. All runs preserved the unrelated sentinel topic and observed an ACK after `Completed` became durable.

At `Resuming`, the harness independently replays the sealed `RetirePredecessor` request and requires `AlreadyApplied` from the durable completed control receipt both before SIGKILL and after restart. Earlier boundaries do not issue this mutating probe.

## Historical diagnostic distributions

Values are `maximum acknowledged-write gap / operation duration`, in milliseconds.

These early repetitions reused tenant and operation identities against a persistent broker. They are retained only as diagnostic timing history and are not the final acceptance artifacts.

| Kill point | Run 1 | Run 2 | Run 3 | Maximum gap |
|---|---:|---:|---:|---:|
| `retiring_before_delete` | 9,550 / 40,932 | 10,079 / 40,599 | 10,066 / 42,073 | 10,079 |
| `retiring_after_delete` | 9,444 / 44,327 | 9,037 / 42,716 | 9,462 / 43,028 | 9,462 |
| `retiring_parked` | 9,865 / 42,630 | 10,549 / 43,573 | 9,388 / 42,687 | 10,549 |
| `resuming` | 9,652 / 41,855 | 9,067 / 42,122 | 9,856 / 42,156 | 9,856 |

The enforced retirement bound is 12,000 ms. The largest observed gap was 10,549 ms, leaving 1,451 ms (13.8 percent) headroom and matching the established cutover bound.

## Authoritative unique-identity validation

After hardening every invocation with a timestamp-and-PID tenant/operation suffix and asserting the operation was absent before CLI initiation, the dedicated shard passed all four cases and its uniqueness validator:

| Kill point | ACK gap | Operation duration | Receipt replay before/after restart |
|---|---:|---:|---|
| `retiring_before_delete` | 9,764 ms | 41,348 ms | false / false |
| `retiring_after_delete` | 9,668 ms | 43,887 ms | false / false |
| `retiring_parked` | 10,173 ms | 43,731 ms | false / false |
| `resuming` | 9,662 ms | 41,967 ms | true / true |

All authoritative gaps remained below 12,000 ms. The Resuming probes received `AlreadyApplied` from the exact durable completed `RetirePredecessor` receipt both before SIGKILL and after restart.

## Recovery findings

- Restart must defer hosted-range validation until activation recovery selects its map; otherwise a valid target-only `r2` host set is rejected against the static predecessor map.
- A target-phase operation journal is the authority that promotes non-range-zero activation recovery to the exact target map. A terminal activation receipt alone cannot distinguish a pre-CAS Activated crash from post-publication recovery.
- All post-activation restarts host authoritative `r0,r2`; pre-activation `Restored` recovery continues to host `r0,r1`.
- The `AfterDelete` wrapper returns an injected error only after the real broker deletion succeeds. Restart observes the absent topic, advances the sidecar to `Parked`, and does not issue a second delete.
