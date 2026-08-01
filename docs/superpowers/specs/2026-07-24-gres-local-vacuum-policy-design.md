# Gres Local Vacuum Policy Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Scope

Expose the operational policy for Gres's adaptive local MVCC vacuum loop.
The loop runs only for the in-memory and `--data-dir` engines.
Substrate/replicated engines report `supports_local_vacuum() == false` and use
checkpoint-time garbage collection, so this policy has no CRD surface.

## Configuration Surface

`ServeArgs` gains one flattened `LocalVacuumOptions` group. Every field is
optional at parsing time, has an environment binding, and has no Clap default:

| CLI | Environment | Effective local default |
|---|---|---:|
| `--local-vacuum-idle-interval-ms` | `CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS` | `2000` |
| `--local-vacuum-backoff-floor-ms` | `CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS` | `25` |
| `--local-vacuum-hot-debt` | `CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT` | effective base key budget |
| `--local-vacuum-key-budget` | `CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET` | `crabka_pgexec::VACUUM_STEP_KEY_BUDGET` |
| `--local-vacuum-max-key-budget` | `CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET` | checked `4 ×` effective base key budget |
| `--local-vacuum-step-fast-ms` | `CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS` | `3` |
| `--local-vacuum-step-slow-ms` | `CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS` | `12` |
| `--local-vacuum-idle-after-ms` | `CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS` | `1000` |

Reuse the existing `refined_type`-backed `PositiveMillis` and `PositiveUsize`
types. Use `NonZeroU64` for hot debt; it already provides the required scalar
validation without another wrapper.

## Effective Policy

One internal, copyable `LocalVacuumPolicy` contains primitive `Duration`,
`usize`, and `u64` values. It is constructed only when a local engine can run
the loop. Construction applies defaults and validates:

- backoff floor is no greater than the idle interval;
- base key budget is no greater than the maximum key budget;
- the fast-step threshold is strictly less than the slow-step threshold;
- deriving `4 ×` base budget and converting the derived hot-debt default to
  `u64` cannot overflow.

Any explicit local-vacuum option with `--substrate-bootstrap` is rejected
before tenant, Kafka, object-store, or runtime I/O. This prevents accepted but
unused configuration. No explicit option is required for ordinary local mode.

## Runtime Wiring

`VacuumPacer` receives and stores `LocalVacuumPolicy`; it no longer reads
module constants. The policy controls:

- initial and settled-loop cadence;
- multiplicative-backoff floor and ceiling;
- debt threshold for back-to-back steps;
- initial/minimum and maximum step budgets;
- fast/slow latency thresholds for budget adaptation.

`run_local_vacuum_loop` receives the same policy. It uses the configured
foreground-idle window and uses the configured idle interval as the existing
maintenance-rotation ceiling. There is no second maintenance knob because the
current behavior intentionally couples those cadences.

## Fixed Semantics

The following remain code, not configuration:

- vacuum stays enabled for local engines and disabled for substrate engines;
- hot mode uses zero delay;
- backoff and budget growth/shrink use factors of two;
- the default maximum is four times the effective base budget;
- debt uses saturating arithmetic;
- clean-cycle, dirty-cycle, foreground-idle, and store-settled state
  transitions;
- the statistics included in `versions_settled` and `swept_anything`;
- maintenance runs only after physical mutation.

These are algorithm/state-machine semantics rather than deployment policy.

## Tests and Verification

Tests must prove:

- parser defaults remain absent while CLI overrides environment;
- every environment binding works in an isolated child process;
- zero scalar values and all three invalid cross-field relationships reject;
- every explicit local-vacuum option rejects in substrate mode before runtime
  I/O;
- the effective defaults reproduce current pacing;
- a custom policy changes hot-debt activation, backoff bounds, adaptive budget
  bounds, latency decisions, foreground-idle detection, and maintenance
  cadence;
- local engines spawn the loop with the effective policy and substrate engines
  do not.

Run the full Gres suite, strict all-target Clippy, formatting, help-surface
checks, `git diff --check`, and a focused runtime-value scan. Independent
review must confirm that every exposed value has one owner and a live consumer.
