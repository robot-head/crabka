# crabka-gres-loadtest

Scenario-driven scalability and fault-injection harness for `crabka-gres`, Crabka's PostgreSQL-compatible SQL engine.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

The harness boots a real multi-process cluster — one `crabka-broker` plus N `crabka-gres` compute nodes — with every inter-node and client-facing TCP endpoint fronted by a chaos proxy. A YAML scenario describes the topology, the timestamp-source mode (Percolator-style `logical-tso` or hybrid logical clock `hlc`), the SQL workload mix, and a timeline of network faults. A run produces a JSON report plus a Markdown summary; `compare` runs the same scenario under both timestamp modes and renders them side by side, so the cost of the global-timestamp path is measurable per workload shape and per fault. An external mode (`run --external`) drives the identical workload against any pgwire-speaking SQL system without launching crabka — see [External systems](#external-systems).

## Prerequisites

Build the binaries the harness launches:

```bash
cargo build -p crabka-gres -p crabka-broker -p crabka-cli
```

Binaries are resolved from `target/debug/` under the workspace root; point `CRABKA_GRES_LOADTEST_GRES_BIN`, `CRABKA_GRES_LOADTEST_BROKER_BIN`, or `CRABKA_GRES_LOADTEST_CLI_BIN` at other builds to override.

## Quick Start

```bash
# Parse and validate a scenario without running it
cargo run -p crabka-gres-loadtest -- validate \
  --scenario crates/gres-loadtest/scenarios/tso-partition.yaml

# Run one scenario (reports land in loadtest-out/)
cargo run -p crabka-gres-loadtest -- run \
  --scenario crates/gres-loadtest/scenarios/tso-partition.yaml

# Force a timestamp mode regardless of the scenario's own
cargo run -p crabka-gres-loadtest -- run \
  --scenario crates/gres-loadtest/scenarios/baseline-single-shard.yaml \
  --mode hlc --hlc-max-offset-ms 250

# Run under logical-tso then hlc and render a side-by-side comparison
cargo run -p crabka-gres-loadtest -- compare \
  --scenario crates/gres-loadtest/scenarios/baseline-single-shard.yaml
```

`--out <dir>` changes the report directory; `--keep-work-dir` retains the cluster's data and log directory after a successful run (it is always retained on failure).

## Bundled Scenarios

| Scenario | Purpose |
|----------|---------|
| `baseline-single-shard` | Pure single-shard insert saturation, no faults — the headline scalability number and the cost of the global timestamp path on the hot write loop. |
| `mixed-oltp` | Representative OLTP mix at a fixed rate — steady-state efficiency (txn per CPU-second) and tail latency between modes at identical offered load. |
| `cross-shard-heavy` | Cross-shard-transaction-dominated load — worst case for the centralized oracle, best probe of 2PC + global-timestamp coordination overhead. |
| `tso-partition` | Partitions range 0 (catalog, coordinator, timestamp authority) mid-run — how much availability the timestamp mode buys when the oracle's link dies. |
| `node-crash` | SIGKILLs node 2 and restarts it 10 s later — blast radius, WAL fence-and-replay recovery time, post-restart throughput recovery. |
| `flappy-network` | Flaps the link to range 1 and throttles range 2 — reconnect storms and retry behavior under an unreliable rather than cleanly-partitioned network. |
| `wan-latency` | 50 ms ± 10 ms one-way delay on every inter-node link for the whole window — which coordination steps a node-local clock removes from the critical path. |
| `clock-skew` | HLC-only: skews the authority node's wall clock +400 ms, beyond the 250 ms uncertainty bound — throughput and correctness-adjacent symptoms under skew. |

## Scenario Schema

A scenario is one YAML document:

```yaml
name: my-scenario          # used in report file names
description: One line for the report header.
topology:
  nodes: 3                 # crabka-gres processes
  ranges: 4                # ranges r0..r3, round-robin over nodes (r0 on node 0)
  clock_skew_ms: { 0: 400 }  # per-node HLC wall-clock offset (hlc mode only)
  cpus_per_node: 3           # optional: pin each node to 3 dedicated CPUs
                             # (broker gets CPUs 0-1). Makes each node a
                             # fixed-capacity "host" so single-machine scaling
                             # curves measure the architecture, not one box
                             # partitioned N ways. For full isolation run the
                             # harness itself under `taskset` on the leftover
                             # CPUs. Fails fast if 2 + nodes*cpus exceeds the
                             # machine.
mode: logical-tso          # or:  mode: { hlc: { max_offset_ms: 250 } }
workload:
  connections: 24          # concurrent clients, round-robin over node front doors
  rate: saturate           # or:  rate: { fixed: { tps: 2000 } }
  warmup_s: 10             # unrecorded load before measurement
  duration_s: 60           # measured window
  mix: { single_shard_insert: 70, read_only: 30 }
  hot_rows: 1000           # contended-update table size
  zipf_exponent: 1.1       # contended-update skew
faults:
  - at_s: 20               # seconds after the measurement window starts
    partition: { target: "range:0", duration_s: 15, style: blackhole }
```

### Workload mix classes

Weights are relative; classes with weight 0 are never issued.

| Class | What it exercises |
|-------|-------------------|
| `single_shard_insert` | Autocommit single-row insert into one range — the hot write loop and per-range write throughput. |
| `cross_shard_txn` | Explicit transaction writing two different ranges — 2PC and the global-timestamp path. |
| `read_only` | Bounded snapshot read on one range — read-path latency and snapshot timestamp acquisition. |
| `contended_update` | Zipf-distributed update of a small hot table — serialization conflicts and retry behavior. |

### Fault actions

Every action takes a `target`; timed actions un-apply themselves after `duration_s`.

| Action | Fields | Effect |
|--------|--------|--------|
| `partition` | `target`, `duration_s`, `style` | Cuts the target's link, heals it after the duration. |
| `latency` | `target`, `ms`, `jitter_ms`, `duration_s` | One-way delay per direction (a round trip pays roughly twice `ms`). |
| `throttle` | `target`, `bytes_per_sec`, `duration_s` | Per-direction bandwidth cap. |
| `kill_node` | `node`, `restart_after_s` | SIGKILLs the node's process; restarts it after the delay, or leaves it down when omitted. |
| `flap` | `target`, `period_s`, `duration_s` | Alternates blackhole-partition and heal every `period_s`; always ends healed by `at_s + duration_s`. |

Targets: `range:<id>` (one range's RPC endpoint), `all-ranges`, `sql:<node>` (one node's SQL front door), `all-sql`.

Partition styles: `blackhole` (default) makes packets vanish — live connections stall without FIN/RST, peers see timeouts, and connections survive if the link heals before the application gives up, like a real network partition. `reset` closes live connections and refuses new ones — peers see immediate errors, like an administratively-down endpoint.

## Reports

Each run writes two files into the output directory, named `<scenario>-<mode-slug>` (`logical-tso` or `hlc`):

- `<scenario>-<mode>.json` — the full `RunReport`: committed/failed transactions and mean TPS, latency percentiles (p50/p95/p99/p99.9/max) per operation class, an error taxonomy (serialization retries, unavailable, connection errors, other), a per-second timeline (throughput dips make faults visible), per-process CPU core-seconds and peak RSS, cluster-wide efficiency (committed txn per CPU core-second), and the applied-fault log.
- `<scenario>-<mode>.md` — the same run rendered as Markdown, with the timeline reduced to the interesting seconds (near a fault or deviating from median throughput).

`compare` additionally writes `<scenario>-comparison.md`: both modes side by side with relative deltas and both fault logs.

## External systems

`run --external` points the identical workload, measurement, and reporting pipeline at any pgwire-speaking SQL system — CockroachDB, YugabyteDB, PostgreSQL, or a remote crabka cluster — without launching any crabka processes:

```bash
cargo run -p crabka-gres-loadtest -- run \
  --scenario crates/gres-loadtest/scenarios/mixed-oltp.yaml \
  --external "127.0.0.1:26257,127.0.0.1:26258,127.0.0.1:26259" \
  --external-user roach --external-database bench
```

- `--external` takes a comma-separated `host:port` list; the workload's connections spread round-robin across the endpoints exactly as they do across crabka node front doors.
- `--external-user` and `--external-database` are required (external mode never guesses credentials); `--external-password` is optional, and omitting it means no password.
- Schema prep is the same standard SQL as against crabka (`DROP TABLE IF EXISTS` + `CREATE TABLE t<id> (id int4)` per table, a `(id int4, v int4)` hot table, multi-row `INSERT` seeding), so re-runs against a persistent system start clean.
- The scenario's `topology.ranges` sets the number of `t{i * 1000000}` workload tables; an external system spreads those tables by its own sharding, so "ranges" simply means "N tables" there. `topology.nodes` is only recorded in the report.
- The scenario's `mode` is ignored (with a logged notice) — the external system uses its own timestamp source — and the report's `mode` field is the string `external`; reports are named `<scenario>-external.{json,md}`.
- The scenario's `faults` must be empty: no chaos proxies front an external system, so `run --external` fails fast on a fault timeline. `compare` does not support `--external` for the same reason it exists — it contrasts crabka's timestamp modes on a harness-launched cluster; run `run --external` once per target and diff the reports.

**Resource sampling.** The harness discovers which local processes serve the given ports (`/proc/net/tcp{,6}` listening-socket inodes matched against `/proc/<pid>/fd`) and samples them under `ext:<port>` labels. Discovery only covers loopback endpoints — a remote system's processes cannot be read from the local `/proc`. For multi-process systems (e.g. a YugabyteDB master + tserver) or when `/proc` is permission-restricted, pass the roster manually:

```bash
--external-pids "master=41230,tserver=41288"
```

If discovery finds nothing and no override is given, the run warns and proceeds: throughput and latency are still measured, the report's resource and efficiency sections are just empty.

**Fairness caveats for any cross-system comparison.** Numbers from `run --external` are only comparable to a crabka run (or another external run) after accounting for:

- *Replication factor.* The harness launches crabka unreplicated (one broker, replication factor 1); CockroachDB and YugabyteDB default to 3× replication and pay for it on every write. Compare against a 1× external cluster, or a replicated crabka deployment, not across durability models.
- *Commit durability.* fsync/commit settings differ per system (`synchronous_commit`, YCQL/YSQL durability knobs, crabka's broker flush policy). Align them explicitly before quoting a delta.
- *No fault injection.* External runs measure fault-free steady state only; crabka scenario results that include fault windows are not comparable.
- *Resource attribution scope.* `ext:<port>` rows cover only the discovered (or manually listed) local processes — a remote node, a separate storage service, or an undiscovered helper process contributes load but no CPU/RSS rows, so txn-per-CPU-second is comparable only when the rosters cover equivalent scopes.
- *Everything in [Limitations](#limitations)* — debug vs release builds, shared-machine noise — applies doubly across systems.

### Worked example: local PostgreSQL

```bash
# A throwaway PostgreSQL 16 on the default port.
docker run --rm -d --name loadtest-pg -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 postgres:16

# Wait for readiness, then create the benchmark database.
docker exec loadtest-pg pg_isready -t 30
docker exec loadtest-pg psql -U postgres -c 'CREATE DATABASE bench'

# Drive the mixed OLTP scenario against it.
cargo run --release -p crabka-gres-loadtest -- run \
  --scenario crates/gres-loadtest/scenarios/mixed-oltp.yaml \
  --external "127.0.0.1:5432" \
  --external-user postgres --external-password secret --external-database bench

docker stop loadtest-pg
```

Reports land in `loadtest-out/mixed-oltp-external.{json,md}`. Note that a containerized server may defeat pid discovery from the host's `/proc` view depending on the runtime; `--external-pids "postgres=$(docker inspect -f '{{.State.Pid}}' loadtest-pg)"` pins the roster explicitly (that pid samples the postmaster only — PostgreSQL backends are separate forked processes, so per-connection CPU is not attributed; treat resource rows as a lower bound).

## Limitations

- **Single-authority HLC today.** Non-r0 nodes still fetch timestamps from range 0 via RPC, so per-node `clock_skew_ms` is only observable on the node hosting range 0 until multi-node HLC stamping lands.
- **Restarted nodes report as separate rows.** The `/proc` sampler follows a live process roster: a node restarted by `kill_node` is attached at the next sample tick under a `label#N` entry (e.g. `node2#2`), so its post-restart CPU/RSS is counted alongside — not merged into — the original incarnation's row.
- **Localhost only.** The chaos proxies model the network in user space; OS-level `netem`/`tc` shaping is out of scope.
- **Relative numbers, not benchmarks.** Results depend on the machine, debug/release build, and concurrent load; they are for comparing modes and scenarios on the same host, not for absolute claims.

## Documentation

- [Bundled scenarios](scenarios/)
- [Style guides](../../docs/style_guides/README.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
