# crabka-gres-loadtest

Scenario-driven scalability and fault-injection harness for `crabka-gres`, Crabka's PostgreSQL-compatible SQL engine.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

The harness boots a real multi-process cluster: one `crabka-broker` plus N `crabka-gres` compute nodes. A chaos proxy fronts every inter-node and client-facing TCP endpoint. A YAML scenario describes the topology, the timestamp-source mode, the SQL workload mix, and a timeline of network faults. The timestamp-source mode is Percolator-style `logical-tso` or hybrid logical clock `hlc`.

A run produces a JSON report and a Markdown summary. `compare` runs the same scenario under both timestamp modes and renders them side by side, so you can measure the cost of the global-timestamp path per workload shape and per fault. An external mode (`run --external`) drives the identical workload against any pgwire-speaking SQL system and launches no crabka processes. See [External systems](#external-systems).

## Prerequisites

Build the binaries the harness launches:

```bash
cargo build -p crabka-gres -p crabka-broker -p crabka-cli
```

The harness resolves the binaries from `target/debug/` under the workspace root. To use other builds, point `CRABKA_GRES_LOADTEST_GRES_BIN`, `CRABKA_GRES_LOADTEST_BROKER_BIN`, or `CRABKA_GRES_LOADTEST_CLI_BIN` at them.

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
  --mode hlc --hlc-max-offset 250ms

# Run under logical-tso then hlc and render a side-by-side comparison
cargo run -p crabka-gres-loadtest -- compare \
  --scenario crates/gres-loadtest/scenarios/baseline-single-shard.yaml
```

`--out <dir>` changes the report directory. `--keep-work-dir` keeps the cluster's data and log directory after a successful run. The harness always keeps that directory after a failure.

## Bundled Scenarios

| Scenario | Purpose |
|----------|---------|
| `baseline-single-shard` | Pure single-shard insert saturation with no faults. Gives the headline scalability number and the cost of the global timestamp path on the hot write loop. |
| `mixed-oltp` | Representative OLTP mix at a fixed rate. Gives the steady-state efficiency in txn per CPU-second and the tail latency between modes at identical offered load. |
| `cross-shard-heavy` | Load dominated by cross-shard transactions. This is the worst case for the centralized oracle and the best probe of 2PC and global-timestamp coordination overhead. |
| `tso-partition` | Partitions range 0, the catalog, coordinator, and timestamp authority, in the middle of the run. Shows how much availability the timestamp mode gives when the oracle's link fails. |
| `node-crash` | SIGKILLs node 2 and restarts it 10 s later. Shows the extent of the failure, the WAL fence-and-replay recovery time, and the throughput recovery after the restart. |
| `flappy-network` | Flaps the link to range 1 and throttles range 2. Shows reconnect storms and retry behavior on an unreliable network instead of a cleanly-partitioned network. |
| `wan-latency` | 50 ms ± 10 ms one-way delay on every inter-node link for the whole window. Shows which coordination steps a node-local clock removes from the critical path. |
| `clock-skew` | HLC only. Skews the authority node's wall clock +400 ms, which is more than the 250 ms uncertainty bound. Shows throughput and correctness-adjacent symptoms under skew. |

## Scenario Schema

A scenario is one YAML document. **Every dimensioned value carries its unit**, for example `60s`, `250ms`, `2000/s`, and `128KiB/s`. The harness rejects a bare number and does not guess it, because `30` can mean seconds or milliseconds and the unit settles that. Durations accept `ns`/`µs`/`ms`/`s`/`m`/`h`/`d`. Sizes and byte rates accept the binary prefixes `KiB`, `MiB`, and `GiB`, and the decimal prefixes `kB`, `MB`, and `GB`. Event rates accept `/s`, `/min`, or `Hz`.

```yaml
name: my-scenario          # used in report file names
description: One line for the report header.
topology:
  nodes: 3                 # crabka-gres processes
  ranges: 4                # ranges r0..r3, round-robin over nodes (r0 on node 0)
  clock_skew: { 0: 400ms }   # per-node HLC wall-clock offset (hlc mode only)
  cpus_per_node: 3           # optional: pin each node to 3 dedicated CPUs
                             # (broker gets CPUs 0-1). Makes each node a
                             # fixed-capacity "host" so single-machine scaling
                             # curves measure the architecture, not one box
                             # partitioned N ways. For full isolation run the
                             # harness itself under `taskset` on the leftover
                             # CPUs. Fails fast if 2 + nodes*cpus exceeds the
                             # machine.
mode: logical-tso          # or:  mode: { hlc: { max_offset: 250ms } }
workload:
  connections: 24          # concurrent clients, round-robin over node front doors
  rate: saturate           # or:  rate: { fixed: { target_rate: 2000/s } }
  warmup: 10s              # unrecorded load before measurement
  duration: 60s            # measured window
  mix: { single_shard_insert: 70, read_only: 30 }
  hot_rows: 1000           # contended-update table size
  zipf_exponent: 1.1       # contended-update skew
faults:
  - at: 20s                # offset from the start of the measurement window
    partition: { target: "range:0", duration: 15s, style: blackhole }
```

### Workload mix classes

Weights are relative. The harness never issues a class with weight 0.

| Class | What it exercises |
|-------|-------------------|
| `single_shard_insert` | Autocommit single-row insert into one range. This is the hot write loop and the per-range write throughput. |
| `cross_shard_txn` | Explicit transaction that writes two different ranges. This exercises 2PC and the global-timestamp path. |
| `read_only` | Bounded snapshot read on one range. This exercises read-path latency and snapshot timestamp acquisition. |
| `contended_update` | Zipf-distributed update of a small hot table. This exercises serialization conflicts and retry behavior. |

### Fault actions

Every action takes a `target`. Timed actions un-apply themselves after `duration`.

| Action | Fields | Effect |
|--------|--------|--------|
| `partition` | `target`, `duration`, `style` | Cuts the target's link, heals it after the duration. |
| `latency` | `target`, `delay`, `jitter`, `duration` | One-way delay per direction. A round trip costs about twice `delay`. The harness adds a uniform draw from `0..jitter`. |
| `throttle` | `target`, `rate`, `duration` | Per-direction bandwidth cap, for example `rate: 128KiB/s`. |
| `kill_node` | `node`, `restart_after` | SIGKILLs the node's process and restarts it after the delay. If you omit `restart_after`, the node stays down. |
| `flap` | `target`, `period`, `duration` | Alternates blackhole-partition and heal every `period`. The link is always healed by `at + duration`. |

Targets: `range:<id>` (one range's RPC endpoint), `all-ranges`, `sql:<node>` (one node's SQL front door), `all-sql`.

Partition styles: `blackhole` is the default and discards packets. Live connections stall without FIN/RST, peers see timeouts, and connections survive if the link heals before the application gives up. This is the behavior of a real network partition. `reset` closes live connections and refuses new ones. Peers see immediate errors, as they do with an administratively-down endpoint.

## Reports

Each run writes two files into the output directory. Their names use `<scenario>-<mode-slug>`, where the mode slug is `logical-tso` or `hlc`:

- `<scenario>-<mode>.json` holds the full `RunReport`. It has the committed and failed transaction counts and the mean transaction rate, the p50/p95/p99/p99.9/max latency percentiles per operation class, and an error taxonomy of serialization retries, unavailable, connection errors, and other. It also has a per-second timeline, the per-process CPU time and peak RSS, the cluster-wide efficiency in committed txn per CPU core-second, and the applied-fault log. Throughput dips in the timeline make the faults visible.
- `<scenario>-<mode>.md` holds the same run in Markdown. The timeline keeps only the interesting seconds: the seconds near a fault, and the seconds that deviate from the median throughput.

Dimensioned JSON fields come in two encodings. Values that a person reads carry their unit as a string: `throughput.mean_rate` (`"1000/s"`), `resources[].max_rss` (`"512MiB"`), and `faults[].at` (`"20s"`). Values that a script compares or plots are exact integers in a fixed unit. The latency percentiles and `timeline[].mean_latency` are in **nanoseconds**, `timeline[].t` is in **seconds**, and `duration`, `resources[].cpu_time`, and `efficiency.total_cpu` are in **milliseconds**. `started_unix_ms` stays a raw epoch-milliseconds stamp, because it is an instant, not an extent.

`compare` also writes `<scenario>-comparison.md`. That file shows both modes side by side with relative deltas and both fault logs.

## External systems

`run --external` points the identical workload, measurement, and reporting pipeline at any pgwire-speaking SQL system, such as CockroachDB, YugabyteDB, PostgreSQL, or a remote crabka cluster. It launches no crabka processes:

```bash
cargo run -p crabka-gres-loadtest -- run \
  --scenario crates/gres-loadtest/scenarios/mixed-oltp.yaml \
  --external "127.0.0.1:26257,127.0.0.1:26258,127.0.0.1:26259" \
  --external-user roach --external-database bench
```

- `--external` takes a comma-separated `host:port` list. The workload's connections spread round-robin across the endpoints exactly as they do across crabka node front doors.
- `--external-user` and `--external-database` are required, because external mode never guesses credentials. `--external-password` is optional. If you omit it, the run uses no password.
- Schema prep uses the same standard SQL as a crabka run: `DROP TABLE IF EXISTS` and `CREATE TABLE t<id> (id int4)` for each table, a `(id int4, v int4)` hot table, and multi-row `INSERT` seed statements. So re-runs against a persistent system start clean.
- The scenario's `topology.ranges` sets the number of `t{i * 1000000}` workload tables. An external system spreads those tables with its own sharding, so "ranges" means "N tables" there. The report records `topology.nodes` only.
- The harness ignores the scenario's `mode` and logs a notice, because the external system uses its own timestamp source. The report's `mode` field is the string `external`, and the report file names are `<scenario>-external.{json,md}`.
- The scenario's `faults` must be empty. No chaos proxy fronts an external system, so `run --external` fails fast on a fault timeline. `compare` does not support `--external`, because `compare` contrasts crabka's timestamp modes on a harness-launched cluster. Run `run --external` once per target and diff the reports.

**Resource sampling.** The harness finds which local processes serve the given ports and samples them under `ext:<port>` labels. It matches the `/proc/net/tcp{,6}` listening-socket inodes against `/proc/<pid>/fd`. Discovery covers loopback endpoints only, because the local `/proc` cannot show a remote system's processes. Pass the roster manually for a multi-process system, for example a YugabyteDB master and tserver, or when the permissions on `/proc` are restricted:

```bash
--external-pids "master=41230,tserver=41288"
```

If discovery finds nothing and you give no override, the run writes a warning and continues. It still measures throughput and latency, but the report's resource and efficiency sections are empty.

**Fairness caveats for any cross-system comparison.** You can compare numbers from `run --external` to a crabka run, or to another external run, only after you account for these points:

- *Replication factor.* The harness launches crabka unreplicated, with one broker and replication factor 1. CockroachDB and YugabyteDB default to 3× replication and pay that cost on every write. Compare against a 1× external cluster or a replicated crabka deployment. Do not compare across durability models.
- *Commit durability.* fsync and commit settings differ per system: `synchronous_commit`, the YCQL/YSQL durability knobs, and crabka's broker flush policy. Align them explicitly before you quote a delta.
- *No fault injection.* External runs measure fault-free steady state only. You cannot compare crabka scenario results that include fault windows.
- *Resource attribution scope.* `ext:<port>` rows cover only the local processes that discovery found, or that you listed manually. A remote node, a separate storage service, or an undiscovered helper process adds load but no CPU/RSS rows. So txn-per-CPU-second is comparable only when the rosters cover equivalent scopes.
- *Everything in [Limitations](#limitations)* applies across systems, and with more force. This includes debug builds against release builds, and shared-machine noise.

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

Reports land in `loadtest-out/mixed-oltp-external.{json,md}`. A containerized server can defeat pid discovery from the host's `/proc` view, and the result depends on the runtime. `--external-pids "postgres=$(docker inspect -f '{{.State.Pid}}' loadtest-pg)"` pins the roster explicitly. That pid samples the postmaster only. PostgreSQL backends are separate forked processes, so the report does not attribute per-connection CPU. Treat the resource rows as a lower bound.

## Limitations

- **Single-authority HLC today.** Non-r0 nodes fetch timestamps from range 0 through RPC. So per-node `clock_skew` is visible only on the node that hosts range 0, until multi-node HLC stamping arrives.
- **Restarted nodes report as separate rows.** The `/proc` sampler follows a live process roster. When `kill_node` restarts a node, the sampler attaches that node at the next sample tick under a `label#N` entry, for example `node2#2`. The report counts the CPU/RSS after the restart in a row beside the original row. It does not merge the two rows.
- **Localhost only.** The chaos proxies model the network in user space. OS-level `netem`/`tc` shaping is out of scope.
- **Relative numbers, not benchmarks.** Results depend on the machine, the debug or release build, and the concurrent load. Use them to compare modes and scenarios on the same host. Do not use them for absolute claims.

## Documentation

- [Bundled scenarios](scenarios/)
- [Style guides](../../docs/style_guides/README.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
