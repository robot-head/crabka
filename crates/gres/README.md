# crabka-gres

PostgreSQL-compatible tenant compute service for Crabka Gres.

`crabka-gres` wires `crabka-pgexec` into the `crabka-pgwire` server runtime,
including trust/SCRAM startup authentication, PostgreSQL SSLRequest handling, an
ephemeral in-memory default engine, optional durable local storage via
`--data-dir`, G-2 substrate-mode runtime wiring, and Crabka's Kafka foreign-data
scanner registration.

Local durable mode keeps using fjall directly:

```bash
crabka-gres --listen 127.0.0.1:5433 --data-dir /var/lib/crabka-gres
```

Substrate mode adds `--substrate-bootstrap`, `--tenant`, and optional
`--cache-dir` flags. The cache is disposable read-model state; the tenant WAL
topic is the durable truth. Startup reads the tenant record from
`__gres_cfg.<tenant>`, applies checkpoint defaults from that record when CLI
flags do not override them, fences older computes with the tenant's
transactional producer id, writes a recovery barrier, replays committed WAL
records through that barrier, and then serves from the rebuilt cache. Without a
checkpoint object store, substrate mode intentionally remains G-2 full-replay
mode:

```bash
crabka-gres --listen 127.0.0.1:54398 \
  --substrate-bootstrap 127.0.0.1:9092 \
  --tenant smoke \
  --cache-dir /tmp/crabka-gres-smoke-cache
```

By default, substrate mode authenticates SQL clients with the SCRAM verifier from
the tenant config topic. The verifier is isolated per tenant and is never a
plaintext password. For local development only, `--auth trust` overrides the
tenant SCRAM config; `--auth scram --user-cred USER=PASSWORD` keeps the older
explicit credential path for non-substrate runs.

`memory://` remains available as an in-process substrate seam for tests and
local smoke runs that do not need a broker.

## Multi-range hosting

When `--ranges` describes multiple ranges, every compute gateway must include
`r0` in `--host-ranges`. A compute that serves range 2, for example, uses
`--host-ranges r0,r2`. Configurations that omit `r0` are rejected at
configuration and startup; Crabka never implicitly adds it. Remote range-0
transaction decision and barrier support is not implemented.

## Range RPC security

`--range-listen` is a TLS-only mTLS listener. It refuses startup unless
`--range-tls-cert`, `--range-tls-key`, `--range-tls-ca`,
`--range-tls-server-name`, and at least one `--range-allowed-principal` are
configured. The CA verifies both peer directions; the server name is verified
by forwarding clients and sent as SNI. Each allowed principal is the exact
certificate subject DN returned by `crabka_security::extract_principal_from_cert`
(for example `CN=tenant-a-range-client`). A completed mTLS handshake alone is
not authorization: the listener executes no RPC until the peer DN is in this
tenant's immutable allowlist. Remote range routing likewise refuses to start
without this client identity and trust configuration; it never falls back to
plaintext TCP.

Checkpoint flags reserve the G-3 binary/config surface for bounded replay:

```bash
crabka-gres --substrate-bootstrap 127.0.0.1:9092 \
  --tenant smoke \
  --checkpoint-bucket crabka-gres-checkpoints \
  --checkpoint-region us-east-1 \
  --checkpoint-prefix dev/smoke \
  --checkpoint-frames 10000 \
  --checkpoint-bytes 67108864 \
  --checkpoint-retain 2
```

Use `--checkpoint-store gcs` with `--checkpoint-bucket` and the GCS credential
flags, `--checkpoint-store local --checkpoint-local-root <dir>` for local object
storage, or `--checkpoint-store in-memory` for tests. Thresholds and retention
knobs are validated with the object-store config, and checkpoint options are only
valid in substrate mode. The current substrate crate exposes checkpoint codec,
restore, and prune helpers but not the long-running checkpointer spawn hook, so a
configured checkpoint store fails fast with a clear unsupported-runtime error
instead of silently serving without checkpoints. Omit all `--checkpoint-*` flags
to keep full-replay behavior.

## Control-plane setup

Create fleet registry metadata with the CLI when testing front-door rendering or
operator-adjacent flows:

```bash
crabka gres create-tenant \
  --bootstrap 127.0.0.1:9092 \
  --name smoke \
  --user crab \
  --password-file ./smoke.password
```

The G-4 control plane stores the fleet view in compacted `__gres_tenants` and the
compute view in compacted `__gres_cfg.<tenant>`. `crabka-gres` consumes only the
per-tenant config topic at startup, so standalone substrate computes need the
operator path or an equivalent writer to populate `__gres_cfg.<tenant>` before
they start. Existing smoke scripts cover pgwire and durable-local restart
(`scripts/gres-psql-smoke.sh` and `scripts/gres-durable-restart-smoke.sh`).
`scripts/gres-substrate-smoke.sh` exists for substrate WAL replay and should be
updated for G-4 to create the tenant config before starting the compute.

The runtime is ported from the donor `crabgresql` top-level crate at commit
`93f3d17168d056a28b4abe60af3b489d4bf62f1d`. Donor replicated `node` mode keeps
its CLI shape but is blocked until the donor cluster/range runtime is ported into
Crabka crates.
