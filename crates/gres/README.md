# crabka-gres

PostgreSQL-compatible tenant compute service for Crabka Gres.

`crabka-gres` wires `crabka-pgexec` into the `crabka-pgwire` server runtime. The
runtime supplies trust/SCRAM startup authentication, PostgreSQL SSLRequest
handling, and an ephemeral in-memory default engine. It also supplies optional
durable local storage with `--data-dir`, G-2 substrate-mode runtime wiring, and
registration of Crabka's Kafka foreign-data scanner.

Local durable mode uses fjall directly:

```bash
crabka-gres --listen 127.0.0.1:5433 --data-dir /var/lib/crabka-gres
```

Substrate mode adds `--substrate-bootstrap`, `--tenant`, and optional
`--cache-dir` flags. The cache is disposable read-model state. The tenant WAL
topic is the durable truth. Startup reads the tenant record from
`__gres_cfg.<tenant>` and applies checkpoint defaults from that record when CLI
flags do not override them. It then fences older computes with the tenant's
transactional producer id and writes a recovery barrier. It replays committed
WAL records through that barrier, and then serves from the rebuilt cache.
Substrate mode intentionally stays in G-2 full-replay mode when there is no
checkpoint object store:

```bash
crabka-gres --listen 127.0.0.1:54398 \
  --substrate-bootstrap 127.0.0.1:9092 \
  --tenant smoke \
  --cache-dir /tmp/crabka-gres-smoke-cache
```

Substrate mode authenticates SQL clients by default with the SCRAM verifier from
the tenant config topic. The verifier is isolated per tenant and is never a
plaintext password. For local development only, `--auth trust` overrides the
tenant SCRAM config. `--auth scram --user-cred USER=PASSWORD` keeps the older
explicit credential path for non-substrate runs.

`memory://` stays available as an in-process substrate seam for tests and
local smoke runs that do not need a broker. In-memory bootstraps have no
config topic, so startup skips the tenant-record read and serves with CLI
defaults. Use `--auth trust` or `--auth scram --user-cred USER=PASSWORD`
to control SQL authentication:

```bash
crabka-gres --listen 127.0.0.1:54399 \
  --substrate-bootstrap memory:// \
  --tenant smoke \
  --auth trust \
  --cache-dir /tmp/crabka-gres-memory-cache
```

## Multi-range hosting

Every compute gateway must include `r0` in `--host-ranges` when `--ranges`
describes multiple ranges. A compute that serves range 2, for example, uses
`--host-ranges r0,r2`. Crabka rejects configurations that omit `r0` at
configuration and at startup, and it never adds `r0` implicitly. Remote range-0
transaction decision and barrier support is not implemented.

## Range RPC security

`--range-listen` is a TLS-only mTLS listener. It refuses to start unless the
configuration sets `--range-tls-cert`, `--range-tls-key`, `--range-tls-ca`,
`--range-tls-server-name`, and at least one `--range-allowed-principal`. The CA
verifies both peer directions. Forwarding clients verify the server name and
send it as SNI. Each allowed principal is the exact certificate subject DN that
`crabka_security::extract_principal_from_cert` returns, for example
`CN=tenant-a-range-client`. A completed mTLS handshake alone is not
authorization. The listener executes no RPC until the peer DN is in this
tenant's immutable allowlist. Remote range routing also refuses to start without
this client identity and trust configuration, and it never falls back to
plaintext TCP.

Checkpoint flags reserve the G-3 binary/config surface for bounded replay:

```bash
crabka-gres --substrate-bootstrap 127.0.0.1:9092 \
  --tenant smoke \
  --checkpoint-bucket crabka-gres-checkpoints \
  --checkpoint-region us-east-1 \
  --checkpoint-prefix dev/smoke \
  --checkpoint-frames 10000 \
  --checkpoint-size 64MiB \
  --checkpoint-retain 2
```

Use `--checkpoint-store gcs` with `--checkpoint-bucket` and the GCS credential
flags, `--checkpoint-store local --checkpoint-local-root <dir>` for local object
storage, or `--checkpoint-store in-memory` for tests. Crabka validates the
thresholds and the retention knobs with the object-store config. Checkpoint
options are only valid in substrate mode. The substrate crate exposes checkpoint
codec, restore, and prune helpers, but not the long-running checkpointer spawn
hook. So a configured checkpoint store fails fast with a clear
unsupported-runtime error. It does not serve silently without checkpoints. Omit
all `--checkpoint-*` flags to keep full-replay behavior.

## Control-plane setup

Create fleet registry metadata with the CLI when you test front-door rendering or
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
they start. The smoke scripts `scripts/gres-psql-smoke.sh` and
`scripts/gres-durable-restart-smoke.sh` cover pgwire and durable-local restart.
`scripts/gres-substrate-smoke.sh` covers substrate WAL replay. It needs an update
for G-4 so that it creates the tenant config before it starts the compute.

The runtime comes from the donor `crabgresql` top-level crate at commit
`93f3d17168d056a28b4abe60af3b489d4bf62f1d`. Donor replicated `node` mode keeps
its CLI shape, but it is blocked until the donor cluster/range runtime moves into
Crabka crates.
