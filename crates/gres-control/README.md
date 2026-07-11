# crabka-gres-control

Internal control-plane library for Chapter Gres. It defines the tenant registry
records stored in the compacted `__gres_tenants` topic and provides pure folding,
validation, serialization, in-memory store seams, and a Kafka-backed registry
client for operator and CLI integration.

## G-4 control-plane topics

G-4 splits tenant metadata by audience:

- `__gres_tenants` is the fleet registry. It is a compacted, single-partition
  topic keyed by tenant name and stores whole `TenantRecord` snapshots plus
  tombstones. CLI and operator surfaces use it to list tenants and render the
  front door.
- `__gres_cfg.<tenant>` is the per-tenant compute config topic. It stores the
  same validated tenant record under a fixed `config` key so a tenant compute can
  read only its own config topic when ACLs are enabled.

Tenant records contain DNS-label tenant identity, lifecycle state, SQL user,
SCRAM verifier, WAL replication factor, optional checkpoint thresholds, and idle
timeout metadata. The SCRAM field is a PostgreSQL `SCRAM-SHA-256$...` verifier;
the control plane does not store plaintext SQL passwords in either Kafka topic.

Equal-version consistency policy: byte-identical retries are idempotent, but two
different snapshots for the same tenant and `record_version` are rejected as
divergent. This deliberately amends the original G-4 draft's "last record wins"
tie rule: accepting an ordering-dependent tie would hide a split writer. Only a
strictly greater version may replace a different snapshot.

## PgDog rendering

The crate also owns typed PgDog rendering helpers. They turn active tenant
endpoints into `pgdog.toml`, optionally route suspended tenants to an activator,
and render `users.toml` from explicit user inputs. Production passthrough entries
contain only user/database identity and omit passwords; password-bearing entries
are a local-development mode. The renderer targets pinned PgDog 0.1.47 field
names and millisecond timeout units, requires client TLS when a certificate is
configured, and validates duplicate routes and timeout budgets before producing
TOML. PgDog 0.1.47 exposes effective routes through `SHOW POOLS` (there is no
`SHOW DATABASES` command), so the operator uses that supported admin view for
confirmed-generation reloads.
