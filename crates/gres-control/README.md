# crabka-gres-control

Internal control-plane library for Chapter Gres. It defines the tenant registry
records in the compacted `__gres_tenants` topic. It also supplies pure folding,
validation, serialization, in-memory store seams, and a Kafka-backed registry
client for operator and CLI integration.

## G-4 control-plane topics

G-4 splits tenant metadata by audience:

- `__gres_tenants` is the fleet registry. It is a compacted, single-partition
  topic with the tenant name as the key. It stores whole `TenantRecord`
  snapshots and tombstones. CLI and operator surfaces use it to list tenants and
  to render the front door.
- `__gres_cfg.<tenant>` is the compute config topic of one tenant. It stores the
  same validated tenant record under a fixed `config` key. A tenant compute can
  then read only its own config topic when ACLs are enabled.

Tenant records contain DNS-label tenant identity, lifecycle state, SQL user,
SCRAM verifier, WAL replication factor, optional checkpoint thresholds, and idle
timeout metadata. The SCRAM field is a PostgreSQL `SCRAM-SHA-256$...` verifier.
The control plane does not store plaintext SQL passwords in either Kafka topic.

The equal-version consistency policy works as follows. Byte-identical retries
are idempotent. The control plane rejects two different snapshots for the same
tenant and `record_version` as divergent. This policy deliberately amends the
"last record wins" tie rule of the original G-4 draft. An ordering-dependent tie
would hide a split writer. Only a strictly greater version may replace a
different snapshot.

## PgDog rendering

The crate also owns typed PgDog rendering helpers. They turn active tenant
endpoints into `pgdog.toml`, and they can route suspended tenants to an
activator. They render `users.toml` from explicit user inputs. Production
passthrough entries contain only the user and database identity, and they omit
passwords. Entries that carry a password are a local-development mode.

The renderer targets pinned PgDog 0.1.47 field names and millisecond timeout
units. It needs client TLS when a certificate is configured. It validates
duplicate routes and timeout budgets before it writes the TOML. PgDog 0.1.47
exposes effective routes through `SHOW POOLS`, and it has no `SHOW DATABASES`
command. So the operator uses `SHOW POOLS` for confirmed-generation reloads.
