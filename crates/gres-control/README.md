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

## PgDog rendering

The crate also owns typed PgDog rendering helpers. They turn active tenant
endpoints into `pgdog.toml`, optionally route suspended tenants to an activator,
and render `users.toml` only from explicit local/dev user inputs. The renderer
validates duplicate routes and timeout budgets before producing TOML.
