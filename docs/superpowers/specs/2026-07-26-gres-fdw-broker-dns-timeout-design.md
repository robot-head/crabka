# Gres FDW Broker DNS Timeout Design

## Goal

Bound every Kafka broker DNS lookup performed by the Gres foreign-data
wrapper with one validated process-level timeout. Standalone Gres exposes the
policy through CLI and environment configuration; operator-managed Gres
exposes the same policy through the `Gres` CRD.

The existing ten-second `ClientDnsTimeout` default remains unchanged.

## Scope

This slice covers broker hostname resolution performed by
`crabka-gres-fdw`:

- metadata connections used by foreign-table scans;
- the raw broker connection used for `ListOffsets` and fetch requests; and
- metadata connections used by `IMPORT FOREIGN SCHEMA`.

This slice does not add a foreign-server option, a new resolver abstraction,
or schema-registry HTTP DNS policy. Other FDW connection, request, and fetch
limits remain separate audit owners.

## Configuration Contract

### Standalone Gres

`crabka-gres` adds:

- CLI: `--fdw-broker-dns-timeout-ms`
- environment: `CRABKA_GRES_FDW_BROKER_DNS_TIMEOUT_MS`
- default: `ClientDnsTimeout::default()` (10,000 ms)
- precedence: CLI over environment over the typed default

The value is accepted in local and substrate modes because both modes can use
Kafka foreign tables. Zero is rejected by Clap before runtime startup.

The parsed boundary uses the existing positive-millisecond refined type and
constructs `ClientDnsTimeout`; no new validation type is introduced.

### Operator-managed Gres

`GresSpec.compute` adds the optional camel-case field:

```yaml
spec:
  compute:
    fdwBrokerDnsTimeoutMs: 10000
```

The generated schema declares an integer minimum of `1`. Effective-policy
construction validates the value as `ClientDnsTimeout` and reports invalid
input as:

```text
spec.compute.fdwBrokerDnsTimeoutMs: <validation error>
```

Each rendered tenant compute receives exactly one
`--fdw-broker-dns-timeout-ms <value>` pair. The operator supplies the typed
default explicitly when the CRD field is absent.

## Runtime Ownership and Data Flow

`KafkaFdw` owns two process defaults:

- the optional substrate bootstrap address; and
- the validated broker DNS timeout.

Gres resolves CLI/environment configuration once at startup and passes both
values when registering the scanner. The scanner carries the timeout unchanged
to all broker connection paths:

```text
CLI/env or spec.compute
  -> ClientDnsTimeout
  -> KafkaFdw
  -> scan metadata admin connection
  -> scan raw lookup_host
  -> import metadata admin connection
```

`ConnProfile` remains catalog-derived connection data and does not gain
process policy. SQL `CREATE SERVER` therefore cannot override the timeout.

## Admin and Raw Resolution

`AdminClient` adds
`connect_secured_with_dns_timeout(bootstrap, security, timeout)`. It builds the
same standard secured admin options as `connect_secured`, changing only
`dns_timeout`. Existing plaintext `connect_with_dns_timeout` delegates to the
new method with no security, avoiding duplicate option construction.

Both FDW admin call sites use the new secured method.

The raw scan connection extracts a small async `lookup_first` helper. It wraps
`tokio::net::lookup_host` in `tokio::time::timeout`, selects the first returned
address, and preserves the current ordered first-address behavior. The
post-resolution `ConnectionOptions.dns_timeout` assignment is removed because
the connection receives a `SocketAddr` and performs no DNS lookup.

## Errors

Raw lookup failures retain their current context:

- resolver error: `DNS lookup <host:port>: <error>`
- empty result: `no addresses for <host:port>`
- deadline: `DNS lookup <host:port> timed out after <milliseconds> ms`

Admin lookup failures continue through `AdminError` and the existing FDW
operation prefix (`admin connect` or `import: admin connect`). No fallback to
an unbounded lookup is allowed after timeout.

## Compatibility

- Existing deployments retain the 10-second default.
- Existing foreign-server and foreign-table catalog definitions are unchanged.
- TLS and SASL settings continue through the secured admin path.
- Bootstrap addresses are tried in their existing order.
- The raw scan still uses the first address returned for the first bootstrap
  entry.

## Tests and Verification

The implementation must provide focused evidence for:

1. a paused-clock raw resolver test proving a pending lookup expires at the
   configured millisecond deadline;
2. secured admin construction preserving security and standard admin
   connect/request defaults while changing only DNS timeout;
3. Gres default, environment, and CLI precedence;
4. zero rejection at the CLI boundary;
5. CRD default, override, schema minimum, and field-specific validation error;
6. exact-once compute deployment rendering in single- and multi-range modes;
7. two fresh nine-file CRD generations matching each other and
   `deploy/crds`; and
8. affected-package tests, strict all-target Clippy, CLI help, formatting, and
   diff checks.

## Acceptance Criteria

- No FDW Kafka broker hostname lookup remains unbounded.
- All scan and import broker DNS paths receive the same validated process
  policy.
- Standalone and Kubernetes configuration surfaces use the exact names in this
  document.
- Invalid values fail before broker or Kubernetes resource I/O.
- Existing defaults, catalog semantics, security, and address ordering remain
  intact.
