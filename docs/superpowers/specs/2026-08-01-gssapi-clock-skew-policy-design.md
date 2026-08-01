# GSSAPI Clock-Skew Policy Design

## Goal

Make the broker's incoming Kerberos AP-REQ clock-skew tolerance configurable
without changing its five-minute default.

## Design

Add `max_time_skew: Time` to `crabka_security::gssapi::GssapiConfig` and pass
it explicitly to `SspiAcceptor::new`. The acceptor lowers the UOM value to
`std::time::Duration` only when constructing SSPI `ServerProperties`. Zero is
valid and means no clock-skew tolerance.

Add optional `max_time_skew: Option<Time>` to `FileGssapiConfig`. Omission
resolves to `5m`, preserving existing broker TOML behavior. Invalid or
non-finite values are rejected by the existing UOM deserialization boundary.

Add optional camel-case `maxTimeSkew: Option<Time>` to the existing
`ListenerAuthenticationGssapi` CRD object. The operator renders a supplied
value as `max_time_skew` in the existing broker-global `[gssapi]` TOML block.
All GSSAPI listeners must already share the same canonical configuration, so
the existing conflict check automatically covers this field.

## Compatibility

- Existing library callers and omitted TOML/CRD fields retain `5m`.
- No new configuration subtree or environment lookup is added.
- Keytab loading, KDC discovery, principal mapping, and GSSAPI protocol
  behavior are unchanged.
- The fixed KDC URL fallback remains implementation-only because the accept
  path performs no KDC network I/O.

## Verification

- Security tests prove the default and explicit acceptor boundary values.
- Broker file-config tests prove omitted/default and explicit UOM values.
- Operator CRD and reconciliation tests prove schema and rendered TOML flow.
- Affected package tests, workspace check, strict Clippy, nightly formatting,
  and diff hygiene close the slice.
