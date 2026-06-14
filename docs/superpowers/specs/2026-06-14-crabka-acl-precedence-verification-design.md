# ACL Authorization Precedence Verification — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (exhaustive enumeration + proptest of a sequential pure decision function — NOT stateright).

## Goal

Exhaustively verify `SimpleAclAuthorizer::authorize` (`crates/authz/src/simple.rs`) — the broker + gateway
ACL decision point — against an **independent oracle**. The authorizer composes several precedence /
matching rules whose interaction is security-critical (a bug grants or denies access incorrectly):

1. **Super-user bypass** — `principal ∈ super_users ⇒ Allow`, regardless of ACLs.
2. **Deny-wins** — any matching `Deny` entry ⇒ `Deny`, even when a matching `Allow` also exists.
3. **Default-deny** — no matching `Allow` (and no matching `Deny`) ⇒ `Deny`.
4. **Resource matching** (in `AclSource::matching_acls`) — `Literal` exact, `Literal "*"` wildcard,
   `Prefixed` `starts_with`; `resource_type` must equal.
5. **Principal match** — `User:*` or exact `User:{name}`.
6. **Host match** — `*` or exact peer-IP string.
7. **Operation implication** (one-way) — `{Read,Write,Delete,Alter} → Describe`,
   `AlterConfigs → DescribeConfigs`, `All → everything`; exact otherwise.

Honest discovery odds: **low** — the logic is simple and already has ~20 example unit tests. The value is
(a) **defense-in-depth on a security boundary**, (b) a **regression guard**: the oracle declares the
precedence + implication table *independently*, so dropping/flipping an arrow, breaking deny-wins, or a
matching regression is caught exhaustively rather than by spot-check. This is the quota-precedence slice
(#535) applied to authorization — same shape, higher stakes.

## Why exhaustive-enum + proptest, NOT stateright

`authorize(source, req) -> Allow|Deny` is a **sequential pure function** with no transitions or shared
mutable state. A stateright model would be degenerate subset-enumeration. Exhaustive enumeration over the
decision-relevant input space + proptest at large N is the stronger and honest fit — exactly the call made
(and confirmed by the user) on the quota lookup. **No production change** is expected; `authorize` is
already a clean single function (no dedup opportunity like the fetch/throttle slices had).

## The independent oracle

`fn oracle_decision(super_users: &HashSet<String>, entries: &[AclEntry], req: &AuthorizationRequest) ->
AuthorizationResult`, written from first principles with its OWN matching predicates and its OWN implication
table (declared as explicit arrow pairs, NOT by calling the production `implies`/`matches_*`):

```text
if req.principal.name ∈ super_users        -> Allow
matched = entries.filter(e =>
    oracle_resource_match(e, req.resource_type, req.resource_name)
    && oracle_principal_match(e, req.principal.name)
    && oracle_host_match(e, req.host.ip())
    && oracle_op_match(e.operation, req.operation))
if matched.any(Deny)                        -> Deny
else if matched.any(Allow)                  -> Allow
else                                        -> Deny
```

`oracle_op_match` declares the implication table as an explicit set of `(stored, requested)` pairs +
`stored == requested` + `stored == All`. Independence from production is the whole point — the two must
agree on every input.

## Exhaustive enumeration (quota-style)

A fixed pool of **K ≈ 10 candidate `AclEntry` values** spanning every decision dimension — `{Allow, Deny}` ×
ops `{Read, Describe, All, AlterConfigs, DescribeConfigs, Create}` × patterns `{Literal "foo", Literal "*",
Prefixed "te"}` × principals `{User:alice, User:*}` × hosts `{*, 10.0.0.1}` (chosen, not full cross-product,
to keep K small while exercising deny-vs-allow, implication, both wildcards, both patterns, principal/host
filtering). Enumerate all **2^K presence masks**. For each mask:

- Build a real `MetadataImage` (broker source) from the present subset.
- For each of ~12 **representative requests** (varying `operation`, `resource_name` ∈ {`foo`, `team-x`,
  `other`, `*`}, `principal` ∈ {alice, bob}, `host` ∈ {10.0.0.1, 10.0.0.2}) and `super_users` ∈ {∅,
  {alice}}: assert `authorizer.authorize(&image, &req) == oracle_decision(...)`.
- **Source parity:** also assert `authorize(&image, ...) == authorize(&AclCache::new(subset), ...)` — the
  broker `MetadataImage` and gateway `AclCache` decision paths must never drift (extends the existing
  matching-level parity test to the full decision).

~1024 masks × ~12 requests = ~12k assertions; fast (no checker, plain loops).

## proptest fuzz

Random `Vec<AclEntry>` (random `permission`/`operation` over all 11 ops/`pattern`/`principal`/`host`/
`resource_name` over a small alphabet, all `ResourceType`s) + random `AuthorizationRequest`. Assert
`authorize == oracle_decision` AND image-vs-cache parity after each. Covers the implication arrows not in
the exhaustive pool (`Write/Delete/Alter → Describe`) and the no-implication ops (`Create`, `ClusterAction`,
`IdempotentWrite`) + cross-resource-type matching.

## RED handling

If real ≠ oracle, determine which side matches Apache Kafka's `StandardAuthorizer`. If production is wrong →
fix `authorize`/`implies`/matching (RED→GREEN, recording the counterexample). If the oracle is wrong → fix
the oracle. Expectation: GREEN (confirmation + regression guard).

## Out of scope (YAGNI)

- `AllowAllAuthorizer` (trivially always-Allow), OPA / external authorizers.
- The `AclCache` refresh / `DescribeAcls` filter plumbing (only its `matching_acls` is exercised, via parity).
- `MetadataImage::apply` ACL indexing internals (covered by the image's own tests; the parity check pins the
  observable matching).

## Verification discipline

- Pure test addition in `crates/authz/` (no stateright, no watchdog needed — bounded loops + proptest).
- `cargo +nightly fmt -p crabka-authz`; `cargo clippy -p crabka-authz --all-targets -- -D warnings` clean
  (watch float/precision/`doc_markdown` style lints as in prior slices).

## Success criteria

1. Independent oracle + exhaustive 2^K × requests enumeration: real `authorize` matches the oracle on every
   case (or a real bug is found + fixed RED→GREEN).
2. Image-vs-cache decision parity holds across the enumeration + proptest.
3. proptest passes at large N, exercising all 11 operations + all resource types + cross-source parity.
4. fmt + clippy clean. After this slice, the program returns to COMPLETE (this was the one defensible
   defense-in-depth slice; no further candidates remain without a new subsystem).
