# Quota Lookup Precedence Verification (KIP-13/124/612) — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (verification of a pure core) — **exhaustive enumeration + proptest, no stateright**.

## Goal

Lock down the **client-quota entity-matching precedence** so it can't silently drift, verified two
complementary ways over the already-pure `lookup_quota_with_key` / `lookup_ip_quota_with_key`
(`crates/broker/src/quota/lookup.rs`):

1. an **exhaustive enumeration** over the full presence lattice of the candidate entity set (complete
   coverage, no sampling), and
2. a **proptest** at large N over random quota images + probes (broad input space).

No production change — the functions are already pure. **No stateright model**: these are stateless
precedence lookups with no transitions/interleavings, so a stateright model would be a degenerate
subset-enumeration that the exhaustive test does more completely. (This is a deliberate,
honest deviation from the program's stateright-per-slice pattern for an ill-fitting target.) This is a
confirmation slice — the bug class is fairness, not data-loss, and the functions are already
unit-tested; the value is proving the documented precedence order is exhaustively correct.

## Background

`lookup_quota_with_key(image, principal, client_id, quota_key) -> Option<(EntityKey, f64)>` walks 8
candidate `EntityKey`s in Kafka's documented precedence (most-specific first) and returns the first
present in `image.client_quotas()` that carries `quota_key`:

```
1. (client-id=C, user=U)   2. (client-id=C, user=default)
3. (client-id=default, user=U)   4. (client-id=default, user=default)
5. (user=U)   6. (client-id=C)   7. (user=default)   8. (client-id=default)
```

`lookup_ip_quota_with_key(image, peer_ip, quota_key)` is the 2-priority IP path:
`(ip=peer)` then `(ip=default)`.

## Verification

### Exhaustive enumeration (complete)

For a fixed probe `(principal="u", client_id="c", quota_key="k")`, enumerate **all 2⁸ subsets** of the
8 candidate keys. For each subset, build a `MetadataImage` configuring exactly those candidate keys
(each with a distinct value so the matched value is identifiable), call `lookup_quota_with_key`, and
assert:
- **first-match-wins / precedence**: the returned key is the **minimum-index present** candidate; no
  earlier (higher-priority) candidate is present.
- **value-match**: the returned value equals the configured value of that candidate.
- **completeness**: `Some` iff the subset is non-empty; `None` iff empty.

Independently, enumerate all 2² IP subsets for a fixed peer and assert the same (IP-specific beats
IP-default; value-match; completeness).

This is a deterministic `for mask in 0u16..256` (and `0..4` for IP) loop — genuinely exhaustive over
the precedence lattice, with no state-space tuning or watchdog needed.

### proptest (broad input space)

Generate random images: a random set of quota entities drawn from a universe that includes the 8
candidate keys for a random probe **plus non-matching decoys** (`(client-id=other, user=other)`,
mismatched specifics, unrelated entity types), each with random values across multiple `quota_key`s;
and random probes (principal/client-id strings; IPv4 + IPv6 peers). Assert:
- precedence: the returned key, if any, is the first present candidate in the fixed order for that
  probe;
- **non-matching ignored**: a decoy entity (not a candidate for the probe) is never returned;
- value-match; user/IP paths are disjoint (a user/client lookup never returns an `ip` entity and vice
  versa).

## Out of scope (YAGNI)

- The quota *enforcement* / throttle-time computation (separate; the token-bucket slice #531 covered
  the bucket). This slice is the *lookup precedence* only.
- Dynamic `AlterClientQuotas` add/remove ordering — precedence is a pure function of the current
  config, so add/remove order is irrelevant (no temporal property).

## Verification discipline

- `cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`
  clean. No watchdog needed (no stateright; the exhaustive loop + proptest are bounded and fast).

## Success criteria

1. The exhaustive test covers all 2⁸ user/client + 2² IP presence configs, asserting precedence +
   value-match + completeness; passes.
2. The proptest passes at large N (precedence + non-matching-ignored + value-match + path disjointness).
3. The existing quota unit tests pass unchanged; fmt + clippy clean.
