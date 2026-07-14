# F-1 driver goldens and M0 exit wave

Complete the remaining exit work for Task 4 of
`docs/superpowers/plans/2026-07-09-crabka-gres-sql-parity-program.md` after the
review-clean typed GUC/session core at `48e8c817`.

## Exact capture-backed goldens

1. Capture the exact pinned clients used by the F-0 gate:
   `tokio-postgres 0.7.18`, `sqlx 0.9.0`, and `psycopg 3.2.9`.
2. Decode and allowlist PostgreSQL startup parameters separately from SQL
   simple-query batches. Never store usernames, database names, passwords,
   connection URLs, arbitrary query payloads, bind values, or raw unreviewed
   wire bytes. Redact or reject unexpected fields rather than serializing them.
3. Capture both direct client-to-PostgreSQL/Gres startup behavior and any SQL
   `SET` batches PgDog 0.1.6 applies to its backend. Empty/default SQL batches
   must be represented explicitly as empty; do not invent SQL from source-code
   assumptions.
4. Every fixture must carry capture provenance: exact package version/lock
   source, capture target, pinned PgDog image/tag/commit where applicable,
   capture date, and a schema version. Provide a deterministic recapture command.

## Executable replay and anti-rot

1. Add a payload-safe capture/validation helper and checked fixtures under the
   conformance crate (or another clearly owned F-1 location).
2. Add tests that validate fixture schema, versions against `Cargo.lock` and
   `requirements-driver-smoke.txt`, allowlisted startup keys, forbidden-secret
   absence, and exact distinction between startup parameters and SQL batches.
3. Replay every captured startup parameter and backend SQL batch directly
   against Gres. The test must fail if a captured setting becomes unsupported;
   narrative-only or parser-only validation is insufficient.
4. Wire the validator/replay into the mandatory Gres CI/static gate so fixture
   drift cannot silently pass. Keep execution bounded and deterministic.

## Final F-1/M0 evidence

1. Run the compatibility matrix anti-rot self-test and normal check, relevant
   parser/pgexec/pgwire/conformance tests, all-target check/clippy with
   `-D warnings`, nightly fmt, diff check, and F-0 structural gate.
2. Run the complete provisioned live E2E with PgDog 0.1.6, all three exact
   drivers, two-client F-1 GUC gate, base corpus floor, and extended parity 6/6.
   Do not claim a skipped ACL leg as run; the already-reviewed ACL evidence may
   be referenced accurately if unchanged.
3. Update `docs/PG_COMPAT_MATRIX.md` only for behavior now executable and
   proven. Publish a dated M0 evidence artifact/state naming exact commands,
   driver versions, PgDog pin, corpus totals, skips, and checked deviations.
   Do not advance later SQL waves or overclaim full PostgreSQL parity.
4. Reconcile `.superpowers/sdd/f1-guc-completion-report.md`, record commits and
   full RED/GREEN/capture/live evidence, and change F-1 status to complete only
   when every item above is authoritative and green.

Use strict TDD for validators/replay. Commit logical layers. Keep the worktree
clean and return exact hashes, commands/results, fixture provenance, and any
remaining concern.
