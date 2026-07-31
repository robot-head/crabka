# pg_regress parser-gap triage (measured 2026-07-30)

Produced by a 14-agent probe + adversarial-verify workflow against a live PostgreSQL
18.4 oracle. Every truth-table row below was run on the oracle by one agent and
re-run by a second, adversarial agent whose instructions were to refute it.

**619 oracle truth rows gathered; 9 were wrong.** All seven specs came back
`PARTLY_WRONG`, almost entirely from drifted line numbers and from narrative
predictions rather than from the truth tables. Read each feature's *corrections*
before implementing: several corrections change what the fix has to do.

Full truth tables (619 rows) and every code anchor live in the workflow journal:
`.claude/projects/*/subagents/workflows/wf_5b6faf2d-c1d/journal.jsonl`.

| feature | statements | difficulty | execution reachable | oracle rows | wrong |
|---|---|---|---|---|---|
| CREATE TRIGGER / DROP TRIGGER (65 regress statements, the si | 65 | medium | **NO** | 76 | 6 |
| WITHIN GROUP ordered-set aggregates: percentile_cont / perce | 33 | large | yes | 152 | 0 |
| CREATE OR REPLACE VIEW (plus the `[TEMP|TEMPORARY]`, `[RECUR | 32 | medium | yes | 60 | 0 |
| Subscripted INSERT target columns — `INSERT INTO t (a[1:5],  | 29 | medium | yes | 71 | 2 |
| Named (labeled) function arguments: `f(param := value)` and  | 24 | medium | yes | 86 | 0 |
| ALTER SEQUENCE — the full PostgreSQL 18.4 action list (AS <t | 24 | medium | yes | 105 | ? |
| FILTER (WHERE ...) on a plain (non-window) aggregate call —  | 21 | medium | yes | 69 | 1 |

## Per-feature verifier corrections

### FILTER (WHERE ...) on a plain (non-window) aggregate call — `agg(args) FILTER (WHERE predicate)` in the select list, HAVING, and ORDER BY of an aggregate query, with and without GROUP BY, with DISTINCT inside the aggregate, and over grouping sets.

- difficulty **medium**, execution reachable: **yes**
- current crabka behavior: SQLSTATE 42601, message `syntax error at position <N>: FILTER is only supported on a window function call`.

Source: `crates/pgparser/src/parser.rs:1552-1559` inside `Parser::func_call` — after `opt_filter_clause()` and `opt_over_clause()` have both run, the `let Some(over) = over else { ... }` branch checks `if filter.is_some()` and returns `ParseError::new("FILTER is only supported on a window function call", self.peek_pos())`. `ParseError::new` (crates/pgparser/src/error.rs:22-28) hard-codes `sqlstate: "42601"` and prefixes the text with `syntax error at position {position}: `. The grammar itself is complete — `opt_filter_clause` (parser.rs:1638-1646) parses `FILTER ( WHERE <expr> )` unconditionally, `filter` is in `NOT_BARE_LABEL_WORDS` (parser.rs:9928) so `SELECT count(*) filter FROM t` is already a 42601 like PostgreSQL — and the `Expr::Func(FuncCall{name,distinct,args})` returned at parser.rs:1561-1566 simply has nowhere to carry the predicate: `pub struct FuncCall` (crates/pgparser/src/ast.rs:2452-2457) has no `filter` field. Nothing in pgexec ever sees a filtered plain aggregate today.

Related gate that stays: `sum(v ORDER BY v) FILTER (...)` still dies earlier at parser.rs:1541-1549 with 0A000 `aggregate ORDER BY is not supported` (aggregate ORDER BY is unimplemented engine-wide), and `row_number() FILTER (...) OVER ()` already gives the PostgreSQL-exact 0A000 `FILTER is not implemented for non-aggregate window functions` from crates/pgexec/src/window.rs:310-313.
- verifier verdict **PARTLY_WRONG** (1 truth rows wrong, 4 anchors wrong)

<details><summary>corrections</summary>

```
SCOPE OF VERIFICATION: I re-ran all 69 truth-table rows against the oracle (PostgreSQL 18.4 Debian 18.4-1.pgdg13+1 at 127.0.0.1:54320), using CREATE TEMP TABLE aggfilterv_t/aggfilterv_e with the spec's exact fixture data. I checked 52 code anchors (19 entries in the `anchors` array + 33 further file:line:function/helper references made in the prose). I re-ran 12 additional edge cases the spec omitted. Probe scripts: /tmp/claude-1001/-home-matt-git-crabka--claude-worktrees-sql-postgresql-18-4-conformance-4ea05c/6537af26-a0b9-4c1b-a9cd-65e083bdfcbc/scratchpad/probes/{setup.sql,rows.sql,errs.txt,gaps.txt}. No repo file was modified.

=====================================================================
1. TRUTH TABLE: 68/69 CORRECT, 1 WRONG
=====================================================================

Rows 2-44 (all the success rows) and rows 45-68 (all the error rows) reproduced EXACTLY, including every SQLSTATE, every message string, every NULL, every row count, every ordering, and the two subtle ones I most expected to break (avg(int4) = 15.0000000000000000 with 16 fractional digits; FILTER-before-DISTINCT giving g=2 count(DISTINCT v)=2 / array_agg(DISTINCT v)={1,3} / sum(DISTINCT v)=4). This is an unusually accurate table.

** THE ONE WRONG ROW (last entry in truth_table) **

Spec SQL:   SELECT (select count(*) filter (where outer_c <> 0) from (values (1)) t0(inner_c)) FROM (values (2),(3)) t1(outer_c);
Spec claims: "2 (two rows, each 1)\nActual: count\n-------\n     1\n     1  (2 rows)"

REAL ORACLE OUTPUT (run twice, standalone and in batch):
 count
-------
     2
(1 row)

That is ONE row containing 2 — not two rows containing 1. The spec has the row count and the value both wrong, and they are not transposable typos: it explicitly writes out "1\n1  (2 rows)".

The spec ALSO gets the reason wrong. Its note says "crabka has NO correlated subqueries ... so this stays failing". The real semantic is aggregate-level assignment: because the FILTER references an outer-level column (outer_c), PostgreSQL assigns the Aggref to the OUTER query level, so the whole statement becomes an aggregation over t1 — hence one output row, and count(*) counts t1's 2 rows. The repo's own expected file proves it: crates/gres-conformance/corpus-regress/aggregates/aggregates.out:2345-2351 shows `count / 2 / (1 row)` with the source comment "-- outer query is aggregation query" (aggregates.sql:944).

The spec appears to have copied the output of the ADJACENT regress statement. I verified the neighbours:
- aggregates.sql:939-941 `select (select count(*) from (values (1)) t0(inner_c)) from (values (2),(3)) t1(outer_c);` -> oracle gives `1 / 1 / (2 rows)`. That is where "two rows, each 1" came from.
- aggregates.sql:945-947 `select (select count(inner_c) filter (where outer_c <> 0) ...)` -> comment says "inner query is aggregation query"; .out:2353-2360 gives `1 / 1 / (2 rows)`.
So the correct pairing is: count(*) FILTER -> 1 row of 2; count(inner_c) FILTER -> 2 rows of 1. The spec inverted it.

CONSEQUENCE: this row as written would become a wrong test expectation, and worse, the wrong REASON would send an implementer to look for correlated-subquery support when what is actually missing is aggregate-level assignment (a feature crabka has no notion of at all — nothing in agg.rs or subquery.rs assigns an aggregate to an outer query level).

=====================================================================
2. "CURRENT CRABKA BEHAVIOR" CLAIM: VERIFIED, NOT FABRICATED
=====================================================================

Every element checks out:
- The string "FILTER is only supported on a window function call" IS in the source, at exactly one place: crates/pgparser/src/parser.rs:1557, inside the `let Some(over) = over else { if filter.is_some() { ... } }` branch of `Parser::func_call` (fn starts parser.rs:1507). Gate spans parser.rs:1551-1559 as claimed.
- crates/pgparser/src/error.rs:22-28 `ParseError::new` does `message: format!("syntax error at position {position}: {}", ...)` and `sqlstate: "42601"` hard-coded (field declared error.rs:18). So the full text is `syntax error at position <N>: FILTER is only supported on a window function call`, SQLSTATE 42601. CORRECT.
- `opt_filter_clause` at parser.rs:1638-1647 parses `FILTER ( WHERE <expr> )` unconditionally. CORRECT.
- `FuncCall` (crates/pgparser/src/ast.rs:2451-2457) has exactly `name`, `distinct`, `args` — no filter field. CORRECT.
- "filter" IS in NOT_BARE_LABEL_WORDS (parser.rs:9928, list starts 9919, consumed by `is_bare_label_word` parser.rs:9971-9975). CORRECT.
  Minor mechanism nit: for `SELECT count(*) filter FROM t` the 42601 actually comes from `opt_filter_clause`'s `self.expect(&Token::LParen)` (parser.rs:1642) firing before any alias parsing, not from NOT_BARE_LABEL_WORDS. The bare-label list is what handles `SELECT g FILTER (WHERE true)`. Same SQLSTATE either way; the attribution is just imprecise.
- parser.rs:1538-1549 aggregate-ORDER-BY gate: message is "aggregate ORDER BY is not supported" without OVER, "aggregate ORDER BY is not implemented for window functions" with OVER, both via new_sqlstate("0A000", ...). CORRECT.
- crates/pgexec/src/window.rs:310-313 emits 0A000 "FILTER is not implemented for non-aggregate window functions", byte-identical to the oracle (my run E16 confirms PostgreSQL's exact wording). CORRECT.

=====================================================================
3. ANCHORS: 52/52 LOCATIONS CORRECT, 4 PRESCRIPTIONS DEFECTIVE
=====================================================================

Every file exists. Every line number lands inside or within 3 lines of the named function. Every named helper and template exists. I found ZERO fabricated anchors. Verified spot-by-spot: ast.rs:2452 `pub struct FuncCall {`; parser.rs:1507 `fn func_call`, :1552 the gate, :1638 `fn opt_filter_clause`, :9928 "filter", :12259 the test; error.rs:22 `ParseError::new`; agg.rs:435 `struct AggSpec`, :442 `distinct
```

</details>

Existing tests that assert the about-to-change behavior:
- `crates/pgparser/tests/window_clause.rs` :: `window_syntax_errors_are_reported` — Its list (lines 297-306) contains `"SELECT count(*) FILTER (WHERE a > 1) FROM t"` with the comment "FILTER without OVER has no executor support and is refused, not aliased." — this becomes a VALID parse. Remove that one entry (and its comment) and keep the sibling `"SELECT count(*) FILTER (a > 1) OVER () FROM t"` entry, which stays a syntax error per the oracle. Replace it with a positive assertion that the filter now lands on the FuncCall.
- `crates/pgparser/src/parser.rs` :: `aggregate_order_by_is_refused_as_unsupported (line ~12259)` — NOT stale — it asserts `"SELECT array_agg(v ORDER BY v) FILTER (WHERE v > 1) OVER () FROM w"` is 0A000 `aggregate ORDER BY is not implemented for window functions`, which the untouched ORDER BY gate at parser.rs:1541-1549 still produces. Listed so nobody 'fixes' it: it fires BEFORE the FILTER path.
- `crates/pgparser/tests/window_clause.rs` :: `filter_attaches_to_the_window_call (line 250)` — NOT stale in behavior, but it constructs an expected `WindowCall { ... }`; if the implementer also touches WindowCall it must be updated. It is the template for the new plain-aggregate parser test.
- `crates/pgexec/tests/window_functions.rs` :: `line 470, `SELECT row_number() FILTER (WHERE v > 1) OVER () FROM w`` — NOT stale — oracle-confirmed 0A000 `FILTER is not implemented for non-aggregate window functions`. Do not relax window.rs:310-313.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `{"file": "aggregates/aggregates.sql", "matched": 318, "total": 545}` — Not a test, but the gate to be aware of: `check_report` only fails when `matched < baseline.matched` (crates/gres-conformance/src/lib.rs:535, 581), so an improvement passes without editing the file. Ratchet 318 upward (and groupingsets/groupingsets.sql) once measured. The harness compares SQLSTATE + rows only (lib.rs:747-766) — error MESSAGE text is not compared, so the 42803/42804/42809 codes are what matter for the error rows.

### CREATE OR REPLACE VIEW (plus the `[TEMP|TEMPORARY]`, `[RECURSIVE]`, and optional column-alias-list spellings that ride on the same grammar rule)

- difficulty **medium**, execution reachable: **yes**
- current crabka behavior: SQLSTATE **42601**, message `syntax error at position 7: expected Keyword(Table), found Keyword(Or)`.

Trace (read, not run): `Parser::statement` sees `Token::Keyword(Keyword::Create)` and calls `create_statement` (crates/pgparser/src/parser.rs:3792). `create_object_keyword_offset` (parser.rs:2676) returns 1 because `peek_n(1)` is `Keyword(Or)`, which is not one of the `GLOBAL/LOCAL/TEMP/TEMPORARY/UNLOGGED` modifiers it skips. The first match arm, `Token::Keyword(Keyword::Or) if self.peeked_create_routine().is_some()` (parser.rs:3801), fails its guard: `peeked_create_routine` (parser.rs:9023) advances past `OR REPLACE` to offset 3 and finds `View` (or `temp` for the `CREATE OR REPLACE TEMP VIEW` spelling), neither of which is `FUNCTION`/`PROCEDURE`, so it returns `None`. No later arm matches `Or`, so the catch-all `_ if self.statement_has_top_level_as()` (parser.rs:3861) fires — every one of these statements has a top-level `AS` — and dispatches to `create_table_as` (parser.rs:4910). That function does `self.expect(&Token::Keyword(Keyword::Table))?` at parser.rs:4913, which fails on the `Or` token. `Parser::expect` (parser.rs:284-294) formats `format!("expected {want:?}, found {:?}", self.peek())` at `self.peek_pos()`; `ParseError::new` (crates/pgparser/src/error.rs) prefixes `syntax error at position {position}: ` and defaults the SQLSTATE to `42601`. `peek_pos` (parser.rs:263) is the byte offset of the `Or` token, i.e. 7 for `CREATE OR REPLACE VIEW …`.

This exact string is already recorded in the triage table: crates/gres-conformance/corpus-regress/TRIAGE.md:121 — `| 36 | 42601 | syntax error at position N: expected Keyword(Table), found Keyword(Or) | create or replace view agg_view1 as … |`.

Two adjacent spellings fail differently and are also unsupported today, because `create_view` (parser.rs:6019) goes `expect(Create) → expect(View) → expect_object_name() → expect(As)` with nothing in between:
- `CREATE VIEW v(a,b) AS …` → 42601 `syntax error at position N: expected Keyword(As), found LParen`
- `CREATE VIEW v WITH (security_barrier=true) AS …` → 42601 `syntax error at position N: expected Keyword(As), found Keyword(With)`
- verifier verdict **PARTLY_WRONG** (0 truth rows wrong, 6 anchors wrong)

<details><summary>corrections</summary>

```
I could not refute the truth table or the anchors. All 60 rows reproduced verbatim against PostgreSQL 18.4 (probes at scratchpad/probes/v1.sql pre-overwrite, orviewv_p3/p4/p5.sql), including every SQLSTATE, every HINT, the "?column?" literals, the 44000 DETAIL line, the reloptions transitions, and the pg_get_viewdef text. All 13 primary code anchors land exactly on the named function. The defects are all in the NOTES/reachability narrative — the part that guides implementation rather than the part that becomes test expectations.

=== DEFECT 1 (most consequential; spec is silent on this): derived_name does not unwrap Cast or Collate, so three regress cases will produce the WRONG 42P16 message even after the feature lands ===
crates/pgexec/src/exec.rs:10387 `derived_name` has exactly four arms — Expr::Column, Expr::Func, Expr::SqlJson, `_ => "?column?"`. No Cast arm, no Collate arm (grep -c 'Cast\|Collate' over 10387-10401 returns 0). It is the only label source: exec.rs:10333 `let name = alias.clone().unwrap_or_else(|| derived_name(expr));`.
Oracle, run: `create temp table orviewv_lbl(b int, c numeric(10,1)); select b::numeric, c::numeric(10,2), b+1, (b) from orviewv_lbl;`
Output header: ` b | c | ?column? | b `
Oracle, run: `create temp table orviewv_lbl2(d text collate "C", e int); select d collate "POSIX", (d), d::text, -e, e::int8 from orviewv_lbl2;`
Output header: ` d | d | d | ?column? | e `
So PostgreSQL's FigureColname unwraps casts, COLLATE, and parens; crabka labels all of them "?column?". Consequences:
- create_view.sql:103 `SELECT a, b::numeric, c, d` — spec counts this among the three "should fail" cases crabka gets RIGHT. It will not. crabka computes column 2's name as "?column?", the NAME check precedes the TYPE check within a column, so crabka emits `cannot change name of view column "b" to "?column?"` where the oracle emits `cannot change data type of view column "b" from integer to numeric`. Still a mismatch.
- create_view.sql:107 `c::numeric(10,2)` — same. Even after the typmod hop at exec.rs:502 is fixed, this case still mismatches on the label.
- create_view.sql:111 `d COLLATE "POSIX"` — the spec's stated failure mode is WRONG. It predicts crabka "will SUCCEED where PostgreSQL errors ... a silent-wrong-answer shape". It will not silently succeed: Expr::Collate is accepted and recursed into by validate_view_expr (exec.rs:900) but labelled "?column?", so crabka raises 42P16 `cannot change name of view column "d" to "?column?"`. Wrong message, but an error, not a silent wrong answer. The label bug MASKS the collation gap rather than exposing it.
Net: of the five "should fail" cases in create_view.sql:81-115, crabka would match 2 (95 drop-columns, 99 the SELECT 1,* rename), not 3. The honest delta is smaller than the spec's "roughly 10-11 statements" unless derived_name is taught to unwrap Cast/Collate/parens. That fix also corrects plain CREATE VIEW's stored column names.

=== DEFECT 2: the stated check ORDER is wrong as a global ordering ===
Spec notes: "Full order is: count -> name -> type -> collation." Refuted.
Run: `CREATE TEMP TABLE orviewv_o(a int, b int, c text COLLATE "C", d text); CREATE TEMP VIEW orviewv_ov AS SELECT a,b,c,d FROM orviewv_o; CREATE OR REPLACE VIEW pg_temp.orviewv_ov AS SELECT a::numeric AS a, b AS bb, c, d FROM orviewv_o;`
Output: `ERROR:  42P16: cannot change data type of view column "a" from integer to numeric` / LOCATION: checkViewColumns, view.c:302
Column 1 changes TYPE and column 2 changes NAME, and PostgreSQL reports the TYPE error. A global name-pass-then-type-pass implementation would wrongly report `cannot change name of view column "b" to "bb"`.
Confirming case: `... SELECT a, b, c COLLATE "POSIX" AS c, d AS dd FROM orviewv_o;` → `ERROR:  42P16: cannot change collation of view column "c" from "C" to "POSIX"` (collation on col 3 beats name on col 4).
Correct rule: count check first (view.c:272), then iterate columns in POSITION order, checking name (view.c:288) then type (view.c:302) then collation (view.c:316) within each column; the first offending column wins. Same column changing both type and collation reports type: `... SELECT a, b, c::varchar(9) COLLATE "POSIX" AS c, d ...` → `cannot change data type of view column "c" from text to character varying(9)`.
The spec's own truth-table rows 37/38 are individually correct — they just never exercise the cross-column case.

=== DEFECT 3: two 0A000 message texts in the reachability analysis are wrong, one exactly backwards ===
crates/pgparser/src/ast.rs shows TableExpr has four variants: Table (1920), Derived (1930), Join (1938), Function (1948). A JOIN is therefore ONE FROM item.
- Spec: "alter_table.sql:1752 and :1841 — my_locks joins pg_locks to pg_class, refused as multiple FROM items." WRONG. `from pg_locks l join pg_class c on l.relation = c.oid` is a single TableExpr::Join, so `select.from.len() > 1` (exec.rs:856) is false; it fails the `TableExpr::Table` pattern and gets exec.rs:862-866 `CREATE VIEW does not support joins or derived tables`.
- Spec: all six aggregates cases "refused at exec.rs:862-866 with 0A000 CREATE VIEW does not support joins or derived tables". HALF WRONG. aggregates.sql:756, 764, 793 have TWO comma-separated FROM items (`from (values …) v(a,b,c), generate_series(1,3) i`), so they hit exec.rs:856-860 `CREATE VIEW does not support joins or multiple FROM items` first. Only 772, 779, 786 (derived table alone) hit the derived-tables message.
Both are 0A000, so the SQLSTATE claims survive; the message text claims do not, and TRIAGE.md buckets on message text.

=== DEFECT 4: statement count is 32, not 33; create_view.sql has 16, not 17 ===
`grep -rniE 'create[[:space:]]+or[[:space:]]+replace[[:space:]]+(temp[a-z]*[[:space:]]+)?(recursive[[:space:]]+)?view' --include=*.sql .` → 33, but create_view.sql:69 is the comment `-- CREATE OR REPLACE VIEW`. Anchoring the regex to line start (`^[[:space:]]*create`) gives 16 create_view.sql / 6 aggregates / 5 window / 4 alter
```

</details>

Existing tests that assert the about-to-change behavior:
- `crates/pgparser/src/parser.rs` :: `parses_view_ddl_and_retains_definition (test fn at parser.rs:10260, destructuring at parser.rs:10261)` — Exhaustively destructures `let Statement::CreateView { name, definition, query } = one("CREATE VIEW \"Sales View\" AS SELECT id FROM orders WHERE id > 1")`. Adding or_replace/columns/recursive fields to the AST variant makes this a hard compile error (E0027). Extend it with the new fields and add OR REPLACE / TEMP / RECURSIVE / column-list cases.
- `crates/pgexec/src/exec.rs` :: `execute_ddl's CreateView arm (exec.rs:491) — not a test, but the second exhaustive destructuring that will fail to compile` — Same E0027 as above; listed here so the parser change is not landed without it.
- `crates/gres-conformance/corpus-regress/TRIAGE.md` :: `the triage row at TRIAGE.md:121` — Records `| 36 | 42601 | syntax error at position N: expected Keyword(Table), found Keyword(Or) | create or replace view agg_view1 as … |` as a current top failure bucket. Regenerating triage will drop or shrink this row; if the file is hand-maintained it is stale documentation the moment the parser lands.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `the per-file matched counts for create_view/create_view.sql (total 307, matched 78), aggregates/aggregates.sql (545/318), window/window.sql (391/228), with/with.sql (308/133), alter_table/alter_table.sql (1675/953)` — These are the five .sql corpus files containing the 33 `create or replace … view` statements (17 lines in create_view.sql, 6 in aggregates.sql, 5 in window.sql, 4 in alter_table.sql, 1 in with.sql). Any statement that starts matching ratchets these numbers, and a ratcheted baseline is required for the gate to stay green.
- `crates/gres-conformance/baseline.json` :: `whole-file baseline` — Same ratchet reason as the regress baseline; check whether the smoke corpus counts move.
- `crates/pgparser/src/parser.rs` :: `rejects_sharded_in_invalid_create_table_positions (assertion at parser.rs:10736)` — NOT expected to break — it asserts `message.contains("expected Keyword(Table)")` for `CREATE SHARDED TABLE t (id int4)`, which still routes through create_table_as. Flagged only because it is the sole other in-repo test keyed on that error text, so a grep for the current message will surface it; verify it still passes rather than editing it.
- `crates/pgexec/src/exec.rs` :: `create_view_refuses_duplicate_output_column_names (exec.rs:13153)` — Stays correct for the plain-CREATE path (42701 `column "x" specified more than once`), but it is the natural home for the new OR-REPLACE duplicate case, which the oracle answers with the DIFFERENT text `column "q" of relation "v" already exists`. Extend rather than replace, and do not let the new case reuse the old message.

### WITHIN GROUP ordered-set aggregates: percentile_cont / percentile_disc (scalar + array-argument forms), mode(), and rank / dense_rank / percent_rank / cume_dist as hypothetical-set aggregates.

- difficulty **large**, execution reachable: **yes**
- current crabka behavior: SQLSTATE 42601, message `syntax error at position 29: expected ; or end of input, found Ident("within")` for the headline regress statement `select p, percentile_cont(p) within group (order by x::float8) from generate_series(1,5) x, (values (0::float8),...) v(p) group by p order by p;` (position 29 is the byte offset of the `within` token; for the shorter `select percentile_cont(0.5) within group (order by x) from t` it is 28).

Where it comes from, by reading code (not by running the engine): `within` is listed in `NOT_BARE_LABEL_WORDS` (crates/pgparser/src/parser.rs:9956), so `Parser::projection_list` (parser.rs:6948) refuses it as a bare column alias via `opt_bare_col_label`, the SELECT then terminates with no FROM, and `Parser::program_spanned` (parser.rs:2649-2670) hits its trailing-token arm at parser.rs:2661-2666: `ParseError::new(format!("expected ; or end of input, found {other:?}"), self.peek_pos())`. `ParseError::new` (crates/pgparser/src/error.rs:22-29) prefixes `syntax error at position {position}: ` and sets sqlstate `"42601"`.

Secondary current behavior worth knowing: the regress line `select p, percentile_cont(p order by p) within group (order by x) -- error` does NOT reach that path. `Parser::func_call` (parser.rs:1507) calls `eat_aggregate_order_by` (parser.rs:1573), then at parser.rs:1539-1550 returns SQLSTATE `0A000` with message `aggregate ORDER BY is not supported` (the `over.is_some()` branch giving `aggregate ORDER BY is not implemented for window functions` does not apply, since `within` is neither FILTER nor OVER). PostgreSQL gives 42601 `cannot use multiple ORDER BY clauses with WITHIN GROUP` there.

Third: `percentile_disc(0.5) within group (order by thousand) filter (where hundred=1)` (regress line 1059) will still fail after WITHIN GROUP parsing is added, because `Parser::func_call` refuses aggregate FILTER without OVER at parser.rs:1552-1560 with 42601 `FILTER is only supported on a window function call`.
- verifier verdict **PARTLY_WRONG** (0 truth rows wrong, 8 anchors wrong)

<details><summary>corrections</summary>

```
I re-ran all 152 truth-table rows against PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on 127.0.0.1:54320. Probe scripts: /tmp/claude-1001/-home-matt-git-crabka--claude-worktrees-sql-postgresql-18-4-conformance-4ea05c/6537af26-a0b9-4c1b-a9cd-65e083bdfcbc/scratchpad/probes/v1.sql .. v4.sql. Temp objects were slug-prefixed (withingroupv_four, withingroupv_null, withingroupv_empty, withingroupv_n, withingroupv_gs); the spec's table names were substituted 1:1 (withingroup_four -> withingroupv_four etc.), nothing else changed.

=== TRUTH TABLE: 0 of 152 rows wrong ===
Every value, type, SQLSTATE, message, HINT, DETAIL, row count, ordering and float rendering matched exactly. I could not refute a single row. Spot-checks of the least-obvious ones, verbatim from psql:
- `select percentile_cont(0.5) within group (order by b) from (values (1.1::float4),(2.3::float4)) v(b);` -> `1.699999988079071`
- `select percentile_cont(0.25) within group (order by x) from (values ('1 month'::interval),('3 months')) v(x);` -> `1 mon 15 days`
- `select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by x) from generate_series(0,999) x;` -> `{{NULL,999,499},{749,249,NULL}}`
- `select percentile_cont(1.5) within group (order by x) from generate_series(1,0) x;` -> `ERROR:  22003: percentile value 1.5 is not between 0 and 1` / `LOCATION:  percentile_cont_final_common, orderedsetaggs.c:549` (the empty-input landmine is real)
- `select rank(3) within group (order by x, y) from (values (1,2)) v(x,y);` -> `ERROR:  42883: function rank(integer, integer, integer) does not exist` / `HINT:  To use the hypothetical-set aggregate rank, the number of hypothetical direct arguments (here 1) must match the number of ordering columns (here 2).`
- `select rank(x) within group (order by x) from generate_series(1,5) x;` -> `ERROR:  42803: column "x.x" must appear in the GROUP BY clause or be used in an aggregate function` / `DETAIL:  Direct arguments of an ordered-set aggregate must use only grouped columns.`
- `select rank(2) ... percent_rank(2) ... cume_dist(2) ... from (values (1),(2),(3),(null::int)) v(x);` -> `r=2, dr=2, pr=0.25, cd=0.6`; same without the NULL row -> `pr=0.3333333333333333, cd=0.75`
- `select rank('adam'::text collate "C") within group (order by x collate "POSIX") ...` -> `ERROR:  42P21: collation mismatch between explicit collations "C" and "POSIX"`
One nit only: row 143's note is self-contradictory prose ("Non-null N=2? no — N=3"); the stated result 2 is right.

=== CURRENT CRABKA BEHAVIOR: not fabricated, fully corroborated ===
The message template exists: `crates/pgparser/src/parser.rs:2664` -> `format!("expected ; or end of input, found {other:?}")`, inside the trailing-token arm at 2660-2666 of `Parser::program_spanned` (fn actually starts at line **2642**, not 2649 as the spec's "2649-2670" implies — a 7-line miss on a range, not a wrong claim). `ParseError::new` (crates/pgparser/src/error.rs:22) prefixes `syntax error at position {position}: ` and sets sqlstate `"42601"`. `Token::Ident(String)` (crates/pgparser/src/token.rs:5) so `{:?}` renders `Ident("within")`. `Keyword::Within` genuinely does not exist — `grep -rn 'Within\b' crates/pgparser/src/` returns nothing; `"within"` appears only at parser.rs:9956 (NOT_BARE_LABEL_WORDS, const at 9919) and parser.rs:12124 (a test). Position claims check out: crates/pgparser/src/lexer.rs:129-139 pushes `(tok, start)` with `start = i`, a 0-based byte offset, so `within` is at offset 29 in `select p, percentile_cont(p) within …` and 28 in `select percentile_cont(0.5) within …`. And crates/gres-conformance/corpus-regress/TRIAGE.md:124 records the identical signature from a real engine run. The secondary 0A000 claim also verifies by reading parser.rs:1533 -> 1537 -> 1538 -> 1539-1549 (`over` is None because `within` is neither "filter" nor "over", so the message is `aggregate ORDER BY is not supported`). The alias chain is right too: projection_list (6948) -> opt_bare_col_label (335, called at 6969) -> is_bare_label_word (9971) -> NOT_BARE_LABEL_WORDS (9919).

=== 8 WRONG ANCHORS ===
1. `crates/pgtypes/src/lib.rs:679` "enum Datum (the Interval variant)" — WRONG FILE. crates/pgtypes/src/lib.rs is a 25-line module-declaration stub. The real location is `crates/pgtypes/src/datum.rs:679` -> `Interval(crate::datetime::Interval),` in `pub enum Datum` (enum at datum.rs:652). Line 679 was right, the file was not.
2. Same anchor's sub-claims `lib.rs:151/175/240` for ColumnType::array_of / ElemType::Interval — wrong file AND wrong lines. Real: `pub enum ElemType` datum.rs:112, its `Interval,` datum.rs:123, `pub enum ColumnType` datum.rs:376, its `Interval,` datum.rs:404, `pub fn array_of(elem: ColumnType) -> Option<Self>` datum.rs:511.
3. `crates/pgparser/src/parser.rs:1573` "Parser::eat_aggregate_order_by" — the fn is at **1580**; line 1573 is the `})` closing `func_call`. The spec also says func_call "calls eat_aggregate_order_by (parser.rs:1573)"; the call site is **1533**.
4. `crates/pgparser/src/parser.rs:1666` "opt_over_clause" (in the template field of the func_call anchor) — `fn opt_over_clause` is at **1650**; line 1666 is inside the doc comment of `fn window_spec` (1667), a different function. The substance holds: opt_over_clause does key off `self.eat_ident_eq("over")` at 1651, matching opt_filter_clause's `eat_ident_eq("filter")` at 1639 (fn at 1638, correct).
5. `crates/pgexec/src/agg.rs:1366-1369` "the NULL gate (`if !spec.func.keeps_nulls() && args.iter().any(Datum::is_null)`)" — the quoted code is verbatim correct but lives at **1375-1377**. Line 1366 is `*n += 1;` inside the count(*) fast path.
6. `crates/pgexec/src/agg.rs:1370-1374` "Acc::fold_row's `Some(tuples) => tuples.push(args)` branch" — real location **1378-1382**. Line 1370 is `let args = spec.eval_args(scope, row, ctx)?;`.
7. `crates/pgexec/src/agg.rs:1352` "Acc::fold_row" — `fn fold_row` is at **1356**; 1352 is the `}` closing `Acc::new` (1347).
8. `crates/pgexec/src/agg.rs:144
```

</details>

Existing tests that assert the about-to-change behavior:
- `crates/gres-conformance/corpus-regress/TRIAGE.md` :: `the failure-taxonomy table, row at line 124` — Anti-rot documentation row asserting exactly the behavior about to change: `| 33 | 42601 | syntax error at position N: expected ; or end of input, found Ident("within") | select p, percentile_cont(p) within group (order by x::float8) ... |`. Both the count (33) and the error text become wrong. Row 117 (`Ident("partition")`) and row 40 (`aggregate ORDER BY is not supported`) are unrelated and stay.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `the `aggregates/aggregates.sql` entry (total 545, matched 318)` — The baseline is a FLOOR — crates/gres-conformance/src/lib.rs:535 and :581 only fail when `matched < baseline.matched` — so nothing breaks, but the entry must be ratcheted upward once the ~22 succeeding WITHIN GROUP statements start matching. This is the same ratchet already tracked by the pending 'ratchet baselines' task.
- `crates/pgparser/src/parser.rs` :: `aggregate_order_by_is_refused_as_unsupported (line 12259)` — Does not itself use WITHIN GROUP (it only covers array_agg/string_agg), so it should keep passing — but it PINS the exact 0A000 messages `aggregate ORDER BY is not implemented for window functions` and `aggregate ORDER BY is not supported` produced by the very block at parser.rs:1539-1550 that must gain a third WITHIN-GROUP branch. Any refactor of that block has to keep both strings and both SQLSTATEs byte-identical.
- `crates/pgparser/src/parser.rs` :: `bare_column_labels_follow_the_barelabel_list (line 12086)` — Asserts `parse("SELECT 1 within")` is an error. WITHIN GROUP only follows a function call's closing paren, so this must keep passing — verify rather than change it. It is the guard that would catch an implementation that made `within` a general keyword.
- `crates/gres-conformance/src/lib.rs` :: `the two error-text fixtures at lines 1919 and 1924 (`expected ; or end of input, found LParen`)` — NOT stale — same message template, different token, unrelated statements. Listed only so the grep for `expected ; or end of input` is not mistaken for a WITHIN GROUP guard.

### Named (labeled) function arguments: `f(param := value)` and `f(param => value)`, mixed with leading positional args, with defaults filled for omitted parameters

- difficulty **medium**, execution reachable: **yes**
- current crabka behavior: SQLSTATE 42601, message `syntax error at position 27: expected RParen, found Colon` for `select make_interval(years := 178956971)`. Path (read, not run): `crates/pgparser/src/lexer.rs:634` lexes a lone `:` as `Token::Colon` (there is NO `:=` token; `=>` likewise lexes as `Token::Eq` + `Token::Gt`, there is no `=>` token). `crates/pgparser/src/parser.rs:1507 fn func_call` parses each argument with `self.expr(0)?` at line 1526, `eat_comma()` at 1527 fails on `Colon`, `eat_aggregate_order_by()` at 1533 returns false, and `self.expect(&Token::RParen)?` at line 1534 fails. `expect` (parser.rs:284-294) builds `ParseError::new(format!("expected {want:?}, found {:?}", self.peek()), self.peek_pos())`, and `ParseError::new` (crates/pgparser/src/error.rs:23-30) wraps it as `syntax error at position {position}: {message}` with sqlstate `"42601"`. `peek_pos()` (parser.rs:263) is the token's 0-based byte offset; the `:` in `select make_interval(years := 178956971)` is at byte 27. Wire mapping: `crates/pgexec/src/error.rs:293` — `ExecError::Parse(e) => PgError::error(e.sqlstate(), e.to_string())`. The `=>` spelling gives a DIFFERENT error today: `years` parses as a column, `Token::Eq` is taken as binary `=`, and its right operand hits the `prefix` catch-all at parser.rs:1302 — `syntax error at position 28: unexpected token Gt`.
- verifier verdict **PARTLY_WRONG** (0 truth rows wrong, 5 anchors wrong)

<details><summary>corrections</summary>

```
I re-ran all 86 truth-table rows against the PG 18.4 oracle (`PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1)`) in a single session, using my slug for temp objects (`namedargsv_t`, `pg_temp.namedargsv_f/p/v/srf` — the only substitution vs. the spec's `namedargs_*`, which changes only the function name echoed inside 42883 messages). Every SQLSTATE, every message, every row set, every ordering, and every column label matched. **0 of 86 rows wrong.** I also confirmed 11 of the spec's supplementary note-claims (`select make_interval(0,0,0,0,0,0,1e308)` = 22003 float_overflow_error; `select 'inf'::float::int` = 22003 dtoi4; positional `secs 'inf'`/`'NaN'` = 22008; `make_interval` pronargs=7/pronargdefaults=7 and proargnames `{years,months,weeks,days,hours,mins,secs}`; `make_timestamp` = `{year,month,mday,hour,min,sec}` (mday, not day); abs/count/row_number/lag proargnames all NULL; all 9 `generate_series` overloads NULL; exactly 10 pg_catalog aggregates with proargnames, exactly the 10 listed; 218 pg_catalog proargnames rows; `=>` emits no notice even at client_min_messages=debug1). The truth table is trustworthy as test expectations — I could not break a single row.

I also verified the 22003/22008 discriminator the spec proposes for the datetime.rs fix, which it asserted but never probed: `select make_interval(secs := 1e303)` -> `ERROR: 22003: value out of range: overflow / LOCATION: float_overflow_error, float.c:88` (product overflows f64) versus `select make_interval(secs := 1e300)` -> `ERROR: 22008: interval out of range / LOCATION: make_interval, timestamp.c:1576` (product finite ~1e306 but outside int64). `-1e303` is likewise 22003. So `(secs * 1e6).is_infinite() && secs.is_finite()` => 22003 is the empirically correct split.

CURRENT-CRABKA-BEHAVIOR CLAIM: NOT fabricated, correctly derived. The literal string `expected RParen, found Colon` appears nowhere in the repo (`grep -rn "expected RParen, found Colon"` and `grep -rn "found Colon"` outside target/ are both empty) because it is built at runtime by `format!("expected {want:?}, found {:?}")` at crates/pgparser/src/parser.rs:290. I verified the whole chain independently: lexer.rs:634 `b':' => (Token::Colon, 1)` with no `:=` arm and no `=>` arm (b'=' -> Eq at 645, b'>' -> Gt at 647); `Token::Colon` has NO infix binding power (its only uses are parser.rs:1318/1323 subscripts and 1906/1965 json), so `self.expr(0)` at 1526 stops before it; `eat_comma()` at 1527 fails; `eat_aggregate_order_by` (parser.rs:1580) returns `Ok(false)` on a non-ORDER token; `self.expect(&Token::RParen)` at 1534 then produces the message at the `:` byte offset. Byte offset 27 for `select make_interval(years := 178956971)` is correct. `years` is not a keyword (no Years/Months/Weeks variant in token.rs), so it does lex as `Token::Ident`. The `=>` variant claim also checks out: `prefix` matches on `self.peek().clone()` (parser.rs:1073), so the catch-all at 1302 reports `self.peek_pos()` = the position of the offending token, i.e. `>` at byte 28 -> `syntax error at position 28: unexpected token Gt`.

WRONG ANCHOR / CLAIM 1 (MATERIAL — scope undercount, 5 statements missed). The spec's "SCOPE OF THE 24 STATEMENTS" is wrong: there are 29 named-arg statements in the corpus, not 24. It only counted the `:=` spelling and missed five `=>`-spelled named-arg calls that PostgreSQL SUCCEEDS on, all in files the harness already runs:
  - crates/gres-conformance/corpus-regress/name/name.sql:70 `SELECT parse_ident('foo.boo[]', strict => false); -- ok` -> oracle `parse_ident = {foo,boo}` (1 row). (Positional-only `SELECT parse_ident('foo.boo[]')` is `ERROR: 22023: string is not a valid identifier: "foo.boo[]"` / LOCATION parse_ident, misc.c:983 — so the named arg is load-bearing, not decorative.) proargnames `{str,strict}`, pronargdefaults 1.
  - crates/gres-conformance/corpus-regress/jsonb/jsonb.sql:1272 `select jsonb_set_lax('{"a":1,"b":2}', '{b}', null, null_value_treatment => 'raise_exception') as raise_exception;` -> `ERROR: 22004: JSON value must not be null` / DETAIL `Exception was raised because null_value_treatment is "raise_exception".` / HINT `To avoid, either change the null_value_treatment argument or ensure that an SQL NULL is not passed.` / LOCATION jsonb_set_lax, jsonfuncs.c:4944
  - jsonb.sql:1273 `... null_value_treatment => 'return_target') as return_target;` -> `return_target = {"a": 1, "b": 2}` (1 row)
  - jsonb.sql:1274 `... => 'delete_key') as delete_key;` -> `delete_key = {"a": 1}` (1 row)
  - jsonb.sql:1275 `... => 'use_json_null') as use_json_null;` -> `use_json_null = {"a": 1, "b": null}` (1 row)
  jsonb_set_lax proargnames `{jsonb_in,path,replacement,create_if_missing,null_value_treatment}`, pronargdefaults 2. Both functions are in the spec's own list of built-ins-with-proargnames, so the omission is a counting slip, not a knowledge gap — but it means the parameter-name table must also cover parse_ident and jsonb_set_lax, and it changes the win count from 23 to 28 winnable of 29.
  Consequence for the stale-test list: it must also include the baseline.json entries `{"file": "name/name.sql", "total": 40, "matched": 28}` and `{"file": "jsonb/jsonb.sql", "total": 1084, "matched": 780}`, which the spec never mentions.

WRONG ANCHOR / CLAIM 2 (MATERIAL — stale-test entry for TRIAGE.md is doubly wrong). The spec's stale_tests entry names "crates/gres-conformance/corpus-regress/TRIAGE.md row 29 (`syntax error at position N: expected RParen, found LBracket`)" and justifies it with "the `expected RParen, found Colon` signature will disappear from it." Both halves are false. (a) The Colon signature is NOT in TRIAGE.md — `grep -n "Colon" crates/gres-conformance/corpus-regress/TRIAGE.md` returns nothing. TRIAGE.md is 132 lines and its lowest-count root-cause row is 27 (`| 27 | 42601 | syntax error at position N: unexpected token after ALTER: Ident("type") |`); the named-arg signature has only ~24 occurrences, below the cutoff, which is why it never appears. (b) Row 29 (
```

</details>

Existing tests that assert the about-to-change behavior:
- `crates/gres-conformance/corpus/make_justify.sql` :: `file header comment, lines 9 and 12-15` — The header explicitly documents the gap that is about to close: line 9 labels make_interval `(POSITIONAL)` and lines 12-15 read `-- Exclusions (intentional, spec §1.3 deferred): NAMED arguments / -- (make_interval(days => 5)) — the parser has no name => value syntax; SP38 / -- supports the positional call only.` This is the anti-rot guard for the current behavior and must be rewritten, and the file should gain named-arg cases.
- `crates/pgtypes/src/datetime.rs` :: `FieldSource for IntervalFields :: display_year doc comment, line 2708` — The rustdoc says `(make_interval(months => -12)) renders YYYY as -0001` — it already cites a spelling the engine cannot parse. Once `=>` works this becomes a claim that can actually be tested; today it is aspirational.
- `crates/pgparser/src/lexer.rs` :: `cast_operator_wins_maximal_munch_over_the_lone_colon` — Not strictly stale — it asserts `toks("a : b")` yields `Token::Colon` with spaces, which still holds after `:=` is added. But it is the test that guards the maximal-munch arm ORDER being changed at lexer.rs:630-634, so it must be extended with `toks("a := b")`, `toks("a:=b")` and `toks("a => b")` or the new arms ship untested.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `files[] entry for interval/interval.sql (total 446, matched 218)` — The ratchet must be re-measured; ~20-22 of the 23 named-arg statements in interval/interval.sql should flip from mismatch to match.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `files[] entry for window/window.sql (total 391, matched 228)` — window.sql:1560 `SELECT nth_value_def(n := 2, val := ten) OVER (...)` will stop being a 42601 but will NOT become a match (see notes), so the count may not move — re-measure rather than assume.
- `crates/gres-conformance/corpus-regress/TRIAGE.md` :: `row 29 (`syntax error at position N: expected RParen, found LBracket`) and any named-arg root-cause rows` — TRIAGE.md is generated from mismatch root-cause grouping; the `expected RParen, found Colon` signature will disappear from it. Confirmed there is NO Rust test anywhere in the repo asserting the current text — a repo-wide grep for `expected RParen` / `found Colon` outside target/ hits only this file.

### ALTER SEQUENCE — the full PostgreSQL 18.4 action list (AS <type>, INCREMENT BY, MINVALUE/NO MINVALUE, MAXVALUE/NO MAXVALUE, START WITH, RESTART [[WITH] n], CACHE, CYCLE/NO CYCLE, OWNED BY, RENAME TO, OWNER TO, SET SCHEMA, SET {LOGGED|UNLOGGED}, IF EXISTS). crabka does not parse ALTER SEQUENCE at all today, and its catalog Sequence record has no data-type field, so `AS <type>` is unrepresentable even for CREATE SEQUENCE (where the type is currently parsed and thrown away).

- difficulty **medium**, execution reachable: **yes**
- current crabka behavior: 42601, message `syntax error at position 0: unexpected token after ALTER: Ident("sequence")`.

Source: `crates/pgparser/src/parser.rs:2877` opens the `Token::Ident(s) if s == "alter" => match self.peek2()` dispatch inside `Parser::statement`. Its arms cover function/procedure/routine, TABLE, SCHEMA, SERVER, USER, database, extension, system, statistics, type, domain — there is no `sequence` arm. Control falls to the catch-all at `crates/pgparser/src/parser.rs:2900-2904`:

    _ => Err(ParseError::new(
        format!("unexpected token after ALTER: {:?}", self.peek2()),
        self.peek_pos(),
    )),

`ParseError::new` (crates/pgparser/src/error.rs:22-28) prefixes `syntax error at position {position}: ` and hardcodes `sqlstate: "42601"`. `peek_pos()` (parser.rs:263) returns the offset of the *current* token, which at that dispatch point is still `alter` — i.e. offset 0 for a statement that starts the string. So every one of the 24 regress `ALTER SEQUENCE` statements dies in the parser before any executor code runs. There is no `CommandIdentity::AlterSequence` (crates/pgparser/src/command.rs `command_identities!` jumps from `AlterSchema` at :31 to `AlterServer` at :32), no `Statement::AlterSequence` variant, and no `COMMAND_PROBES` entry for "ALTER SEQUENCE".

Note the shape crabka uses for the sequence statements it *does* support: CREATE SEQUENCE is desugared into `Statement::CreateIndex { table: "__crabka_sequence__", keys: <options as "k=v" strings> }` (parser.rs:6039-6060 + parser.rs:10168 `encode_sequence_options`), decoded back in `execute_ddl` at crates/pgexec/src/exec.rs:559-570 via `sequence_from_encoded_options` (exec.rs:1243). DROP SEQUENCE is desugared into `Statement::DropTable` with `__crabka_sequence__:<name>` names (parser.rs:6124-6140, decoded at exec.rs:356).
- verifier verdict **?** (? truth rows wrong, ? anchors wrong)

<details><summary>corrections</summary>

```
(none)
```

</details>

Existing tests that assert the about-to-change behavior:
- `docs/PG_COMPAT_MATRIX.md` :: `tools/check-pg-compat-matrix.sh (CI gate, .github/workflows/ci.yml:1036-1037)` — Line 58 says `| ALTER SEQUENCE | Wave-assigned(D3) | Sequence lifecycle and sharded allocation. |`. The checker's `validate()` (tools/check-pg-compat-matrix.py:452-462) errors the moment the parser accepts a command whose matrix row is not Implemented/Mapped/Error-with-notice, and `validate_behavior_probes()` (line 375-383) separately errors on a parser-accepted Wave-assigned command. This is the single hardest anti-rot guard on this feature.
- `crates/gres-conformance/src/parser_commands.rs` :: `statement_shape (line 810) — exhaustive `match statement`` — Adding `Statement::AlterSequence` breaks the exhaustive match and will not compile until an arm is added. Deliberate design: the doc comment at line 705-711 says the exhaustive match exists to force this module to account for new statement variants.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `the `sequence/sequence.sql` entry: {"total": 261, "matched": 123}` — The 24 ALTER SEQUENCE statements plus their downstream SELECTs move from mismatched to matched, so `matched` must be ratcheted up. Same for crates/gres-conformance/baseline.json if it carries a sequence-related count.
- `crates/pgexec/tests/catalog_introspection.rs` :: `sequences_appear_as_relations_and_in_their_own_catalogs (line 251)` — Not strictly stale — the probe sequence `counter` is bigint, so the asserted `data_type == "bigint"` stays right. But it is the only test pinning information_schema.sequences.data_type, so it is the natural place to extend with smallint/integer rows once the type field lands; if `Sequence` gains a field with a different default the assertion will move.
- `crates/pgexec/src/exec.rs` :: `sequence_bounds_and_cycle_are_enforced (inline #[tokio::test], around line 15239)` — Asserts only `err.code == "2200H"` for nextval exhaustion, which stays correct. It will need a message assertion once `sequence_wrapped_value` (seq.rs:180) starts emitting PostgreSQL's `reached maximum value of sequence "bounded" (3)` text, and the sibling setval assertions must move from 2200H to 22003.
- `crates/pgcatalog/src/lib.rs` :: `list_sequences_reports_every_sequence_by_name_in_order (inline test, line 2940)` — Calls `Sequence::new(7, 2, None, None, None, true)` with six positional args. Adding a data-type parameter to `Sequence::new` breaks this call site (and every other `Sequence::new` caller, notably crates/pgexec/src/exec.rs:1266).

### Subscripted INSERT target columns — `INSERT INTO t (a[1:5], b[1:1][1:2][1:2], c) VALUES (...)`, i.e. PostgreSQL's `insert_column_item := ColId opt_indirection`, where an entry in the INSERT column list carries an array subscript / slice chain (or a jsonb subscript chain) and assigns *into* a fresh NULL container instead of replacing the column.

- difficulty **medium**, execution reachable: **yes**
- current crabka behavior: SQLSTATE 42601, message `syntax error at position 22: expected RParen, found LBracket` for the headline `INSERT INTO arrtest (a[1:5], b[1:1][1:2][1:2], c, d, f, g) VALUES (...)`. (For `insert into inserttest (f2[1], f2[2]) values (1,2)` the same error reads `syntax error at position 26: expected RParen, found LBracket`.) Provenance, all by reading code: `Parser::insert` at crates/pgparser/src/parser.rs:6730 parses the column list as a bare `expect_col_id()` loop (6743/6746) with no `opt_indirection`, so after consuming `a` the `[` is not a comma, the loop breaks, and `self.expect(&Token::RParen)?` at parser.rs:6748 fails. `Parser::expect` (parser.rs:284-294) formats `format!("expected {want:?}, found {:?}", self.peek())` — `Token` derives Debug (crates/pgparser/src/token.rs:3), so the variants print as `RParen`/`LBracket`. `ParseError::new` (crates/pgparser/src/error.rs:23-28) prefixes `syntax error at position {position}: ` and sets sqlstate `"42601"`; `position` is `self.peek_pos()` = `self.toks[self.pos].1`, the 0-based byte offset of the `[` (parser.rs:263-265). The MERGE spelling `WHEN NOT MATCHED THEN INSERT (id, a[2]) VALUES (...)` fails the same way through `parse_parenthesized_ident_list` (parser.rs:7101-7115). The deferral is documented at docs/PG_COMPAT_MATRIX.md:226: "Deferred: subscripted INSERT target columns (`INSERT INTO t (a[1:5]) VALUES (...)`) are 42601".
- verifier verdict **PARTLY_WRONG** (2 truth rows wrong, 7 anchors wrong)

<details><summary>corrections</summary>

```
I re-ran all 71 truth-table rows against PostgreSQL 18.4 (verified `select version()` = "PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1)"). The SQL semantics are, to my surprise, almost entirely correct — 69/71 rows reproduce byte-for-byte, including every SQLSTATE, every message, every DETAIL, every `array_dims` value, and the two "crabka would silently get this wrong" claims. I could not refute the semantic core. What I did break is the code map: 7 of 27 anchors name functions that do not exist, and the worst of them hides four real ripple sites and one false claim about existing behavior. Two further silent-wrong-answer risks are missed entirely.

=== (1) TWO TRUTH-TABLE ROWS ARE WRONG AS WRITTEN ===

Both are the same failure mode — the stated `oracle_result` does not follow from the stated `sql`, so anyone turning the row into a test gets a red test.

ROW 3 (`arrays.sql:44` / `:49`). Claimed output has row 1's `e` = `[0:1]={1.1,2.2}`. Real output for exactly the SQL given (fresh session, the three INSERTs and nothing else):
```
      a      |        b        |     c     |       d       |     e     |        f        |      g
-------------+-----------------+-----------+---------------+-----------+-----------------+-------------
 {1,2,3,4,5} | {{{0,0},{1,2}}} | {}        | {}            |           | {}              | {}
 {11,12,23}  | {{3,4},{4,5}}   | {foobar}  | {{elt1,elt2}} | {3.4,6.7} | {"abc  ",abcde} | {abc,abcde}
 {}          | {3,4}           | {foo,bar} | {bar,foo}     |           |                 |
(3 rows)
```
`e` is NULL for row 1, not `[0:1]={1.1,2.2}`. The row's own note admits it depends on "the intervening `UPDATE arrtest SET e[0]='1.1'`", but those UPDATEs are not in the `sql` field. Everything else in the row is right — I confirmed the per-row dims claim exactly: `da/db/dd` = `[1:5] / [1:1][1:2][1:2] / (null)`, `[1:3] / [1:2][1:2] / [1:1][1:2]`, `(null) / [1:2] / [1:2]`, and the "declared dimensionality is not enforced" point (`b int4[][][]` really does end up `[1:2][1:2]`).

ROW 57 (zero source rows). Claimed `SELECT i, a, b FROM subscriptinsert_t;` returns `(0 rows)`. Run in the spec's own accumulated sequence — row 54 (`INSERT INTO subscriptinsert_t (b[1]) VALUES (5.7)`) already succeeded and left `{6}` in the table — the real output is:
```
INSERT 0 0
 i | a |  b
---+---+-----
   |   | {6}
(1 row)
```
The load-bearing claim (zero source rows ⇒ `INSERT 0 0`, no error from the subscript) is correct; the printed table is not.

=== (2) SEVEN WRONG ANCHORS — ALL FABRICATED FUNCTION NAMES ===

I checked every anchor for file existence, line-lands-in-function, and helper/template existence.

WRONG-1. anchor `crates/pgparser/src/parser.rs:6741`, template field: "`Parser::update_assignments` single-target branch — parser.rs:4707-4736 … Copy it verbatim." **There is no `update_assignments`.** `grep -n "fn update_assignments" crates/pgparser/src/parser.rs` → empty. The function containing line 4707 is `fn assignment_list(` at parser.rs:4674. (The anchor itself is fine: line 6741 is inside `fn insert` at parser.rs:6732, and the Dot-rejection template really is there at 4716-4721, one line later than the spec's 4715.)

WRONG-2. anchor `crates/pgparser/src/parser.rs:4863`, "`Parser::merge_when_clause` (the INSERT action arm)". **There is no `merge_when_clause`.** The function is `fn merge_when(&mut self, relation: &str)` at parser.rs:4828. Line 4863 does land in it and does call `parse_parenthesized_ident_list()` as claimed.

WRONG-3 (the worst). anchor `crates/pgexec/src/exec.rs:1810`, "`resolve_params` (the `Statement::Insert` arm of the parameter-rewrite walk)". **There is no `resolve_params` anywhere in pgexec.** Line 1810 is inside `fn resolve_write_subqueries(` at exec.rs:1780, whose doc says "The write path's evaluator executes no subqueries of its own" — it rewrites SUBQUERIES, not parameters. Three consequences the spec gets wrong:
  (a) Its template claim is false: "exec.rs:1821-1826 — the `Statement::Update { assignments }` arm calls `resolve_assignments(assignments)`, which walks each Assignment's subscripts." The `resolve_assignments` closure at exec.rs:1792-1806 matches only on `&mut assignment.value` (`AssignmentValue::Expr` / `Row` / `Subquery`). It never touches `assignment.subscripts`. So subscripts are NOT walked on the UPDATE path either — there is no template to copy here.
  (b) The real parameter machinery, which the spec never mentions, is in session.rs: `ParamBinder::bind_statement_params` (session.rs:5936, `Statement::Insert` arm at 5946) and `max_statement_param` (session.rs:6679, Insert arm at 6682). Both walk only `InsertSource::Values` rows / the query. Both must gain the target-list subscripts for the spec's own oracle-verified `PREPARE subscriptinsert_p1 (int, int) AS INSERT INTO subscriptinsert_pp (a[$1]) VALUES ($2)` case.
  (c) Worse, `ParamBinder::insert_target_types` (session.rs:6131) is entirely absent from the spec's ripple list, and it is not a type ripple — it is a correctness site. It returns `table.columns[idx].ty` per target and feeds it to `bind_expr` as the expected type of `$n`. For a subscripted target the expected type must become the ELEMENT type for an index chain and the ARRAY type for a slice chain — exactly the distinction the spec's own rows 4/5/55 prove (`(b[2]) VALUES(now())` wants `integer`, `(b[1:2]) VALUES(now())` wants `integer[]`, `(a[1]) VALUES ('{1,2}')` is `22P02: invalid input syntax for type integer: "{1,2}"`). Left alone, `INSERT INTO t (a[1]) VALUES ($1)` binds `$1` as `int[]`.

WRONG-4. anchor `crates/pgexec/src/exec.rs:2083`, "`execute_write` (the main `Statement::Insert` arm)". Line 2083 is inside `async fn execute_write_body(` at exec.rs:2037. `execute_write` is at exec.rs:1517 and does not contain 2083.

WRONG-5. anchor `crates/pgexec/src/exec.rs:4744`, "`plan_timestamp_write` (the `Statement::Insert { columns, source }` arm)". **There is no `plan_timestamp_write`.** Line 4744 is inside `pub(crate) fn execute_
```

</details>

Existing tests that assert the about-to-change behavior:
- `crates/pgparser/src/parser.rs` :: `insert_sources_cover_values_query_and_default_values` — Line 15711 asserts `columns == Some(vec!["a".into(), "b".into()])` for `INSERT INTO t (a, b) VALUES (1, 2)`. Changing `Statement::Insert.columns` to `Option<Vec<InsertTarget>>` breaks this at compile time; it should become `Some(vec![InsertTarget{name:"a", subscripts: vec![]}, ...])` and gain a subscripted case mirroring update_parses_subscripted_set_targets.
- `crates/pgparser/src/parser.rs` :: `merge_parses_every_when_clause_shape` — Lines 15863-15884 match `MergeAction::Insert { values: Some(_), .. }` and compare against `MergeAction::Insert { columns: None, values: None }`. Both still compile after the element-type change, but the test is the natural home for the new MERGE `INSERT (id, a[2]) VALUES (...)` parse coverage, and the `columns: None` literal should be re-checked once the field type moves.
- `crates/pgparser/src/parser.rs` :: `update_parses_subscripted_set_targets` — Not stale, but it is the exact assertion shape to clone for the new INSERT-target parse test (it asserts the Assignment's `subscripts: vec![ArraySubscript::Index(...), ...]`).
- `docs/PG_COMPAT_MATRIX.md` :: `tools/check-pg-compat-matrix.py (the matrix gate)` — The ARRAY row at line 226 states the 42601 deferral as fact. The gate parses this file, so the row must be edited in the same change or the documented behaviour contradicts the engine.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `arrays/arrays.sql — matched 390 / total 515` — Five statements in arrays/arrays.sql (lines 34, 44, 49, 52, 54) start matching, plus every downstream statement that depends on arrtest's contents (SELECT * FROM arrtest and the whole UPDATE/DELETE sequence at lines 56-116 currently diverges because the first three INSERTs never landed). The ratchet must be raised or the gate fails on improvement. arrays.sql:189 (point_tbl(f1[0])) stays unmatched — crabka has no point type.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `insert/insert.sql — matched 156 / total 387` — insert/insert.sql:75-78 become matching (78 as 0A000 `cannot set an array element to DEFAULT`), plus the `select * from inserttest` that reads them. Lines 86-91, 101-104 and the whole inserttesta/inserttestb block at 113-158 stay unmatched: they need CREATE TYPE composites and CREATE DOMAIN, neither of which the parser accepts (there is no Keyword::Domain in crates/pgparser/src/parser.rs).
- `crates/gres-conformance/corpus-regress/TRIAGE.md` :: `root-cause row 128 and per-file rows 41 / 76` — Row 128 (`| 29 | 42601 | syntax error at position N: expected RParen, found LBracket | INSERT INTO arrtest (a[1:5], ...`) is the deliberate record of this deferral and becomes stale. The per-file rows `| arrays | 387 | 515 |` (line 41) and `| insert | 60 | 387 |` (line 76) are a dated snapshot and both move. Note these disagree with baseline.json (390 and 156) — baseline.json is the gate.
- `crates/pgexec/src/array_fn.rs` :: `doc comment on array_assign (lines 1122-1127) and on array_ref (lines 964-971)` — array_assign's doc describes only the UPDATE entry point; it should state the INSERT NULL-base rule. Separately, array_ref's doc at line 969 says "a plain index in a slice chain means `[i:i]`" while the code below it (array_fn.rs:1024-1028) and the oracle both say `1:i` — the comment is simply wrong and worth fixing while in the file.

### CREATE TRIGGER / DROP TRIGGER (65 regress statements, the single largest parser gap) — feasibility study, not an implementation plan.

- difficulty **medium**, execution reachable: **NO**
- current crabka behavior: SQLSTATE 42601. Exact text for `create trigger ttdummy …` (corpus-regress/alter_table/alter_table.sql:1817): `syntax error at position 7: expected Keyword(Table), found Ident("trigger")`. Exact text for `DROP TRIGGER t ON trunc_trigger_test;` (truncate/truncate.sql:162): `syntax error at position 5: expected Keyword(Table), found Ident("trigger")` — i.e. CREATE TRIGGER and DROP TRIGGER produce the SAME error signature, which is why the TRIAGE bucket is 65 = 37 CREATE + 28 DROP, not 65 CREATE.

Origin, exactly: `trigger` is not a keyword (crates/pgparser/src/token.rs has no entry for it; the lexer lowercases every word at crates/pgparser/src/lexer.rs:134), so it lexes to `Token::Ident("trigger")`. `Parser::create_statement` (crates/pgparser/src/parser.rs:3792) has no `Token::Ident(s) if s == "trigger"` arm, so the `_ =>` fallthrough at parser.rs:3862 calls `create_table()`, whose `self.expect(&Token::Keyword(Keyword::Table))?` at parser.rs:5015 fails. Symmetrically the DROP dispatch (parser.rs:2740-2789) falls through at parser.rs:2788 to `drop_table()`, whose `expect(Keyword::Table)` at parser.rs:6689 fails. The message is built by `Parser::expect` (parser.rs:284-294, `format!("expected {want:?}, found {:?}", self.peek())`) and framed by `ParseError::new` (crates/pgparser/src/error.rs:22-28), which prepends `syntax error at position {position}: ` and sets sqlstate `42601`. `position` is the 0-based byte offset of the offending token (crates/pgparser/src/parser.rs:263-265 `peek_pos`).

`CREATE OR REPLACE TRIGGER` would instead give `expected Keyword(Table), found Keyword(Or)` (same fallthrough, different token) — that spelling does not appear in the corpus. `ALTER TRIGGER` does not appear in the corpus either.

There is NO `CommandIdentity::CreateTrigger`/`DropTrigger`/`AlterTrigger` (crates/pgparser/src/command.rs), no `Statement::CreateTrigger` (crates/pgparser/src/ast.rs), and no trigger token/keyword anywhere in crates/pgparser — the word `trigger` appears exactly once in the whole crate, in a doc comment at crates/pgparser/src/ast.rs:1277.
- verifier verdict **PARTLY_WRONG** (6 truth rows wrong, 7 anchors wrong)

<details><summary>corrections</summary>

```
VERDICT SUMMARY: The grammar/validation half of the truth table (rows 1-62, 72-76) is exceptionally accurate — I re-ran all 76 rows against PG 18.4 and every SQLSTATE, message, DETAIL, tgtype, tgattr, tgargs and pg_get_triggerdef string in the DDL block reproduced byte-for-byte. The 6 wrong rows are ALL in the firing-semantics block (rows 63-69), which is exactly the part the spec's own recommendation says not to implement — but they would still become wrong tests. 7 of 27 code anchors are wrong, including 3 outright fabricated or misattributed function names. The "current crabka behavior" claim is fully corroborated (NOT fabricated). The stale-test list is accurate.

=== WRONG ORACLE ROWS (6 of 76) ===

(1) ROW 67 (b2_skip). Spec: "(0 rows)\nINSERT 0 0; SELECT count(*) → 0". The RETURNING and command tag are right; the count is WRONG. Run as written, immediately after row 66 which left 101|x in trigger_t6:
  psql> CREATE TRIGGER b2_skip BEFORE INSERT ON triggerv_t6 FOR EACH ROW EXECUTE FUNCTION pg_temp.triggerv_skip();
        INSERT INTO triggerv_t6 VALUES (1,'x'),(2,'y') RETURNING *;
        SELECT count(*) FROM triggerv_t6;
  → ` a | b \n---+---\n(0 rows)` ; `INSERT 0 0` ; ` cnt_after_fire2 \n-----------------\n               1`
Real answer is 1, not 0. The row-66 row is still present.

(2) ROW 68 (a3_skip) — the worst row. Spec: "INSERT 0 1; SELECT count(*) → 1". Actual, run as written with b1_mod and b2_skip still attached (nothing in the spec drops them):
  psql> CREATE TRIGGER a3_skip AFTER INSERT ON triggerv_z FOR EACH ROW EXECUTE FUNCTION pg_temp.triggerv_zskip();
        INSERT INTO triggerv_z VALUES (3,'z');
        SELECT count(*) FROM triggerv_z;
  → `INSERT 0 0` ; ` actual_count \n--------------\n            0`
I reproduced this twice (once continuing the spec's own sequence on trigger_t6, once on a clean table triggerv_z with only b1_mod/b2_skip/a3_skip). BEFORE ROW triggers fire alphabetically, so b2_skip returns NULL and suppresses the row; the AFTER ROW trigger never runs. The row's *note* ("an AFTER ROW trigger's return value is ignored") is true, but the quoted output is unreproducible from the quoted SQL. As a test expectation this is a guaranteed false failure.

(3) ROW 69 (trigger firing order) — not reproducible as written. The claimed 3-row log output requires b1_mod, b2_skip and a3_skip to have been dropped first; the spec never says so. With b2_skip still attached the insert is suppressed and NO AFTER ROW trigger fires, so the log gets only the mmm_log row. After I explicitly `DROP TRIGGER b1_mod/b2_skip/a3_skip` and `DELETE FROM triggerv_t6`, the claimed output IS exact:
  → `mmm_log|BEFORE|STATEMENT|INSERT` / `aaa_log|AFTER|ROW|INSERT` / `zzz_log|AFTER|ROW|INSERT` (3 rows)
So the behavioral claim is right; the row is unusable as written. (Row 70 has the same latent defect — it needs trigger_t6log cleared, undocumented, though I confirmed its output after `DELETE FROM triggerv_t6log`.)

(4) ROW 64 (DROP FUNCTION with dependents) — DETAIL line ORDER is backwards. Spec: "DETAIL: trigger trigger_k3 ... \ntrigger trigger_k4 ...". Actual:
  psql> DROP FUNCTION pg_temp.triggerv_k();
  → ERROR: 2BP01: cannot drop function pg_temp_43.triggerv_k() because other objects depend on it
     DETAIL:  trigger triggerv_k4 on table triggerv_t4 depends on function pg_temp_43.triggerv_k()
     trigger triggerv_k3 on table triggerv_t4 depends on function pg_temp_43.triggerv_k()
     HINT:  Use DROP ... CASCADE to drop the dependent objects too.
k4 comes FIRST. I proved the ordering rule is OID/creation order, not alphabetical, with a controlled probe: creating aa_first then zz_second yields "DETAIL: trigger aa_first ... \ntrigger zz_second ...". In the spec's own session k4 was created (its row 54) before k2→k3 (its row 63), so k4 has the lower OID and must print first. The spec has it inverted.

(5) ROW 65 (DROP FUNCTION ... CASCADE) — same inversion. Actual:
  → NOTICE: drop cascades to 2 other objects
     DETAIL:  drop cascades to trigger triggerv_k4 on table triggerv_t4
     drop cascades to trigger triggerv_k3 on table triggerv_t4
     DROP FUNCTION

(6) ROW 63 (ALTER TRIGGER RENAME) — output incomplete. Spec: "(SELECT tgname FROM pg_trigger WHERE tgrelid='trigger_t4'::regclass AND NOT tgisinternal → trigger_k3)". That query returns TWO rows in the spec's own session, because its row 54 created trigger_k4 on the same table:
  psql> ALTER TRIGGER triggerv_k2 ON triggerv_t4 RENAME TO triggerv_k3;
        SELECT tgname FROM pg_trigger WHERE tgrelid='triggerv_t4'::regclass AND NOT tgisinternal ORDER BY tgname;
  → `ALTER TRIGGER` ; `triggerv_k3` and `triggerv_k4` (2 rows)

=== FALSE CLAIM IN AN OTHERWISE-CORRECT ROW (row 71) ===
The row's oracle output is right (`UPDATE 0` then `UPDATE 1` then `2 | x`), but its note — "the only trigger function PostgreSQL ships that is callable without a PL: the built-in C suppress_redundant_updates_trigger" — is FALSE, and it is load-bearing for the execution_reachable verdict:
  psql> SELECT p.proname, l.lanname FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.prorettype='trigger'::regtype AND p.pronamespace='pg_catalog'::regnamespace ORDER BY 1;
  → 17 rows, all lanname=internal: RI_FKey_cascade_del, RI_FKey_cascade_upd, RI_FKey_check_ins, RI_FKey_check_upd, RI_FKey_noaction_del, RI_FKey_noaction_upd, RI_FKey_restrict_del, RI_FKey_restrict_upd, RI_FKey_setdefault_del, RI_FKey_setdefault_upd, RI_FKey_setnull_del, RI_FKey_setnull_upd, suppress_redundant_updates_trigger, trigger_in, tsvector_update_trigger, tsvector_update_trigger_column, unique_key_recheck
And tsvector_update_trigger is genuinely user-attachable and fires with no PL:
  psql> CREATE TRIGGER triggerv_tsv BEFORE INSERT OR UPDATE ON triggerv_ts FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(tsv, 'pg_catalog.english', title, body);
        INSERT INTO triggerv_ts VALUES ('hello','world') RETURNING tsv;
  → `'hello':1 'world':2` ; `INSERT 0 1`
Consequence: the an
```

</details>

Existing tests that assert the about-to-change behavior:
- `docs/PG_COMPAT_MATRIX.md` :: `row 128 `| CREATE TRIGGER | Wave-assigned(P4) | Trigger support. |`` — tools/check-pg-compat-matrix.py `validate()` (line ~450) errors `parser accepts command(s) without a resolved Implemented/Mapped/Error-with-notice matrix row` the moment `CommandIdentity::CreateTrigger` exists. Must flip to `Implemented` (or `Mapped(...)`) in the same change. Also `validate_behavior_probes()` at line 375 errors `parser-accepted wave-assigned command(s) lack intentional refusal` if a probe is added while the row stays Wave-assigned.
- `docs/PG_COMPAT_MATRIX.md` :: `row 175 `| DROP TRIGGER | Wave-assigned(P4) | Trigger support. |`` — same checker, same reason. DROP TRIGGER is half the 65-statement bucket, so it has to land with CREATE TRIGGER.
- `docs/PG_COMPAT_MATRIX.md` :: `row 72 `| ALTER TRIGGER | Wave-assigned(P4) | Trigger lifecycle. |`` — only if ALTER TRIGGER … RENAME TO is implemented too (it appears nowhere in the regress corpus, so it is optional and wins 0 statements).
- `docs/PG_COMPAT_MATRIX.md` :: `row 245, the `pg_catalog` object-definition-functions prose: "pg_get_ruledef/pg_get_triggerdef/pg_get_partkeydef/pg_get_function_* answer NULL, matching PostgreSQL's answer for an oid with no such object"` — becomes a false claim as soon as pg_trigger has rows. Prose, not machine-checked, but it is the documented behavior contract.
- `docs/PG_COMPAT_MATRIX.md` :: `row 244, the `pg_catalog` introspection-relations prose listing pg_trigger among relations that "resolve and return zero rows rather than 42P01"` — same — pg_trigger stops being an always-empty relation.
- `crates/gres-conformance/src/parser_commands.rs` :: `fn statement_shape (line 810) — exhaustive `match statement`` — compile error, not a test failure: the match has no `_` arm, so new Statement variants break the build. Documented at parser_commands.rs:707-710 as deliberate ("an exhaustive Statement match forces this module to account for new statement variants").
- `crates/pgexec/src/session.rs` :: `fn establishes_transaction_activity (line 2017) and the DDL routing match at line 3891` — both are exhaustive over Statement; both are compile errors without new arms.
- `crates/gres-conformance/corpus-regress/baseline.json` :: `per-file `matched` counts for copy, truncate, create_table, insert, update, with, alter_table` — the regress ratchet (crates/gres-conformance/src/lib.rs:581) fails a run whose matched count drops, and refuses to silently accept a rise — the baseline has to be re-measured and bumped for the seven files that contain trigger statements.
- `crates/gres-conformance/corpus-regress/TRIAGE.md` :: `line 106, the `| 65 | 42601 | … found Ident("trigger") |` root-cause row` — documentation-only, but it is the row this whole study is against and it becomes wrong. Note the same file's partitioning rows (lines 104-105, 254 + 115 statements for PARTITION OF / PARTITION BY) are ALREADY stale — partitioning is implemented now (crates/pgexec/src/partition.rs, exec.rs:10636 range/list/hash), which is why most trigger target relations do exist today.
- `crates/pgexec/tests/catalog_introspection.rs` :: `every_named_catalog_relation_resolves (line 88 lists pg_catalog.pg_trigger)` — NOT stale — it only asserts the relation resolves, not that it is empty. Listed so a reader does not assume it needs touching.


## Next steps, with what was learned attempting them

### `public.`-qualified relation names (the 75 x 3F000 cluster, plus create_table/create_view cascade)

`public` is now accepted as an identifier (`expect_ident` takes `Keyword::Public`), so `SELECT 1 AS public`
and a `public.t` in FROM position both parse. What still fails is *resolution*: `public.t` reaches the
catalog as the literal name `"public.t"` and misses.

Do NOT fix this in the parser. Two attempts were made and reverted:

1. Collapsing `public.`/`pg_temp.` inside `Parser::expect_object_name` breaks the routine path, which has
   its own schema check that depends on the dot NOT being consumed (`crates/pgparser/tests/routines.rs`
   :: `a_schema_other_than_public_does_not_exist` expects 3F000 for `CREATE FUNCTION other.f()`).
2. Raising a blanket 3F000 for any non-`public` qualifier is wrong because schemas ARE created here, just
   inert — it broke `catalog_introspection.rs` :: `comments_reach_pg_description_and_the_description_functions`,
   which does `CREATE SCHEMA shop` and then references `shop.`. The parser has no catalog and cannot tell a
   created-but-inert schema from an absent one.

Relation names are also read by bare `expect_ident()` at ~8 sites (create_table, alter, drop, insert,
create index, …), not through `expect_object_name`, so a parser fix would have to touch all of them.

**The catalog half of this is now DONE.** `crabka_pgkv::key::unqualified_relation` strips a leading
`public.`/`pg_temp.` and is applied by `catalog_key`, `catalog_sharding_key`, `pgcatalog::view_key` and
`catalog_sequence_key`, and `get_table` normalizes the name it stores so the qualifier cannot reach an error
message or `pg_class`. Reads now resolve: `SELECT a FROM public.t` finds `t` where it was 42P01 before.
pgkv (53) and pgcatalog (80) tests pass and the full workspace is green at 14569/0.

**What REMAINS is the parser half.** `INSERT INTO public.t`, `CREATE TABLE public.u`, `ALTER TABLE public.t`,
`DROP TABLE public.u` and `CREATE INDEX ON public.t` are still 42601, because those paths read the relation
name with a bare `expect_ident()` that cannot consume a dot at all — the catalog never gets a chance to
normalize what it is never handed. That is the ~8 sites listed above; they want a shared
`expect_relation_name()` that accepts `ident [. ident]` and hands the whole dotted string to the catalog,
which now knows what to do with it. `expect_object_name` is NOT that helper — see the two reverted attempts.

Original analysis of the seam, kept for context: `crabka_pgkv::key::catalog_key` (crates/pgkv/src/key.rs:337) is a plain
`system_prefix("catalog") + name` concatenation that every relation read AND write goes through, alongside
its siblings `catalog_sharding_key`, `view_key` and `catalog_sequence_key`. Normalising the name there —
stripping a leading `public.` or `pg_temp.`, which is what crabka's single flat namespace means under
PostgreSQL's default `search_path` — makes `public.t` and `t` the same relation for every path at once.

Care needed, because this sits under all catalog access:
- Apply it in ONE shared helper used by all four key builders, not copy-pasted.
- `get_table` stores `name: name.to_string()`, so a `Table` built from `public.t` would carry the qualified
  spelling into error messages and `pg_class`. Normalise the stored name too.
- `information_schema.tables` and `pg_catalog.*` are resolved BY their dotted names today
  (`parser.rs` :: `parses_information_schema_qualified_tables` asserts `name == "information_schema.tables"`),
  so the strip must be limited to `public`/`pg_temp` and must not touch those.
- Verify with the differential, not just unit tests: the earlier FILTER work found three separate fast paths
  that silently bypassed a change which unit tests happily accepted.

### Honest sizing for the rest

The per-feature loop lands roughly 10-30 regress statements at a time. At 8936/14272 the remaining distance
is dominated by foreign keys, real schema support, full-text search, PL/pgSQL and triggers, and the long tail
of types — each a wave, not a patch. `CREATE TRIGGER` (65 statements, the single largest parser gap) cannot
pay off at all until there is a PL interpreter, because PostgreSQL SUCCEEDS on those statements: parsing them
converts a 42601 into a different mismatch and wins nothing.

### Measurement hazards to fix first, both found by measuring rather than reasoning

- A corpus statement set the connecting role NOLOGIN and locked the oracle out entirely; `DISCARD ALL` cannot
  undo catalog state. Connect as a role the corpus never names, or re-assert `ALTER ROLE <role> LOGIN` BEFORE
  each file. Recreating the container is the only recovery once it happens.
- Always run `cargo test --workspace --no-fail-fast`. Without it cargo stops at the first failing test BINARY;
  three runs in this session each reported "1 failure" and each named a different test. The suite is 595
  result lines / 14,569 tests.
