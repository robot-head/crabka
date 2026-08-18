# scalar_types cluster — root-cause triage (read-only, 2026-08-17)

Files: name, text, float8, numeric, uuid, enum, strings, numerology, regex, encoding,
conversion, expressions, arrays, case, errors, pg_lsn, inet, collate, collate.utf8_1.
Total changed lines: 4990. Planner-only estimate: 286 (numeric 60, regex 63,
expressions 11, arrays 2, case 60, pg_lsn 14, inet 76).

## Per-file attribution (whole-block)

| file | lines | first failing statement | cascade | attribution |
|---|---|---|---|---|
| name | 102 | `SELECT * FROM NAME_TBL` (72-char value not truncated to 63) | no | name-type 82, named-args 6, parse-ident-detail 5, from-func-unknown-arg 9 |
| text | 138 | `select length(42)` message shape | no | undefined-object-msg 5, datestyle-concat 4, variadic-call 82, format-star 47 |
| float8 | 465 | `SELECT float8send(...)` | no | send-recv 247, float8-math 116, erf-gamma 100, error-position 2 |
| numeric | 269 | cross-join order (`FROM v v1(x1), v v2(x2) WHERE x2 != 0`) | no | PLANNER 60, typmod-overflow+negscale 69, overflow-detect 22, width_bucket 40, to_char/to_number 18, arith-precision 12, gen-series 22, sql-fn-context 2, lcm-dscale 24 |
| uuid | 254 | `CREATE TABLE guid1 (... TEXT DEFAULT(now()))` | YES | assignment-cast-io-to-string 165 (producer), uuidv7/extract 89; unblocked statements need error-position (12) |
| enum | 382 | `SELECT COUNT(*) FROM pg_enum ...` → 0 | no | pg_enum 79, alter-msgs 8, enum-funcs 104, unknown-literal-adopts-type 128, arrays-any-elem 36, anyenum 9, unsafe-new-value 18 |
| strings | 432 | lexer error shape (continued string with comment) | no | syntax-parity 38, scs 82, bytea-input-msgs 24, similar-substring 17, similar-explain-fold 55, regex-engine 18, regexp-arg-msgs 18, toast 6, to_bin/oct 64, int-bytea 96, unistr 15 |
| numerology | 110 | `SELECT 123abc` | no | syntax-parity 108, error-position 2 |
| regex | 445 | `select 'bbbbb' ~ '^([bc])\1*$'` | no | regex-engine 381, PLANNER 63, regexp-arg-msgs 1 |
| encoding | 183 | `INSERT ... test_bytea_to_text('\xc3')` | YES | regress-c-encoding-functions 172, convert_to 8, error-position 3 |
| conversion | 448 | `SELECT FROM test_enc_setup()` | YES | select-empty-target 4, param-mode-after-name 2, conversion-ddl 12, multi-encoding-library+regress-c 430 |
| expressions | 255 | `now()::timetz::text = current_time::text` → f | partial | sql-value-fn-precision 62, current_catalog 6, typmod-casts-views 50, explain-view/verbose 9, PLANNER 11, explain-verbose-output 14, shell-operators 103 |
| arrays | 689 | `INSERT INTO arrtest (a[1:5], ...)` | partial | insert-target-subscripts 124, naming 84, pg_input-type-brackets 24, assign-validation 5, error-position 11, row-order 6, point-subscript 24, array-fn-family 89, arrays-any-elem 185, concat-op 7, anyall-msgs 8, PLANNER 2, like-any 48, array-literal-detail 26, sql-fn-no-inline 19, fipshash-resolution 18, width_bucket 9 |
| case | 138 | CASE column name | no | naming 22, const-folding 33, PLANNER 60, row-order 2, operator-on-domain 13, enum-funcs 8 |
| errors | 207 | `select;` | no | syntax-parity 156, select-empty-target 4, error-position 18, for-update-groupby 4, rename-col-inherit 2, txn-warning 2, create-aggregate 4, obj-msg-quoting 7, drop-rule 10 |
| pg_lsn | 22 | EXPLAIN over generate_series join | no | PLANNER 14, explain-expr-deparse 8 |
| inet | 80 | EXPLAIN with enable_seqscan off | no | PLANNER 76, sort-tie-order 4 |
| collate | 305 | `a int COLLATE "C"` position | no | collation-derivation 119, error-position 6, naming 8, domain-func-resolution 7, row-in-subquery 13, syntax-parity 4, scalar-subq-arg 18, index-collation 14, explain-sortkey-collation 12, create-collation 82, collation-for 20, literal-collate-fold 2 |
| collate.utf8_1 | 168 | `CREATE COLLATION ... provider = builtin` | YES | create-collation+builtin-provider 168 (true target: collate.utf8.out) |

## Key source facts
- pgtypes/src/datum.rs:1002 `"text" | "name" => ColumnType::Text` (no 63-byte truncation).
- pgparser/src/parser.rs:3010 `positional_from_named` (only make_interval); parser.rs:1900 `func_call` has no VARIADIC.
- pgexec/src/string_fn.rs:1119 `format_sql` (no `*` width, no HINT); func.rs:726 concat uses `text_render` (no DateStyle).
- pgexec/src/func.rs:3128 `power`, eval.rs:1370 Pow: no dpow special cases; overflow -> TypeError::Overflow ("integer out of range").
- pgexec/src/regexp_fn.rs:472 `compile_pattern` hands the raw pattern to the Rust `regex` crate; pattern.rs:117 `similar_to_regex` too.
- pgparser/src/error.rs:34 `ParseError::new` "syntax error at position N"; lexer.rs:699 `at_or_near` and parser.rs:13684 `syntax_error_at_token` exist but are rarely used.
- pgtypes/src/cast.rs:314 `assignment_cast_allowed` refuses non-string -> string (PG: I/O casts TO string types are assignment-level) — uuid producer.
- pgtypes/src/datum.rs:261 `ElemType` closed enum (no enum/record/composite/int2vector/oidvector/xid/point/domain elements).
- pgexec/src/eval.rs:1473 `coerce_untyped_literal_operands`: literal adopts sibling type only for temporal/array/range; not enum.
- pgexec/src/usertype.rs:505-535,1281 enum label rules; no pg_enum rows.
- pgparser/src/parser.rs:1346 `expect_collation_name` (default/C/POSIX only); ast.rs:3753 `Expr::Collate` no-op.
- pgparser/src/ast.rs:2156 CREATE/DROP CONVERSION refused.
- pgexec/src/math_fn.rs:792 `width_bucket` own algorithm.
- pgtypes/src/numeric.rs:48 `Typmod{u16,u16}`; `apply_typmod` -> TypeError::Overflow.
- pgexec/src/exec.rs:24919 `named_expr_inner` (FigureColname subset).
- pgexec/src/routine.rs:2326 SQL functions only by inlining.
- pgexec/src/useroperator.rs:373 duplicate operator check; no shell operators.
- pgexec/src/exec.rs:24179 sorts are stable; inet tie order needs a live probe.
