# Verification: jx-array-elem-types (cluster json_xml)

Verdict: root cause CONFIRMED, attribution CONFIRMED (757 vs 763 claimed), fix
location PARTLY WRONG (one decision point, several symptom sites named as fixes,
two real fix sites missing), oracle facts CONFIRMED.

## 1. Root cause, per file (whole-block rule)

### json (json.diff) — 368 lines to this root
- L15 hunk (`@@ -396,30`): `array_to_json(array_agg(q),false|true)` -> `+ERROR: arrays of record are not supported` = 6 + 8 = 14 lines. Third block (ARRAY[ROW(x.*,...)]) fails FIRST on the parser: `syntax error at position 109: expected identifier, found Star` (`x.*` inside ROW). 6 lines -> parser root, would then need record[] (fails longer here).
- L226: `reca jpop[]` in CREATE TYPE jsrec -> 1 line. jsrec does NOT reference j_ordered_pair; the domain-over-composite failure two statements earlier is independent. The other jsrec fields (`_int4`, `int[]`, `int[][]`, `int[][][]`, domain over int[], `char(10)[]`, `json[]`, `jpop`) all resolve today (`from_sql_name("_int4")` tested in datum.rs:2279; `ElemType::Char(Some(10))`; domains created without error), so jpop[] is the ONLY blocker of CREATE TYPE jsrec.
- L234 hunk (`@@ -1514,353 +1443,147`): 344 lines, every block is `+ERROR: type "jsrec" does not exist` (71 occurrences of that message in the file). Cascade of L226.
- L718 hunk: jspoptest `(json_populate_record(NULL::jsrec, js)).*` 8 lines + `DROP TYPE jsrec` 1 line = 9.
- NOT this root in json: domain over composite (`j_ordered_pair`): 1 + 14 + 15 + 1 = 31 lines; populate_recordset array-into-timestamp (7 lines, Gres accepts `[100,200,300]` as a timestamp); array_to_json of 2-D int[] flattening (6 lines).

### jsonb (jsonb.diff) — 354 lines
- L363: `reca jbpop[]` 1 line.
- L371 hunk (360 changed): 344 lines are `type "jsbrec" does not exist` cascade; the remaining 16 (L795, L806) are `jsonb_populate_record_valid` missing — different root.
- L984 hunk: 8 + 1 (`DROP TYPE jsbrec`) = 9.
- jsonb_agg(q) with ARRAY[ROW(x.*,...)] (L66 hunk, 6 lines): parser first (`position 88 ... found Star`), NOT this root; analyst's evidence citation for jsonb H5 is wrong.
- Domain over composite (`jb_ordered_pair`): 31 lines, separate root.

### sqljson_queryfuncs — 19 lines
- CREATE TYPE sqljsonb_reca (reca sqljsonb_rec[]) 1 line; `unnest((JSON_QUERY ... RETURNING sqljsonb_reca)).reca)` 7 lines (cascade); `unnest(JSON_QUERY(... RETURNING sqljsonb_rec[]))` 7 lines. Both unnest statements will FAIL LONGER: Gres' JSON_QUERY RETURNING <composite> today goes through the text cast (`malformed record literal`, or NULL) rather than the populate_record path (see the `RETURNING sqljsonb_rec` blocks that already fail without arrays).
- `RETURNING int[] DEFAULT (SELECT '{1}')::oid[]::int[] ON ERROR` 4 lines: parser 0A000 for `oid[]` fires first, but the oracle expects `can only specify a constant, non-aggregate function, or operator expression for DEFAULT` — after the fix these 4 lines belong to the DEFAULT-expression restriction root (its neighbours RelabelType/CollateExpr already fail on it).

### sqljson_jsontable — 6 lines
- `js1 oid[] PATH '$.d2' DEFAULT '{1}'::int[]::oid[] ON EMPTY` -> `{1}` expected; 6 lines.

### xml — 10 lines (first failure only)
- Two CTE statements with `proargtypes::oid[]` (5 lines each). Parser raises the 0A000 for `oid[]` before it reaches `xmltable(... PASSING ...)`, which Gres does not parse at all (40 x `expected RParen, found Ident("passing")` in the file). These 10 lines will FAIL LONGER on the XMLTABLE root; honestly they are XMLTABLE's lines.

Total strictly-first-failure: 368 + 354 + 19 + 6 + 10 = 757 (analyst: 763). Excluding the two blocks that a different root ultimately owns (queryfuncs DEFAULT 4, xml 10): 743.

## 2. Fix location

The one decision point is `ElemType::from_column_type` (crates/pgtypes/src/datum.rs:355-446), reached via `ColumnType::array_of` (datum.rs:1118). Everything the analyst listed in pgparser/pgexec is a CALLER that maps the `None` to the 0A000/Unsupported message:
- parser.rs:773 `parse_array_type_suffix` -> `ColumnType::array_of(base).ok_or_else(0A000)`
- agg.rs:503 `array_of` helper; eval.rs:607, 5082 (`array_literal_elem_type`); subquery.rs:572, 787; exec.rs:17002 — all `ElemType::from_column_type(..).ok_or_else(Unsupported)`.
None of these need to change once `from_column_type` answers for composite/enum/domain/oid/anonymous record.

What exists today: `ElemType` is a closed enum (24 variants: 22 in `ALL` + `Range(RangeRef)` + `Multirange(MultirangeRef)`), `Copy`, with a stable persisted `code()` byte (datum.rs:563), `write_code`/`read_code` (600-663) that already carry a 4-byte type oid for Range/Multirange, `array_oid()` (454) that already knows `user_multirange_array_oid`, `array_name()` (505), `from_array_oid()` (667). `UserTypeRef`/`DomainRef` are `Copy` (usertype.rs:34, 60) and `user_array_oid(type_oid) = type_oid + 2` (usertype.rs:1001) is already reserved and answered by visibility.rs:377 (`_jpop`). `ArrayValue.elems` is `Vec<Datum>` and the row encoder (pgkv/src/rowenc.rs:385 `encode_array`) reuses the tagged-field encoding per element, so composite/enum/oid elements already round-trip on disk. Array text I/O already dispatches per element (`cast_in` cast.rs:736 -> `cast_in(text, elem.column_type())`; encoding.rs:220 -> `encode_text_in` per element), so no change in `crates/pgtypes/src/array.rs` (it is a type-agnostic literal parser/printer). json_record.rs is already generic (`populate_array` -> `populate_value(elem.column_type())` -> `populate_composite`), so `reca jpop[]` populate needs no change there.

Corrected fix set:
1. crates/pgtypes/src/datum.rs — `ElemType`: add `Composite(UserTypeRef)`, `Enum(UserTypeRef)`, `Domain(DomainRef)`, `Oid`, `Record` (anonymous, oid 2287 `RECORDARRAY` already in oids.rs:19); update `column_type`, `from_column_type`, `array_oid` (user_array_oid), `array_name` (needs an interned `"<name>[]"`), `code`/`write_code`/`read_code`, `from_array_oid` (scan `usertype::all()` for `user_array_oid`), `ALL`/tests.
2. crates/pgcatalog/src/serde.rs:585-615 `read_elem_type_with` — extend the oid-carrying decode for the new codes (analyst missed).
3. crates/pgexec/src/exec.rs ~23260-23300 (`_<typname>` for pg_type array rows) and pg_type user-type array rows if not already emitted; exec.rs:24973 `column_type_from_oid` fallback (through `from_array_oid`) so a `_jpop` RowDescription oid resolves.
4. crates/pgtypes/src/encoding.rs:569 `encode_array_binary(a, a.elem.oid())` — works once `oid()` answers; verify record/enum binary element encoding.
5. crates/pgtypes/src/cast.rs — element cast paths already generic; check `unify_types` for `Record(None)` in `ARRAY[ROW(..),ROW(..)]`.

## 3. Dependencies / hidden prerequisites
- Storage: new `ElemType::code` values (append-only; SCHEMA_VERSION 12 in pgcatalog/serde.rs:50 need not bump but greenfield allows it).
- Catalog: pg_type array row for each user type (`typarray`, `_name`), DROP TYPE dependency (a composite that embeds `jpop[]` must block `DROP TYPE jpop`).
- Unblocked json/jsonb statements: likely pass (populate_array/HINT/DETAIL/domain-check/char(10) padding all present; `jsb_ia`/`jsb_char2` neighbours already match). Risks: `(json_populate_record(NULL::jsrec, js)).*` 17-column expansion; 17-field `ROW(...)::jsrec` cast with a nested typed row and NULL array; `array_to_json(array_agg(q), true)` pretty output.
- Fails longer: queryfuncs unnest blocks (14 lines) on SQL/JSON RETURNING-composite populate; queryfuncs DEFAULT block (4) on DEFAULT-expression restriction; xml (10) on XMLTABLE.
- ARRAY[ROW(x.*, ...)] blocks (json 6, jsonb 6) need the parser fix for `x.*` inside ROW() first, then record[].
- Cross-cluster (22 diff files): also `posint` (domain), `rainbow` (enum), `tid`, `xid`, `regclass`, `int2vector`, `oidvector`, `tsvector`, `point`, `inet`, `varbit`, and `arrays of integer[]`/`text[]` (nested ARRAY constructor flattening). A general "ElemType is any non-array ColumnType" design covers them; that is where XL comes from.

## 4. Oracle facts
All quoted strings verified in the oracle/expected side of json.diff L234-L656: `{1,2,NULL,4}`, `expected JSON array` + `HINT: See the value of key "ia".`, `HINT: See the array element [1] of key "ia".`, `malformed JSON array` + `DETAIL: Multidimensional arrays must have sub-arrays with matching dimensions.`, `value for domain js_int_array_1d violates check constraint "js_int_array_1d_check"`, `value too long for type character(10)`, `{"(abc,456,)",NULL,"(,,\"Thu Jan 02 00:00:00 2003\")"}`. Occurrence count is 71 (not 60) per file.

## 5. Size
Core json_xml subset (composite/enum/domain/oid/record elements): L (2-4 days). XL only if it absorbs the whole cross-cluster element-type family. Compat matrix line 318/351 lists composite arrays as deferred, not a non-goal.
