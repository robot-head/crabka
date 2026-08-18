# json_xml cluster — root-cause triage (2026-08-17)

Files: json 829, jsonb 1386, jsonpath 909, jsonpath_encoding_2 131, jsonb_jsonpath 2060,
sqljson 716, sqljson_queryfuncs 477, sqljson_jsontable 313, xml 1088, xmlmap_1 131 = 8040 lines.
Counting: every in-hunk +/- line except the two file headers; whole change block attributed to one root.
Tallies come from tag.py + per-file rules_*.txt in this directory.

Planner-only lines: jsonb 36 (three EXPLAIN blocks expecting Bitmap Heap/Index Scan on GIN index jidx),
sqljson_jsontable 12 (row order of `generate_series x, generate_series y, JSON_TABLE(...)` without ORDER BY =
PG join order). EXPLAIN VERBOSE `Output:` / `Table Function Call:` lines (sqljson 105, sqljson_jsontable 56)
need the EXPLAIN renderer + deparser, not a planner (listed under jx-sqljson-deparse, dep EXPLAIN-VERBOSE).

## Brief corrections
1. jsonpath_encoding_2.out is the skip variant (`\quit` when encoding not UTF8/SQL_ASCII). Gres reports UTF8 and
   runs the file; the real target is jsonpath_encoding.out (108 changed lines vs the oracle run). Root = jsonpath
   lexer unicode escapes + error text + LINE cursor.
2. xml: the CI oracle was built WITHOUT libxml — results/xml.out == expected/xml_1.out and results/xmlmap.out ==
   xmlmap_1.out. pg_regress picks the closest variant per file: xml -> xml.out (1088; vs xml_1: 1327) because Gres has
   a native XML implementation (crates/pgtypes/src/xml.rs, quick-xml); xmlmap -> xmlmap_1.out (131; vs xmlmap.out
   1329). Aim xml at xml.out and xmlmap at xmlmap_1.out (unsupported-feature stubs). Oracle facts for xml come
   from expected/xml.out.
3. Named arguments: tests use `silent => true` / `vars => ...`, never `target := ...`. Parser resolves named args
   itself (positional_from_named, crates/pgparser/src/parser.rs:3005) with a table that knows only make_interval.
   571 lines in jsonb_jsonpath + 22 in jsonb.
4. "104 syntax error ... of jsonpath input" undercounts: biggest jsonpath sub-issue is numeric literal lexing
   (exponent dropped: '1e1' prints 1), then paren canonicalisation, unary folding, $"a" quoting, last scoping,
   escapes, like_regex flags, method args. All 909 lines = one grammar/printer root.
5. ".datetime(template) not supported" (69) is the tip: datetime family in jsonb_jsonpath ~1265 lines.
6. sqljson "142 explain-ish" = EXPLAIN VERBOSE Output deparse, planner-only 0. sqljson_jsontable has 56 EXPLAIN
   lines (Table Function Scan; view inlining), non-planner.
7. file_stats error_lines unreliable.

## Per-file (first failing statement / cascade / roots)
json 829: `SELECT repeat('[', 10000)::json;` HINT missing. Cascades: CREATE TYPE jsrec (reca jpop[]) fails -> 354;
  domain over composite -> 29. array-elem-types 368, tsvector 217, variadic 83, row-star 41, domain-over-composite 31,
  json_object shape 24, srf-record-in-targetlist 23, set-time-zone 14, pg_stats 7, populate coercion 7,
  array_to_json multidim 6, srf colname 4, diagnostics 4.
jsonb 1386: same first failure. array-elem-types 354, tsvector 229, jsonb scalar casts 158, subscript polish 133,
  variadic 83, populate_record_valid 80, srf-record 74, #- + set path msgs 51, domain-over-composite 31, GIN 7 (+36
  planner), pg_column_size 30, set_lax 28 (22 named-arg + 6), diagnostics 30, json_object shape 20, set-time-zone
  14, pg_stats 7, populate coercion 7, row-star 6, srf colname 4, stack HINT 2, btree order 2.
jsonpath 909: `select '$.a.**{5 to last}.b'::jsonpath;` one root (numeric ~490, parens ~106, unary ~86, regex flags
  ~73, last ~48, method args ~42, escapes ~36, @-root ~22, (a>b).c 6); 35 blocks also lack LINE/^.
jsonpath_encoding_2 131 (108 vs UTF8): `SELECT '"\u"'::jsonpath;` unicode escapes + cursor.
jsonb_jsonpath 2060: `select jsonb_path_query('[1]', 'strict $[1]', silent => true);` named-args 571, datetime 1265,
  grammar 117, evaluator misc 107.
sqljson 716: `SELECT JSON();` JSON aggregates 248, constructors 293, deparse 125, JSON_ARRAY(subquery) 50.
sqljson_queryfuncs 477: `SELECT JSON_VALUE(jsonb 'true', '$');` returning coercion 201, analysis checks 184,
  constraint deparse 40, index immutability 27, array-elem-types 25.
sqljson_jsontable 313: `SELECT JSON_TABLE('[]', '$');` deparse 143, explain 56, user-type oid in view 52, cursor 40,
  planner 12, oid[] 6, DROP VIEW multi 4.
xml 1088: `INSERT INTO xmltest VALUES (3, '<wrong');` xpath 425, xmltable 349, xmlelement family 253, SET XML OPTION
  40, oid[] 10, cursor 8, second libxml error line 3.
xmlmap_1 131: `SELECT table_to_xml('testxmlschema.test1', false, false, '');` xmlmap fns 115, refcursor 12,
  user-type oid (CTAS domain column) 4.

Roots and fix locations: see StructuredOutput (same session).
