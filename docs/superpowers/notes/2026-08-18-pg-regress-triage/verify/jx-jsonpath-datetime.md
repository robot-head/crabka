# Verification: jx-jsonpath-datetime

Verdict: root cause CONFIRMED; attribution CONFIRMED (my count 1280 of 2060 in
jsonb_jsonpath, analyst 1265); fix location PARTLY WRONG; dependencies PARTLY WRONG.

## 1. Root cause (jsonb_jsonpath.diff)

Recount by statement block (whole-block rule, script count_jx_dt.py):

| class                                    | lines | stmts |
|------------------------------------------|-------|-------|
| .datetime("template")                    | 348   | 69    |
| .time(n)/.timestamp(n)/... precision     | 205   | 41    |
| typed items / tz rules / rendering       | 705   | 117   |
| 6 hunks that start mid-statement (all datetime comparisons under _tz) | 18 | 6 |
| `set time zone` reorder artefacts inside the datetime section | 4 | - |
| **datetime total**                       | **1280** | |
| named args (`silent =>`, `vars =>`)      | 568   | 88    |
| .decimal(p,s) args                       | 66    | 17    |
| other (like_regex, keyvalue id, `.**{last}`, NaN messages, unary +, ...) | 168 | 53 |

Of the 1280, 2 lines (`"1000000-01-01"` overflow comparison, expected `true`,
Gres `null`) will still fail after this root: jiff `civil::Date` stops at year
9999 (crates/pgtypes/src/datetime.rs lines 38-44, 1071). Net 1278.

Not a cascade: every statement is independent. First datetime failure is
`select jsonb_path_query('"bogus"', '$.datetime()')` (expected
`datetime format is not recognized: "bogus"` + HINT; Gres
`.datetime() format is not recognized: "bogus"`).

Sub-claims checked against source:
- template refused at parse time: crates/pgexec/src/jsonpath.rs:1015-1024
  (`ExecError::Unsupported("jsonpath .datetime(template) is not supported")`),
  precision args -> `self.error_here()` syntax error at line 1023. CONFIRMED.
- datetime_method (jsonpath.rs:1995-2040) casts through
  `crabka_pgtypes::cast::cast(&source, target, &TimeZone::UTC)` and returns
  `JsonbValue::String`; renders with `encoding::encode_text` and swaps the
  first space for `T`. CONFIRMED. Consequences seen in the diff: `.datetime()`
  on "2017-03-10 12:34:56" yields "2017-03-10" (date cast accepts trailing
  time; PG's std-mode "yyyy-mm-dd" template does not), "2017-03-10t12:34:56+3:10"
  accepted (PG: not recognized), offsets rendered `+03` (PG `+03:00`),
  `.type()` says "string".
- compare (jsonpath.rs:1784-1807) compares strings bytewise. CONFIRMED.
- "no _tz variants": WRONG. json_fn.rs:212-215 maps
  jsonb_path_exists_tz/match_tz/query_array_tz/query_first_tz to the same
  JsonFunc, srf.rs:321 maps jsonb_path_query_tz to Srf::JsonbPathQuery, and
  builtin_procs_*.tsv.zst already hold pg_proc rows 1177/1179/1180/2023/2030.
  What is missing is the useTz flag (and the session time zone) reaching the
  evaluator; JsonPath::query/exists/predicate (jsonpath.rs:1151-1226) take no
  tz and no flag.

## 2. Fix location

Correct as named: jsonpath.rs (Method args, Accessor parser at ~1007-1035,
datetime_method, compare, Exec, printer at ~2172); json_fn.rs path_args /
eval_path_func (json_fn.rs:874-927) and jsonb_path_query_rows (1714-1727)
plus json_path_operator (~930-945) must take use_tz + &ctx.time_zone
(EvalCtx.time_zone exists, crates/pgexec/src/clock.rs:65).

Wrong / missing:
- format_fn.rs is only a wrapper. The template engine is
  crates/pgtypes/src/datetime.rs `parse_by_template` (5481), `Scanner`
  (5049), `Assembly` (5490), `tokenize_template` (4613). It has NO std mode:
  no "input string is too short for datetime format", "trailing characters
  remain in input string after datetime format", "unmatched format character",
  "invalid datetime format separator" (grep finds none), and `ParsedDateTime`
  (4254) exports no field mask (has_year/has_mon/has_day live privately in
  Assembly; has_tz privately in TmFromChar 4739). PG's executeDateTimeMethod
  calls parse_datetime(..., std=true) and picks date/time/timetz/timestamp/
  timestamptz from the returned fmask + tz flag; the no-template `.datetime()`
  and the `.date()/.time()/...` methods run the same fixed std-format list.
  So the engine work lands in pgtypes/datetime.rs, plus a small caller in
  jsonpath.rs.
- builtin_procs_*.tsv.zst: nothing to add (rows exist).
- srf.rs `Srf::JsonbPathQuery` (line 755 in expand) is where jsonb_path_query
  is dispatched with ctx available; jsontable.rs:346/385/532 also call
  JsonPath::query and PG runs SQL/JSON paths with useTz=true.
- WARNING "TIME(10) precision reduced to maximum allowed, 6" (8 lines here):
  no eval-time warning channel exists (notice_tx is on Session,
  session.rs:2686; EvalCtx has no sink; the message string exists nowhere in
  crates/). Shared with expressions.diff `current_time(7)`.
- Rendering: PG JsonEncodeDateTime prints offsets as +HH:MM; a jsonpath-side
  renderer is needed rather than encoding::encode_text.

## 3. Dependencies

- jx-named-args-builtins: NOT needed. No datetime statement in the file uses
  `=>` (grep of oracle .out: 92 `=>` lines, 0 with datetime methods).
- jx-jsonpath-grammar-printer: needed for the argument syntax and printer
  (`$.time(6)`, `$.datetime("...")` in jsonpath.diff), and shared with
  `.decimal(p,s)`.
- Hidden: pgtypes std-mode template parser + field mask; EvalCtx tz plumbing
  through json_fn/srf/jsontable; eval-time WARNING channel; jiff year-9999
  limit (2 lines stay).
- Related but outside: sqljson_queryfuncs "functions in index expression must
  be marked IMMUTABLE" for JSON_QUERY with datetime methods (jspIsMutable),
  ~44 lines, not counted here.

## 4. Oracle facts

All quoted messages and outputs verified in self-check-serial/results/
jsonb_jsonpath.out: "cannot convert value from X to Y without time zone usage"
(48 occurrences across 7 pairs) + HINT "Use *_tz() function for time zone
support."; "12:34:56.789+05:30"; precision-out-of-range messages; std
template errors; "datetime format is not recognized" + HINT. Also expected:
`invalid datetime format separator: "a"`, DETAIL "Value must be an integer."
Session tz matters: `jsonb_path_query_tz('"2023-08-15 12:34:56"',
'$.timestamp_tz().string()')` -> "2023-08-15T12:34:56-07:00" (PST8PDT).

## 5. Size

XL is fair (I would not go lower): std-mode engine + field mask in pgtypes,
typed transient item + cross-type comparison/conversion matrix, tz plumbing,
renderer, precision rounding + warning, parser/printer args.
