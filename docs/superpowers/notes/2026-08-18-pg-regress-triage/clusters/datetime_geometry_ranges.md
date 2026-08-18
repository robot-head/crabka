# Cluster datetime_geometry_ranges — root-cause triage

Files: date 8, timestamp 873, timestamptz 1209, interval 173, horology 671, box 183,
polygon 157, geometry 1346, rangetypes 124, multirangetypes 186 = 4930 changed lines.
Method: every hunk read; per-hunk counts via hunks.py; pure-reorder check via reorder.py
(geometry: 15/17 hunks are pure reorders of identical row multisets).

## Root totals (whole-block attribution)
| root | lines | size |
|---|---|---|
| dgr-datetime-typmod-precision (timestamp(2)/interval(p)/field masks parsed and DISCARDED) | 1980 | L |
| dgr-planner-only (index/bitmap/index-only scans, KNN, nested-loop side order) | 1641 | PLANNER |
| dgr-datetime-range-representation (jiff civil range +-9999 vs PG 4713 BC..294276 / 5874897) | 247 | XXL |
| dgr-missing-datetime-functions (pg_sleep, timestamptz(date,time[tz]), date_add/subtract, interval_hash, avg(interval)) | 165 | M |
| dgr-explain-verbose-output-and-range-support | 97 | L |
| dgr-overlaps-predicate | 96 | M |
| dgr-multirange-literal-parser | 83 | M |
| dgr-format-fn-fidelity (to_char RM/IYYY-BC, to_timestamp DETAIL/HINT, to_timestamp(float) inf/NaN) | 76 | M |
| dgr-generate-series-timestamp (lazy under LIMIT, infinite step, 4-arg tz) | 73 | M |
| dgr-tz-abbrev-zone-lookup (LMT/MMT/MSK gap, to_char TZ, SHOW TIME ZONE posix) | 70 | M |
| dgr-box-input-adjacent-points | 63 | S (cascade in box) |
| dgr-unknown-literal-args | 59 | M |
| dgr-cross-type-datetime-compare | 41 | M |
| dgr-variadic-call-args | 40 | S |
| dgr-explain-const-typing-and-folding | 30 (+co-dependency of 293 planner lines) | L |
| dgr-epoch-literal-utc | 30 | S |
| dgr-range-type-ddl-fidelity | 29 | M |
| dgr-between-symmetric | 28 | S |
| dgr-date-bin-overflow | 21 | S |
| dgr-interval-fidelity | 16 | S |
| dgr-datetime-error-caret | 14 | M |
| dgr-type-privileges | 12 | L (cross-cluster) |
| dgr-decode-strictness-julian-T | 8 | S |
| dgr-multirange-adjacent-op | 4 | S |
| dgr-array-expr-column-name | 4 | S |
| dgr-drop-cascade-notice-hint | 3 | S |

## Per-file (see StructuredOutput for details)
- date 8: range representation.
- timestamp 873: pg_sleep 35 (local txn cascade), caret 2, typmod 718, range 44, date_bin 12, to_char 36, generate_series 26.
- timestamptz 1209: pg_sleep 35, unknown-literal AT TIME ZONE 12, caret 2, range 91, tz-abbrev 48, typmod 879, epoch 26, date_bin 9, to_char 8, generate_series 47, date_add/subtract 32, to_timestamp(float) 14, explain-const 6.
- interval 173: caret 6, planner 14, avg/interval_hash 15, typmod 91, sql_standard sign 6, unknown-literal 31, plpgsql style 2, date_trunc inf 8.
- horology 671: decode 8, range 98, timestamptz(date,time) 48, typmod 292, epoch 4, caret 4, OVERLAPS 96, cross-type 41, explain-const 12, BETWEEN SYMMETRIC 28, to_timestamp detail 18, tz-abbrev 22.
- box 183: box_in 63 (cascade), planner 120.
- polygon 157: planner 157.
- geometry 1346: planner join order 1334 (pure reorders), explain-const 12. No geometry function/operator failures.
- rangetypes 124: range 6, planner 16, range-ddl 1, drop-cascade 2, array name 2, explain-verbose+range-support 97.
- multirangetypes 186: literal parser 83, VARIADIC 40 (cascade), unknown-literal 16, adjacent 4, drop-cascade 1, range-ddl 28, type-privileges 12, array name 2.

## Planner-only per file
date 0, timestamp 0, timestamptz 0, interval 14, horology 0, box 120, polygon 157, geometry 1334, rangetypes 16, multirangetypes 0 = 1641.

## Brief corrections
1. geometry: not one geometric function/operator fails; 1334/1346 lines are cross-join row order (PG nested loop puts the smaller-estimated relation inner; Gres always makes the first FROM item outer) and 12 are EXPLAIN const-fold deparse.
2. box/polygon: operators are correct; 277/340 lines are EXPLAIN index plans; the only engine defect is box_in rejecting '(0,0)(0,100)'.
3. The datetime focus list (ISO 8601, EXTRACT numeric, make_*, age(), justification) passes; the dominant defect is timestamp(p)/interval(p) typmod being discarded (1980 lines = 40% of the cluster), then jiff's +-9999 calendar range (247).
4. rangetypes: 97/124 lines are EXPLAIN VERBOSE Output lines (cross-cluster explain work), not range-type gaps.
5. file_stats explain_lines undercount: geometry 12, timestamptz 6, horology 12.
