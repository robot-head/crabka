# Verification: dgr-datetime-typmod-precision

Verdict: CONFIRMED (root cause, fix locations, attribution). Two hidden prerequisites and one wrong detail.

## Root cause (confirmed)
- timestamp.out L4 `CREATE TABLE TIMESTAMP_TBL (d1 timestamp(2) without time zone)`; timestamptz.out L4 same with time zone; rows L88-89 insert `17:32:01.000001` / `17:32:01.999999`. Gres prints them unrounded (timestamp.diff H3, H10 …), so every result set that shows d1 widens by 7 chars and rewrites every row.
- horology reads TIMESTAMP_TBL / TIMESTAMPTZ_TBL (horology.out L754, L824, L1004 …), so H3/H4/H6/H7/H9/H16-H21 inherit the same two rows.
- interval.diff H4/H5: `interval(0) '…'` and `interval(2) '…'` return unrounded; `interval '…' second(2)` / `day to second(2)` -> "syntax error … found LParen"; `f1::INTERVAL DAY TO MINUTE` -> "syntax error … found Ident("day")". Matches the claim.
- Not a cascade. First failing statement in timestamp/timestamptz is `pg_sleep` (H1, 35 lines each: separate root); horology H1 is an input-error text; interval H1 a caret. The typmod blocks stand alone.

## Source (confirmed, exists)
- crates/pgparser/src/parser.rs L651-663: parse_type_name bumps `(p)` and discards for timestamp/timestamptz/time/timetz/interval. interval_literal (L1852-1880) accepts `field [TO field]` and lowers via datetime::parse_interval_ranged, but no `(p)`; interval_field() has no `(p)` either. parse_type_name accepts no `INTERVAL field TO field`.
- crates/pgtypes/src/datum.rs L757-769: `Time`, `Timetz`, `Timestamp`, `Timestamptz`, `Interval` unit variants; typmod() at L1391 returns -1 for them. 256 match sites in 22 files (datum.rs 62, cast.rs 30, datetime_fn.rs 26, pgcatalog/serde.rs 25, format_fn.rs 17, session.rs 13, exec.rs 13, window.rs 12, srf.rs 11, …).
- crates/pgexec/src/exec.rs coerce L12671; datetime arms L12800-12813 pass through. catalog_typmod L21529 -> other.typmod().
- crates/pgtypes/src/cast.rs cast_in L512 / cast_assign_in L423 — cast_assign_in already special-cases Varchar/Char/Bit typmods; a datetime arm goes there.
- crates/pgtypes/src/datetime.rs IntervalField L1977, mask_bit L2044, SUBSECOND_FIELDS L2064, parse_interval_ranged L2086.

## Hidden prerequisites the claim missed
1. crates/pgcatalog/src/serde.rs L355-376 / L500-524: the column-type encoding writes a reserved 0 byte after TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ/INTERVAL and the reader REJECTS a nonzero byte ("unsupported datetime precision"). Precision fits that byte; the interval range mask needs more (PG packs 16-bit range | precision). Storage-format change; SCHEMA_VERSION (L50, =12) bump per the file's convention.
2. crates/pgexec/src/func.rs builtin_format_type L2745: `1186 => ("interval", NoMod)` — PG's intervaltypmodout prints `interval day to minute`, `interval second(2)`, `interval(2)`. Timestamp/time already handled (TypmodKind::Seconds), so format_type/`\d` need only the interval spelling.
3. crates/pgexec/src/viewdef.rs deparse of `::timestamp(2) without time zone` / `::interval day to second(2)` (used by other tests once the payload exists).
4. pgwire RowDescription type_modifier is hard-wired -1 (crates/pgwire/src/stub.rs L396/408) — wire-exactness only, invisible to psql/pg_regress.

## Wrong detail
- fix_location[2].why says "round half-even on microseconds". PG AdjustTimestampForTypmod / AdjustIntervalForTypmod round HALF AWAY FROM ZERO (TimestampOffsets = scale/2, negated symmetrically). oracle_facts already says half-away-from-zero; the fix note contradicts it.

## Attribution (whole-block recount)
- timestamp: H3 8, H5-H8 32, H9 8 (diff table half; the date_bin 12 are overflow root), H10 268, H11 134, H12 134, H13 134 (16 are the 294270 range root), H15-H18 8 => 726
- timestamptz: H5 10, H7 8, H9 10, H11 8, H12 10, H14 272, H15-H17 408, H18 136 (16 range root), H24/H25/H26/H28 2 each => 870. ~20 of these lines are co-caused by the `'epoch'` -> local-midnight defect (Wed Dec 31 16:00:00 1969 PST expected, Gres Thu Jan 01 00:00:00 1970 PST) and stay after the typmod fix.
- interval: H4 18, H5 73 => 91
- horology: H3 8 (of 16; rest timestamptz() function), H4 8, H6 10 (of 50; rest timestamptz()), H7 10, H9 212, H16-H21 48 => 296
- Total 1983 vs claimed 1980. Within 1%.

## Fail-longer notes
- timestamp/timestamptz H1: after pg_sleep exists, `WHERE d1 = timestamp(2) without time zone 'now'` (expected 2) needs `'now'` = transaction start; datetime.rs clock_now() L972 reads the system clock ("agrees except across a transaction boundary"). Not typmod.
- timestamptz big tables keep the `'epoch'` row diff (2 lines/table) after typmod.
- Planner lines in these blocks: 0.
