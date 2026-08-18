# Verification: tsfr-ts-dictionaries (tsdicts)

Verdict: CONFIRMED with small corrections. Attribution 594 exact.

## 1. Root cause
- tsdicts.diff first hunk @@ -5: `CREATE TEXT SEARCH DICTIONARY ispell (Template=ispell, ...)` -> Gres
  `ERROR: text search template "ispell" does not exist`. Decided in
  crates/pgexec/src/text_search_catalog.rs `execute`, Create/Dictionary arm lines 118-126:
  base must be an existing dictionary or `simple|snowball`. Nothing fails before line 5 (lines 1-4 are comments + the CREATE head).
- Cascade: every ts_lexize afterwards -> `dictionary_template` (line 250) returns 42704. All 15 CREATE ispell/hunspell*/synonym/thesaurus/tsdict_case fail the same way.
- Config sections (hunks @@ -527..-662): Gres output is english_stem-only because
  parser.rs `alter_text_search` (line 6579) skips everything after the name that is not RENAME TO
  (bump loop), and `execute` Alter arm returns no ops. `normalized_terms`/`normalize_query_inner`
  (text_search_fn.rs 1026/498) only ask `config_is_simple`. So these 126 lines need BOTH this root
  and tsfr-ts-config-mapping-ddl.

## 2. Fix locations
- text_search_catalog.rs: exists; `DictionaryTemplate` has only Simple|Snowball (line 236); the
  stored KV value is just the template/base string (line 133-136) — must become template + option list.
- text_search_fn.rs `lexize` (903), `normalized_terms` (1026), `normalize_query_inner` (498): exist;
  hard-code fold + rust-stemmers English + inline `is_stopword` list.
- parser.rs `create_text_search` (6534) keeps only Template; `alter_text_search` (6579) skips options.
  Also `def_arg_name` (439) accepts only col_label / qualified name — must widen to NumericOnly and Sconst
  for `CaseSensitive = 1` / `= 2` (needed once ALTER options are parsed). Analyst did not name this.
- MISSED: crates/pgexec/src/exec.rs `text_search_catalog_rows` (line 21710) renders `dictinitoption`
  as the template base string; must render PG serialize_deflist output (`synonyms = 'synonym_sample', casesensitive = 1`).
  21 changed lines (3 x 7) in hunk 1 land there.
- Overstated: "snowball.rs" is not needed — rust-stemmers is already a dependency (Cargo.toml:145) and used.

## 3. Attribution (whole-block rule)
Per hunk: 459, 7, 18, 66, 24, 18, 2, 5 = 599 (file_stats says 599). Last hunk (5 lines: token type /
DROP MAPPING errors) belongs to config-mapping DDL. This root: 594 = 468 dictionary-only
(hunks 1,2,7) + 126 config-in-use (hunks 3-6, joint with mapping DDL). Analyst 594: exact.

## 4. Dependencies
- tsfr-ts-config-mapping-ddl: hard, for the 126 config lines (mapping storage, ADD/ALTER/REPLACE/DROP MAPPING,
  dictionary-existence validation). Also: if mapping DDL is fixed BEFORE dictionaries, the four
  ALTER MAPPING ... WITH ispell statements will start erroring (dictionary does not exist) — new lines.
- tsfr-ts-default-parser: soft for tsdicts. All inputs are ASCII words; Gres `words()` yields the same
  positions. What is needed is a token-type classifier so a mapping "FOR asciiword" applies; the full
  parser is not required by this file.
- Parser: def_arg widening; CREATE option list preserved verbatim with quoted-case.
- Storage: KV value format for catalog/text-search/d/<name> changes; not under pgcatalog SCHEMA_VERSION.
- pgtypes TsQuery: multi-lexeme output requires building And/Or subtrees from a single term (pushval_morph),
  and TsVector needs several lexemes at one position (already supported by per-lexeme positions).

## 5. Oracle facts
All checked against self-check-serial/results/tsdicts.out: lines 422/428/449/494/689 error strings; 490/505
dictinitoption. One transcription nit: the flag error is `invalid affix flag "SZ\"` (one backslash), the
JSON in the claim decodes to two.
- Compat matrix line 108: "parser/template DDL remains a C-bound non-goal" and rows 216 restrict TEMPLATE to
  simple|snowball, so this is closer to a matrix non-goal than the analyst's `false` suggests.
- "tsdict_case test" is a dictionary name inside tsdicts (line 683), not a separate test file.
