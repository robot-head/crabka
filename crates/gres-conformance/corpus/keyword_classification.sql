-- PostgreSQL's keyword categories, exercised in the four grammar positions that
-- tell them apart. `ColId` (a table alias, a column-alias-list entry, an INSERT
-- or UPDATE target column) admits the unreserved and col_name keywords and
-- refuses the reserved and type/function-name ones; `BareColLabel` (a select-item
-- alias written without `AS`) is an independent list, so reserved words such as
-- `in` are bare labels while unreserved ones such as `filter` are not; and
-- `ColLabel` (after `AS`) admits every keyword. Quoting a word strips it of all
-- three restrictions.
CREATE TABLE kw (a int4, b text);
INSERT INTO kw VALUES (1, 'x'), (2, 'y');

-- ColId: accepted, in both the AS and the bare spelling.
SELECT count(*) FROM kw AS between;
SELECT count(*) FROM kw between;
SELECT count(*) FROM kw AS exists;
SELECT count(*) FROM kw AS values;
SELECT count(*) FROM kw AS char;
SELECT count(*) FROM kw AS row;
SELECT count(*) FROM kw AS copy;
SELECT count(*) FROM kw copy;
SELECT count(*) FROM kw AS set;
SELECT count(*) FROM kw AS index;
SELECT count(*) FROM kw AS delete;
SELECT count(*) FROM kw AS if;
SELECT count(*) FROM kw AS over;
SELECT count(*) FROM kw over;
SELECT count(*) FROM kw AS filter;
-- ColId: refused, in both spellings.
SELECT count(*) FROM kw AS authorization;
SELECT count(*) FROM kw authorization;
SELECT count(*) FROM kw AS collation;
SELECT count(*) FROM kw AS verbose;
SELECT count(*) FROM kw AS binary;
SELECT count(*) FROM kw AS freeze;
SELECT count(*) FROM kw AS is;
SELECT count(*) FROM kw AS like;
SELECT count(*) FROM kw AS tablesample;
SELECT count(*) FROM kw AS window;
SELECT count(*) FROM kw AS fetch;
SELECT count(*) FROM kw AS lateral;
SELECT count(*) FROM kw AS select;
-- Quoted, every one of them is an ordinary alias again.
SELECT count(*) FROM kw AS "window";
SELECT count(*) FROM kw "tablesample";
SELECT count(*) FROM kw AS "select";

-- BareColLabel: accepted, including the reserved and type/function-name words
-- that are also infix operators — the operator reading needs an operand in sight.
SELECT a is FROM kw ORDER BY 1;
SELECT a like FROM kw ORDER BY 1;
SELECT a ilike FROM kw ORDER BY 1;
SELECT a and FROM kw ORDER BY 1;
SELECT a or FROM kw ORDER BY 1;
SELECT a in FROM kw ORDER BY 1;
SELECT a between FROM kw ORDER BY 1;
SELECT a not FROM kw ORDER BY 1;
SELECT a collate FROM kw ORDER BY 1;
SELECT a similar FROM kw ORDER BY 1;
SELECT a cross FROM kw ORDER BY 1;
SELECT a select FROM kw ORDER BY 1;
SELECT a values FROM kw ORDER BY 1;
-- …and the operator reading is untouched when the operand is there.
SELECT a FROM kw WHERE a IS NOT NULL AND a in (1, 2) ORDER BY 1;
SELECT a FROM kw WHERE b LIKE 'x' OR a BETWEEN 2 AND 3 ORDER BY 1;
-- `ISNULL` / `NOTNULL` are PostgreSQL's postfix spellings of IS [NOT] NULL.
SELECT a isnull FROM kw ORDER BY 1;
SELECT a notnull FROM kw ORDER BY 1;
SELECT count(*) FROM kw WHERE a notnull;
-- BareColLabel: refused, including the unreserved words.
SELECT a over FROM kw;
SELECT a filter FROM kw;
SELECT a window FROM kw;
SELECT a fetch FROM kw;
SELECT a grant FROM kw;
SELECT a char FROM kw;
SELECT a character FROM kw;
SELECT a precision FROM kw;
SELECT a day FROM kw;
SELECT a hour FROM kw;
SELECT a year FROM kw;
SELECT a varying FROM kw;
SELECT a within FROM kw;
SELECT a without FROM kw;
SELECT a overlaps FROM kw;
-- Quoted, they are labels again.
SELECT a "over" FROM kw ORDER BY 1;
SELECT a "year" FROM kw ORDER BY 1;
-- ColLabel takes every keyword, reserved ones included.
SELECT a AS select FROM kw ORDER BY 1;
SELECT a AS from FROM kw ORDER BY 1;
SELECT a AS over FROM kw ORDER BY 1;
SELECT a AS window FROM kw ORDER BY 1;

-- A column-alias list is a list of ColId.
SELECT * FROM (SELECT 1) v(between);
SELECT * FROM (SELECT 1) v(values);
SELECT * FROM (SELECT 1) v(is);
SELECT * FROM (SELECT 1) v(tablesample);
SELECT * FROM kw AS q(between, exists) ORDER BY 1;

-- So are an INSERT target list and an UPDATE SET target.
INSERT INTO kw (values) VALUES (1);
INSERT INTO kw (tablesample) VALUES (1);
INSERT INTO kw (a) VALUES (3);
UPDATE kw SET values = 1;
UPDATE kw SET is = 1;
UPDATE kw SET a = a WHERE a = 3;
DELETE FROM kw WHERE a = 3;
SELECT a, b FROM kw ORDER BY 1;
DROP TABLE kw;
