-- SP1 smoke corpus: the stub's full surface, plus statements the stub is
-- expected to fail (visible mismatches keep the dashboard honest).
SELECT 1;
-- F-2: `version()` names the PostgreSQL version clients parse out of it, then
-- the build that answered — which differs between the oracle (a Debian package)
-- and the subject (crabka). Only the parsed prefix is engine-independent.
SELECT version() LIKE 'PostgreSQL 18.4 %';
SELECT 2;
SELECT 'hello';
SELECT 1 + 1;

-- EXPLAIN analyses its statement before planning it, so a missing relation or
-- column is the statement's own error rather than a plan for a query that could
-- never run.
CREATE TABLE explain_analysis (a int);
EXPLAIN (COSTS OFF) SELECT a FROM explain_analysis;
EXPLAIN (COSTS OFF) SELECT nosuchcol FROM explain_analysis;
EXPLAIN (COSTS OFF) SELECT * FROM nosuchtable_explain;
EXPLAIN INSERT INTO nosuchtable_explain VALUES (1);
EXPLAIN UPDATE nosuchtable_explain SET a = 1;
EXPLAIN DELETE FROM nosuchtable_explain;
DROP TABLE explain_analysis;
