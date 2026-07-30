
-- CREATE OR REPLACE VIEW. A replacement query may APPEND output columns but may
-- not drop, rename, or retype an existing one; the count is checked first, then
-- each existing column in POSITION order has its name checked and then its type,
-- and the first offending column decides the error. A column label looks through
-- a cast and a COLLATE, as PostgreSQL's FigureColname does.
CREATE TABLE orv (a int, b int, c text, d text);
INSERT INTO orv VALUES (1,2,'x','y');
CREATE VIEW orv1 AS SELECT a, b FROM orv;
CREATE OR REPLACE VIEW orv1 AS SELECT a, b FROM orv;
CREATE OR REPLACE VIEW orv1 AS SELECT a, b, c FROM orv;
SELECT a, b, c FROM orv1;
CREATE OR REPLACE VIEW orv1 AS SELECT a FROM orv;
CREATE OR REPLACE VIEW orv1 AS SELECT b, a, c FROM orv;
CREATE OR REPLACE VIEW orv1 AS SELECT a, b::numeric, c FROM orv;
CREATE OR REPLACE VIEW orv1 AS SELECT a AS z, b, c FROM orv;
-- One column changing type and a LATER one changing name reports the TYPE error.
CREATE VIEW orv2 AS SELECT a, b, c, d FROM orv;
CREATE OR REPLACE VIEW orv2 AS SELECT a::numeric AS a, b AS bb, c, d FROM orv;
-- OR REPLACE over a name that is not a view is still 42P07.
CREATE OR REPLACE VIEW orv AS SELECT a FROM orv;
-- OR REPLACE where nothing exists yet simply creates it.
CREATE OR REPLACE VIEW orv3 AS SELECT a FROM orv;
SELECT a FROM orv3;
-- The positional column alias list, on create and on replace.
CREATE VIEW orv4 (x, y) AS SELECT a, b FROM orv;
SELECT x, y FROM orv4;
CREATE OR REPLACE VIEW orv4 (x, y, z) AS SELECT a, b, c FROM orv;
SELECT x, y, z FROM orv4;
CREATE OR REPLACE VIEW orv4 (q, y, z) AS SELECT a, b, c FROM orv;
-- A label looks through a cast and a COLLATE.
SELECT a::numeric, (a), -a, a+1 FROM orv;
DROP VIEW orv1;
DROP VIEW orv2;
DROP VIEW orv3;
DROP VIEW orv4;
DROP TABLE orv;
