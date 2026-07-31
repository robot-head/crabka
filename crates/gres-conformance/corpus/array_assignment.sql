-- Subscripted assignment: `SET a[i] = v`, `SET a[lo:hi] = v`, writes past the
-- end (which extend and NULL-fill), writes into a NULL array, and the errors a
-- multidimensional target raises instead of extending.
-- Relations are prefixed `aas_` so no other corpus file collides.

CREATE TABLE aas_t (id int, a int[], b int[]);
INSERT INTO aas_t VALUES (1, '{1,2,3}', NULL), (2, NULL, NULL);

-- In-range element assignment.
UPDATE aas_t SET a[2] = 99 WHERE id = 1;
SELECT a FROM aas_t WHERE id = 1;

-- Past the end: the array extends and the gap is NULL.
UPDATE aas_t SET a[6] = 7 WHERE id = 1;
SELECT a, array_dims(a) FROM aas_t WHERE id = 1;

-- Below the start: the lower bound moves down.
UPDATE aas_t SET a[0] = -1 WHERE id = 1;
SELECT a, array_dims(a) FROM aas_t WHERE id = 1;

-- Into a NULL array: the result is a one-slot array at the written subscript.
UPDATE aas_t SET b[3] = 5 WHERE id = 2;
SELECT b, array_dims(b) FROM aas_t WHERE id = 2;

-- A slice write extends the same way.
UPDATE aas_t SET b[1:2] = '{8,9}' WHERE id = 2;
SELECT b, array_dims(b) FROM aas_t WHERE id = 2;

UPDATE aas_t SET a[2:3] = '{50,60}' WHERE id = 1;
SELECT a, array_dims(a) FROM aas_t WHERE id = 1;

-- A slice source shorter than the slice is an error.
UPDATE aas_t SET a[2:3] = '{1}' WHERE id = 1;
SELECT a FROM aas_t WHERE id = 1;

-- A slice write into a NULL array keeps the written bounds.
UPDATE aas_t SET a = NULL WHERE id = 1;
UPDATE aas_t SET a[2:3] = '{1,2}' WHERE id = 1;
SELECT a, array_dims(a) FROM aas_t WHERE id = 1;

-- Assigning NULL to an element.
UPDATE aas_t SET a = NULL WHERE id = 1;
UPDATE aas_t SET a[2] = NULL WHERE id = 1;
SELECT a, array_dims(a) FROM aas_t WHERE id = 1;

-- A NULL subscript in an assignment is an error, unlike a NULL subscript in a read.
UPDATE aas_t SET a[NULL] = 1 WHERE id = 1;

-- Two subscripted entries for the same column both apply, in order.
UPDATE aas_t SET a = '{1,2,3}' WHERE id = 1;
UPDATE aas_t SET a[1] = 10, a[3] = 30 WHERE id = 1;
SELECT a FROM aas_t WHERE id = 1;

-- The value is coerced to the column's element type.
UPDATE aas_t SET a[1] = 5.0 WHERE id = 1;
SELECT a FROM aas_t WHERE id = 1;
UPDATE aas_t SET a[1] = 'x' WHERE id = 1;

-- An expression subscript and an expression value.
UPDATE aas_t SET a[id + 1] = id * 100 WHERE id = 1;
SELECT a FROM aas_t WHERE id = 1;

-- RETURNING sees the written array.
UPDATE aas_t SET a[1] = 42 WHERE id = 1 RETURNING a;

-- Multidimensional targets: in range writes work, out of range does not extend.
CREATE TABLE aas_m (m int[]);
INSERT INTO aas_m VALUES ('{{1,2},{3,4}}');
UPDATE aas_m SET m[1][2] = 9;
SELECT m FROM aas_m;
UPDATE aas_m SET m[3][1] = 9;
SELECT m FROM aas_m;
UPDATE aas_m SET m[2] = 5;
UPDATE aas_m SET m[1:2][1:1] = '{{7},{8}}';
SELECT m FROM aas_m;
UPDATE aas_m SET m[:1][:2] = '{{20,21}}';
SELECT m FROM aas_m;

-- An array that would exceed PostgreSQL's maximum size is refused, not built.
CREATE TABLE aas_big (pk int, f1 int[]);
INSERT INTO aas_big VALUES (10, '{1,2,3}');
UPDATE aas_big SET f1[2147483647] = 42 WHERE pk = 10;
UPDATE aas_big SET f1[2147483646:2147483647] = ARRAY[4,2] WHERE pk = 10;
SELECT f1 FROM aas_big WHERE pk = 10;

-- Text and other element types assign the same way.
CREATE TABLE aas_txt (t text[], n numeric[], d date[]);
INSERT INTO aas_txt VALUES ('{a,b,c}', '{1.5,2.5}', '{2020-01-01}');
UPDATE aas_txt SET t[2] = 'zz', n[3] = 9.75, d[2] = '2021-02-03';
SELECT t, n, d FROM aas_txt;
UPDATE aas_txt SET t[1:2] = '{p,q}';
SELECT t FROM aas_txt;

DROP TABLE aas_txt;
DROP TABLE aas_big;
DROP TABLE aas_m;
DROP TABLE aas_t;
