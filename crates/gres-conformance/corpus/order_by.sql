-- SP39: plain SELECT ORDER BY parity, diffed against PostgreSQL 18.
CREATE TABLE ob (a int4, b int4, name text);
INSERT INTO ob VALUES (1,20,'a'),(2,10,'b'),(3,30,'c'),(1,40,'d');

SELECT name FROM ob ORDER BY 1 DESC;
SELECT a AS b FROM ob ORDER BY b;
SELECT a AS b FROM ob ORDER BY ob.b;
SELECT a AS b FROM ob ORDER BY b + 0;
SELECT a, a FROM ob ORDER BY a;
SELECT a AS x, a AS x FROM ob ORDER BY x;
SELECT DISTINCT a AS x FROM ob ORDER BY x DESC;
SELECT DISTINCT a AS x FROM ob ORDER BY 1 DESC;
SELECT DISTINCT a FROM ob ORDER BY ob.a DESC;
SELECT a, count(*) AS c FROM ob GROUP BY a ORDER BY c DESC, a;
SELECT a, count(*) AS c FROM ob GROUP BY a ORDER BY 2 DESC, 1;
SELECT a FROM ob ORDER BY 0;
SELECT a FROM ob ORDER BY 9;
SELECT a FROM ob ORDER BY 2147483648;
SELECT a FROM ob ORDER BY 999999999999999999999999999;
SELECT a AS x, b AS x FROM ob ORDER BY x;
SELECT a AS b, b FROM ob ORDER BY b;
SELECT DISTINCT a FROM ob ORDER BY b;

-- A bare constant is an output position, and `-` folds into it the way
-- PostgreSQL's doNegate() does — so `ORDER BY -1` is position -1 (42P10), not a
-- constant expression that silently drops the sort. Any other bare constant is
-- 42601. Unary `+` is an operator, not a sign, so `+1` IS a constant expression.
SELECT a FROM ob ORDER BY -1;
SELECT a, b FROM ob ORDER BY -1, 1;
SELECT a FROM ob ORDER BY -0;
SELECT a FROM ob ORDER BY 1.0;
SELECT a FROM ob ORDER BY 2.0;
SELECT a FROM ob ORDER BY 1e0;
SELECT a FROM ob ORDER BY 'x';
SELECT a FROM ob ORDER BY true;
SELECT a FROM ob ORDER BY NULL;
SELECT a FROM ob ORDER BY 3000000000;
SELECT a FROM ob ORDER BY (1), 2;
SELECT a, b FROM ob ORDER BY +1, 1, 2;
SELECT a FROM ob GROUP BY -1;
SELECT a FROM ob GROUP BY 1.0;
SELECT a FROM ob GROUP BY 'x';
SELECT a, count(*) FROM ob GROUP BY 1 ORDER BY 1;
SELECT a FROM ob UNION SELECT b FROM ob ORDER BY -1;
SELECT a FROM ob UNION SELECT b FROM ob ORDER BY 1.0;
SELECT a FROM ob UNION SELECT b FROM ob ORDER BY 1;
VALUES (3), (1), (2) ORDER BY -1;
VALUES (3), (1), (2) ORDER BY 1.0;
VALUES (3), (1), (2) ORDER BY 1;
SELECT +1;
SELECT +1.5;
SELECT +(-2);
SELECT + 3 * 2;
SELECT +'x'::text;
SELECT +true;
SELECT +NULL::int4;

-- `ORDER BY ... USING <op>` takes its direction, and so its NULL placement, from
-- the ordering operator; a non-ordering operator is 42809.
SELECT a FROM ob ORDER BY a USING <;
SELECT a FROM ob ORDER BY a USING >;
SELECT b FROM ob ORDER BY b USING > NULLS LAST;
SELECT b FROM ob ORDER BY b USING < NULLS FIRST;
SELECT name FROM ob ORDER BY name USING >, name;
SELECT a FROM ob ORDER BY a USING <=;
SELECT a FROM ob ORDER BY a USING =;

-- `FOR READ ONLY` locks nothing and is accepted as a no-op.
SELECT a FROM ob ORDER BY a FOR READ ONLY;
SELECT a FROM ob ORDER BY a LIMIT 2 FOR READ ONLY;

-- LIMIT/OFFSET coerce to bigint by assignment, so a type with no such cast is
-- 42804 naming it — an untyped literal still resolves.
SELECT a FROM ob LIMIT true;
SELECT a FROM ob OFFSET true;
SELECT a FROM ob LIMIT '2'::text;
SELECT a FROM ob LIMIT '1 day'::interval;
SELECT a FROM ob ORDER BY a LIMIT '2';
SELECT a FROM ob ORDER BY a LIMIT 1.7;
SELECT a FROM ob ORDER BY a LIMIT 2.0::numeric;

-- A base-table alias may carry a column list, exactly like a derived table's.
SELECT * FROM ob AS q(x) ORDER BY q.x, 2;
SELECT * FROM ob q(x, y) ORDER BY x, y;
SELECT x FROM ob AS q(x, y, z) WHERE z = 'a';
SELECT * FROM ob AS q(w, x, y, z);
SELECT * FROM ob AS q(x) TABLESAMPLE SYSTEM (100) ORDER BY 1, 2;

-- COLLATE is a postfix operator on the collated types. This engine has exactly
-- the collations its pg_collation reports — `default`, `C` and `POSIX`, all of
-- which order text by byte value — so those spellings behave as in PostgreSQL.
SELECT name FROM ob ORDER BY name COLLATE "C";
SELECT name FROM ob ORDER BY name COLLATE "POSIX";
SELECT name COLLATE "C" FROM ob ORDER BY 1;
SELECT name FROM ob WHERE name COLLATE "C" = 'a';
SELECT name COLLATE "C" || 'x' FROM ob ORDER BY 1;
SELECT DISTINCT name COLLATE "C" FROM ob ORDER BY 1;
-- COLLATE on a non-collatable operand is 42804, naming the type.
SELECT a COLLATE "C" FROM ob;
SELECT a FROM ob ORDER BY a COLLATE "C";
-- `collate` is a bare_label_keyword, so with no name after it it is a label.
SELECT a collate FROM ob ORDER BY 1;

-- A set-returning function in ORDER BY is a junk target-list entry in
-- PostgreSQL, so it multiplies the output rows exactly as a select-list call
-- would.
SELECT a FROM ob ORDER BY a, generate_series(1, 2);
SELECT a FROM ob ORDER BY generate_series(1, 3), a;
SELECT a, b FROM ob ORDER BY a, generate_series(1, 2) DESC, b;
SELECT a FROM ob ORDER BY a, unnest(ARRAY[1, 2]);
SELECT a FROM ob ORDER BY a, generate_series(1, 2) LIMIT 3;
