-- `string_agg`, the boolean and bitwise aggregates, and the statistical family
-- (single- and two-variable), diffed against PostgreSQL 18.
--
-- The `numeric` variance/stddev results exercise PostgreSQL's exact
-- `numeric_stddev_internal` display scale, and the `float8` ones its
-- Youngs-Cramer transition, so the printed digits are the point of the test.
CREATE TABLE ax (id int4, s text, b bool, i4 int4, i8 int8, f8 float8, q numeric, y float8, x float8);
INSERT INTO ax VALUES
  (1, 'a', true, 3, 3, 1, 1, 1, 1),
  (2, 'b', false, 5, 5, 2, 2, 2, 2),
  (3, NULL, NULL, NULL, NULL, 3, 3, 3, 4),
  (4, 'c', true, 6, 6, 4, 4, NULL, 5);

-- string_agg
SELECT string_agg(s, ',') FROM ax;
SELECT string_agg(s, '') FROM ax;
SELECT string_agg(s, ',') FROM ax WHERE false;
SELECT string_agg(DISTINCT s, ',') FROM ax;
SELECT b, string_agg(s, '|') FROM ax GROUP BY b ORDER BY b;
SELECT string_agg(s, ',') FROM ax WHERE s IS NULL;

-- boolean aggregates
SELECT bool_and(b), bool_or(b), every(b) FROM ax;
SELECT bool_and(b), bool_or(b) FROM ax WHERE id = 1;
SELECT bool_and(b), bool_or(b) FROM ax WHERE id = 3;
SELECT bool_and(b), bool_or(b) FROM ax WHERE false;
SELECT b, bool_and(b), bool_or(b) FROM ax GROUP BY b ORDER BY b;

-- bitwise aggregates
SELECT bit_and(i4), bit_or(i4), bit_xor(i4) FROM ax;
SELECT bit_and(i8), bit_or(i8), bit_xor(i8) FROM ax;
SELECT bit_and(i4) FROM ax WHERE false;
SELECT bit_xor(i4) FROM ax WHERE id = 1;

-- single-variable statistics over integer, numeric and float8 inputs
SELECT var_pop(i4), var_samp(i4), variance(i4) FROM ax;
SELECT stddev(i4), stddev_pop(i4), stddev_samp(i4) FROM ax;
SELECT var_pop(q), var_samp(q), stddev(q), stddev_pop(q) FROM ax;
SELECT var_pop(f8), var_samp(f8), variance(f8), stddev(f8), stddev_pop(f8), stddev_samp(f8) FROM ax;
SELECT var_pop(i4), var_samp(i4) FROM ax WHERE id = 1;
SELECT var_pop(i4), var_samp(i4), stddev(i4) FROM ax WHERE false;
SELECT var_pop(x), var_samp(x), stddev_pop(x), stddev_samp(x) FROM (VALUES (1), (2), (3), (4)) v(x);
SELECT var_pop(x), stddev_pop(x) FROM (VALUES (1.0000), (1.0001)) v(x);
SELECT var_pop(x), stddev_pop(x) FROM (VALUES (100000), (200000), (300000)) v(x);
SELECT b, var_pop(i4) FROM ax GROUP BY b ORDER BY b;

-- two-variable statistics
SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM ax;
SELECT regr_count(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM ax;
SELECT regr_avgx(y, x), regr_avgy(y, x), regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM ax;
SELECT corr(y, x), covar_samp(y, x), regr_slope(y, x) FROM ax WHERE id = 1;
SELECT regr_count(y, x), corr(y, x) FROM ax WHERE false;
SELECT regr_r2(y, x), regr_slope(y, x) FROM (VALUES (1, 1), (2, 1), (3, 1)) v(y, x);
SELECT corr(y, x) FROM (VALUES (1, 1), (2, 2), (3, 3)) v(y, x);

-- the aggregates compose with grouping, HAVING and scalar wrappers
SELECT b, count(*), round(var_pop(q), 4) FROM ax GROUP BY b HAVING count(*) > 1 ORDER BY b;
SELECT upper(string_agg(s, ',')) FROM ax;
DROP TABLE ax;
