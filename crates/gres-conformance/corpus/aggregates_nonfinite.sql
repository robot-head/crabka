-- Non-finite inputs to the statistical aggregate family, diffed against
-- PostgreSQL 18.
--
-- PostgreSQL accumulates variance and regression with the Youngs-Cramer
-- algorithm, whose running sums of squared deviations are forced to NaN the
-- moment a NaN or an infinity arrives. Every member of the family therefore
-- answers NaN rather than a variance of zero. The only case that is an error
-- instead is a running sum that overflows while every input feeding it was
-- finite, which is 22003.

-- single non-finite row, float8: population forms are NaN, sample forms NULL
SELECT var_pop('inf'::float8), var_samp('inf'::float8);
SELECT var_pop('-inf'::float8), var_samp('-inf'::float8);
SELECT var_pop('nan'::float8), var_samp('nan'::float8);
SELECT stddev_pop('inf'::float8), stddev_samp('inf'::float8);
SELECT stddev_pop('nan'::float8), stddev_samp('nan'::float8);
SELECT variance('inf'::float8), stddev('inf'::float8);

-- the float4 overloads accumulate in float8 and behave identically
SELECT var_pop('inf'::float4), var_samp('inf'::float4);
SELECT var_pop('nan'::float4), var_samp('nan'::float4);
SELECT stddev_pop('inf'::float4), stddev_samp('inf'::float4);
SELECT stddev_pop('nan'::float4), stddev_samp('nan'::float4);

-- a non-finite row after a finite one, and before one, and mixed signs
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
  FROM (VALUES ('1'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
  FROM (VALUES ('infinity'), ('1')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
  FROM (VALUES ('infinity'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
  FROM (VALUES ('-infinity'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
  FROM (VALUES ('-infinity'), ('-infinity')) v(x);
SELECT var_pop(x::float8), var_samp(x::float8), stddev_pop(x::float8)
  FROM (VALUES ('nan'), ('1'), ('2')) v(x);

-- the two-variable family: only the sums the non-finite argument feeds go NaN
SELECT covar_pop(1::float8, 'inf'::float8), covar_samp(3::float8, 'inf'::float8);
SELECT covar_pop(1::float8, 'nan'::float8), covar_samp(3::float8, 'nan'::float8);
SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x),
       regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x),
       regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x),
       regr_avgx(y, x), regr_avgy(y, x), regr_count(y, x)
  FROM (VALUES (1::float8, 'inf'::float8), (2, 3)) v(y, x);
SELECT corr(y, x), covar_pop(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x)
  FROM (VALUES ('nan'::float8, 1::float8), (2, 3)) v(y, x);
SELECT corr(y, x), covar_pop(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x)
  FROM (VALUES ('-inf'::float8, 'inf'::float8), (2, 3)) v(y, x);

-- the exact numeric accumulators short-circuit on a special the same way
SELECT var_pop(x), var_samp(x), stddev_pop(x), stddev_samp(x)
  FROM (VALUES ('nan'::numeric), (1)) v(x);
SELECT var_pop(x), var_samp(x) FROM (VALUES ('inf'::numeric), (1)) v(x);
SELECT var_pop(x), var_samp(x) FROM (VALUES ('-inf'::numeric), ('inf'::numeric)) v(x);

-- finite inputs whose running sums overflow are 22003, not NaN
SELECT var_pop(x::float8) FROM (VALUES (1e308), (-1e308), (1e308)) v(x);
SELECT var_pop(x::float8), var_samp(x::float8) FROM (VALUES (1e308), (1e308)) v(x);
SELECT regr_sxx(y, x) FROM (VALUES (1e308::float8, 1e308::float8), (-1e308, -1e308)) v(y, x);

-- the finite path is unchanged
SELECT var_pop(x::float8), var_samp(x::float8), stddev_pop(x::float8), stddev_samp(x::float8)
  FROM (VALUES (1), (2), (3), (4)) v(x);
SELECT var_pop(x::float8), var_samp(x::float8) FROM (VALUES (2), (2)) v(x);
SELECT corr(y, x), covar_pop(y, x), regr_r2(y, x)
  FROM (VALUES (1::float8, 1::float8), (2, 2), (3, 3)) v(y, x);
