-- Number theory (gcd/lcm/factorial/div), the numeric scale trio, width_bucket,
-- the trigonometric / hyperbolic / logarithmic family, and the pseudo-random
-- pair, diffed against PostgreSQL 18.
--
-- Float8 results are restricted to values every libm agrees on, or are pinned
-- through `round(x::numeric, n)`, so the text diff is deterministic. `random()`
-- is asserted by range and type rather than by value: crabka's generator is
-- xoroshiro128** like PostgreSQL's but does not reproduce its seeded stream.
CREATE TABLE mx (id int4, i4 int4, i8 int8, q numeric, f float8);
INSERT INTO mx VALUES (1, 12, 12, 1.230, 2.0), (2, -8, -8, 100.00, 0.5), (3, 0, 0, 0.5, 9.0);

-- gcd / lcm over the integer widths and numeric
SELECT gcd(8, 12), gcd(0, 0), gcd(-4, 6), gcd(4, -6), gcd(0, 5), gcd(5, 0);
SELECT gcd(8::int8, 12::int8), lcm(4::int8, 6::int8);
SELECT lcm(4, 6), lcm(0, 5), lcm(-4, 6), lcm(7, 7);
SELECT gcd(1.5, 2.5), lcm(1.5, 2.5), gcd(0.75, 0.5);
SELECT id, gcd(i4, 6), lcm(i4, 6) FROM mx ORDER BY id;
SELECT gcd((-2147483648)::int4, 0);
SELECT lcm(2147483647, 2147483646);

-- factorial / div
SELECT factorial(0), factorial(1), factorial(5), factorial(20);
SELECT factorial(-1);
SELECT div(9, 4), div(-9, 4), div(9, -4), div(9.5, 4.1), div(9.9, 3.3), div(-9.9, 3.3);
SELECT div(9, 0);

-- numeric scale introspection
SELECT scale(1.230), scale(1), scale(0.000);
SELECT min_scale(1.230), min_scale(1.200), min_scale(0), min_scale(1.0);
SELECT trim_scale(1.230), trim_scale(1.000), trim_scale(0.000), trim_scale(100);
SELECT id, scale(q), min_scale(q), trim_scale(q) FROM mx ORDER BY id;

-- width_bucket, ascending and descending bounds
SELECT width_bucket(5.0, 1.0, 10.0, 3), width_bucket(0.0, 1.0, 10.0, 3), width_bucket(20.0, 1.0, 10.0, 3);
SELECT width_bucket(2.5, 1, 10, 3), width_bucket(1.0, 10.0, 1.0, 3), width_bucket(5, 10, 1, 3);
SELECT width_bucket(5.0::float8, 1.0::float8, 10.0::float8, 3), width_bucket(11::float8, 10::float8, 1::float8, 3);
SELECT width_bucket(1.0, 1.0, 1.0, 3);
SELECT width_bucket(1.0, 1.0, 10.0, 0);

-- radian trigonometry (exact values only)
SELECT sin(0), cos(0), tan(0), cot(1), asin(0), acos(1), atan(0), atan2(1, 1);
SELECT asin(2);
SELECT sin('Infinity'::float8);
SELECT degrees(1), radians(180), degrees(0), radians(0);

-- degree trigonometry: PostgreSQL guarantees exact answers at the quadrant marks
SELECT sind(0), sind(30), sind(45), sind(90), sind(180), sind(270), sind(360);
SELECT cosd(0), cosd(30), cosd(60), cosd(90), cosd(180), cosd(270);
SELECT tand(0), tand(30), tand(45), tand(90), tand(135), cotd(45);
SELECT asind(0), asind(0.5), asind(1), asind(-1), asind(-0.5), asind(0.8);
SELECT acosd(0), acosd(0.5), acosd(1), acosd(-1), acosd(-0.5), acosd(0.8);
SELECT atand(1), atand(0.5), atan2d(1, 1), atan2d(1, 0), atan2d(0, 1), atan2d(-1, 0);
SELECT asind(2);

-- hyperbolic
SELECT sinh(0), cosh(0), tanh(0), asinh(0), acosh(1), atanh(0);
SELECT sinh(1), cosh(1), tanh(1), asinh(1);
SELECT acosh(0.5);
SELECT atanh(1);
SELECT atanh(2);

-- roots and logarithms
SELECT cbrt(1), cbrt(8), cbrt(27), cbrt(-27), cbrt(64), cbrt(125), cbrt(1000), cbrt(0.001);
SELECT cbrt(2), cbrt(3), cbrt(5), cbrt(10), cbrt(100);
SELECT log10(100), log10(100.0), log10(100::float8), log10(1);
SELECT log10(0);
SELECT id, cbrt(f), log10(f * 100) FROM mx WHERE f = 9.0 ORDER BY id;

-- random / setseed: range and type, not a specific draw
SELECT random() >= 0 AND random() < 1;
SELECT random(1, 10) BETWEEN 1 AND 10, random(5, 5), random(-3, -3);
SELECT random(1000000000000::int8, 1000000000000::int8);
SELECT random(2.50, 2.50);
SELECT setseed(0.5);
SELECT setseed(2);
SELECT random(10, 1);

-- NULL propagation across the whole family
SELECT gcd(NULL::int4, 1), factorial(NULL), div(NULL, 1), scale(NULL::numeric), trim_scale(NULL::numeric);
SELECT sind(NULL), cbrt(NULL), log10(NULL), atan2(NULL, 1), width_bucket(NULL, 1, 10, 3);
DROP TABLE mx;
