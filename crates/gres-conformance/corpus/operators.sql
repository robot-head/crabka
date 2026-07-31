-- Operator surface: POSIX regex matching, integer bitwise operators,
-- exponentiation, modulo, and the prefix operators (absolute value, square
-- root, cube root, bitwise NOT) -- with the precedence that binds them.

-- Regex match: case-sensitive, case-insensitive, and their negations.
SELECT 'abc' ~ 'b';
SELECT 'abc' ~ 'B';
SELECT 'abc' ~* 'B';
SELECT 'abc' !~ 'B';
SELECT 'abc' !~* 'B';
SELECT 'abc' ~ '^a';
SELECT 'abc' ~ 'c$';
SELECT 'abc' !~ '^a';
SELECT 'abc' ~ '(b|z)';
SELECT 'abc' ~ '[[:alpha:]]+';
SELECT 'a1' ~ '^[a-z][0-9]$';
SELECT 'ABC' ~* 'a.c';
SELECT 'a' ~ '';
SELECT 'aaa' ~ 'a{3}';
SELECT NULL ~ 'a';
SELECT 'abc'::text ~ NULL;
SELECT 'abc' ~ 'b' = true;

-- Bitwise operators on integers.
SELECT 5 & 3;
SELECT 5 | 3;
SELECT 5 # 3;
SELECT ~5;
SELECT 1 << 3;
SELECT 16 >> 2;
SELECT 5 & 3, 5 | 3, 5 # 3, ~5, 1 << 3, 16 >> 2;
SELECT 0 & 0, 0 | 0, 0 # 0;
SELECT ~5::int8;
SELECT 5::int8 & 3::int8;
SELECT 5::int8 & 3::int4;
SELECT 1::int8 << 40;

-- Shift counts wrap to the left operand's width; `>>` is arithmetic.
SELECT -1::int4 << 31;
SELECT 1::int4 << 31;
SELECT 1::int4 << 32;
SELECT 1::int4 >> 33;
SELECT (-8)::int4 >> 1;
SELECT (-1)::int4 >> 1;
SELECT 1::int8 << 63;
SELECT 1::int8 << 64;

-- The bitwise operators share one precedence level, left-associative.
SELECT 1 # 2 & 3;
SELECT 1 | 2 # 3;
SELECT 5 & 3 | 8;
SELECT 8 | 4 & 3;
SELECT 1 << 2 + 1;
SELECT 5 & 3 = 1;

-- Exponentiation: float8 for an all-integer pair, left-associative, and
-- binding tighter than `*` but looser than unary minus.
SELECT 2^3;
SELECT 2^0;
SELECT 2^3^2;
SELECT 2 ^ 2 * 3;
SELECT 3 * 2 ^ 2;
SELECT -2^2;
SELECT 2 ^ -2;
SELECT 4 ^ 0.5;
SELECT 0 ^ -1;
SELECT (-2) ^ 0.5;

-- Modulo.
SELECT 4 % 3;
SELECT -7 % 3;
SELECT 7 % -3;
SELECT 10 % 5;
SELECT 5 % 0;
SELECT 1.5::float8 % 2;
SELECT 7 % 3 + 1;
SELECT 1 + 7 % 3;

-- Prefix operators, which bind LOOSELY (the "any other operator" level).
SELECT @ -5;
SELECT @ 5;
SELECT @ -5.5;
SELECT @ -5::int4;
SELECT |/ 16.0;
SELECT |/ 25;
SELECT ||/ 8.0;
SELECT ||/ 125.0;
SELECT ||/ -8.0;
SELECT ||/ 0.0;
SELECT |/ -1;
SELECT @ 5 - 8;
SELECT ~ 5 + 1;
SELECT ~ 5 & 3;
SELECT |/ 16.0 + 2;

-- Operator/type mismatches are 42883, not a wrong answer.
SELECT 2.5 & 1;
SELECT 'abc' ~ 1;

-- `!=` is PostgreSQL's alternative spelling of `<>`.
SELECT 1 != 2;
SELECT 1 != 1;
SELECT 'a' != 'b';
