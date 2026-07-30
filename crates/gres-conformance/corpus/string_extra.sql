-- `format`, the digest and binary-encoding families, SQL quoting, the search
-- and rewrite utilities, and the Unicode surface, diffed against PostgreSQL 18.
--
-- `bytea` values are built with `decode(...)` rather than a `'\x..'::bytea`
-- cast, which crabka's cast table does not yet carry.
CREATE TABLE sx (id int4, s text, n int4);
INSERT INTO sx VALUES (1, 'hello world', 255), (2, 'a,b,c', 16), (3, NULL, 0);

-- format(): the four conversions, positional selectors, and the width field
SELECT format('%s and %s', 'a', 1);
SELECT format('%I', 'foo bar'), format('%L', 'a''b'), format('%L', NULL::text), format('%%');
SELECT format('%2$s %1$s', 'a', 'b'), format('%1$s %1$s', 'a');
SELECT format('%s', NULL), format('hello'), format(NULL);
SELECT format('[%10s]', 'a'), format('[%-10s]', 'a'), format('[%5L]', 'a');
SELECT format('%I', 'foo'), format('%I', 'select'), format('%I', 'Foo'), format('%I', 'a"b');
SELECT id, format('%s=%I', id, s) FROM sx WHERE s IS NOT NULL ORDER BY id;
SELECT format('%s %s', 'a');
SELECT format('%z', 'a');
SELECT format('%I', NULL);
SELECT format('%0$s', 'a');
SELECT format('%');

-- message digests
SELECT md5('abc'), md5(''), md5('hello world');
SELECT sha224('abc'), sha256('abc'), sha384('abc'), sha512('abc');
SELECT encode(sha256('abc'), 'hex'), encode(sha224(''), 'hex');
SELECT md5(NULL), sha256(NULL);
SELECT id, md5(s) FROM sx WHERE s IS NOT NULL ORDER BY id;

-- encode / decode
SELECT encode(decode('616263', 'hex'), 'hex'), encode(decode('616263', 'hex'), 'base64'), encode(decode('616263', 'hex'), 'escape');
SELECT encode(decode('00ff41', 'hex'), 'escape'), encode(decode('00ff41', 'hex'), 'base64');
SELECT decode('616263', 'hex'), decode('YWJj', 'base64'), decode('abc', 'escape'), decode('\\000', 'escape');
SELECT encode(decode('', 'hex'), 'hex'), decode('', 'base64');
SELECT encode(decode('61', 'hex'), 'nope');
SELECT decode('zz', 'hex');
SELECT to_hex(255), to_hex(255::int8), to_hex(-1), to_hex((-1)::int8), to_hex(0);
SELECT id, to_hex(n) FROM sx ORDER BY id;

-- SQL quoting
SELECT quote_ident('foo'), quote_ident('foo bar'), quote_ident('Foo'), quote_ident('a"b'), quote_ident('select'), quote_ident('value');
SELECT quote_ident('_x'), quote_ident('x1'), quote_ident('1x'), quote_ident('');
SELECT quote_literal('a''b'), quote_literal(E'a\\b'), quote_literal(NULL), quote_literal(42);
SELECT quote_nullable('a'), quote_nullable(NULL), quote_nullable(42);

-- search and rewrite
SELECT split_part('a,b,c', ',', 2), split_part('a,b,c', ',', -1), split_part('a,b,c', ',', 9), split_part('a,b,c', ',', -9), split_part('abc', '', 1);
SELECT id, split_part(s, ',', 2) FROM sx WHERE s IS NOT NULL ORDER BY id;
SELECT split_part('a,b', ',', 0);
SELECT translate('abcdef', 'abc', 'xy'), translate('abc', '', ''), translate('12345', '143', 'ax');
SELECT starts_with('abc', 'ab'), starts_with('abc', 'b'), starts_with('', ''), starts_with('abc', '');
SELECT concat(), concat_ws('-');
SELECT concat('a'), concat(1, 2, NULL, 'a'), concat(true, false);
SELECT concat_ws('-', 'a', 'b'), concat_ws('-', 1, NULL, 'a', true), concat_ws(NULL, 1, 2), concat_ws('-', NULL, NULL);
SELECT octet_length('abc'), bit_length('abc'), octet_length('hello world');

-- Unicode
SELECT unistr('d\0061t\+000061'), unistr('\0441\043B\043E\043D'), unistr('a\\b');
SELECT unistr('bad\');
SELECT parse_ident('a.b'), parse_ident('"A".b'), parse_ident('a.b.c.d'), parse_ident('  x . y  ');
SELECT parse_ident('1abc');
SELECT parse_ident('a.b[]', true);
SELECT normalize('abc'), is_normalized('abc'), is_normalized('a', 'NFC'), is_normalized('a', 'NFD');
SELECT is_normalized('a', 'NFZ');
SELECT to_ascii('abc');

-- type introspection
SELECT pg_typeof(1), pg_typeof(1::int8), pg_typeof('a'), pg_typeof('a'::text), pg_typeof(1.5::float8), pg_typeof(true), pg_typeof(NULL);
SELECT pg_typeof(now()), pg_typeof(current_date), pg_typeof('{1}'::int[]), pg_typeof('{}'::jsonb), pg_typeof(1 + 1), pg_typeof('a' || 'b');
SELECT format_type(23, -1), format_type(1043, 10), format_type(1700, 655367), format_type(25, -1), format_type(1184, -1), format_type(0, -1);
SELECT format_type(23, NULL), format_type(NULL, NULL), format_type(1042, 14), format_type(1015, 10), format_type(1231, 655367);
SELECT format_type(16, -1), format_type(21, -1), format_type(20, -1), format_type(700, -1), format_type(701, -1), format_type(2950, -1), format_type(3802, -1);
SELECT pg_input_is_valid('1', 'integer'), pg_input_is_valid('x', 'integer'), pg_input_is_valid('1.5', 'numeric'), pg_input_is_valid('2024-01-01', 'date'), pg_input_is_valid('zz', 'date');
SELECT pg_input_is_valid('1', 'nosuchtype');

-- conditional / comparison resolution over unknown literals
SELECT coalesce(1, '2'), coalesce(NULL, '2'), coalesce('a', 'b'), coalesce(1, 1.5), coalesce(1::int8, 2);
SELECT coalesce(1, 'x');
SELECT coalesce(1, 'x'::text);
SELECT greatest(1, '2'), least('2', 1), greatest(1, 2.5), least(1, 2::int8), greatest(NULL, 1);
SELECT greatest(1, 'a');
SELECT nullif(1, '1'), nullif('1', 1), nullif(1, 2), nullif(NULL, 1);
SELECT nullif(1, 'a');
SELECT 'flag=' || (1 = 1), 'x' || true, 'x' || false, concat(true), true::text;
DROP TABLE sx;

-- The SQL-standard call forms that spell their arguments with keywords rather
-- than commas: SUBSTRING's FROM/FOR and pattern spellings, TRIM's side
-- keywords, POSITION's IN, and OVERLAY's PLACING/FROM/FOR.
SELECT substring('abcdef' FROM 2 FOR 3);
SELECT substring('abcdef' FROM 2);
SELECT substring('abcdef' FOR 3);
SELECT substring('abcdef' FROM 0 FOR 3);
SELECT substring('abcdef' FROM -1 FOR 3);
SELECT substring('abcdef' FROM 2 FOR -1);
SELECT substring('abcdef' FROM 'b.d');
SELECT substring('abcdef' FROM '(b)(.d)');
SELECT substring('abcdef' FROM 'x');
SELECT substring('abcdef' SIMILAR '%#"b_d#"%' ESCAPE '#');
SELECT trim(' x ');
SELECT trim(both from ' x ');
SELECT trim(leading 'x' from 'xxa');
SELECT trim(trailing 'x' from 'axx');
SELECT trim(both 'x' from 'xxaxx');
SELECT trim('x' from 'xxaxx');
SELECT trim(leading from '  xxa');
SELECT position('b' in 'abc');
SELECT position('z' in 'abc');
SELECT position('' in 'abc');
SELECT overlay('abcdef' placing 'ZZ' from 2 for 3);
SELECT overlay('abcdef' placing 'ZZ' from 2);
SELECT overlay('abcdef' placing 'ZZ' from 2 for 0);
SELECT overlay('abcdef' placing '' from 2 for 2);
SELECT overlay('abcdef' placing 'ZZ' from 0);
