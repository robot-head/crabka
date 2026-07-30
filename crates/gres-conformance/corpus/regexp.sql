-- The scalar `regexp_*` family, diffed against PostgreSQL 18.
--
-- Patterns stay inside the operator set POSIX AREs and the RE2 family agree on
-- (literals, classes, alternation, quantifiers, anchors); back-references and
-- look-around inside a pattern are a documented dialect divergence and are not
-- exercised here.
CREATE TABLE rx (id int4, s text);
INSERT INTO rx VALUES (1, 'foobarbaz'), (2, 'a1b22c'), (3, 'AbcAbc'), (4, NULL);

-- regexp_replace: first match, global, capture groups, case folding
SELECT regexp_replace('foobarbaz', 'b..', 'X'), regexp_replace('foobarbaz', 'b..', 'X', 'g');
SELECT regexp_replace('foobarbaz', 'b(..)', '[\1]', 'g'), regexp_replace('foobarbaz', 'b(..)', 'X\1Y');
SELECT regexp_replace('ABC', 'b', 'x', 'i'), regexp_replace('a1b2', '[0-9]', '#', 'g'), regexp_replace('AbcAbc', 'a', 'X', 'gi');
SELECT regexp_replace(NULL, 'a', 'b'), regexp_replace('a.b', '.', 'X'), regexp_replace('abc', '(a)(b)', '\2\1');
SELECT regexp_replace('abcabc', 'b', 'X', 1, 2), regexp_replace('abcabc', 'b', 'X', 1, 0), regexp_replace('abcabc', 'b', 'X', 4);
SELECT regexp_replace('abc', 'b', '[\&]'), regexp_replace('abc', 'b', '\\');
SELECT id, regexp_replace(s, '[0-9]+', '#', 'g') FROM rx WHERE s IS NOT NULL ORDER BY id;
SELECT regexp_replace('abc', 'b', 'x', 0);
SELECT regexp_replace('abc', 'b', 'x', 1, -1);

-- regexp_count
SELECT regexp_count('aaa', 'a'), regexp_count('abcabc', 'bc'), regexp_count('ABAB', 'a', 1, 'i'), regexp_count('aaa', 'a', 2);
SELECT regexp_count('abcabcabc', 'bc', 4), regexp_count('', 'a'), regexp_count('aaa', '');
SELECT id, regexp_count(s, '[ab]') FROM rx WHERE s IS NOT NULL ORDER BY id;
SELECT regexp_count('a', 'a', 0);
SELECT regexp_count('a', 'a', 1, 'g');

-- regexp_instr
SELECT regexp_instr('abcdef', 'cd'), regexp_instr('abcabc', 'bc', 1, 2), regexp_instr('abc', 'x'), regexp_instr('abcdef', 'cd', 1, 1, 1);
SELECT regexp_instr('abcabc', 'b', 1, 2), regexp_instr('abcabc', 'b', 1, 2, 1), regexp_instr('abcabc', '(b)(c)', 1, 1, 0, '', 2);
SELECT regexp_instr('abc', 'x', 0);
SELECT regexp_instr('abc', 'b', 1, 1, 2);

-- regexp_like
SELECT regexp_like('abc', 'b'), regexp_like('ABC', 'b', 'i'), regexp_like('abc', 'x'), regexp_like(NULL, 'a');
SELECT id, regexp_like(s, '^a') FROM rx WHERE s IS NOT NULL ORDER BY id;
SELECT regexp_like('a', 'a', 'g');

-- regexp_substr
SELECT regexp_substr('abcdef', 'c.e'), regexp_substr('abc', 'x'), regexp_substr('abcabc', 'b(c)', 1, 1, '', 1);
SELECT regexp_substr('abcabc', 'b(c)', 1, 2), regexp_substr('abcabc', 'b(c)', 1, 2, '', 1);
SELECT regexp_substr('abc', 'b', 1, 1, '', -1);
SELECT regexp_substr('a', 'a', 1, 1, 'g');

-- regexp_match and regexp_split_to_array
SELECT regexp_match('foobarbaz', 'b(..)'), regexp_match('abc', 'x'), regexp_match('abc', 'b'), regexp_match('abc', '(x)?(b)'), regexp_match('abc', '(a)(b)');
SELECT regexp_match('a', 'a', 'g');
SELECT regexp_split_to_array('a,b,,c', ','), regexp_split_to_array('abc', ''), regexp_split_to_array('a1b22c', '[0-9]+');
SELECT regexp_split_to_array('helloWORLD', '[A-Z]'), regexp_split_to_array('a,b', ',', 'i');
SELECT id, regexp_split_to_array(s, '[0-9]+') FROM rx WHERE s IS NOT NULL ORDER BY id;
DROP TABLE rx;
