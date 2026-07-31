-- Lexical surface: non-decimal integer literals, `_` digit separators,
-- dollar-quoted strings, `E'...'` escape strings, and PostgreSQL's rule that
-- two literals separated by whitespace containing a newline are one literal.
--
-- Keep this file pure ASCII: the harness's statement splitter is byte-oriented,
-- so a multi-byte character in the SQL *source* would not survive it. Escapes
-- that PRODUCE non-ASCII text (E'\U0001F600') are fine.

-- Non-decimal integer literals (PG16+); the radix prefix is case-insensitive.
SELECT 0x7FFFFFFF;
SELECT 0X1f;
SELECT 0o273;
SELECT 0O17;
SELECT 0b100101;
SELECT 0B11;
SELECT 0x0, 0o0, 0b0;
SELECT 0x10 + 0o10 + 0b10;
SELECT -0x10;
SELECT 0xdeadbeef;

-- Width promotion: a literal too wide for int4 becomes int8, exactly as a
-- decimal literal does.
SELECT 0x80000000;
SELECT 0x7FFFFFFFFFFFFFFF;
SELECT -0x7FFFFFFFFFFFFFFF;

-- A bare radix prefix names its radix (42601).
SELECT 0x;
SELECT 0X;
SELECT 0o;
SELECT 0b;
SELECT 0x_;

-- Running a literal into an identifier character is trailing junk (42601).
SELECT 0xg;
SELECT 0x1g;
SELECT 0b12;
SELECT 0o18;

-- Out of range for the target type is 22003, not a lexical error.
SELECT 0b10000000000000000000000000000000::int4;
SELECT 0x7fffffff + 1;

-- `_` digit separators, in every radix and every part of a numeric literal.
SELECT 1_000;
SELECT 1_000_000;
SELECT 0x1_F;
SELECT 0x_1F;
SELECT 0b_1;
SELECT 0o1_7;
SELECT 1_000.000_1;
SELECT 1e1_0;
SELECT 1_000e1_0;
SELECT .5_5;
SELECT 1_000 + 2_000;

-- A separator may not lead, trail, or double (42601).
SELECT 1000_;
SELECT 1__000;
SELECT 1_.5;
SELECT 1._5;
SELECT 1.5_;
SELECT 1e_10;
SELECT 1e10_;
SELECT 1abc;
SELECT 1e;
SELECT 1e+;

-- Dollar-quoted strings; the body is verbatim, and distinct tags nest.
SELECT $$hello$$;
SELECT $tag$hello$tag$;
SELECT $$it's fine$$;
SELECT $outer$a $inner$ b$outer$;
SELECT $$$$;
SELECT $_t1$x$_t1$;
SELECT length($$abc$$);
SELECT $$a$$ || $$b$$;
SELECT $$ leading and trailing $$;
SELECT upper($$mixed Case$$);

-- `E'...'` escape strings.
SELECT E'a\tb';
SELECT length(E'a\nb');
SELECT E'a\\b';
SELECT E'\101\102';
SELECT E'\x41\x42';
SELECT E'\x7A';
SELECT E'A';
SELECT E'\U0001F600';
SELECT E'\uD83D\uDE00';
SELECT E'\q';
SELECT E'\z';
SELECT ascii(E'\b'), ascii(E'\f'), ascii(E'\r'), ascii(E'\v');
SELECT e'lower';
SELECT E'a''b';
SELECT length(E'\xc3\xa9');
SELECT 'a''b';
SELECT 'a\nb';
SELECT length('a\nb');

-- An escape that spells an untranslatable byte is 22021, not 42601.
SELECT E'\0';
SELECT E'\400';

-- Adjacent literals separated by whitespace CONTAINING A NEWLINE are one
-- literal; on a single line the same pair is a syntax error. A `--` comment
-- counts as whitespace for this rule; a block comment does NOT.
SELECT 'a'
'b';
SELECT length(E'a\n'
'b\t');
SELECT 'a' --c
'b';
SELECT 'a' 'b';
SELECT 'a'/* c */
'b';
