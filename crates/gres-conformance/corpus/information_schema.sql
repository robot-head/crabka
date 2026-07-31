-- F-2: the SQL-standard information_schema views beyond the starter three.
--
-- The oracle's information_schema also describes its own built-in objects, so
-- every statement filters to the objects this file creates or asks a question
-- whose answer is fixed by the standard.
CREATE TABLE is_t (id int4 PRIMARY KEY, code text NOT NULL UNIQUE, amount numeric(8,2));
CREATE TABLE is_u (id int4 PRIMARY KEY, note text);
CREATE VIEW is_v AS SELECT id, code FROM is_t WHERE id > 0;

-- table_constraints: PRIMARY KEY and UNIQUE, plus the PG18 NOT NULL rows.
SELECT constraint_name, table_name, constraint_type, is_deferrable, initially_deferred FROM information_schema.table_constraints WHERE table_name = 'is_t' AND constraint_type = 'PRIMARY KEY';
SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'is_t' AND constraint_type = 'UNIQUE';
SELECT count(*) FROM information_schema.table_constraints WHERE table_name IN ('is_t', 'is_u') AND constraint_type IN ('PRIMARY KEY', 'UNIQUE');
SELECT table_catalog = current_database(), table_schema FROM information_schema.table_constraints WHERE table_name = 'is_t' AND constraint_type = 'PRIMARY KEY';

-- key_column_usage: which columns a keyed constraint covers, in key order.
SELECT constraint_name, table_name, column_name, ordinal_position FROM information_schema.key_column_usage WHERE table_name = 'is_t' ORDER BY constraint_name, ordinal_position;
SELECT count(*) FROM information_schema.key_column_usage WHERE table_name IN ('is_t', 'is_u');

-- constraint_column_usage: the same columns, from the referenced side.
SELECT table_name, column_name, constraint_name FROM information_schema.constraint_column_usage WHERE table_name = 'is_t' ORDER BY constraint_name, column_name;

-- referential_constraints: crabka has no foreign keys yet, so this is empty for
-- our tables — the view must still exist and answer, not 42P01.
SELECT count(*) FROM information_schema.referential_constraints WHERE constraint_name IN ('is_t_pkey', 'is_u_pkey', 'is_t_code_key');

-- views: the SQL-standard view catalogue. A simple single-table SELECT is
-- auto-updatable; a DISTINCT or grouped one is not.
CREATE VIEW is_vd AS SELECT DISTINCT code FROM is_t;
CREATE VIEW is_vg AS SELECT code, count(*) FROM is_t GROUP BY code;
SELECT table_name, is_updatable, is_insertable_into FROM information_schema.views WHERE table_name IN ('is_v', 'is_vd', 'is_vg') ORDER BY table_name;
SELECT table_name, check_option, is_updatable, is_insertable_into FROM information_schema.views WHERE table_name = 'is_v';
SELECT table_catalog = current_database(), table_schema FROM information_schema.views WHERE table_name = 'is_v';
SELECT view_definition FROM information_schema.views WHERE table_name = 'is_v';

-- routines / parameters: no user-defined routines exist, and both views must
-- answer rather than error.
SELECT count(*) FROM information_schema.routines WHERE routine_name IN ('is_t', 'is_u', 'is_v');
SELECT count(*) FROM information_schema.parameters WHERE specific_name IN ('is_t', 'is_u', 'is_v');

-- sequences.
SELECT count(*) FROM information_schema.sequences WHERE sequence_name IN ('is_t', 'is_u', 'is_v');

-- privileges: the owner holds every table privilege on what it created.
SELECT count(*) > 0 FROM information_schema.table_privileges WHERE table_name = 'is_t' AND privilege_type = 'SELECT';
SELECT count(*) FROM information_schema.column_privileges WHERE table_name = 'is_t' AND column_name = 'nonesuch';

-- role views: the connected role is always enabled for itself.
SELECT count(*) > 0 FROM information_schema.enabled_roles WHERE role_name = current_user;
SELECT count(*) FROM information_schema.applicable_roles WHERE grantee = 'nonesuch_role';

-- The starter three still answer, with the new tables in them.
SELECT table_name, table_type FROM information_schema.tables WHERE table_name IN ('is_t', 'is_u', 'is_v') ORDER BY table_name;
SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'is_t' ORDER BY ordinal_position;
SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'public';

DROP VIEW is_vg;
DROP VIEW is_vd;
DROP VIEW is_v;
DROP TABLE is_u;
DROP TABLE is_t;
