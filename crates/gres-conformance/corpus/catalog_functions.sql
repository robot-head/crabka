-- F-2: the pg_catalog functions psql's \d family, ORM preambles and migration
-- tools call. Every statement asks something whose answer is the same on any
-- PostgreSQL 18.4 server: a definition rebuilt from objects this file creates,
-- a fixed formatting rule, or a boolean visibility/privilege test.
CREATE TABLE cf_t (id int4 PRIMARY KEY, label text NOT NULL, amount numeric(12,3));
CREATE INDEX cf_t_label_idx ON cf_t (label);
CREATE VIEW cf_simple AS SELECT id, label FROM cf_t WHERE id > 0;
CREATE VIEW cf_alias AS SELECT id AS key, label FROM cf_t;
CREATE VIEW cf_grouped AS SELECT label, count(*) FROM cf_t GROUP BY label HAVING count(*) > 1 ORDER BY label LIMIT 5;
CREATE VIEW cf_distinct AS SELECT DISTINCT label FROM cf_t WHERE id IN (1, 2, 3);

-- pg_get_viewdef: both the parenthesized default and the pretty form, by name
-- and through the relation oid.
SELECT pg_catalog.pg_get_viewdef('cf_simple');
SELECT pg_catalog.pg_get_viewdef('cf_simple', true);
SELECT pg_catalog.pg_get_viewdef('cf_alias');
SELECT pg_catalog.pg_get_viewdef('cf_alias', true);
SELECT pg_catalog.pg_get_viewdef('cf_grouped');
SELECT pg_catalog.pg_get_viewdef('cf_grouped', true);
SELECT pg_catalog.pg_get_viewdef('cf_distinct');
SELECT pg_catalog.pg_get_viewdef('cf_distinct', true);
SELECT pg_catalog.pg_get_viewdef(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_simple';
SELECT pg_catalog.pg_get_viewdef(c.oid, true) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_alias';
-- The wrap-column overload implies pretty-printing.
SELECT pg_catalog.pg_get_viewdef(c.oid, 80) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_simple';

-- pg_get_indexdef / pg_get_constraintdef.
SELECT pg_catalog.pg_get_indexdef(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t_pkey';
SELECT pg_catalog.pg_get_indexdef(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t_label_idx';
SELECT pg_catalog.pg_get_constraintdef(con.oid) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid WHERE c.relname = 'cf_t' AND con.contype = 'p';

-- pg_get_userbyid: the bootstrap superuser owns every object.
SELECT pg_catalog.pg_get_userbyid(10);
SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';

-- pg_get_serial_sequence: NULL for a column with no sequence default.
SELECT pg_catalog.pg_get_serial_sequence('cf_t', 'label') IS NULL;

-- Visibility: everything crabka exposes is on the search path.
SELECT pg_catalog.pg_table_is_visible(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';
SELECT pg_catalog.pg_type_is_visible(23);
SELECT pg_catalog.pg_type_is_visible(25);

-- Identity. (`current_catalog` is a reserved-word spelling the parser does not
-- accept yet; F-2 owns the catalog surface, not expression parsing.)
SELECT current_database();
SELECT current_schema();
SELECT current_schemas(true);
SELECT current_schemas(false);
SELECT pg_catalog.pg_backend_pid() > 0;
SELECT pg_catalog.pg_postmaster_start_time() <= now();
SELECT pg_catalog.pg_is_in_recovery();
SELECT pg_catalog.pg_encoding_to_char(6);
SELECT pg_catalog.pg_char_to_encoding('UTF8');
SELECT version() LIKE 'PostgreSQL 18.4 %';

-- Sizes: pg_size_pretty's unit boundaries are a fixed formatting rule.
SELECT pg_catalog.pg_size_pretty(0::bigint);
SELECT pg_catalog.pg_size_pretty(10239::bigint);
SELECT pg_catalog.pg_size_pretty(10240::bigint);
SELECT pg_catalog.pg_size_pretty(1048576::bigint);
SELECT pg_catalog.pg_size_pretty(20971520::bigint);
SELECT pg_catalog.pg_size_pretty(21474836480::bigint);
SELECT pg_catalog.pg_size_pretty((-10240)::bigint);
SELECT pg_catalog.pg_relation_size('cf_t') >= 0;
SELECT pg_catalog.pg_total_relation_size('cf_t') >= 0;

-- Comments.
COMMENT ON TABLE cf_t IS 'a commented table';
COMMENT ON COLUMN cf_t.label IS 'a commented column';
SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';
SELECT pg_catalog.col_description(c.oid, 2) FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';
SELECT pg_catalog.col_description(c.oid, 1) IS NULL FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';
SELECT count(*) FROM pg_catalog.pg_description d JOIN pg_catalog.pg_class c ON c.oid = d.objoid WHERE c.relname = 'cf_t';
COMMENT ON TABLE cf_t IS NULL;
SELECT pg_catalog.obj_description(c.oid) IS NULL FROM pg_catalog.pg_class c WHERE c.relname = 'cf_t';

-- Privileges: the connected role owns everything it created.
SELECT pg_catalog.has_table_privilege('cf_t', 'SELECT');
SELECT pg_catalog.has_table_privilege('cf_t', 'INSERT');
SELECT pg_catalog.has_table_privilege('cf_t', 'UPDATE');
SELECT pg_catalog.has_table_privilege('cf_t', 'DELETE');
SELECT pg_catalog.has_table_privilege('cf_t', 'TRUNCATE');
SELECT pg_catalog.has_table_privilege('cf_t', 'REFERENCES');
SELECT pg_catalog.has_table_privilege('cf_t', 'TRIGGER');
SELECT pg_catalog.has_table_privilege(current_user, 'cf_t', 'SELECT');
SELECT pg_catalog.has_schema_privilege('public', 'USAGE');
SELECT pg_catalog.has_database_privilege(current_database(), 'CONNECT');
SELECT pg_catalog.has_table_privilege('cf_t', 'NONESUCH');
SELECT pg_catalog.has_table_privilege('cf_nonesuch', 'SELECT');

DROP VIEW cf_distinct;
DROP VIEW cf_grouped;
DROP VIEW cf_alias;
DROP VIEW cf_simple;
DROP TABLE cf_t;
