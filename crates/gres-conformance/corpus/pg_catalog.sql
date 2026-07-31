-- F-2: the pg_catalog relations psql's \d family and ORM preambles read.
--
-- Catalog CONTENTS legitimately differ between the oracle and the subject (the
-- oracle carries thousands of built-in types, functions and operators), so
-- every statement here asks a question whose answer depends only on the objects
-- this file itself creates, or on a relation's own shape.
CREATE TABLE pgc_t (id int4 PRIMARY KEY, label text NOT NULL, amount numeric(10,2), note text DEFAULT 'none');
CREATE INDEX pgc_t_label_idx ON pgc_t (label);
CREATE VIEW pgc_v AS SELECT id, label FROM pgc_t WHERE id > 0;

-- pg_class: relkind, column count and index flag for our own relations.
SELECT relname, relkind, relnatts, relhasindex, relpersistence, relispartition FROM pg_catalog.pg_class WHERE relname = 'pgc_t';
SELECT relname, relkind, relnatts FROM pg_catalog.pg_class WHERE relname = 'pgc_v';
SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname IN ('pgc_t_label_idx', 'pgc_t_pkey') ORDER BY relname;
SELECT count(*) FROM pg_catalog.pg_class WHERE relname LIKE 'pgc%';
SELECT relhasrules, relhastriggers, relrowsecurity, relforcerowsecurity, relispopulated, relreplident FROM pg_catalog.pg_class WHERE relname = 'pgc_t';

-- pg_attribute: the column metadata every introspection query joins on.
SELECT a.attname, a.attnum, a.attnotnull, a.atthasdef, a.attisdropped, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'pgc_t' AND a.attnum > 0 ORDER BY a.attnum;
SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'pgc_t' AND a.attnum > 0 ORDER BY a.attnum;
SELECT count(*) FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'pgc_t' AND a.attnum > 0 AND NOT a.attisdropped;

-- pg_namespace: the three schemas every crabka database has.
SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname IN ('public', 'pg_catalog', 'information_schema') ORDER BY nspname;
SELECT count(*) FROM pg_catalog.pg_namespace n WHERE n.nspname = 'public' AND n.nspowner IS NOT NULL;

-- pg_index: unique/primary flags for the indexes we just made.
SELECT c2.relname, i.indisunique, i.indisprimary, i.indisvalid, i.indisready, i.indnatts FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indrelid JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid WHERE c.relname = 'pgc_t' ORDER BY c2.relname;

-- pg_constraint: PRIMARY KEY and the PG18 NOT NULL constraints.
SELECT conname, contype, condeferrable, condeferred, convalidated FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid WHERE c.relname = 'pgc_t' AND con.contype = 'p';
SELECT count(*) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid WHERE c.relname = 'pgc_t' AND con.contype = 'n';

-- pg_attrdef: the stored default of a column that has one.
SELECT count(*) FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_class c ON c.oid = d.adrelid WHERE c.relname = 'pgc_t';

-- pg_am: the built-in access methods, by their fixed oids.
SELECT amname, amtype FROM pg_catalog.pg_am WHERE amname IN ('btree', 'hash') ORDER BY amname;
SELECT count(*) FROM pg_catalog.pg_am am JOIN pg_catalog.pg_class c ON c.relam = am.oid WHERE c.relname = 'pgc_t_label_idx' AND am.amname = 'btree';

-- pg_tablespace / pg_database / pg_collation: the fixed cluster-wide rows.
SELECT spcname FROM pg_catalog.pg_tablespace ORDER BY spcname;
SELECT datname = current_database(), datallowconn, datistemplate FROM pg_catalog.pg_database WHERE datname = current_database();
SELECT collname FROM pg_catalog.pg_collation WHERE collname IN ('default', 'C', 'POSIX') ORDER BY collname;

-- pg_rewrite: a view carries exactly one _RETURN rule.
SELECT r.rulename, r.ev_type, r.is_instead FROM pg_catalog.pg_rewrite r JOIN pg_catalog.pg_class c ON c.oid = r.ev_class WHERE c.relname = 'pgc_v';

-- Relations for object kinds crabka has none of must exist and return no rows,
-- not 42P01: every psql \d query and ORM preamble joins at least one of them.
SELECT count(*) FROM pg_catalog.pg_trigger;
SELECT count(*) FROM pg_catalog.pg_enum;
SELECT count(*) FROM pg_catalog.pg_range WHERE rngtypid = 0;
SELECT count(*) FROM pg_catalog.pg_inherits;
SELECT count(*) FROM pg_catalog.pg_depend WHERE classid = 0;
SELECT count(*) FROM pg_catalog.pg_replication_slots;
SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype = 'nonesuch';
SELECT count(*) FROM pg_catalog.pg_proc WHERE proname = 'nonesuch_function_name';
SELECT count(*) FROM pg_catalog.pg_description WHERE objoid = 0;
SELECT count(*) FROM pg_catalog.pg_sequence WHERE seqrelid = 0;
SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'nonesuch_extension';
SELECT lanname FROM pg_catalog.pg_language WHERE lanname IN ('internal', 'c', 'sql') ORDER BY lanname;

-- pg_tables / pg_views / pg_indexes: the reporting views clients list from.
SELECT schemaname, tablename, hasindexes FROM pg_catalog.pg_tables WHERE tablename = 'pgc_t';
SELECT schemaname, viewname FROM pg_catalog.pg_views WHERE viewname = 'pgc_v';
SELECT schemaname, tablename, indexname FROM pg_catalog.pg_indexes WHERE tablename = 'pgc_t' ORDER BY indexname;

-- pg_stat_activity: the current backend is always there and always active.
SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid();

-- pg_roles / pg_authid: the connected role.
SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = current_user;
SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = current_user;

DROP VIEW pgc_v;
DROP TABLE pgc_t;
