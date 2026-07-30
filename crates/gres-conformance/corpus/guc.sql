-- F-1 GUC breadth: SET / SET LOCAL / RESET / SHOW transactional semantics,
-- current_setting()/set_config(), pg_settings, and the 42704 an unknown
-- parameter still raises.
SHOW enable_seqscan;
SET enable_seqscan = off;
SHOW enable_seqscan;
SET enable_seqscan TO on;
SHOW enable_seqscan;
SET enable_seqscan = false;
SHOW enable_seqscan;
RESET enable_seqscan;
SHOW enable_seqscan;

-- The planner-toggle family: every one of them is accepted and reported back.
SET enable_hashagg = off;
SET enable_sort = off;
SET enable_bitmapscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET enable_memoize = off;
SET enable_material = off;
SET enable_gathermerge = off;
SET enable_incremental_sort = off;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;
SET enable_partition_pruning = off;
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;
SET enable_presorted_aggregate = off;
SET enable_tidscan = off;
SET enable_async_append = off;
SET enable_distinct_reordering = off;
SET enable_group_by_reordering = off;
SET enable_self_join_elimination = off;
SELECT name, setting FROM pg_settings WHERE name LIKE 'enable\_%' ORDER BY name;
RESET ALL;
SELECT name, setting FROM pg_settings WHERE name LIKE 'enable\_%' ORDER BY name;

-- Memory and time units round-trip through SHOW the way PostgreSQL renders them.
SHOW work_mem;
SET work_mem = '64MB';
SHOW work_mem;
SET work_mem = 8192;
SHOW work_mem;
SET work_mem = '3000kB';
SHOW work_mem;
RESET work_mem;
SHOW work_mem;
SHOW maintenance_work_mem;
SHOW effective_cache_size;
SHOW temp_buffers;
SHOW min_parallel_table_scan_size;
SHOW deadlock_timeout;
SET lock_timeout = '5s';
SHOW lock_timeout;
RESET lock_timeout;

-- Numeric and real parameters.
SHOW seq_page_cost;
SHOW random_page_cost;
SHOW cpu_operator_cost;
SHOW hash_mem_multiplier;
SET seq_page_cost = 1.5;
SHOW seq_page_cost;
RESET seq_page_cost;
SET max_parallel_workers_per_gather = 0;
SHOW max_parallel_workers_per_gather;
SET jit_above_cost = 0;
SHOW jit_above_cost;
RESET jit_above_cost;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
RESET ALL;

-- Enum parameters validate their value.
SHOW bytea_output;
SET bytea_output = 'escape';
SHOW bytea_output;
SET bytea_output = 'nonsense';
RESET bytea_output;
SHOW client_min_messages;
SET client_min_messages = 'warning';
SHOW client_min_messages;
RESET client_min_messages;
SHOW constraint_exclusion;
SET constraint_exclusion = 'on';
SHOW constraint_exclusion;
RESET constraint_exclusion;
SHOW default_transaction_isolation;
SHOW plan_cache_mode;
SET plan_cache_mode = 'force_generic_plan';
SHOW plan_cache_mode;
RESET plan_cache_mode;

-- String parameters and the always-present client settings.
SHOW standard_conforming_strings;
SHOW search_path;
SHOW datestyle;
SHOW intervalstyle;
SHOW extra_float_digits;
SHOW server_version_num;
SHOW block_size;
SHOW max_identifier_length;
SHOW integer_datetimes;
SET escape_string_warning = off;
SHOW escape_string_warning;
RESET escape_string_warning;
SET default_statistics_target = 10;
SHOW default_statistics_target;
RESET default_statistics_target;
SET geqo = off;
SET geqo_threshold = 20;
SET join_collapse_limit = 1;
SET from_collapse_limit = 1;
SHOW join_collapse_limit;
RESET ALL;

-- SET LOCAL is transaction-scoped; a plain SET inside a block survives commit
-- and is undone by rollback.
BEGIN;
SET LOCAL enable_seqscan = off;
SHOW enable_seqscan;
COMMIT;
SHOW enable_seqscan;
BEGIN;
SET enable_seqscan = off;
SHOW enable_seqscan;
COMMIT;
SHOW enable_seqscan;
RESET enable_seqscan;
BEGIN;
SET enable_seqscan = off;
SHOW enable_seqscan;
ROLLBACK;
SHOW enable_seqscan;

-- current_setting()/set_config() reach the same registry.
SELECT current_setting('enable_seqscan');
SELECT set_config('enable_seqscan', 'off', false);
SHOW enable_seqscan;
SELECT current_setting('enable_seqscan');
SELECT set_config('enable_seqscan', 'on', false);
SELECT current_setting('no_such_parameter_at_all', true);
SELECT current_setting('no_such_parameter_at_all');
BEGIN;
SELECT set_config('enable_hashagg', 'off', true);
SELECT current_setting('enable_hashagg');
COMMIT;
SELECT current_setting('enable_hashagg');

-- An unknown parameter is 42704 in every spelling.
SET no_such_parameter_at_all = 1;
SHOW no_such_parameter_at_all;
RESET no_such_parameter_at_all;

-- A two-part name is a customized option, created on first assignment.
SET crabka_corpus.flag = 'on';
SHOW crabka_corpus.flag;
SELECT current_setting('crabka_corpus.flag');
SET crabka_corpus.flag = 'off';
SHOW crabka_corpus.flag;

-- pg_settings exposes one row per parameter.
SELECT name, setting, vartype FROM pg_settings WHERE name = 'enable_seqscan';
SELECT name, vartype FROM pg_settings WHERE name IN ('work_mem', 'seq_page_cost', 'bytea_output', 'search_path') ORDER BY name;
SELECT setting, boot_val, reset_val FROM pg_settings WHERE name = 'enable_hashagg';
SELECT count(*) FROM pg_settings WHERE name = 'no_such_parameter_at_all';

-- Numeric parameters are range-checked in base units: every one of these is
-- 22023, and the parameter keeps the value it had.
SET work_mem = '63kB';
SET work_mem = 0;
SET work_mem = -5;
SHOW work_mem;
SET work_mem = '64kB';
SHOW work_mem;
RESET work_mem;
SET cursor_tuple_fraction = 99;
SET cursor_tuple_fraction = -0.1;
SET cursor_tuple_fraction = 1;
SHOW cursor_tuple_fraction;
RESET cursor_tuple_fraction;
SET from_collapse_limit = 0;
SET geqo_threshold = 1;
SET geqo_effort = 11;
SET extra_float_digits = 10;
SET extra_float_digits = -16;
SET commit_delay = 100001;
SET commit_siblings = 1001;
SET default_statistics_target = 0;
SET effective_io_concurrency = 1001;
SET join_collapse_limit = 0;
SET max_parallel_workers = 1025;
SET hash_mem_multiplier = 0.5;
SET hash_mem_multiplier = 1001;
SET temp_buffers = 5;
SET effective_cache_size = 0;
SET min_parallel_table_scan_size = -1;
SET min_parallel_index_scan_size = -1;
SET maintenance_work_mem = '63kB';
SET deadlock_timeout = 0;
SET lock_timeout = -1;
SET idle_in_transaction_session_timeout = -1;
SET max_stack_depth = 99;
SET statement_timeout = -1;
SET vacuum_cost_delay = 200;
SET recursive_worktable_factor = 2000000;
SET recursive_worktable_factor = 0.0001;
SET jit_above_cost = -2;
SET seq_page_cost = -1;
SET random_page_cost = -1;
SET parallel_setup_cost = -1;
SET cpu_tuple_cost = -1;

-- A value whose base-unit magnitude overflows the parameter's C int is
-- rejected before the range check, with the raw spelling in the message.
SET work_mem = '2TB';
SET work_mem = '2147483648kB';
SET statement_timeout = '25d';

-- A value the parameter's own parser cannot read is 22023 too.
SET work_mem = 'abc';
SET from_collapse_limit = 'abc';
SET seq_page_cost = 'abc';
SET bytea_output = 'bogus';
SET enable_seqscan = 'maybe';

-- Parameters whose pg_settings.context forbids session assignment are 55P02,
-- on SET and on RESET alike, and keep their compiled-in value.
SET block_size = 4096;
RESET block_size;
SHOW block_size;
SET server_version_num = 1;
SET max_identifier_length = 10;
SET integer_datetimes = off;
SET in_hot_standby = on;
SET server_encoding = 'LATIN1';
SET wal_level = 'logical';
SET allow_alter_system = off;
SET shared_buffers = '256MB';
SET max_connections = 200;
SHOW shared_buffers;
SHOW max_connections;
SHOW geqo_effort;

-- pg_settings reports the unit, the range and the accepted enum values.
SELECT name, setting, unit, min_val, max_val FROM pg_settings WHERE name IN ('work_mem', 'statement_timeout', 'seq_page_cost') ORDER BY name;
SELECT name, unit, min_val, max_val FROM pg_settings WHERE name IN ('effective_cache_size', 'temp_buffers', 'vacuum_cost_delay', 'cursor_tuple_fraction', 'recursive_worktable_factor', 'shared_buffers') ORDER BY name;
SELECT name, min_val, max_val FROM pg_settings WHERE name IN ('cpu_tuple_cost', 'jit_above_cost', 'hash_mem_multiplier', 'geqo_effort', 'max_connections') ORDER BY name;
SELECT name, context, enumvals FROM pg_settings WHERE name IN ('bytea_output', 'synchronous_commit', 'default_transaction_isolation', 'block_size', 'wal_level', 'server_encoding') ORDER BY name;
SELECT unit, min_val, max_val, enumvals FROM pg_settings WHERE name = 'enable_seqscan';
SELECT unit, min_val, max_val, enumvals FROM pg_settings WHERE name = 'search_path';

-- PostgreSQL's integer-GUC input syntax: signs, 0x/0b prefixes, leading-zero
-- octal, and decimals rounded half away from zero. `0o` is not a prefix.
SET from_collapse_limit = '0x10';
SHOW from_collapse_limit;
SET from_collapse_limit = '0b11';
SHOW from_collapse_limit;
SET from_collapse_limit = '010';
SHOW from_collapse_limit;
SET from_collapse_limit = '12.6';
SHOW from_collapse_limit;
SET from_collapse_limit = '12.4';
SHOW from_collapse_limit;
SET from_collapse_limit = '  +9  ';
SHOW from_collapse_limit;
SET from_collapse_limit = '0o10';
SET from_collapse_limit = '0_1';
RESET from_collapse_limit;
SET extra_float_digits = '0x2';
SHOW extra_float_digits;
SET extra_float_digits = '0b10';
SHOW extra_float_digits;
SET extra_float_digits = '010';
SET extra_float_digits = '0o2';
RESET extra_float_digits;
SELECT name, setting, vartype, boot_val, reset_val FROM pg_settings WHERE name IN ('work_mem', 'statement_timeout', 'search_path', 'enable_seqscan', 'bytea_output', 'temp_buffers', 'effective_cache_size', 'max_stack_depth', 'maintenance_work_mem') ORDER BY name;
SELECT name, context FROM pg_settings WHERE name IN ('deadlock_timeout', 'commit_delay', 'max_stack_depth', 'log_statement', 'lc_messages', 'session_replication_role', 'work_mem') ORDER BY name;
