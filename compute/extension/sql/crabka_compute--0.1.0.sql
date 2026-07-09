-- Crabka compute extension SQL scaffold.
--
-- The smgr hook is installed from _PG_init via shared_preload_libraries after
-- the PG17 smgr-hook patch has been applied. No SQL-callable functions are
-- needed for the PG5 compute bootstrap path.

DO $$
BEGIN
    RAISE NOTICE 'crabka_compute is loaded through shared_preload_libraries';
END
$$;
