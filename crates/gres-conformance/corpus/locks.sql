-- S3 LOCK TABLE (all eight modes) and the advisory-lock function family.
CREATE TABLE lk_a (id int4);
CREATE TABLE lk_b (id int4);

-- LOCK TABLE is only legal inside a transaction block.
LOCK TABLE lk_a;
LOCK TABLE lk_a IN ACCESS SHARE MODE;

BEGIN;
LOCK TABLE lk_a IN ACCESS SHARE MODE;
LOCK TABLE lk_a IN ROW SHARE MODE;
LOCK TABLE lk_a IN ROW EXCLUSIVE MODE;
LOCK TABLE lk_a IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE lk_a IN SHARE MODE;
LOCK TABLE lk_a IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE lk_a IN EXCLUSIVE MODE;
LOCK TABLE lk_a IN ACCESS EXCLUSIVE MODE;
LOCK TABLE lk_a;
LOCK lk_a;
LOCK TABLE lk_a NOWAIT;
LOCK TABLE lk_a IN ACCESS SHARE MODE NOWAIT;
LOCK TABLE lk_a, lk_b IN SHARE MODE;
LOCK TABLE ONLY lk_a IN SHARE MODE;
SELECT id FROM lk_a ORDER BY id;
COMMIT;

-- An unknown relation is resolved before any lock is taken.
BEGIN;
LOCK TABLE lk_no_such_table;
ROLLBACK;
BEGIN;
LOCK TABLE lk_a, lk_no_such_table IN SHARE MODE;
ROLLBACK;

-- Advisory locks: the one-argument int8 spelling.
SELECT pg_advisory_lock(4711);
SELECT pg_try_advisory_lock(4712);
SELECT pg_advisory_unlock(4711);
SELECT pg_advisory_unlock(4712);
SELECT pg_advisory_unlock(4713);
SELECT pg_advisory_lock_shared(4714);
SELECT pg_try_advisory_lock_shared(4715);
SELECT pg_advisory_unlock_shared(4714);
SELECT pg_advisory_unlock_shared(4715);
SELECT pg_advisory_unlock_shared(4716);

-- Advisory locks are counted: two acquisitions need two releases.
SELECT pg_advisory_lock(4720);
SELECT pg_advisory_lock(4720);
SELECT pg_advisory_unlock(4720);
SELECT pg_advisory_unlock(4720);
SELECT pg_advisory_unlock(4720);

-- The two-int4 key spelling.
SELECT pg_advisory_lock(1, 2);
SELECT pg_try_advisory_lock(1, 3);
SELECT pg_advisory_unlock(1, 2);
SELECT pg_advisory_unlock(1, 3);
SELECT pg_advisory_lock_shared(1, 4);
SELECT pg_advisory_unlock_shared(1, 4);

-- pg_advisory_unlock_all releases every session-level advisory lock.
SELECT pg_advisory_lock(4730);
SELECT pg_advisory_lock_shared(4731);
SELECT pg_advisory_unlock_all();
SELECT pg_advisory_unlock(4730);
SELECT pg_advisory_unlock_shared(4731);

-- Transaction-scoped advisory locks release themselves at the end of the
-- transaction, so pg_advisory_unlock never finds them.
BEGIN;
SELECT pg_advisory_xact_lock(4740);
SELECT pg_try_advisory_xact_lock(4741);
SELECT pg_advisory_xact_lock_shared(4742);
SELECT pg_try_advisory_xact_lock_shared(4743);
COMMIT;
SELECT pg_advisory_unlock(4740);
SELECT pg_advisory_unlock(4741);
SELECT pg_advisory_unlock_shared(4742);
SELECT pg_advisory_unlock_shared(4743);

BEGIN;
SELECT pg_advisory_xact_lock(4750);
ROLLBACK;
SELECT pg_advisory_unlock(4750);

DROP TABLE lk_a;
DROP TABLE lk_b;

-- The advisory family is strict, so an untyped NULL key resolves against the
-- ordinary overloads and returns NULL rather than failing resolution.
SELECT pg_advisory_lock(NULL) IS NULL;
SELECT pg_advisory_lock_shared(NULL) IS NULL;
SELECT pg_advisory_xact_lock(NULL) IS NULL;
SELECT pg_try_advisory_lock(NULL) IS NULL;
SELECT pg_try_advisory_lock_shared(NULL) IS NULL;
SELECT pg_advisory_unlock(NULL) IS NULL;
SELECT pg_advisory_unlock_shared(NULL) IS NULL;
SELECT pg_try_advisory_lock(NULL, 1) IS NULL;
SELECT pg_try_advisory_lock(1, NULL) IS NULL;
SELECT pg_advisory_unlock(NULL, NULL) IS NULL;
