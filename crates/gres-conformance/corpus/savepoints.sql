-- S1 savepoint corpus: SAVEPOINT / ROLLBACK TO / RELEASE plus PostgreSQL 18's
-- aborted-transaction-block rule (25P02 until the block ends).
-- The harness runs every statement of a file over ONE connection, so a
-- BEGIN..COMMIT block spans statements naturally.
CREATE TABLE sp_t (id int4);

-- Outside a transaction block all three commands are 25P01.
SAVEPOINT sp_outside;
ROLLBACK TO SAVEPOINT sp_outside;
RELEASE SAVEPOINT sp_outside;

BEGIN;
SAVEPOINT sp1;
SELECT 1;
SAVEPOINT sp2;
RELEASE SAVEPOINT sp2;
RELEASE SAVEPOINT sp2;
ROLLBACK TO SAVEPOINT sp1;
SELECT 2;
ROLLBACK TO sp1;
RELEASE sp1;
ROLLBACK TO sp1;
COMMIT;

-- Reusing a savepoint name stacks levels: the inner one hides the outer until
-- it is released.
BEGIN;
SAVEPOINT dup;
SAVEPOINT dup;
RELEASE dup;
ROLLBACK TO dup;
RELEASE dup;
ROLLBACK TO dup;
COMMIT;

-- ROLLBACK TO is the other way out of an aborted block.
BEGIN;
SELECT 1;
SAVEPOINT recover;
SELECT sp_no_such_column;
SELECT 2;
ROLLBACK TO SAVEPOINT recover;
SELECT 3;
COMMIT;
SELECT 4;

-- Every statement after an error in a block is 25P02 until COMMIT/ROLLBACK,
-- including DDL, DML, SET and SHOW.
BEGIN;
SELECT 1;
SELECT sp_no_such_column;
SELECT 1;
INSERT INTO sp_t VALUES (1);
CREATE TABLE sp_never (id int4);
SET enable_seqscan = off;
SHOW enable_seqscan;
SAVEPOINT sp_after_error;
ROLLBACK;
SELECT 5;

-- COMMIT of an aborted block rolls it back, so nothing the block wrote lands.
BEGIN;
INSERT INTO sp_t VALUES (2);
SELECT sp_no_such_column;
COMMIT;
SELECT id FROM sp_t ORDER BY id;

-- The rule does NOT apply outside a transaction block: an autocommit error is
-- its own transaction and the next statement runs normally.
SELECT sp_no_such_column;
SELECT 6;

-- A syntax error aborts an open block exactly like any other error.
BEGIN;
SELECT 1;
SELECT FROM FROM;
SELECT 1;
ROLLBACK;
SELECT 7;

DROP TABLE sp_t;
