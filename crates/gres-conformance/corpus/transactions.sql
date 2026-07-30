-- SP4 transaction corpus: verifies crabgresql matches PostgreSQL 18 for
-- BEGIN/ROLLBACK/COMMIT within a single session.
-- The conformance harness runs all statements in a file over ONE persistent
-- connection, so BEGIN..COMMIT/ROLLBACK spans naturally across statements.
CREATE TABLE tx (id int4);
BEGIN;
INSERT INTO tx VALUES (1), (2);
ROLLBACK;
SELECT id FROM tx ORDER BY id;
BEGIN;
INSERT INTO tx VALUES (3), (4);
COMMIT;
SELECT id FROM tx ORDER BY id;
DROP TABLE tx;

-- AND CHAIN ends the block and opens another with the same characteristics;
-- outside a block it is 25P01 and changes nothing.
COMMIT AND CHAIN;
ROLLBACK AND CHAIN;
END AND CHAIN;
CREATE TABLE chain_t (id int4);
BEGIN;
INSERT INTO chain_t VALUES (1);
COMMIT AND CHAIN;
INSERT INTO chain_t VALUES (2);
ROLLBACK;
SELECT id FROM chain_t ORDER BY id;
BEGIN;
INSERT INTO chain_t VALUES (3);
ROLLBACK AND CHAIN;
INSERT INTO chain_t VALUES (4);
COMMIT;
SELECT id FROM chain_t ORDER BY id;
BEGIN;
COMMIT AND NO CHAIN;
SELECT id FROM chain_t ORDER BY id;
DROP TABLE chain_t;

-- transaction_isolation reports the level the block is actually running at,
-- and an omitted ISOLATION LEVEL takes default_transaction_isolation.
SHOW transaction_isolation;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
COMMIT;
BEGIN ISOLATION LEVEL READ COMMITTED;
SHOW transaction_isolation;
COMMIT;
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SHOW transaction_isolation;
COMMIT;
SHOW transaction_isolation;
SET default_transaction_isolation = 'repeatable read';
SHOW default_transaction_isolation;
SHOW transaction_isolation;
BEGIN;
SHOW transaction_isolation;
COMMIT;
SHOW transaction_isolation;
RESET default_transaction_isolation;
SHOW transaction_isolation;
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
COMMIT;
SHOW transaction_isolation;
BEGIN;
SET transaction_isolation = 'repeatable read';
SHOW transaction_isolation;
COMMIT;
SHOW transaction_isolation;
BEGIN ISOLATION LEVEL REPEATABLE READ;
COMMIT AND CHAIN;
SHOW transaction_isolation;
ROLLBACK;
SET transaction_isolation = 'bogus';
SET default_transaction_isolation = 'bogus';

-- The transaction_mode list: READ ONLY / READ WRITE / [NOT] DEFERRABLE, in any
-- order, commas optional, and mixed with ISOLATION LEVEL. A READ ONLY block
-- refuses writes with 25006 and aborts, like any other in-block error.
CREATE TABLE ro_modes (a int);
START TRANSACTION READ ONLY;
SELECT count(*) FROM ro_modes;
SHOW transaction_read_only;
INSERT INTO ro_modes VALUES (1);
ROLLBACK;
BEGIN READ ONLY, DEFERRABLE;
SELECT 1;
ROLLBACK;
START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;
SELECT 1;
ROLLBACK;
START TRANSACTION READ WRITE;
INSERT INTO ro_modes VALUES (2);
COMMIT;
SELECT a FROM ro_modes ORDER BY a;
DROP TABLE ro_modes;

-- A unique index built inside a transaction back-validates against that
-- transaction's OWN uncommitted rows, so a later duplicate is rejected. The
-- backfill scanned only committed rows before, which built the index empty and
-- let the duplicate through — and then committed it, leaving a table that
-- violated its own unique index.
BEGIN;
CREATE TABLE uqx (x integer, x10 integer);
INSERT INTO uqx SELECT x, x / 10 FROM generate_series(1, 20) x;
CREATE UNIQUE INDEX ON uqx (x, x10);
INSERT INTO uqx VALUES (1, 0);
ROLLBACK;
