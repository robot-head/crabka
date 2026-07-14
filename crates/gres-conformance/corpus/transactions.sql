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
