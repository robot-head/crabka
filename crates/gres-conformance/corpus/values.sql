-- SP39: VALUES query expressions and derived row sources.

VALUES (1, 'a'), (2, 'b') ORDER BY 1;
VALUES (2), (1), (3) ORDER BY 1 LIMIT 2 OFFSET 1;
VALUES (NULL), (2) ORDER BY 1;
VALUES ('5'), (2) ORDER BY 1;

SELECT id, name
FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, name)
ORDER BY id;

VALUES (1), (2)
UNION
SELECT 2
ORDER BY 1;

VALUES (1), (1), (2)
UNION ALL
VALUES (2), (3)
ORDER BY 1;

-- PostgreSQL resolves this all-unknown VALUES column to text before the set op,
-- so the SELECT branch is cast explicitly for parity-deterministic output.
VALUES (NULL), ('5') UNION SELECT 2::text ORDER BY 1;
