-- Array subscripts and slices: `a[i]`, `a[lo:hi]`, the omitted-bound spellings,
-- multidimensional and clipped slices, and the result's renumbered lower bound.
-- Relations are prefixed `asl_` so no other corpus file collides.

-- One-dimensional slices.
SELECT ('{1,2,3,4,5}'::int[])[2:4];
SELECT ('{1,2,3,4,5}'::int[])[:2];
SELECT ('{1,2,3,4,5}'::int[])[3:];
SELECT ('{1,2,3,4,5}'::int[])[:];
SELECT ('{1,2,3,4,5}'::int[])[1:5];
SELECT array_dims(('{1,2,3,4,5}'::int[])[2:4]);

-- Clipping, empty results, and out-of-range ranges are not errors.
SELECT ('{1,2,3,4,5}'::int[])[0:2];
SELECT array_dims(('{1,2,3,4,5}'::int[])[0:2]);
SELECT ('{1,2,3,4,5}'::int[])[4:2];
SELECT array_dims(('{1,2,3,4,5}'::int[])[4:2]);
SELECT ('{1,2,3}'::int[])[10:12];
SELECT array_dims(('{1,2,3}'::int[])[10:12]);
SELECT ('{1,2,3,4,5}'::int[])[4:2] IS NULL;
SELECT ('{1,2,3}'::int[])[2:NULL];
SELECT ('{1,2,3}'::int[])[NULL:2];
SELECT ('{1,2,3}'::int[])[NULL];
SELECT (NULL::int[])[1:2];
SELECT (NULL::int[])[1];

-- A slice of an array with a non-default lower bound renumbers from 1.
SELECT ('[5:9]={1,2,3,4,5}'::int[])[6:8];
SELECT array_dims(('[5:9]={1,2,3,4,5}'::int[])[6:8]);
SELECT ('[2:4]={1,2,3}'::int[])[:3];
SELECT ('[2:4]={1,2,3}'::int[])[3:];

-- Multidimensional subscripting and slicing.
SELECT ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[2][3];
SELECT ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[2:3][1:2];
SELECT array_dims(('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[2:3][1:2]);
SELECT ('{{1,2,3},{4,5,6}}'::int[])[2];
SELECT ('{{1,2,3},{4,5,6}}'::int[])[2] IS NULL;
SELECT ('{{1,2,3},{4,5,6}}'::int[])[2:2];
SELECT ('{{1,2,3},{4,5,6}}'::int[])[1:2][2];
SELECT ('{{{1,2},{3,4}},{{5,6},{7,8}}}'::int[])[2][1][2];
SELECT ('{{{1,2},{3,4}},{{5,6},{7,8}}}'::int[])[1:1][1:2][2:2];
SELECT ('{{1,2},{3,4}}'::int[])[1][1][1];

-- A subscript on a non-subscriptable type, and too many dimensions.
SELECT (1)[1];
SELECT ('{}'::int[])[1][2][3][4][5][6][7];

-- Slices compose with the rest of the expression grammar.
SELECT ('{1,2,3,4,5}'::int[])[2:4] || ARRAY[9];
SELECT cardinality(('{1,2,3,4,5}'::int[])[2:4]);
SELECT ('{1,2,3,4,5}'::int[])[2:4] = '{2,3,4}'::int[];
SELECT 3 = ANY(('{1,2,3,4,5}'::int[])[2:4]);

-- Slices over stored columns, with computed bounds.
CREATE TABLE asl_t (id int, a int[], m int[]);
INSERT INTO asl_t VALUES
  (1, '{1,2,3,4,5}', '{{1,2,3},{4,5,6},{7,8,9}}'),
  (2, '[3:5]={7,8,9}', '{{1,2},{3,4}}'),
  (3, NULL, NULL);
SELECT id, a[2:4], m[1:2][1:2] FROM asl_t ORDER BY id;
SELECT id, a[:2], a[2:] FROM asl_t ORDER BY id;
SELECT id, a[id:id+1] FROM asl_t ORDER BY id;
SELECT id, a[array_lower(a,1):array_upper(a,1)] FROM asl_t ORDER BY id;
SELECT id FROM asl_t WHERE a[2:3] = '{2,3}'::int[] ORDER BY id;
SELECT id, m[2][2] FROM asl_t ORDER BY id;
DROP TABLE asl_t;
