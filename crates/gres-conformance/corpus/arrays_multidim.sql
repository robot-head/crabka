-- Multidimensional arrays: construction, dimension metadata, non-default lower
-- bounds, text I/O, and the operators and functions that see more than one
-- dimension. Relations are prefixed `am_` so no other corpus file collides.

-- Construction: nested literals and nested constructors agree.
SELECT '{{1,2},{3,4}}'::int[];
SELECT ARRAY[[1,2],[3,4]];
SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]];
SELECT ARRAY[[1,2],[3,4]] = '{{1,2},{3,4}}'::int[];
SELECT '{{{1,2},{3,4}},{{5,6},{7,8}}}'::int[];
SELECT ARRAY[ARRAY[ARRAY[1]]];

-- Sub-arrays must have matching dimensions.
SELECT '{{1,2},{3}}'::int[];
SELECT '{1,{2}}'::int[];
SELECT '{{1},2}'::int[];
SELECT ARRAY[[1,2],[3]];

-- Dimension metadata.
SELECT array_ndims('{{1,2},{3,4}}'::int[]);
SELECT array_ndims('{1,2}'::int[]);
SELECT array_ndims('{}'::int[]);
SELECT array_dims('{{1,2},{3,4}}'::int[]);
SELECT array_dims('{{1,2,3},{4,5,6}}'::int[]);
SELECT array_dims('{}'::int[]);
SELECT array_length('{{1,2,3},{4,5,6}}'::int[], 1);
SELECT array_length('{{1,2,3},{4,5,6}}'::int[], 2);
SELECT array_length('{{1,2,3},{4,5,6}}'::int[], 3);
SELECT array_length('{1,2}'::int[], 0);
SELECT array_length('{1,2}'::int[], -1);
SELECT array_lower('{{1,2,3},{4,5,6}}'::int[], 2);
SELECT array_upper('{{1,2,3},{4,5,6}}'::int[], 2);
SELECT cardinality('{{1,2},{3,4}}'::int[]);
SELECT cardinality('{}'::int[]);

-- Non-default lower bounds.
SELECT '[2:4]={1,2,3}'::int[];
SELECT array_dims('[2:4]={1,2,3}'::int[]);
SELECT array_lower('[2:4]={1,2,3}'::int[], 1);
SELECT array_upper('[2:4]={1,2,3}'::int[], 1);
SELECT ('[2:4]={1,2,3}'::int[])[1];
SELECT ('[2:4]={1,2,3}'::int[])[2];
SELECT ('[2:4]={1,2,3}'::int[])[4];
SELECT ('[2:4]={1,2,3}'::int[])[5];
SELECT '[2]={1,7}'::int[];
SELECT '[-1:0]={7,1}'::int[];
SELECT '[0:1][0:1]={{1,2},{3,4}}'::int[];
SELECT ' [1:2] = {1,2} '::int[];
SELECT '[1:2]={1,2,3}'::int[];
SELECT '[1:3]={1,2}'::int[];
SELECT '[1:2]={{1,2},{3,4}}'::int[];
SELECT '[1:0]={}'::int[];
SELECT '[1:-1]={}'::int[];

-- Equality compares the dimension header, including the lower bounds.
SELECT '[2:4]={1,2,3}'::int[] = '{1,2,3}'::int[];
SELECT '[2:3]={1,2}'::int[] > '{1,2}'::int[];
SELECT '{{1,2},{3,4}}'::int[] < '{{1,2},{3,5}}'::int[];
SELECT '{1,2}'::int[] < '{1,2,3}'::int[];
SELECT '{2}'::int[] < '{1,2}'::int[];

-- Text I/O round trips through the literal grammar and through `text`.
SELECT '{{1,2},{3,4}}'::int[]::text;
SELECT '  {  {  1 , 2 } ,  { 3,4 }  }  '::int[];
SELECT '{ ab\c , "ab\"c" }'::text[];
SELECT '{null,n\ull,"null"}'::text[];
SELECT '{a b}'::text[];
SELECT '{a  b , c}'::text[];
SELECT '{{},{}}'::int[];
SELECT '{{}}'::int[];

-- Element types beyond int4.
SELECT '{{a,b},{c,d}}'::text[];
SELECT '{{1.5,2.5},{3.5,4.5}}'::numeric[];
SELECT '{{2020-01-01,2020-01-02}}'::date[];
SELECT '{{2020-01-01 10:00:00,2020-01-02 11:00:00}}'::timestamp[];
SELECT '{{1 day,2 hours}}'::interval[];
SELECT '{{"{\"a\": 1}"}}'::jsonb[];
SELECT '{{1,2},{3,4}}'::int2[];
SELECT '{{1.5,2.5}}'::float4[];
SELECT '{{a2f3c5e0-0000-4000-8000-000000000001}}'::uuid[];

-- ANY/ALL and the set operators look at every element, in any dimension.
SELECT 5 = ANY('{{1,2},{5,6}}'::int[]);
SELECT 9 = ANY('{{1,2},{5,6}}'::int[]);
SELECT 0 < ALL('{{1,2},{5,6}}'::int[]);
SELECT '{{1,2},{3,4}}'::int[] @> '{1,4}'::int[];
SELECT '{{1,2},{3,4}}'::int[] && '{9,4}'::int[];
SELECT array_to_string('{{1,2},{3,4}}'::int[], ',');
SELECT unnest('{{1,2},{3,4}}'::int[]);

-- Concatenation joins along the outermost dimension.
SELECT array_cat('{{1,2},{3,4}}'::int[], '{{5,6}}'::int[]);
SELECT array_cat('{1,2}'::int[], '{{5,6}}'::int[]);
SELECT '{1,2}'::int[] || '{{3,4},{5,6}}'::int[];
SELECT array_append('{{1,2},{3,4}}'::int[], 5);
SELECT array_prepend(1, '{{2,3}}'::int[]);
SELECT 3 || '{{1,2}}'::int[];

-- The remaining functions over more than one dimension.
SELECT array_fill(7, ARRAY[2,2]);
SELECT array_dims(array_fill(7, ARRAY[2,2]));
SELECT array_fill(7, ARRAY[3], ARRAY[2]);
SELECT array_fill(NULL::int, ARRAY[2]);
SELECT array_fill(1, ARRAY[0]);
SELECT array_fill(1, NULL::int[]);
SELECT array_fill(1, ARRAY[2,2], ARRAY[1]);
SELECT array_replace('{{1,2},{2,3}}'::int[], 2, 9);
SELECT array_position(ARRAY[[1,2],[3,4]], 3);
SELECT array_positions(ARRAY[[1,2],[3,4]], 3);
SELECT array_remove(ARRAY[[1,2],[3,4]], 3);
SELECT array_sort('{{3,4},{1,2}}'::int[]);
SELECT array_reverse('{{1,2},{3,4}}'::int[]);
SELECT array_dims(array_sample('{{1,2},{3,4},{5,6}}'::int[], 2));
SELECT array_sample('{1,2,3}'::int[], -1);
SELECT array_sample('{1,2,3}'::int[], 5);
SELECT trim_array(ARRAY[1,2,3], -1);
SELECT trim_array(ARRAY[1,2,3], 4);
SELECT ('{}'::int[])[1][2][3][4][5][6][7];

-- `array_agg` over arrays adds a dimension rather than nesting a type.
SELECT array_agg(ar) FROM (VALUES ('{1,2}'::int[]), ('{3,4}'::int[])) v(ar);
SELECT array_agg(ARRAY['Hello', i::text]) FROM generate_series(9,11) g(i);

-- `ARRAY(subquery)` collects a single-column subquery in its own order.
SELECT ARRAY(SELECT g FROM generate_series(1,4) g);
SELECT ARRAY(SELECT g FROM generate_series(1,4) g ORDER BY g DESC);
SELECT ARRAY(SELECT g FROM generate_series(1,0) g);

-- Multidimensional values survive storage, grouping, and ordering.
CREATE TABLE am_store (id int, m int[], t text[]);
INSERT INTO am_store VALUES (1, '{{1,2},{3,4}}', '{{a,b}}'), (2, '[0:1]={5,6}', '{c}'), (3, NULL, NULL);
SELECT id, m, t FROM am_store ORDER BY id;
SELECT array_dims(m), array_ndims(m) FROM am_store ORDER BY id;
SELECT m FROM am_store ORDER BY m;
SELECT m, count(*) FROM am_store GROUP BY m ORDER BY m;
SELECT DISTINCT m FROM am_store ORDER BY m;
SELECT id FROM am_store WHERE m = '{{1,2},{3,4}}'::int[];
SELECT id FROM am_store WHERE m = '{5,6}'::int[];
CREATE TABLE am_keyed (m int[] PRIMARY KEY);
INSERT INTO am_keyed VALUES ('{{1,2}}'), ('{1,2}');
INSERT INTO am_keyed VALUES ('{{1,2}}');
SELECT m FROM am_keyed ORDER BY m;
DROP TABLE am_keyed;
DROP TABLE am_store;

-- Multidimensional columns declared with any number of `[]`.
CREATE TABLE am_decl (a int[][], b int4[][][], c text[][], d int[4], e int ARRAY[4], f varchar(5)[], g char(5)[]);
INSERT INTO am_decl VALUES ('{{1,2}}', '{{{1}}}', '{{x}}', '{9}', '{8}', '{abc}', '{abc}');
SELECT a, b, c, d, e, f, g FROM am_decl;
INSERT INTO am_decl (f) VALUES ('{"too long"}');
DROP TABLE am_decl;
