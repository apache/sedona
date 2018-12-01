

### ST_Overlaps  


The function ST_Overlaps takes two geometries as input and returns True if the two geometries spatially overlaps.


Below are the few Examples of the ST_Overlaps from postgis.net

--a point on a line is contained by the line and is of a lower dimension, and therefore does not overlap the line
			nor crosses

SELECT ST_Overlaps(a,b) As a_overlap_b,
	ST_Crosses(a,b) As a_crosses_b,
		ST_Intersects(a, b) As a_intersects_b, ST_Contains(b,a) As b_contains_a
FROM (SELECT ST_GeomFromText('POINT(1 0.5)') As a, ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)')  As b)
	As foo

a_overlap_b | a_crosses_b | a_intersects_b | b_contains_a
------------+-------------+----------------+--------------
f           | f           | t              | t

--a line that is partly contained by circle, but not fully is defined as intersecting and crossing,
-- but since of different dimension it does not overlap
SELECT ST_Overlaps(a,b) As a_overlap_b, ST_Crosses(a,b) As a_crosses_b,
	ST_Intersects(a, b) As a_intersects_b,
	ST_Contains(a,b) As a_contains_b
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 0.5)'), 3)  As a, ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)')  As b)
	As foo;

 a_overlap_b | a_crosses_b | a_intersects_b | a_contains_b
-------------+-------------+----------------+--------------
 f           | t           | t              | f

 -- a 2-dimensional bent hot dog (aka buffered line string) that intersects a circle,
 --	but is not fully contained by the circle is defined as overlapping since they are of the same dimension,
--	but it does not cross, because the intersection of the 2 is of the same dimension
--	as the maximum dimension of the 2

SELECT ST_Overlaps(a,b) As a_overlap_b, ST_Crosses(a,b) As a_crosses_b, ST_Intersects(a, b) As a_intersects_b,
ST_Contains(b,a) As b_contains_a,
ST_Dimension(a) As dim_a, ST_Dimension(b) as dim_b, ST_Dimension(ST_Intersection(a,b)) As dima_intersection_b
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 0.5)'), 3)  As a,
	ST_Buffer(ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)'),0.5)  As b)
	As foo;

 a_overlap_b | a_crosses_b | a_intersects_b | b_contains_a | dim_a | dim_b | dima_intersection_b
-------------+-------------+----------------+--------------+-------+-------+---------------------
 t           | f           | t              | f            |     2 |     2 |              2




### Implementation details for ST_Overlaps

1. Register ST_Overlaps in Catalog.scala
2. Test cases are added to predicateJoinTestScala.scala, predicateTestScala.scala
3. Added docs of the function in GeoSparkSQL-Predicate.md
4. Added test to cover it in predicateTestScala.scala, predicateJoinTestScala.scala
