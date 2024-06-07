## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Contains(ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'), ST_GeomFromWKT('POINT(174 149)'))
```

Output:

```
false
```

## ST_Crosses

Introduction: Return true if A crosses B

Format: `ST_Crosses (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Crosses(ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))'),ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))'))
```

Output:

```
false
```

## ST_Disjoint

Introduction: Return true if A and B are disjoint

Format: `ST_Disjoint (A: Geometry, B: Geometry)`

Since: `v1.2.1`

SQL Example

```sql
SELECT ST_Disjoint(ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))
```

Output:

```
true
```

## ST_DWithin

Introduction: Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'.

If `useSpheroid` is passed true, ST_DWithin uses Sedona's ST_DistanceSpheroid to check the spheroid distance between the centroids of two geometries. The unit of the distance in this case is meter.

If `useSpheroid` is passed false, ST_DWithin uses Euclidean distance and the unit of the distance is the same as the CRS of the geometries. To obtain the correct result, please consider using ST_Transform to put data in an appropriate CRS.

If useSpheroid is not given, it defaults to false

Format: `ST_DWithin (leftGeometry: Geometry, rightGeometry: Geometry, distance: Double, useSpheroid: Optional(Boolean) = false)`

Since: `v1.5.1`

SQL Example

```sql
SELECT ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 0)'), 2.5)
```

Output:

```
true
```

```text
Check for distance between New York and Seattle (< 4000 km)
```

```sql
SELECT ST_DWithin(ST_GeomFromWKT(-122.335167 47.608013), ST_GeomFromWKT(-73.935242 40.730610), 4000000, true)
```

Output:

```
true
```

## ST_Equals

Introduction: Return true if A equals to B

Format: `ST_Equals (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Equals(ST_GeomFromWKT('LINESTRING(0 0,10 10)'), ST_GeomFromWKT('LINESTRING(0 0,5 5,10 10)'))
```

Output:

```
true
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Intersects(ST_GeomFromWKT('LINESTRING(-43.23456 72.4567,-43.23456 72.4568)'), ST_GeomFromWKT('POINT(-43.23456 72.4567772)'))
```

Output:

```
true
```

## ST_OrderingEquals

Introduction: Returns true if the geometries are equal and the coordinates are in the same order

Format: `ST_OrderingEquals(A: geometry, B: geometry)`

Since: `v1.2.1`

SQL Example

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'))
```

Output:

```
true
```

SQL Example

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))'))
```

Output:

```
false
```

## ST_Overlaps

Introduction: Return true if A overlaps B

Format: `ST_Overlaps (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Overlaps(ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))'), ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))'))
```

Output:

```
true
```

## ST_Relate

Introduction: The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relationship between the two input geometry objects.

The second variant of the function evaluates whether the two input geometries satisfy a specific spatial relationship defined by the provided `intersectionMatrix` pattern.

!!!Note
    It is important to note that this function is not optimized for use in spatial join operations. Certain DE-9IM relationships can hold true for geometries that do not intersect or are disjoint. As a result, it is recommended to utilize other dedicated spatial functions specifically optimized for spatial join processing.

Format:

`ST_Relate(geom1: Geometry, geom2: Geometry)`

`ST_Relate(geom1: Geometry, geom2: Geometry, intersectionMatrix: String)`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_Relate(
        ST_GeomFromWKT('LINESTRING (1 1, 5 5)'),
        ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))')
)
```

Output:

```
1010F0212
```

SQL Example

```sql
SELECT ST_Relate(
        ST_GeomFromWKT('LINESTRING (1 1, 5 5)'),
        ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'),
       "1010F0212"
)
```

Output:

```
true
```

## ST_RelateMatch

Introduction: This function tests the relationship between two [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrices representing geometry intersections. It evaluates whether the DE-9IM matrix specified in `matrix1` satisfies the intersection pattern defined by `matrix2`. The `matrix2` parameter can be an exact DE-9IM value or a pattern containing wildcard characters.

!!!Note
    It is important to note that this function is not optimized for use in spatial join operations. Certain DE-9IM relationships can hold true for geometries that do not intersect or are disjoint. As a result, it is recommended to utilize other dedicated spatial functions specifically optimized for spatial join processing.

Format: `ST_RelateMatch(matrix1: String, matrix2: String)`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF')
```

Output:

```
true
```

## ST_Touches

Introduction: Return true if A touches B

Format: `ST_Touches (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Touches(ST_GeomFromWKT('LINESTRING(0 0,1 1,0 2)'), ST_GeomFromWKT('POINT(0 2)'))
```

Output:

```
true
```

## ST_Within

Introduction: Return true if A is fully contained by B

Format: `ST_Within (A: Geometry, B: Geometry)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Within(ST_GeomFromWKT('POLYGON((0 0,3 0,3 3,0 3,0 0))'), ST_GeomFromWKT('POLYGON((1 1,2 1,2 2,1 2,1 1))'))
```

Output:

```
false
```

## ST_Covers

Introduction: Return true if A covers B

Format: `ST_Covers (A: Geometry, B: Geometry)`

Since: `v1.3.0`

SQL Example

```sql
SELECT ST_Covers(ST_GeomFromWKT('POLYGON((-2 0,0 2,2 0,-2 0))'), ST_GeomFromWKT('POLYGON((-1 0,0 1,1 0,-1 0))'))
```

Output:

```
true
```

## ST_CoveredBy

Introduction: Return true if A is covered by B

Format: `ST_CoveredBy (A: Geometry, B: Geometry)`

Since: `v1.3.0`

SQL Example

```sql
SELECT ST_CoveredBy(ST_GeomFromWKT('POLYGON((0 0,3 0,3 3,0 3,0 0))'),  ST_GeomFromWKT('POLYGON((1 1,2 1,2 2,1 2,1 1))'))
```

Output:

```
false
```
