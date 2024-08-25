!!!note
    Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Crosses

Introduction: Return true if A crosses B

Format: `ST_Crosses (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Crosses(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Disjoint

Introduction: Return true if A and B are disjoint

Format: `ST_Disjoint (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM geom
WHERE ST_Disjoinnt(geom.geom_a, geom.geom_b)
```

## ST_DWithin

Introduction: Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'. This function essentially checks if the shortest distance between the envelope of the two geometries is <= the provided distance.

Format: `ST_DWithin (leftGeometry: Geometry, rightGeometry: Geometry, distance: Double)`

SQL Example:

```sql
SELECT ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 0)'), 2.5)
```

Output:

```
true
```

## ST_Equals

Introduction: Return true if A equals to B

Format: `ST_Equals (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Equals(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_OrderingEquals

Introduction: Returns true if the geometries are equal and the coordinates are in the same order

Format: `ST_OrderingEquals(A: geometry, B: geometry)`

SQL example 1:

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'))
```

Output: `true`

SQL example 2:

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))'))
```

Output: `false`

## ST_Overlaps

Introduction: Return true if A overlaps B

Format: `ST_Overlaps (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM geom
WHERE ST_Overlaps(geom.geom_a, geom.geom_b)
```

## ST_Relate

Introduction: The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relationship between the two input geometry objects.

The second variant of the function evaluates whether the two input geometries satisfy a specific spatial relationship defined by the provided `intersectionMatrix` pattern.

!!!Note
    It is important to note that this function is not optimized for use in spatial join operations. Certain DE-9IM relationships can hold true for geometries that do not intersect or are disjoint. As a result, it is recommended to utilize other dedicated spatial functions specifically optimized for spatial join processing.

Format:

`ST_Relate(geom1: Geometry, geom2: Geometry)`

`ST_Relate(geom1: Geometry, geom2: Geometry, intersectionMatrix: String)`

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

Format: `ST_Touches (A:geometry, B:geometry)`

```sql
SELECT *
FROM pointdf
WHERE ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Within

Introduction: Return true if A is fully contained by B

Format: `ST_Within (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Covers

Introduction: Return true if A covers B

Format: `ST_Covers (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_CoveredBy

Introduction: Return true if A is covered by B

Format: `ST_CoveredBy (A:geometry, B:geometry)`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_CoveredBy(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```
