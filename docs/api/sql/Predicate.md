## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:

```sql
SELECT * 
FROM pointdf 
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

Output:

```
POINT (1.1 101.1) 
POINT (2.1 102.1) 
POINT (3.1 103.1) 
POINT (4.1 104.1) 
POINT (5.1 105.1) 
POINT (6.1 106.1) 
POINT (7.1 107.1) 
POINT (8.1 108.1) 
POINT (9.1 109.1) 
POINT (10.1 110.1)
POINT (11.1 111.1)
POINT (12.1 112.1)
POINT (13.1 113.1)
POINT (14.1 114.1)
POINT (15.1 115.1)
POINT (16.1 116.1)
POINT (17.1 117.1)
POINT (18.1 118.1)
POINT (19.1 119.1)
POINT (20.1 120.1)
```

## ST_Crosses

Introduction: Return true if A crosses B

Format: `ST_Crosses (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:

```sql
SELECT ST_Crosses(ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))'),ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))'))
```

Output:

```
false
```

## ST_Disjoint

Introduction: Return true if A and B are disjoint

Format: `ST_Disjoint (A:geometry, B:geometry)`

Since: `v1.2.1`

Spark SQL example:

```sql
SELECT ST_Disjoint(ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))
```

Output:

```
true
```

## ST_Equals

Introduction: Return true if A equals to B

Format: `ST_Equals (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Equals(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_OrderingEquals
Introduction: Returns true if the geometries are equal and the coordinates are in the same order

Format: `ST_OrderingEquals(A: geometry, B: geometry)`

Since: `v1.2.1`

Spark SQL example 1:
```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'))
```

Output: `true`

Spark SQL example 2:
```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))'))
```

Output: `false`

## ST_Overlaps

Introduction: Return true if A overlaps B

Format: `ST_Overlaps (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT *
FROM geom
WHERE ST_Overlaps(geom.geom_a, geom.geom_b)
```

## ST_Touches

Introduction: Return true if A touches B

Format: `ST_Touches (A:geometry, B:geometry)`

Since: `v1.0.0`

```sql
SELECT * 
FROM pointdf 
WHERE ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Within

Introduction: Return true if A is fully contained by B

Format: `ST_Within (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Covers

Introduction: Return true if A covers B

Format: `ST_Covers (A:geometry, B:geometry)`

Since: `v1.3.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_CoveredBy

Introduction: Return true if A is covered by B

Format: `ST_CoveredBy (A:geometry, B:geometry)`

Since: `v1.3.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_CoveredBy(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```
