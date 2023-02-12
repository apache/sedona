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

## ST_Crosses

Introduction: Return true if A crosses B

Format: `ST_Crosses (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Crosses(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Disjoint

Introduction: Return true if A and B are disjoint

Format: `ST_Disjoint (A:geometry, B:geometry)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT *
FROM geom
WHERE ST_Disjoinnt(geom.geom_a, geom.geom_b)
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
