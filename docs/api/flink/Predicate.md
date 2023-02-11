## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A:geometry, B:geometry)`

Since: `v1.2.0`

SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Disjoint

Introduction: Return true if A and B are disjoint

Format: `ST_Disjoint (A:geometry, B:geometry)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT *
FROM pointdf 
WHERE ST_Disjoinnt(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A:geometry, B:geometry)`

Since: `v1.2.0`

SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Within

Introduction: Return true if A is within B

Format: `ST_Within (A:geometry, B:geometry)`

Since: `v1.3.0`

SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_OrderingEquals
Introduction: Returns true if the geometries are equal and the coordinates are in the same order

Format: `ST_OrderingEquals(A: geometry, B: geometry)`

Since: `v1.2.1`

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

## ST_Covers

Introduction: Return true if A covers B

Format: `ST_Covers (A:geometry, B:geometry)`

Since: `v1.3.0`

SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_CoveredBy

Introduction: Return true if A is covered by B

Format: `ST_CoveredBy (A:geometry, B:geometry)`

Since: `v1.3.0`

SQL example:
```sql
SELECT * 
FROM pointdf 
WHERE ST_CoveredBy(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```
