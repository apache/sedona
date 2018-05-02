## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT * 
FROM pointdf 
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT * 
FROM pointdf 
WHERE ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

## ST_Within

Introduction: Return true if A is fully contained by B

Format: `ST_Within (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT * 
FROM pointdf 
WHERE ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```