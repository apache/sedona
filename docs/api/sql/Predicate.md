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

## ST_Equals

Introduction: Return true if A equals to B

Format: `ST_Equals (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT * 
FROM pointdf 
WHERE ST_Equals(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Crosses

Introduction: Return true if A crosses B

Format: `ST_Crosses (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT * 
FROM pointdf 
WHERE ST_Crosses(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Touches

Introduction: Return true if A touches B

Format: `ST_Touches (A:geometry, B:geometry)`

Since: `v1.0.0`

```SQL
SELECT * 
FROM pointdf 
WHERE ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```

## ST_Overlaps

Introduction: Return true if A overlaps B

Format: `ST_Overlaps (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT *
FROM geom
WHERE ST_Overlaps(geom.geom_a, geom.geom_b)
```