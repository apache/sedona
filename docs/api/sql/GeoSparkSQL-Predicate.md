### ST_Contains (A:geometry, B:geometry)
Introduction:

*Return true if A fully contains B*

Since: v1.0.0

Spark SQL example:
```
select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

### ST_Intersects (A:geometry, B:geometry)

Introduction:

*Return true if A intersects B*

Since: v1.0.0

Spark SQL example:
```
select * from pointdf where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```

### ST_Within (A:geometry, B:geometry)

Introduction:

*Return true if A is fully contained by B*

Since: v1.0.0

Spark SQL example:
```
select * from pointdf where ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))
```