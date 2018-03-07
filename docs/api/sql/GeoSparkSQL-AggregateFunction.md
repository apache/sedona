### ST_Envelope_Aggr (A:geometryColumn)

Introduction:

*Return the entire envelope boundary of all geometries in A*

Since: v1.0.0

Spark SQL example:
```
select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf
```

### ST_Union_Aggr (A:geometryColumn)

Introduction:

Since: v1.0.0

*Return the polygon union of all polygons in A*

Spark SQL example:
```
select ST_Union_Aggr(polygondf.polygonshape) from polygondf
```