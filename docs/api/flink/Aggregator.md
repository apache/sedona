## ST_Envelope_Aggr

Introduction: Return the entire envelope boundary of all geometries in A

Format: `ST_Envelope_Aggr (A:geometryColumn)`

Since: `v1.3.0`

SQL example:
```sql
SELECT ST_Envelope_Aggr(pointdf.arealandmark)
FROM pointdf
```

## ST_Union_Aggr

Introduction: Return the polygon union of all polygons in A. All inputs must be polygons.

Format: `ST_Union_Aggr (A:geometryColumn)`

Since: `v1.3.0`

SQL example:
```sql
SELECT ST_Union_Aggr(polygondf.polygonshape)
FROM polygondf
```