## ST_Envelope_Aggr

Introduction: Return the entire envelope boundary of all geometries in A

Format: `ST_Envelope_Aggr (A: geometryColumn)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Envelope_Aggr(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
POLYGON ((1.1 101.1, 1.1 120.1, 20.1 120.1, 20.1 101.1, 1.1 101.1))
```

## ST_Intersection_Aggr

Introduction: Return the polygon intersection of all polygons in A

Format: `ST_Intersection_Aggr (A: geometryColumn)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Intersection_Aggr(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
MULTIPOINT ((1.1 101.1), (2.1 102.1), (3.1 103.1), (4.1 104.1), (5.1 105.1), (6.1 106.1), (7.1 107.1), (8.1 108.1), (9.1 109.1), (10.1 110.1))
```

## ST_Union_Aggr

Introduction: Return the polygon union of all polygons in A

Format: `ST_Union_Aggr (A: geometryColumn)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Union_Aggr(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
MULTIPOINT ((1.1 101.1), (2.1 102.1), (3.1 103.1), (4.1 104.1), (5.1 105.1), (6.1 106.1), (7.1 107.1), (8.1 108.1), (9.1 109.1), (10.1 110.1))
```
