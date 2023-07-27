## ST_Contains

Introduction: Return true if A fully contains B

Format: `ST_Contains (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:

```sql
SELECT ST_Contains(ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'), ST_GeomFromWKT('POINT(174 149)'))
```

Output:

```
false
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
SELECT ST_Equals(ST_GeomFromWKT('LINESTRING(0 0,10 10)'), ST_GeomFromWKT('LINESTRING(0 0,5 5,10 10)'))
```

Output:

```
true
```

## ST_Intersects

Introduction: Return true if A intersects B

Format: `ST_Intersects (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:

```sql
SELECT ST_Intersects(ST_GeomFromWKT('LINESTRING(-43.23456 72.4567,-43.23456 72.4568)'), ST_GeomFromWKT('POINT(-43.23456 72.4567772)'))
```

Output:

```
true
```

## ST_OrderingEquals
Introduction: Returns true if the geometries are equal and the coordinates are in the same order

Format: `ST_OrderingEquals(A: geometry, B: geometry)`

Since: `v1.2.1`

Spark SQL example 1:

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'))
```

Output: 

```
true
```

Spark SQL example 2:

```sql
SELECT ST_OrderingEquals(ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))'), ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))'))
```

Output: 

```
false
```

## ST_Overlaps

Introduction: Return true if A overlaps B

Format: `ST_Overlaps (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:

```sql
SELECT ST_Overlaps(ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))'), ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))'))
```

Output:

```
true
```

## ST_Touches

Introduction: Return true if A touches B

Format: `ST_Touches (A:geometry, B:geometry)`

Since: `v1.0.0`

Example:

```sql
SELECT ST_Touches(ST_GeomFromWKT('LINESTRING(0 0,1 1,0 2)'), ST_GeomFromWKT('POINT(0 2)'))
```

Output:

```
true
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
