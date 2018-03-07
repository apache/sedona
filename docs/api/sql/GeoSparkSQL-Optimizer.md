# GeoSparkSQL query optimizer
GeoSpark Spatial operators fully supports Apache SparkSQL query optimizer. It has the following query optimization features:
* Automatically optimizes range join query and distance join query.
* Automatically performs predicate pushdown.

## Range join
Introduction:

*Find geometries from A and geometries from B such that each geometry pair satisfies a certain predicate*

Spark SQL Example:

```
select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape)
select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape)
select * from pointdf, polygondf where ST_Within(pointdf.pointshape, polygondf.polygonshape)

```
Spark SQL Physical plan:
```
== Physical Plan ==
RangeJoin polygonshape#20: geometry, pointshape#43: geometry, false
:- Project [st_polygonfromenvelope(cast(_c0#0 as decimal(24,20)), cast(_c1#1 as decimal(24,20)), cast(_c2#2 as decimal(24,20)), cast(_c3#3 as decimal(24,20)), mypolygonid) AS polygonshape#20]
:  +- *FileScan csv
+- Project [st_point(cast(_c0#31 as decimal(24,20)), cast(_c1#32 as decimal(24,20)), myPointId) AS pointshape#43]
   +- *FileScan csv

```

## Distance join
Introduction:

*Find geometries from A and geometries from B such that the internal Euclidean distance of each geometry pair is less or equal than a certain distance*

Spark SQL Example:

*Only consider "fully within a certain distance"*
```
select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2
```

*Consider "intersects within a certain distance"*
```
select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2
```

Spark SQL Physical plan:
```
== Physical Plan ==
DistanceJoin pointshape1#12: geometry, pointshape2#33: geometry, 2.0, true
:- Project [st_point(cast(_c0#0 as decimal(24,20)), cast(_c1#1 as decimal(24,20)), myPointId) AS pointshape1#12]
:  +- *FileScan csv
+- Project [st_point(cast(_c0#21 as decimal(24,20)), cast(_c1#22 as decimal(24,20)), myPointId) AS pointshape2#33]
   +- *FileScan csv

```
## Predicate pushdown

Introduction:
*Given a join query and a predicate in the same WHERE clause, first executes the Predicate as a filter, then executes the join query*

Spark SQL Example:

```
select * from polygondf, pointdf 

where ST_Contains(polygondf.polygonshape,pointdf.pointshape)

and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)
```

Spark SQL Physical plan:

```
== Physical Plan ==
RangeJoin polygonshape#20: geometry, pointshape#43: geometry, false
:- Project [st_polygonfromenvelope(cast(_c0#0 as decimal(24,20)), cast(_c1#1 as decimal(24,20)), cast(_c2#2 as decimal(24,20)), cast(_c3#3 as decimal(24,20)), mypolygonid) AS polygonshape#20]
:  +- Filter  **org.apache.spark.sql.geosparksql.expressions.ST_Contains$**
:     +- *FileScan csv
+- Project [st_point(cast(_c0#31 as decimal(24,20)), cast(_c1#32 as decimal(24,20)), myPointId) AS pointshape#43]
   +- *FileScan csv
```