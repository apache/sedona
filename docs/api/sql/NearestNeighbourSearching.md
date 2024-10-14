
Sedona supports nearest-neighbour searching on geospatial data by providing a geospatial k-Nearest Neighbors (kNN) join method. This method involves identifying the k-nearest neighbors for a given spatial point or region based on geographic proximity, typically using spatial coordinates and a suitable distance metric like Euclidean or great-circle distance.

## ST_KNN

Introduction: join operation to find the k-nearest neighbors of a point or region in a spatial dataset.

Format: `ST_KNN(R: Table, S: Table, k: Integer, use_spheroid: Boolean)`

Where R is the queries side table and S is the object side table, K is the number of neighbors. use_spheroid is a boolean value that determines whether to use the spheroid distance or not.

Queries side table contains geometries that are used to find the k-nearest neighbors in the object side table.

When either queries or objects data contain non-point data (geometries), we take the centroid of each geometry.

In case there are ties in the distance, the result will include all the tied geometries only when the following sedona config is set to true:

```
spark.sedona.join.knn.includeTieBreakers=true
```

### Filter Pushdown Considerations:

When using ST_KNN with filters applied to the resulting DataFrame, some of these filters may be pushed down to the object side of the kNN join. This means the filters will be applied to the object side reader before the kNN join is executed. If you want the filters to be applied after the kNN join, ensure that you first materialize the kNN join results and then apply the filters.

For example, you can use the following approach:

Scala Example:

```
val knnResult = knnJoinDF.cache()
val filteredResult = knnResult.filter(condition)
```

SQL Example:

```
CREATE OR REPLACE TEMP VIEW knnResult AS
SELECT * FROM (
  -- Your KNN join SQL here
) AS knnView;
CACHE TABLE knnResult;
SELECT * FROM knnResult WHERE condition;
```

### Handling SQL-Defined Tables in ST_KNN Joins:

When creating DataFrames from hard-coded SQL select statements in Sedona, and later using them in `ST_KNN` joins, Sedona may attempt to optimize the query in a way that bypasses the intended kNN join logic. Specifically, if you create DataFrames with hard-coded SQL, such as:

```scala
val df1 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom1")
val df2 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom2")

val df = df1.join(df2, expr("ST_KNN(geom1, geom2, 1)"))
```

Sedona may optimize the join to a form like this:

```sql
SELECT ST_KNN(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0), 1)
```

As a result, the ST_KNN function is handled as a User-Defined Function (UDF) instead of a proper join operation, preventing Sedona from initiating the kNN join execution path. Unlike typical UDFs, the ST_KNN function operates on multiple rows across DataFrames, not just individual rows. When this occurs, the query fails with an UnsupportedOperationException, indicating that the KNN predicate is not supported.

Workaround:

To prevent Spark's optimization from bypassing the kNN join logic, the DataFrames created with hard-coded SQL select statements must be materialized before performing the join. By caching the DataFrames, you can instruct Spark to avoid this undesired optimization:

```scala
val df1 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom1").cache()
val df2 = sedona.sql("SELECT ST_Point(0.0, 0.0) as geom2").cache()

val df = df1.join(df2, expr("ST_KNN(geom1, geom2, 1)"))
```

Materializing the DataFrames with .cache() ensures that the correct kNN join path is followed in the Spark logical plan and prevents the optimization that would treat ST_KNN as a simple UDF.

### SQL Example

Suppose we have two tables `QUERIES` and `OBJECTS` with the following data:

QUERIES table:

```
ID  GEOMETRY            NAME
1   POINT(1 1)	        station1
2   POINT(10 10)	    station2
3   POINT(-0.5 -0.5)	station3
```

OBJECTS table:

```
ID  GEOMETRY            NAME
1	POINT(11 5)         bank1
2	POINT(12 1)         bank2
3	POINT(-1 -1)        bank3
4	POINT(-3 5)         bank4
5	POINT(9 8)          bank5
6	POINT(4 3)          bank6
7	POINT(-4 -5)        bank7
8	POINT(4 -2)         bank8
9	POINT(-3 1)         bank9
10	POINT(-7 3)         bank10
11	POINT(11 5)         bank11
12	POINT(12 1)         bank12
13	POINT(-1 -1)        bank13
14	POINT(-3 5)         bank14
15	POINT(9 8)          bank15
16	POINT(4 3)          bank16
17	POINT(-4 -5)        bank17
18	POINT(4 -2)         bank18
19	POINT(-3 1)         bank19
20	POINT(-7 3)         bank20
```

```sql
SELECT
    QUERIES.ID AS QUERY_ID,
    QUERIES.GEOMETRY AS QUERIES_GEOM,
    OBJECTS.GEOMETRY AS OBJECTS_GEOM
FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOMETRY, OBJECTS.GEOMETRY, 4, FALSE)
```

Output:

```
+--------+-----------------+-------------+
|QUERY_ID|QUERIES_GEOM     |OBJECTS_GEOM |
+--------+-----------------+-------------+
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (4 3)  |
|1       |POINT (1 1)      |POINT (4 3)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (11 5) |
|2       |POINT (10 10)    |POINT (11 5) |
+--------+-----------------+-------------+
```
