The page outlines the steps to manage spatial data using SedonaSQL. ==The example code is written in Java but also works for Scala==.

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:
```java
Table myTable = tableEnv.sqlQuery("YOUR_SQL")
```

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../../../api/flink/Overview)

## Set up dependencies

1. Read [Sedona Maven Central coordinates](../../../setup/maven-coordinates)
2. Add Sedona dependencies in build.sbt or pom.xml.
3. Add [Flink dependencies](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/) in build.sbt or pom.xml.
4. Please see [SQL example project](../../demo/)

## Initiate Stream Environment
Use the following code to initiate your `StreamExecutionEnvironment` at the beginning:
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
```

## Register SedonaSQL

Add the following line after your `StreamExecutionEnvironment` and `StreamTableEnvironment` declaration

```java
SedonaFlinkRegistrator.registerType(env);
SedonaFlinkRegistrator.registerFunc(tableEnv);
```

!!!warning
	Sedona has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

This function will register Sedona User Defined Type and User Defined Function

## Create a Geometry type column

All geometrical operations in SedonaSQL are on Geometry type objects. Therefore, before any kind of queries, you need to create a Geometry type column on a DataFrame.

Assume you have a Flink Table `tbl` like this:

```
+----+--------------------------------+--------------------------------+
| op |                   geom_polygon |                   name_polygon |
+----+--------------------------------+--------------------------------+
| +I | POLYGON ((-0.5 -0.5, -0.5 0... |                       polygon0 |
| +I | POLYGON ((0.5 0.5, 0.5 1.5,... |                       polygon1 |
| +I | POLYGON ((1.5 1.5, 1.5 2.5,... |                       polygon2 |
| +I | POLYGON ((2.5 2.5, 2.5 3.5,... |                       polygon3 |
| +I | POLYGON ((3.5 3.5, 3.5 4.5,... |                       polygon4 |
| +I | POLYGON ((4.5 4.5, 4.5 5.5,... |                       polygon5 |
| +I | POLYGON ((5.5 5.5, 5.5 6.5,... |                       polygon6 |
| +I | POLYGON ((6.5 6.5, 6.5 7.5,... |                       polygon7 |
| +I | POLYGON ((7.5 7.5, 7.5 8.5,... |                       polygon8 |
| +I | POLYGON ((8.5 8.5, 8.5 9.5,... |                       polygon9 |
+----+--------------------------------+--------------------------------+
10 rows in set
```

You can create a Table with a Geometry type column as follows:

```java
tableEnv.createTemporaryView("myTable", tbl)
Table geomTbl = tableEnv.sqlQuery("SELECT ST_GeomFromWKT(geom_polygon) as geom_polygon, name_polygon FROM myTable")
geomTbl.execute().print()
```

The output will be:

```
+----+--------------------------------+--------------------------------+
| op |                   geom_polygon |                   name_polygon |
+----+--------------------------------+--------------------------------+
| +I | POLYGON ((-0.5 -0.5, -0.5 0... |                       polygon0 |
| +I | POLYGON ((0.5 0.5, 0.5 1.5,... |                       polygon1 |
| +I | POLYGON ((1.5 1.5, 1.5 2.5,... |                       polygon2 |
| +I | POLYGON ((2.5 2.5, 2.5 3.5,... |                       polygon3 |
| +I | POLYGON ((3.5 3.5, 3.5 4.5,... |                       polygon4 |
| +I | POLYGON ((4.5 4.5, 4.5 5.5,... |                       polygon5 |
| +I | POLYGON ((5.5 5.5, 5.5 6.5,... |                       polygon6 |
| +I | POLYGON ((6.5 6.5, 6.5 7.5,... |                       polygon7 |
| +I | POLYGON ((7.5 7.5, 7.5 8.5,... |                       polygon8 |
| +I | POLYGON ((8.5 8.5, 8.5 9.5,... |                       polygon9 |
+----+--------------------------------+--------------------------------+
10 rows in set
```

Although it looks same with the input, actually the type of column geom_polygon has been changed to ==Geometry== type.

To verify this, use the following code to print the schema of the DataFrame:

```java
geomTbl.printSchema()
```

The output will be like this:

```
(
  `geom_polygon` RAW('org.locationtech.jts.geom.Geometry', '...'),
  `name_polygon` STRING
)
```

!!!note
	SedonaSQL provides lots of functions to create a Geometry column, please read [SedonaSQL constructor API](../../../api/flink/Constructor).

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in SedonaSQL is same as the unit of all geometries in a Geometry column.

To convert Coordinate Reference System of the Geometry column created before, use the following code:

```java
Table geomTbl3857 = tableEnv.sqlQuery("SELECT ST_Transform(countyshape, "epsg:4326", "epsg:3857") AS geom_polygon, name_polygon FROM myTable")
geomTbl3857.execute().print()
```

The first EPSG code EPSG:4326 in `ST_Transform` is the source CRS of the geometries. It is WGS84, the most common degree-based CRS.

The second EPSG code EPSG:3857 in `ST_Transform` is the target CRS of the geometries. It is the most common meter-based CRS.

This `ST_Transform` transform the CRS of these geometries from EPSG:4326 to EPSG:3857. The details CRS information can be found on [EPSG.io](https://epsg.io/)

!!!note
	Read [SedonaSQL ST_Transform API](../../../api/flink/Function/#st_transform) to learn different spatial query predicates.

For example, a Table that has coordinates in the US will become like this.

Before the transformation:
```
+----+--------------------------------+--------------------------------+
| op |                     geom_point |                     name_point |
+----+--------------------------------+--------------------------------+
| +I |                POINT (32 -118) |                          point |
| +I |                POINT (33 -117) |                          point |
| +I |                POINT (34 -116) |                          point |
| +I |                POINT (35 -115) |                          point |
| +I |                POINT (36 -114) |                          point |
| +I |                POINT (37 -113) |                          point |
| +I |                POINT (38 -112) |                          point |
| +I |                POINT (39 -111) |                          point |
| +I |                POINT (40 -110) |                          point |
| +I |                POINT (41 -109) |                          point |
+----+--------------------------------+--------------------------------+
```

After the transformation:

```
+----+--------------------------------+--------------------------------+
| op |                            _c0 |                     name_point |
+----+--------------------------------+--------------------------------+
| +I | POINT (-13135699.91360628 3... |                          point |
| +I | POINT (-13024380.422813008 ... |                          point |
| +I | POINT (-12913060.932019735 ... |                          point |
| +I | POINT (-12801741.44122646 4... |                          point |
| +I | POINT (-12690421.950433187 ... |                          point |
| +I | POINT (-12579102.459639912 ... |                          point |
| +I | POINT (-12467782.96884664 4... |                          point |
| +I | POINT (-12356463.478053367 ... |                          point |
| +I | POINT (-12245143.987260092 ... |                          point |
| +I | POINT (-12133824.496466817 ... |                          point |
+----+--------------------------------+--------------------------------+
```

After creating a Geometry type column, you are able to run spatial queries.

## Range query

Use ==ST_Contains==, ==ST_Intersects== and so on to run a range query over a single column.

The following example finds all counties that are within the given polygon:

```java
geomTable = tableEnv.sqlQuery(
  "
    SELECT *
    FROM spatialdf
    WHERE ST_Contains (ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape)
  ")
geomTable.execute().print()
```

!!!note
	Read [SedonaSQL Predicate API](../../../api/flink/Predicate) to learn different spatial query predicates.
	
## KNN query

Use ==ST_Distance== to calculate the distance and rank the distance.

The following code returns the 5 nearest neighbor of the given polygon.

```java
geomTable = tableEnv.sqlQuery(
  "
    SELECT countyname, ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape) AS distance
    FROM geomTable
    ORDER BY distance DESC
    LIMIT 5
  ")
geomTable.execute().print()
```

## Join query

This equi-join leverages Flink's internal equi-join algorithm. You can opt to skip the Sedona refinement step  by sacrificing query accuracy. A running example is in [SQL example project](../../demo/).

Please use the following steps:

### 1. Generate S2 ids for both tables

Use [ST_S2CellIds](../../../api/flink/Function/#st_s2cellids) to generate cell IDs. Each geometry may produce one or more IDs.

```sql
SELECT id, geom, name, ST_S2CellIDs(geom, 15) as idarray
FROM lefts
```

```sql
SELECT id, geom, name, ST_S2CellIDs(geom, 15) as idarray
FROM rights
```

### 2. Explode id array

The produced S2 ids are arrays of integers. We need to explode these Ids to multiple rows so later we can join two tables by ids.

```
SELECT id, geom, name, cellId
FROM lefts CROSS JOIN UNNEST(lefts.idarray) AS tmpTbl1(cellId)
```

```
SELECT id, geom, name, cellId
FROM rights CROSS JOIN UNNEST(rights.idarray) AS tmpTbl2(cellId)
```

### 3. Perform equi-join

Join the two tables by their S2 cellId

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
FROM lcs JOIN rcs ON lcs.cellId = rcs.cellId
```

### 4. Optional: Refine the result

Due to the nature of S2 Cellid, the equi-join results might have a few false-positives depending on the S2 level you choose. A smaller level indicates bigger cells, less exploded rows, but more false positives.

To ensure the correctness, you can use one of the [Spatial Predicates](../../../api/Predicate/) to filter out them. Use this query as the query in Step 3.

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
FROM lcs, rcs
WHERE lcs.cellId = rcs.cellId AND ST_Contains(lcs.geom, rcs.geom)
```

As you see, compared to the query in Step 2, we added one more filter, which is `ST_Contains`, to remove false positives. You can also use `ST_Intersects` and so on.

!!!tip
	You can skip this step if you don't need 100% accuracy and want faster query speed.

### 5. Optional: De-duplcate

Due to the explode function used when we generate S2 Cell Ids, the resulting DataFrame may have several duplicate <lcs_geom, rcs_geom> matches. You can remove them by performing a GroupBy query.

```sql
SELECT lcs_id, rcs_id , FIRST_VALUE(lcs_geom), FIRST_VALUE(lcs_name), first(rcs_geom), first(rcs_name)
FROM joinresult
GROUP BY (lcs_id, rcs_id)
```

The `FIRST_VALUE` function is to take the first value from a number of duplicate values.

If you don't have a unique id for each geometry, you can also group by geometry itself. See below:

```sql
SELECT lcs_geom, rcs_geom, first(lcs_name), first(rcs_name)
FROM joinresult
GROUP BY (lcs_geom, rcs_geom)
```

!!!note
	If you are doing point-in-polygon join, this is not a problem and you can safely discard this issue. This issue only happens when you do polygon-polygon, polygon-linestring, linestring-linestring join.

### S2 for distance join

This also works for distance join. You first need to use `ST_Buffer(geometry, distance)` to wrap one of your original geometry column. If your original geometry column contains points, this `ST_Buffer` will make them become circles with a radius of `distance`.

For example. run this query first on the left table before Step 1.

```sql
SELECT id, ST_Buffer(geom, DISTANCE), name
FROM lefts
```

Since the coordinates are in the longitude and latitude system, so the unit of `distance` should be degree instead of meter or mile. You will have to estimate the corresponding degrees based on your meter values. Please use [this calculator](https://lucidar.me/en/online-unit-converter-length-to-angle/convert-degrees-to-meters/#online-converter).

## Convert Spatial Table to Spatial DataStream

### Get DataStream

Use TableEnv's toDataStream function

```java
DataStream<Row> geomStream = tableEnv.toDataStream(geomTable)
```

### Retrieve Geometries

Then get the Geometry from each Row object using Map

```java
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = geomStream.map(new MapFunction<Row, Geometry>() {
            @Override
            public Geometry map(Row value) throws Exception {
                return (Geometry) value.getField(0);
            }
        });
geometries.print();
```

The output will be

```
14> POLYGON ((1.5 1.5, 1.5 2.5, 2.5 2.5, 2.5 1.5, 1.5 1.5))
2> POLYGON ((5.5 5.5, 5.5 6.5, 6.5 6.5, 6.5 5.5, 5.5 5.5))
5> POLYGON ((8.5 8.5, 8.5 9.5, 9.5 9.5, 9.5 8.5, 8.5 8.5))
16> POLYGON ((3.5 3.5, 3.5 4.5, 4.5 4.5, 4.5 3.5, 3.5 3.5))
12> POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))
13> POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))
15> POLYGON ((2.5 2.5, 2.5 3.5, 3.5 3.5, 3.5 2.5, 2.5 2.5))
3> POLYGON ((6.5 6.5, 6.5 7.5, 7.5 7.5, 7.5 6.5, 6.5 6.5))
1> POLYGON ((4.5 4.5, 4.5 5.5, 5.5 5.5, 5.5 4.5, 4.5 4.5))
4> POLYGON ((7.5 7.5, 7.5 8.5, 8.5 8.5, 8.5 7.5, 7.5 7.5))
```

### Store non-spatial attributes in Geometries

You can concatenate other non-spatial attributes and store them in Geometry's `userData` field so you can recover them later on. `userData` field can be any object type.

```java
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = geomStream.map(new MapFunction<Row, Geometry>() {
            @Override
            public Geometry map(Row value) throws Exception {
                Geometry geom = (Geometry) value.getField(0);
                geom.setUserData(value.getField(1));
                return geom;
            }
        });
geometries.print();
```

The `print` command will not print out `userData` field. But you can get it this way:

```java
import org.locationtech.jts.geom.Geometry;

geometries.map(new MapFunction<Geometry, String>() {
            @Override
            public String map(Geometry value) throws Exception
            {
                return (String) value.getUserData();
            }
        }).print();
```

The output will be

```
13> polygon9
6> polygon2
10> polygon6
11> polygon7
5> polygon1
12> polygon8
8> polygon4
4> polygon0
7> polygon3
9> polygon5
```
	
## Convert Spatial DataStream to Spatial Table

### Create Geometries using Sedona FormatUtils

* Create a Geometry from a WKT string

```java
import org.apache.sedona.core.formatMapper.FormatUtils;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
                FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKT, false);
                return formatUtils.readGeometry(value);
            }
        })
```

* Create a Point from a String `1.1, 2.2`. Use `,` as the delimiter.

```java
import org.apache.sedona.core.formatMapper.FormatUtils;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
                FormatUtils<Geometry> formatUtils = new FormatUtils(",", false, GeometryType.POINT);
                return formatUtils.readGeometry(value);
            }
        })
```

* Create a Polygon from a String `1.1, 1.1, 10.1, 10.1`. This is a rectangle with (1.1, 1.1) and (10.1, 10.1) as their min/max corners.

```java
import org.apache.sedona.core.formatMapper.FormatUtils;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Geometry;

DataStream<Geometry> geometries = text.map(new MapFunction<String, Geometry>() {
            @Override
            public Geometry map(String value) throws Exception
            {
            	  // Write some code to get four double type values: minX, minY, maxX, maxY
            	  ...
	            Coordinate[] coordinates = new Coordinate[5];
	            coordinates[0] = new Coordinate(minX, minY);
	            coordinates[1] = new Coordinate(minX, maxY);
	            coordinates[2] = new Coordinate(maxX, maxY);
	            coordinates[3] = new Coordinate(maxX, minY);
	            coordinates[4] = coordinates[0];
	            GeometryFactory geometryFactory = new GeometryFactory();
	            return geometryFactory.createPolygon(coordinates);
            }
        })
```

### Create Row objects

Put a geometry in a Flink Row to a `geomStream`. Note that you can put other attributes in Row as well. This example uses a constant value `myName` for all geometries.

```java
import org.apache.sedona.core.formatMapper.FormatUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.flink.types.Row;

DataStream<Row> geomStream = text.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception
            {
                FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKT, false);
                return Row.of(formatUtils.readGeometry(value), "myName");
            }
        })
```

### Get Spatial Table

Use TableEnv's fromDataStream function, with two column names `geom` and `geom_name`.
```java
Table geomTable = tableEnv.fromDataStream(geomStream, "geom", "geom_name")
```
