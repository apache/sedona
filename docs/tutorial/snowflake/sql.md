<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

After the installation done, you can start using Sedona functions. Please log in to Snowflake again using the user that has the privilege to access the database.

!!!note
	Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## Accessing Sedona functions in snowflake

First make sure to point to the correct functions. By default, the Sedona functions should be accessible in db.schema `SEDONASNOW.SEDONA`.

```sql
USE DATABASE SEDONASNOW;
```

Alternatively, use another database and specify the fully qualified name e.g. `SEDONASNOW.SEDONA.ST_GeomFromText`

## Create a sample table

Let's create a `city_tbl` that contains the locations and names of cities. Each location is a WKT string.

```sql
CREATE OR REPLACE TABLE city_tbl (wkt STRING, city_name STRING);
INSERT INTO city_tbl(wkt, city_name) VALUES ('POINT (-122.33 47.61)', 'Seattle');
INSERT INTO city_tbl(wkt, city_name) VALUES ('POINT (-122.42 37.76)', 'San Francisco');
```

Then we can show the content of this table:

```sql
SELECT *
FROM city_tbl;
```

Output:

```
WKT	CITY_NAME
POINT (-122.33 47.61)	Seattle
POINT (-122.42 37.76)	San Francisco
```

## Create a Geometry/Geography column

All geometrical operations in SedonaSQL are on Geometry/Geography type objects. Therefore, before any kind of queries, you need to create a Geometry/Geography type column on the table.

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT SEDONA.ST_GeomFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

The `geom` column Table `city_tbl_geom` is now in a `Binary` type and data in this column is in a format that can be understood by Sedona. The output of this query will show geometries in WKB binary format like this:

```
GEOM CITY_NAME
010100000085eb51b81e955ec0ae47e17a14ce4740  Seattle
01010000007b14ae47e19a5ec0e17a14ae47e14240  San Francisco
```

To view the content of this column in a human-readable format, you can use `SEDONA.ST_AsText`. For example,

```sql
SELECT SEDONA.ST_AsText(geom), city_name
FROM city_tbl_geom
```

Alternatively, you can also create Snowflake native Geometry and Geography type columns. For example, you can create a Snowflake native Geometry type column as follows (note the function has no `SEDONA` prefix):

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT ST_GeometryFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

The following code creates a Snowflake native Geography type column (note the function has no `SEDONA` prefix):

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT ST_GeographyFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

!!!note
	SedonaSQL provides lots of functions to create a Geometry column, please read [SedonaSQL API](../../api/snowflake/vector-data/Constructor.md).

## Check the lon/lat order

In SedonaSnow `v1.4.1` and before, we use lat/lon order in the following functions:

* SEDONA.ST_Transform
* SEDONA.ST_DistanceSphere
* SEDONA.ST_DistanceSpheroid

We use `lon/lat` order in the following functions:

* SEDONA.ST_GeomFromGeoHash
* SEDONA.ST_GeoHash
* SEDONA.ST_S2CellIDs

In Sedona `v1.5.0` and above, all functions will be fixed to lon/lat order.

If your original data is not in the order you want, you need to flip the coordinate using `SEDONA.ST_FlipCoordinates(geom: Geometry)`.

The sample data used above is in lon/lat order, we can flip the coordinates as follows:

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT SEDONA.ST_FlipCoordinates(geom) AS geom, city_name
FROM city_tbl_geom
```

If we show the content of this table, it is now in lat/lon order:

```sql
SELECT SEDONA.ST_AsText(geom), city_name
FROM city_tbl_geom
```

Output:

```
GEOM	CITY_NAME
POINT (47.61 -122.33)	Seattle
POINT (37.76 -122.42)	San Francisco
```

## Save as an ordinary column

To save a table to some permanent storage, you can simply convert each geometry in the Geometry type column back to a plain String and save it anywhere you want.

Use the following code to convert the Geometry column in a table back to a WKT string column:

```sql
SELECT SEDONA.ST_AsText(geom)
FROM city_tbl_geom
```

!!!note
	SedonaSQL provides lots of functions to save the Geometry column, please read [SedonaSQL API](../../api/snowflake/vector-data/Function.md).

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in SedonaSQL is same as the unit of all geometries in a Geometry column.

To convert Coordinate Reference System of the Geometry column created before, use `SEDONA.ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string)`

The first EPSG code EPSG:4326 in `SEDONA.ST_Transform` is the source CRS of the geometries. It is WGS84, the most common degree-based CRS.

The second EPSG code EPSG:3857 in `SEDONA.ST_Transform` is the target CRS of the geometries. It is the most common meter-based CRS.

This `SEDONA.ST_Transform` transform the CRS of these geometries from EPSG:4326 to EPSG:3857. The details CRS information can be found on [EPSG.io](https://epsg.io/).

!!!note
	This function follows lon/order in 1.5.0+ and lat/lon order in 1.4.1 and before. You can use `SEDONA.ST_FlipCoordinates` to swap X and Y.

We can transform our sample data as follows

```sql
SELECT SEDONA.ST_AsText(SEDONA.ST_Transform(geom, 'epsg:4326', 'epsg:3857')), city_name
FROM city_tbl_geom
```

The output will be like this:

```
POINT (6042216.250411431 -13617713.308741156)  Seattle
POINT (4545577.120361927 -13627732.06291255)  San Francisco
```

`SEDONA.ST_Transform` also supports the CRS string in OGC WKT format. For example, the following query generates the same output but with a OGC WKT CRS string.

```sql
SELECT SEDONA.ST_AsText(SEDONA.ST_Transform(geom, 'epsg:4326', 'PROJCS["WGS 84 / Pseudo-Mercator",
     GEOGCS["WGS 84",
         DATUM["WGS_1984",
             SPHEROID["WGS 84",6378137,298.257223563,
                 AUTHORITY["EPSG","7030"]],
             AUTHORITY["EPSG","6326"]],
         PRIMEM["Greenwich",0,
             AUTHORITY["EPSG","8901"]],
         UNIT["degree",0.0174532925199433,
             AUTHORITY["EPSG","9122"]],
         AUTHORITY["EPSG","4326"]],
     PROJECTION["Mercator_1SP"],
     PARAMETER["central_meridian",0],
     PARAMETER["scale_factor",1],
     PARAMETER["false_easting",0],
     PARAMETER["false_northing",0],
     UNIT["metre",1,
         AUTHORITY["EPSG","9001"]],
     AXIS["Easting",EAST],
     AXIS["Northing",NORTH],
     EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs"],
     AUTHORITY["EPSG","3857"]]')), city_name
FROM city_tbl_geom
```

## Range query

Use ==SEDONA.ST_Contains==, ==SEDONA.ST_Intersects==, ==SEDONA.ST_Within== to run a range query over a single column.

The following example finds all geometries that are within the given polygon:

```sql
SELECT *
FROM city_tbl_geom
WHERE SEDONA.ST_Contains(SEDONA.ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), geom)
```

!!!note
	Read [SedonaSQL API](../../api/snowflake/vector-data/Constructor.md) to learn how to create a Geometry type query window.

## KNN query

Use ==SEDONA.ST_Distance==, ==SEDONA.ST_DistanceSphere==, ==ST_DistanceSpheroid== to calculate the distance and rank the distance.

The following code returns the 5 nearest neighbor of the given point.

```sql
SELECT geom, SEDONA.ST_Distance(SEDONA.ST_Point(1.0, 1.0), geom) AS distance
FROM city_tbl_geom
ORDER BY distance DESC
LIMIT 5
```

## Range join query

!!!warning
	Sedona range join in Snowflake does not trigger Sedona's optimized spatial join algorithm while Sedona Spark does. It uses Snowflake's default Cartesian join which is very slow. Therefore, it is recommended to use Sedona's S2-based join or Snowflake's native ST functions + native `Geography` type to do range join, which will trigger Snowflake's `GeoJoin` algorithm.

Introduction: Find geometries from A and geometries from B such that each geometry pair satisfies a certain predicate.

Example:

Create the illustrative tables:

```sql
CREATE OR REPLACE TABLE polygondf AS
SELECT SEDONA.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') polygonshape;
```

```sql
CREATE OR REPLACE TABLE pointdf AS
SELECT SEDONA.ST_GeomFromText('POINT(0.5 0.5)') pointshape;
```

Now run the following queries with different spatial predicates:

```sql
SELECT *
FROM polygondf, pointdf
WHERE SEDONA.ST_Contains(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM polygondf, pointdf
WHERE SEDONA.ST_Intersects(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM pointdf, polygondf
WHERE SEDONA.ST_Within(pointdf.pointshape, polygondf.polygonshape)
```

The corresponding (faster) spatial joins use only native Snowflake functions:

```sql
WITH polygondf AS (
    SELECT ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') polygonshape
),
pointdf AS (
    SELECT ST_GeomFromText('POINT(0.5 0.5)') pointshape
)
SELECT *
FROM polygondf, pointdf
WHERE ST_Contains(polygondf.polygonshape,pointdf.pointshape)
```

This also work natively with other predicates like `ST_Intersects` and `ST_Within`.

## Distance join

!!!warning
	Sedona distance join in Snowflake does not trigger Sedona's optimized spatial join algorithm while Sedona Spark does. It uses Snowflake's default Cartesian join which is very slow. Therefore, it is recommended to use Sedona's S2-based join or Snowflake's native ST functions + native `Geography` type to do range join, which will trigger Snowflake's `GeoJoin` algorithm.

Introduction: Find geometries from A and geometries from B such that the distance of each geometry pair is less or equal than a certain distance. It supports the planar Euclidean distance calculators `SEDONA.ST_Distance`, `SEDONA.ST_HausdorffDistance`, `SEDONA.ST_FrechetDistance` and the meter-based geodesic distance calculators `SEDONA.ST_DistanceSpheroid` and `SEDONA.ST_DistanceSphere`. Snowflake only natively supports `ST_Distance`.

First create two new illustrative tables with a single geometry.

```sql
CREATE OR REPLACE TABLE pointdf2 AS
SELECT SEDONA.ST_GeomFromText('POINT(0 0)') pointshape;
```

```sql
CREATE OR REPLACE TABLE polygondf2 AS
SELECT SEDONA.ST_GeomFromText('POLYGON((0.5 0.5, 0.5 1, 1 1, 1 0.5, 0.5 0.5))') polygonshape;
```

Examples for planar Euclidean distances:

The usual l2 Euclidean distance between `POINT(0 0)` and `POINT(0.5 0.5)` points is the square root of 0.5, so the following returns the single pair of points.

```sql
SELECT *
FROM pointdf, pointdf2
WHERE SEDONA.ST_Distance(pointdf.pointshape,pointdf2.pointshape) <= sqrt(0.5)
```

The l2 distance between two polygons that are not disjoint is 0, so the following returns the single polygon pair

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_Distance(polygondf.polygonshape,polygondf2.polygonshape) <= 0
```

This is not necessarily the case of the Hausdorff or Fréchet distances. The following queries return no rows:

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_HausdorffDistance(polygondf.polygonshape, polygondf2.polygonshape, 0.3) <= 0
```

```sql
SELECT *
FROM polygondf, polygondf2
WHERE SEDONA.ST_FrechetDistance(polygondf.polygonshape, polygondf2.polygonshape)  <= 0
```

Note that only the Hausdorff distance takes in a third `densityFraction` parameter in $(0,1]$ with smaller values giving more accurate results.

!!!warning
	If you use planar Euclidean distance functions like `SEDONA.ST_Distance`, `SEDONA.ST_HausdorffDistance` or `SEDONA.ST_FrechetDistance` as the predicate, Sedona doesn't control the distance's unit (degree or meter). It is same with the geometry. If your coordinates are in the longitude and latitude system, the unit of `distance` should be degree instead of meter or mile. To change the geometry's unit, please either transform the coordinate reference system to a meter-based system. See [SEDONA.ST_Transform](../../api/snowflake/vector-data/Function.md#st_transform). If you don't want to transform your data, please consider using `SEDONA.ST_DistanceSpheroid` or `SEDONA.ST_DistanceSphere`.

For instance, the following returns roughly 78.45 kilometers, since the geometries are assumed to be in degrees of longitude and latitude, even if no CRS was explicitly set.

```sql
SELECT SEDONA.ST_DistanceSpheroid(pointdf.pointshape,pointdf2.pointshape)
FROM pointdf, pointdf2
```

As the following query illustrates, running Snowflake's native `ST_DISTANCE` on `GEOGRAPHY` objects will return a result similar to `SEDONA.ST_DistanceSphere` and `SEDONA.ST_DistanceSpheroid`:

```sql
SELECT
    SEDONA.ST_DistanceSphere(
        pointdf.pointshape,
        pointdf2.pointshape
    ) SEDONA_ST_DistanceSphere,
    --
    SEDONA.ST_DistanceSpheroid(
        pointdf.pointshape,
        pointdf2.pointshape
    ) SEDONA_ST_DistanceSpheroid,
    --
    ST_DISTANCE(
        TO_GEOGRAPHY(SEDONA.st_astext(pointdf.pointshape)),
        TO_GEOGRAPHY(SEDONA.st_astext(pointdf2.pointshape) )
    ) SFKL_ST_DISTANCE
FROM pointdf, pointdf2
```

Output:

| SEDONA_ST_DISTANCESPHERE | SEDONA_ST_DISTANCESPHEROID | SFKL_ST_DISTANCE |
| ------------------------ | -------------------------- | ---------------- |
| 78626.28640698           | 78451.248031239            | 78626.311089506  |

## Google S2 based approximate equi-join

You can use Sedona's built-in Google S2 functions to perform an approximate equi-join. This algorithm leverages Snowflake's internal equi-join algorithm and might be performant given that you can opt to skip the refinement step  by sacrificing query accuracy. Sedona's S2 functions are a nice complement to Snowflake's native H3 and Geohash functions.

Please use the following steps:

### 1. Generate S2 ids for both tables

Use [SEDONA.ST_S2CellIds](../../api/snowflake/vector-data/Function.md#st_s2cellids) to generate cell IDs. Each geometry may produce one or more IDs.

```sql
SELECT * FROM lefts, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(lefts.geom, 15))) s1
```

```sql
SELECT * FROM rights, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(rights.geom, 15))) s2
```

### 2. Perform equi-join

Join the two tables by their S2 cellId

```sql
SELECT lcs.id AS lcs_id, lcs.geom AS lcs_geom, lcs.name AS lcs_name, rcs.id AS rcs_id, rcs.geom AS rcs_geom, rcs.name AS rcs_name
FROM lcs JOIN rcs ON lcs.cellId = rcs.cellId
```

### 3. Optional: Refine the result

Due to the nature of S2 Cellid, the equi-join results might have a few false-positives depending on the S2 level you choose. A smaller level indicates bigger cells, less exploded rows, but more false positives.

To ensure the correctness, you can use one of the [Spatial Predicates](../../api/snowflake/vector-data/Predicate.md) to filter out them. Use this query instead of the query in Step 2.

```sql
SELECT lcs.id AS lcs_id, lcs.geom AS lcs_geom, lcs.name AS lcs_name, rcs.id AS rcs_id, rcs.geom AS rcs_geom, rcs.name AS rcs_name
FROM lcs, rcs
WHERE lcs.cellId = rcs.cellId AND ST_Contains(lcs.geom, rcs.geom)
```

As you see, compared to the query in Step 2, we added one more filter, which is `ST_Contains`, to remove false positives. You can also use `ST_Intersects` and so on.

!!!tip
	You can skip this step if you don't need 100% accuracy and want faster query speed.

### 4. Optional: De-duplicate

Due to the `Flatten` function used when we generate S2 Cell Ids, the resulting DataFrame may have several duplicate <lcs_geom, rcs_geom> matches. You can remove them by performing a GroupBy query.

```sql
SELECT lcs_id, rcs_id, ANY_VALUE(lcs_geom), ANY_VALUE(lcs_name), ANY_VALUE(rcs_geom), ANY_VALUE(rcs_name)
FROM joinresult
GROUP BY (lcs_id, rcs_id)
```

The `ANY_VALUE` function is to take the first value from a number of duplicate values.

If you don't have a unique id for each geometry, you can also group by geometry itself. See below:

```sql
SELECT lcs_geom, rcs_geom, ANY_VALUE(lcs_name), ANY_VALUE(rcs_name)
FROM joinresult
GROUP BY (lcs_geom, rcs_geom)
```

!!!note
	If you are doing point-in-polygon join, this is not a problem, and you can safely discard this issue. This issue only happens when you do polygon-polygon, polygon-linestring, linestring-linestring join.

### 5. Putting it all together

The following queries creates 2 regions of interests and 5 different spatial queries in the form of bounding boxes in longitude, latitude. It then computes S2 coverings of both tables and merges on the S2 cell ID. A spatial filter can be applied to the original geometries to guarantee the exactness of the join.

The following example uses Snowflake's native `ST_GeogFromText` (treated as geographies), but also works with `ST_GeomFromText`. In both cases, note `SEDONA.ST_S2CellIDs` takes in a Snowflake `GEOGRAPHY` or `GEOMETRY` object along with an integer S2 precision.

```sql
-- lng/lat format: no need to flip
-- lefts are area of interest (AOI)
with lefts AS (
  SELECT index AS poly_id_left, value AS wkt FROM TABLE(
      SPLIT_TO_TABLE (
          'POLYGON ((-74.64966372842101805 44.92318068906040196, -73.05513946490677313 44.92318068906040196, -73.05513946490677313 45.9127817308399031, -74.64966372842101805 45.9127817308399031, -74.64966372842101805 44.92318068906040196))|POLYGON ((-71.72125014775386376 46.58534825561803672, -70.72763860520016976 46.58534825561803672, -70.72763860520016976 47.26007441306773416, -71.72125014775386376 47.26007441306773416, -71.72125014775386376 46.58534825561803672))', '|'
          )
      )

),
-- rights are queries
-- 1st polygon intersects both AOI
-- 2nd polygon contains both AOI
-- 3rd polygon is contained by AOI 1
-- 4th polygon touches AOI 1
-- 5th polygon is disjoint from both
rights AS (
  SELECT index AS poly_id_right, value AS wkt FROM TABLE(
      SPLIT_TO_TABLE (
          'POLYGON ((-73.51160030163114811 45.54300066590783302, -71.31736631503980561 45.54300066590783302, -71.31736631503980561 46.82515071005796869, -73.51160030163114811 46.82515071005796869, -73.51160030163114811 45.54300066590783302))|POLYGON ((-75.26522832 44.23585380, -69.77500453509208 44.23585380, -69.77500453509208 47.59180373699041411, -75.26522832 47.59180373699041411, -75.26522832 44.23585380))|POLYGON ((-73.82001807503861812 45.0759163125215423, -73.34011383205873358 45.0759163125215423, -73.34011383205873358 45.35768613572972185, -73.82001807503861812 45.35768613572972185, -73.82001807503861812 45.0759163125215423))|POLYGON ((-74.64966372842101805 44.92318068906040196, -75.26522832 44.92318068906040196, -75.26522832 44.23585380, -74.64966372842101805 44.23585380, -74.64966372842101805 44.92318068906040196))|POLYGON ((-71.65791191749059408 44.19369241163229844, -70.2497216546401404 44.19369241163229844, -70.2497216546401404 44.96946189775351144, -71.65791191749059408 44.96946189775351144, -71.65791191749059408 44.19369241163229844))', '|'
          )
      )
),
-- compute s2 cells covering the polygons for both tables
-- S2 discretizes the polygons, hence higher resolution yields more accurate queries, at the cost of increased computation
-- both tables need to be discretized at the same level - in this case 10
lefts_s2 AS (
    SELECT * FROM lefts, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(ST_GeogFromText(lefts.wkt), 10)))
),
rights_s2 AS (
    SELECT * FROM rights, TABLE(FLATTEN(SEDONA.ST_S2CellIDs(ST_GeogFromText(rights.wkt), 10)))
)
-- merge on s2 index (int) and group by to retrieve the original polygons rather than the s2 cells
-- add the spatial predicate to be exact and omit if speed is more important
-- expect all queries except the 5th to match
-- expected result: 1 (touches) + 1 (contained) + 2 (intersect both) + 2 (contains both) = 6 rows total
-- AOI 1 (poly_id_left 1) should be present 4 times and AOI 2 (poly_id_left 2) should be present 2 times
SELECT rights_s2.wkt ,  lefts_s2.wkt, LISTAGG(DISTINCT poly_id_right::TEXT) poly_id_right, LISTAGG(DISTINCT poly_id_left::TEXT) poly_id_left
FROM lefts_s2,rights_s2
WHERE rights_s2.value = lefts_s2.value AND NOT ST_DISJOINT(ST_GeogFromText(rights_s2.wkt) , ST_GeogFromText( lefts_s2.wkt ) )
GROUP BY rights_s2.wkt ,  lefts_s2.wkt

```

In this case, omitting `not ST_DISJOINT(ST_GeogFromText(rights_s2.wkt) , ST_GeogFromText( lefts_s2.wkt ) )` yields the same results.

### S2 for distance join

This also works for distance join. You first need to use `SEDONA.ST_Buffer(geometry, distance)` to wrap one of your original geometry column. If your original geometry column contains points, this `SEDONA.ST_Buffer` will make them become circles with a radius of `distance`. Note that Snowflake does not implement a native `ST_Buffer` function.

Since the coordinates are in the longitude and latitude system, so the unit of `distance` should be degree instead of meter or mile. You can get an approximation by performing `METER_DISTANCE/111000.0`, then filter out false-positives. Note that this might lead to inaccurate results if your data is close to the poles or antimeridian.

In a nutshell, run this query first on the left table before Step 1. Please replace `METER_DISTANCE` with a meter distance. In Step 1, generate S2 IDs based on the `buffered_geom` column. Then run Step 2, 3, 4 on the original `geom` column.

```sql
SELECT id, geom, SEDONA.ST_Buffer(geom, METER_DISTANCE/111000.0) AS buffered_geom, name
FROM lefts
```

## Functions that are only available in Sedona

Sedona implements over 200 geospatial vector and raster functions, which are much more than what Snowflake native functions offer. For example:

* [SEDONA.ST_3DDistance](../../api/snowflake/vector-data/Function.md#st_3ddistance)
* [SEDONA.ST_Force2D](../../api/snowflake/vector-data/Function.md#st_force_2d)
* [SEDONA.ST_GeometryN](../../api/snowflake/vector-data/Function.md#st_geometryn)
* [SEDONA.ST_MakeValid](../../api/snowflake/vector-data/Function.md#st_makevalid)
* [SEDONA.ST_Multi](../../api/snowflake/vector-data/Function.md#st_multi)
* [SEDONA.ST_NumGeometries](../../api/snowflake/vector-data/Function.md#st_numgeometries)
* [SEDONA.ST_ReducePrecision](../../api/snowflake/vector-data/Function.md#st_reduceprecision)
* [SEDONA.ST_SubdivideExplode](../../api/snowflake/vector-data/Function.md#st_subdivideexplode)

You can click the links above to learn more about these functions. More functions can be found in [SedonaSQL API](../../api/snowflake/vector-data/Function.md).

## Interoperate with Snowflake native functions

Sedona can interoperate with Snowflake native functions seamlessly. There are two ways to do this:

* Use `Sedona functions` to create a Geometry column, then use Snowflake native functions and Sedona functions to query it.
* Use `Snowflake native functions` to create a Geometry/Geography column, then use Snowflake native functions and Sedona functions to query it.

Now we will show you how to do this.

### Geometries created by Sedona Geometry constructors

In this case, Sedona uses EWKB type as the input/output type for geometry. If you have datasets of built-in Snowflake GEOMETRY/GEOGRAPHY type, you can easily transform them into EWKB through this function.

#### From Snowflake native functions to Sedona functions

In this example, `SEDONA.ST_X` is a Sedona function, `ST_GeommetryFromWkt` and `ST_AsEWKB` are Snowflake native functions.

```sql
SELECT SEDONA.ST_X(ST_AsEWKB(ST_GeometryFromWkt('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))) FROM {{geometry_table}};
```

#### From Sedona functions to Snowflake native functions

In this example, `SEDONA.ST_GeomFromText` is a Sedona function, `ST_AREA` and `to_geometry` are Snowflake native functions.

```sql
SELECT ST_AREA(to_geometry(SEDONA.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')));
```

#### Pros:

Sedona geometry constructors are more powerful than Snowflake native functions. It has the following advantages:

* Sedona offers more constructors especially for 3D (XYZ) geometries, but Snowflake native functions don't.
* WKB serialization is more efficient. If you need to use multiple Sedona functions, it is more efficient to use this method, which might bring in 2X performance improvement.
* SRID information of geometries is preserved. The method below will lose SRID information.

### Geometry / Geography created by Snowflake Geometry / Geography constructors

In this case, Sedona uses Snowflake native GEOMETRY/GEOGRAPHY type as the input/output type for geometry. The serialization format is GeoJSON string.

```sql
SELECT ST_AREA(SEDONA.ST_Buffer(ST_GeometryFromWkt('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), 1));
```

In this example, `SEDONA.ST_Buffer` is a Sedona function, `ST_GeommetryFromWkt` and `ST_AREA` are Snowflake native functions.

As you can see, you can use Sedona functions and Snowflake native functions together without explicit conversion.

#### Pros:

* You don't need to convert the geometry type, which is more convenient.

Note that: Snowflake natively serializes Geometry type data to GeoJSON String and sends to UDF as input. GeoJSON spec does not include SRID. So the SRID information will be lost if you mix-match Snowflake functions and Sedona functions directly without using `WKB`.

In the example below, the SRID=4326 information is lost.

```sql
SELECT ST_AsEWKT(SEDONA.ST_SetSRID(ST_GeometryFromWKT('POINT(1 2)'), 4326))
```

Output:

```
SRID=0;POINT(1 2)
```

## Known issues

1. Sedona Snowflake doesn't support `M` dimension due to the limitation of WKB serialization. Sedona Spark and Sedona Flink support XYZM because it uses our in-house serialization format. Although Sedona Snowflake has functions related to `M` dimension, all `M` values will be ignored.
2. Sedona H3 functions are not supported because Snowflake does not allow embedded C code in UDF.
3. All User Defined Table Functions only work with geometries created by Sedona constructors due to Snowflake current limitation `Data type GEOMETRY is not supported in non-SQL UDTF return type`. This includes:
   	* SEDONA.ST_MinimumBoundingRadius
   	* SEDONA.ST_Intersection_Aggr
   	* SEDONA.ST_SubDivideExplode
   	* SEDONA.ST_Envelope_Aggr
   	* SEDONA.ST_Union_Aggr
   	* SEDONA.ST_Collect
   	* SEDONA.ST_Dump
4. Only Sedona ST functions are available in Snowflake. Raster functions (RS functions) are not available in Snowflake yet.
