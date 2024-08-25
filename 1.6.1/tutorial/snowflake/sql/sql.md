After the installation done, you can start using Sedona functions. Please log in to Snowflake again using the user that has the privilege to access the database.

!!!note
	Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## Create a sample table

Let's create a `city_tbl` that contains the locations and names of cities. Each location is a WKT string.

```sql
CREATE TABLE city_tbl (wkt STRING, city_name STRING);
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
CREATE TABLE city_tbl_geom AS
SELECT Sedona.ST_GeomFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

The `geom` column Table `city_tbl_geom ` is now in a `Binary` type and data in this column is in a format that can be understood by Sedona. The output of this query will show geometries in WKB binary format like this:

```
GEOM CITY_NAME
010100000085eb51b81e955ec0ae47e17a14ce4740  Seattle
01010000007b14ae47e19a5ec0e17a14ae47e14240  San Francisco
```

To view the content of this column in a human-readable format, you can use `ST_AsText`. For example,

```sql
SELECT Sedona.ST_AsText(geom), city_name
FROM city_tbl_geom
```

Alternatively, you can also create Snowflake native Geometry and Geography type columns. For example, you can create a Snowflake native Geometry type column as follows (note the function has no `SEDONA` prefix):

```sql
CREATE TABLE city_tbl_geom AS
SELECT ST_GeometryFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

The following code creates a Snowflake native Geography type column (note the function has no `SEDONA` prefix):

```sql
CREATE TABLE city_tbl_geom AS
SELECT ST_GeographyFromWKT(wkt) AS geom, city_name
FROM city_tbl
```

!!!note
	SedonaSQL provides lots of functions to create a Geometry column, please read [SedonaSQL API](../../api/snowflake/vector-data/Constructor.md).

## Check the lon/lat order

In SedonaSnow `v1.4.1` and before, we use lat/lon order in the following functions:

* ST_Transform
* ST_DistanceSphere
* ST_DistanceSpheroid

We use `lon/lat` order in the following functions:

* ST_GeomFromGeoHash
* ST_GeoHash
* ST_S2CellIDs

In Sedona `v1.5.0` and above, all functions will be fixed to lon/lat order.

If your original data is not in the order you want, you need to flip the coordinate using `ST_FlipCoordinates(geom: Geometry)`.

The sample data used above is in lon/lat order, we can flip the coordinates as follows:

```sql
CREATE OR REPLACE TABLE city_tbl_geom AS
SELECT Sedona.ST_FlipCoordinates(geom) AS geom, city_name
FROM city_tbl_geom
```

If we show the content of this table, it is now in lat/lon order:

```sql
SELECT Sedona.ST_AsText(geom), city_name
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
SELECT ST_AsText(geom)
FROM city_tbl_geom
```

!!!note
	SedonaSQL provides lots of functions to save the Geometry column, please read [SedonaSQL API](../../api/snowflake/vector-data/Function.md).

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in SedonaSQL is same as the unit of all geometries in a Geometry column.

To convert Coordinate Reference System of the Geometry column created before, use `ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string`

The first EPSG code EPSG:4326 in `ST_Transform` is the source CRS of the geometries. It is WGS84, the most common degree-based CRS.

The second EPSG code EPSG:3857 in `ST_Transform` is the target CRS of the geometries. It is the most common meter-based CRS.

This `ST_Transform` transform the CRS of these geometries from EPSG:4326 to EPSG:3857. The details CRS information can be found on [EPSG.io](https://epsg.io/).

!!!note
	This function follows lon/order in 1.5.0+ and lat/lon order in 1.4.1 and before. You can use `ST_FlipCoordinates` to swap X and Y.

We can transform our sample data as follows

```sql
SELECT Sedona.ST_AsText(Sedona.ST_Transform(geom, 'epsg:4326', 'epsg:3857')), city_name
FROM city_tbl_geom
```

The output will be like this:

```
POINT (6042216.250411431 -13617713.308741156)  Seattle
POINT (4545577.120361927 -13627732.06291255)  San Francisco
```

`ST_Transform` also supports the CRS string in OGC WKT format. For example, the following query generates the same output but with a OGC WKT CRS string.

```sql
SELECT Sedona.ST_AsText(Sedona.ST_Transform(geom, 'epsg:4326', 'PROJCS["WGS 84 / Pseudo-Mercator",
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

Use ==ST_Contains==, ==ST_Intersects==, ==ST_Within== to run a range query over a single column.

The following example finds all geometries that are within the given polygon:

```sql
SELECT *
FROM city_tbl_geom
WHERE Sedona.ST_Contains(Sedona.ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), geom)
```

!!!note
	Read [SedonaSQL API](../../api/snowflake/vector-data/Constructor.md) to learn how to create a Geometry type query window.

## KNN query

Use ==ST_Distance==, ==ST_DistanceSphere==, ==ST_DistanceSpheroid== to calculate the distance and rank the distance.

The following code returns the 5 nearest neighbor of the given point.

```sql
SELECT geom, ST_Distance(Sedona.ST_Point(1.0, 1.0), geom) AS distance
FROM city_tbl_geom
ORDER BY distance DESC
LIMIT 5
```

## Range join query

!!!warning
	Sedona range join in Snowflake does not trigger Sedona's optimized spatial join algorithm while Sedona Spark does. It uses Snowflake's default Cartesian join which is very slow. Therefore, it is recommended to use Sedona's S2-based join or Snowflake's native ST functions + native `Geography` type to do range join, which will trigger Snowflake's `GeoJoin` algorithm.

Introduction: Find geometries from A and geometries from B such that each geometry pair satisfies a certain predicate.

Example:

```sql
SELECT *
FROM polygondf, pointdf
WHERE ST_Contains(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM polygondf, pointdf
WHERE ST_Intersects(polygondf.polygonshape,pointdf.pointshape)
```

```sql
SELECT *
FROM pointdf, polygondf
WHERE ST_Within(pointdf.pointshape, polygondf.polygonshape)
```

## Distance join

!!!warning
	Sedona distance join in Snowflake does not trigger Sedona's optimized spatial join algorithm while Sedona Spark does. It uses Snowflake's default Cartesian join which is very slow. Therefore, it is recommended to use Sedona's S2-based join or Snowflake's native ST functions + native `Geography` type to do range join, which will trigger Snowflake's `GeoJoin` algorithm.

Introduction: Find geometries from A and geometries from B such that the distance of each geometry pair is less or equal than a certain distance. It supports the planar Euclidean distance calculators `ST_Distance`, `ST_HausdorffDistance`, `ST_FrechetDistance` and the meter-based geodesic distance calculators `ST_DistanceSpheroid` and `ST_DistanceSphere`.

Example for planar Euclidean distance:

```sql
SELECT *
FROM pointdf1, pointdf2
WHERE ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2
```

```sql
SELECT *
FROM pointDf, polygonDF
WHERE ST_HausdorffDistance(pointDf.pointshape, polygonDf.polygonshape, 0.3) < 2
```

```sql
SELECT *
FROM pointDf, polygonDF
WHERE ST_FrechetDistance(pointDf.pointshape, polygonDf.polygonshape) < 2
```

!!!warning
	If you use planar Euclidean distance functions like `ST_Distance`, `ST_HausdorffDistance` or `ST_FrechetDistance` as the predicate, Sedona doesn't control the distance's unit (degree or meter). It is same with the geometry. If your coordinates are in the longitude and latitude system, the unit of `distance` should be degree instead of meter or mile. To change the geometry's unit, please either transform the coordinate reference system to a meter-based system. See [ST_Transform](../../api/snowflake/vector-data/Function.md#st_transform). If you don't want to transform your data, please consider using `ST_DistanceSpheroid` or `ST_DistanceSphere`.

```sql
SELECT *
FROM pointdf1, pointdf2
WHERE ST_DistanceSpheroid(pointdf1.pointshape1,pointdf2.pointshape2) < 2
```

## Google S2 based approximate equi-join

You can use Sedona built-in Google S2 functions to perform an approximate equi-join. This algorithm leverages Snowflake's internal equi-join algorithm and might be performant given that you can opt to skip the refinement step  by sacrificing query accuracy.

Please use the following steps:

### 1. Generate S2 ids for both tables

Use [ST_S2CellIds](../../api/snowflake/vector-data/Function.md#ST_S2CellIDs) to generate cell IDs. Each geometry may produce one or more IDs.

```sql
SELECT * FROM lefts, TABLE(FLATTEN(ST_S2CellIDs(lefts.geom, 15))) s1
```

```sql
SELECT * FROM rights, TABLE(FLATTEN(ST_S2CellIDs(rights.geom, 15))) s2
```

### 2. Perform equi-join

Join the two tables by their S2 cellId

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
FROM lcs JOIN rcs ON lcs.cellId = rcs.cellId
```

### 3. Optional: Refine the result

Due to the nature of S2 Cellid, the equi-join results might have a few false-positives depending on the S2 level you choose. A smaller level indicates bigger cells, less exploded rows, but more false positives.

To ensure the correctness, you can use one of the [Spatial Predicates](../../api/snowflake/vector-data/Predicate.md) to filter out them. Use this query instead of the query in Step 2.

```sql
SELECT lcs.id as lcs_id, lcs.geom as lcs_geom, lcs.name as lcs_name, rcs.id as rcs_id, rcs.geom as rcs_geom, rcs.name as rcs_name
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

### S2 for distance join

This also works for distance join. You first need to use `ST_Buffer(geometry, distance)` to wrap one of your original geometry column. If your original geometry column contains points, this `ST_Buffer` will make them become circles with a radius of `distance`.

Since the coordinates are in the longitude and latitude system, so the unit of `distance` should be degree instead of meter or mile. You can get an approximation by performing `METER_DISTANCE/111000.0`, then filter out false-positives.  Note that this might lead to inaccurate results if your data is close to the poles or antimeridian.

In a nutshell, run this query first on the left table before Step 1. Please replace `METER_DISTANCE` with a meter distance. In Step 1, generate S2 IDs based on the `buffered_geom` column. Then run Step 2, 3, 4 on the original `geom` column.

```sql
SELECT id, geom, ST_Buffer(geom, METER_DISTANCE/111000.0) as buffered_geom, name
FROM lefts
```

## Functions that are only available in Sedona

Sedona implements over 200 geospatial vector and raster functions, which are much more than what Snowflake native functions offer. For example:

* [ST_3DDistance](../../api/snowflake/vector-data/Function.md#st_3ddistance)
* [ST_Force2D](../../api/snowflake/vector-data/Function.md#st_force_2d)
* [ST_GeometryN](../../api/snowflake/vector-data/Function.md#st_geometryn)
* [ST_MakeValid](../../api/snowflake/vector-data/Function.md#st_makevalid)
* [ST_Multi](../../api/snowflake/vector-data/Function.md#st_multi)
* [ST_NumGeometries](../../api/snowflake/vector-data/Function.md#st_numgeometries)
* [ST_ReducePrecision](../../api/snowflake/vector-data/Function.md#st_reduceprecision)
* [ST_SubdivideExplode](../../api/snowflake/vector-data/Function.md#st_subdivideexplode)

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
   	* ST_MinimumBoundingRadius
   	* ST_Intersection_Aggr
   	* ST_SubDivideExplode
   	* ST_Envelope_Aggr
   	* ST_Union_Aggr
   	* ST_Collect
   	* ST_Dump
4. Only Sedona ST functions are available in Snowflake. Raster functions (RS functions) are not available in Snowflake yet.
