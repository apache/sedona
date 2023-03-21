!!!warning
	Support of Spark 2.X and Scala 2.11 was removed in Sedona 1.3.0+ although some parts of the source code might still be compatible. Sedona 1.3.0+ releases binary for both Scala 2.12 and 2.13.

!!!danger
	Sedona Python currently only works with Shapely 1.x. If you use GeoPandas, please use <= GeoPandas `0.11.1`. GeoPandas > 0.11.1 will automatically install Shapely 2.0. If you use Shapely, please use <= `1.8.4`.

## Sedona 1.4.0

Sedona 1.4.0 is compiled against, Spark 3.3 / Flink 1.12, Java 8.

### Highlights

* [X] **Sedona Spark & Flink** Serialize and deserialize geometries 3 - 7X faster
* [X] **Sedona Spark & Flink** Google S2 based spatial join for fast approximate point-in-polygon join. See [Join query in Spark](../../api/sql/Optimizer/#google-s2-based-approximate-equi-join) and [Join query in Flink](../../tutorial/flink/sql/#join-query)
* [X] **Sedona Spark** Pushdown spatial predicate on GeoParquet to reduce memory consumption by 10X: see [explanation](../../api/sql/Optimizer/#geoparquet)
* [X] **Sedona Spark** Automatically use broadcast index spatial join for small datasets
* [X] **Sedona Spark** New RasterUDT added to Sedona GeoTiff reader.
* [X] **Sedona Spark** A number of bug fixes and improvement to the Sedona R module.

### API change

* **Sedona Spark & Flink** Packaging strategy changed. See [Maven Coordinate](../maven-coordinates). Please change your Sedona dependencies if needed. We recommend `sedona-spark-shaded-3.0_2.12-1.4.0` and `sedona-flink-shaded_2.12-1.4.0`
* **Sedona Spark & Flink** GeoTools-wrapper version upgraded. Please use `geotools-wrapper-1.4.0-28.2`.

### Behavior change

* **Sedona Flink** Sedona Flink no longer outputs any LinearRing type geometry. All LinearRing are changed to LineString.
* **Sedona Spark** Join optimization strategy changed. Sedona no longer optimizes spatial join when use a spatial predicate together with a equijoin predicate. By default, it prefers equijoin whenever possible. SedonaConf adds a config option called `sedona.join.optimizationmode`, it can be configured as one of the following values:
	* `all`: optimize all joins having spatial predicate in join conditions. This was the behavior of Apache Sedona prior to 1.4.0.
	* `none`: disable spatial join optimization.
	* `nonequi`: only enable spatial join optimization on non-equi joins. This is the default mode.

When `sedona.join.optimizationmode` is configured as `nonequi`, it won't optimize join queries such as `SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)`, since it is an equi-join with equi-condition `A.x = B.x`. Sedona will optimize for `SELECT * FROM A, B WHERE ST_Contains(A.geom, B.geom)`


### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-218'>SEDONA-218</a>] -         Flaky test caused by improper handling of null struct values in Adapter.toDf
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-221'>SEDONA-221</a>] -         Outer join throws NPE for null geometries
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-222'>SEDONA-222</a>] -         GeoParquet reader does not work in non-local mode
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-224'>SEDONA-224</a>] -         java.lang.NoSuchMethodError when loading GeoParquet files using Spark 3.0.x ~ 3.2.x
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-225'>SEDONA-225</a>] -         Cannot count dataframes loaded from GeoParquet files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-227'>SEDONA-227</a>] -         Python SerDe Performance Degradation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-230'>SEDONA-230</a>] -         rdd.saveAsGeoJSON should generate feature properties with field names
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-233'>SEDONA-233</a>] -         Incorrect results for several joins in a single stage
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-236'>SEDONA-236</a>] -         Flakey python tests in tests.serialization.test_[de]serializers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-242'>SEDONA-242</a>] -         Update jars dependencies in Sedona R to Sedona 1.4.0 version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-250'>SEDONA-250</a>] -         R Deprecate use of Spark 2.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-252'>SEDONA-252</a>] -         Fix disabled RS_Base64 test
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-255'>SEDONA-255</a>] -         R – Translation issue for ST_Point and ST_PolygonFromEnvelope
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-258'>SEDONA-258</a>] -         Cannot directly assign raw spatial RDD to CircleRDD using Python binding
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-259'>SEDONA-259</a>] -         Adapter.toSpatialRdd in Python binding does not have valid implementation for specifying custom field names for user data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-261'>SEDONA-261</a>] -         Cannot run distance join using broadcast index join when the distance expression references to attributes from the right-side relation
</li>
</ul>
        
### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-156'>SEDONA-156</a>] -         predicate pushdown support for GeoParquet
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-215'>SEDONA-215</a>] -         Add ST_ConcaveHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-216'>SEDONA-216</a>] -         Upgrade jts version to 1.19.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-235'>SEDONA-235</a>] -         Create ST_S2CellIds in Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-246'>SEDONA-246</a>] -         R GeoTiff read/write
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-254'>SEDONA-254</a>] -         R – Add raster type
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-262'>SEDONA-262</a>] -         Don&#39;t optimize equi-join by default, add an option to configure when to optimize spatial joins
</li>
</ul>
        
<h2>        Improvement
</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-205'>SEDONA-205</a>] -         Use BinaryType in GeometryUDT in Sedona Spark
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-207'>SEDONA-207</a>] -         Faster serialization/deserialization of geometry objects
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-212'>SEDONA-212</a>] -         Move shading to separate maven modules
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-217'>SEDONA-217</a>] -         Automatically broadcast small datasets
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-220'>SEDONA-220</a>] -         Upgrade Ubuntu build image from 18.04 to 20.04
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-226'>SEDONA-226</a>] -         Support reading and writing GeoParquet file metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-228'>SEDONA-228</a>] -         Standardize logging dependencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-231'>SEDONA-231</a>] -         Redundant Serde Removal
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-234'>SEDONA-234</a>] -         ST_Point inconsistencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-243'>SEDONA-243</a>] -         Improve Sedona R file readers: GeoParquet and Shapefile
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-244'>SEDONA-244</a>] -         Align R read/write functions with the Sparklyr framework
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-249'>SEDONA-249</a>] -         Add jvm flags for running tests on Java 17 
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-251'>SEDONA-251</a>] -         Add raster type to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-253'>SEDONA-253</a>] -         Upgrade geotools to version 28.2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-260'>SEDONA-260</a>] -         More intuitive configuration of partition and index-build side of spatial joins in Sedona SQL
</li>
</ul>

## Sedona 1.3.1

This version is a minor release on Sedoma 1.3.0 line. It fixes a few critical bugs in 1.3.0. We suggest all 1.3.0 users to migrate to this version.

### Bug fixes

* [SEDONA-204](https://issues.apache.org/jira/browse/SEDONA-204) - Init value in X/Y/Z max should be -Double.MAX
* [SEDONA-206](https://issues.apache.org/jira/browse/SEDONA-206) - Performance regression of ST_Transform in 1.3.0-incubating
* [SEDONA-210](https://issues.apache.org/jira/browse/SEDONA-210) - 1.3.0-incubating doesn't work with Scala 2.12 sbt projects
* [SEDONA-211](https://issues.apache.org/jira/browse/SEDONA-211) - Enforce release managers to use JDK 8
* [SEDONA-201](https://issues.apache.org/jira/browse/SEDONA-201) - Implement ST_MLineFromText and ST_MPolyFromText methods


### New Feature

* [SEDONA-196](https://issues.apache.org/jira/browse/SEDONA-196) - Add ST_Force3D to Sedona
* [SEDONA-197](https://issues.apache.org/jira/browse/SEDONA-197) - Add ST_ZMin, ST_ZMax to Sedona
* [SEDONA-199](https://issues.apache.org/jira/browse/SEDONA-199) - Add ST_NDims to Sedona


### Improvement

* [SEDONA-194](https://issues.apache.org/jira/browse/SEDONA-194) - Merge org.datasyslab.sernetcdf into Sedona
* [SEDONA-208](https://issues.apache.org/jira/browse/SEDONA-208) - Use Spark RuntimeConfig in SedonaConf


## Sedona 1.3.0

This version is a major release on Sedona 1.3.0 line and consists of 50 PRs. It includes many new functions, optimization and bug fixes.

### Highlights

- [X] Sedona on Spark in this release is compiled against Spark 3.3.
- [X] Sedona on Flink in this release is compiled against Flink 1.14.
- [X] Scala 2.11 support is removed.
- [X] Spark 2.X support is removed.
- [X] Python 3.10 support is added.
- [X] Aggregators in Flink are added
- [X] Correctness fixes for corner cases in range join and distance join.
- [X] Native GeoParquet read and write (../../tutorial/sql/#load-geoparquet).
    * `df = spark.read.format("geoparquet").option("fieldGeometry", "myGeometryColumn").load("PATH/TO/MYFILE.parquet")`
    * `df.write.format("geoparquet").save("PATH/TO/MYFILE.parquet")`
- [X] DataFrame style API (../../tutorial/sql/#dataframe-style-api)
    * `df.select(ST_Point(min_value, max_value).as("point"))`
- [X] Allow WKT format CRS in ST_Transform
    * `ST_Transform(geom, "srcWktString", "tgtWktString")`

```yaml

GEOGCS["WGS 84",
  DATUM["WGS_1984",
  SPHEROID["WGS 84",6378137,298.257223563,
  AUTHORITY["EPSG","7030"]],
  AUTHORITY["EPSG","6326"]],
  PRIMEM["Greenwich",0,
  AUTHORITY["EPSG","8901"]],
  UNIT["degree",0.0174532925199433,
  AUTHORITY["EPSG","9122"]],
  AUTHORITY["EPSG","4326"]]

```

### Bug fixes

  * [SEDONA-119](https://issues.apache.org/jira/browse/SEDONA-119) - ST_Touches join query returns true for polygons whose interiors intersect
  * [SEDONA-136](https://issues.apache.org/jira/browse/SEDONA-136) - Enable testAsEWKT for Flink
  * [SEDONA-137](https://issues.apache.org/jira/browse/SEDONA-137) - Fix ST_Buffer for Flink to work
  * [SEDONA-138](https://issues.apache.org/jira/browse/SEDONA-138) - Fix ST_GeoHash for Flink to work
  * [SEDONA-153](https://issues.apache.org/jira/browse/SEDONA-153) - Python Serialization Fails with Nulls
  * [SEDONA-158](https://issues.apache.org/jira/browse/SEDONA-158) - Fix wrong description about ST_GeometryN in the API docs
  * [SEDONA-169](https://issues.apache.org/jira/browse/SEDONA-169) - Fix ST_RemovePoint in accordance with the API document
  * [SEDONA-178](https://issues.apache.org/jira/browse/SEDONA-178) - Correctness issue in distance join queries
  * [SEDONA-182](https://issues.apache.org/jira/browse/SEDONA-182) - ST_AsText should not return SRID
  * [SEDONA-186](https://issues.apache.org/jira/browse/SEDONA-186) - collecting result rows of a spatial join query with SELECT * fails with serde error
  * [SEDONA-188](https://issues.apache.org/jira/browse/SEDONA-188) - Python warns about missing `jars` even when some are found
  * [SEDONA-193](https://issues.apache.org/jira/browse/SEDONA-193) - ST_AsBinary produces EWKB by mistake

### New Features

  * [SEDONA-94](https://issues.apache.org/jira/browse/SEDONA-94) - GeoParquet  Support For Sedona
  * [SEDONA-125](https://issues.apache.org/jira/browse/SEDONA-125) - Allows customized CRS in ST_Transform
  * [SEDONA-166](https://issues.apache.org/jira/browse/SEDONA-166) - Provide Type-safe DataFrame Style API
  * [SEDONA-168](https://issues.apache.org/jira/browse/SEDONA-168) - Add ST_Normalize to Apache Sedona
  * [SEDONA-171](https://issues.apache.org/jira/browse/SEDONA-171) - Add ST_SetPoint to Apache Sedona


### Improvement

  * [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-121) - Add equivalent constructors left over from Spark to Flink
  * [SEDONA-132](https://issues.apache.org/jira/browse/SEDONA-132) - Create common module for SQL functions
  * [SEDONA-133](https://issues.apache.org/jira/browse/SEDONA-133) - Allow user-defined schemas in Adapter.toDf()
  * [SEDONA-139](https://issues.apache.org/jira/browse/SEDONA-139) - Fix wrong argument order in Flink unit tests
  * [SEDONA-140](https://issues.apache.org/jira/browse/SEDONA-140) - Update Sedona Dependencies in R Package
  * [SEDONA-143](https://issues.apache.org/jira/browse/SEDONA-143) - Add missing unit tests for the Flink predicates
  * [SEDONA-144](https://issues.apache.org/jira/browse/SEDONA-144) - Add ST_AsGeoJSON to the Flink API
  * [SEDONA-145](https://issues.apache.org/jira/browse/SEDONA-145) - Fix ST_AsEWKT to reserve the Z coordinate
  * [SEDONA-146](https://issues.apache.org/jira/browse/SEDONA-146) - Add missing output funtions to the Flink API
  * [SEDONA-147](https://issues.apache.org/jira/browse/SEDONA-147) - Add SRID functions to the Flink API
  * [SEDONA-148](https://issues.apache.org/jira/browse/SEDONA-148) - Add boolean functions to the Flink API
  * [SEDONA-149](https://issues.apache.org/jira/browse/SEDONA-149) - Add Python 3.10 support
  * [SEDONA-151](https://issues.apache.org/jira/browse/SEDONA-151) - Add ST aggregators to Sedona Flink
  * [SEDONA-152](https://issues.apache.org/jira/browse/SEDONA-152) - Add reader/writer functions for GML and KML
  * [SEDONA-154](https://issues.apache.org/jira/browse/SEDONA-154) - Add measurement functions to the Flink API
  * [SEDONA-157](https://issues.apache.org/jira/browse/SEDONA-157) - Add coordinate accessors to the Flink API
  * [SEDONA-159](https://issues.apache.org/jira/browse/SEDONA-159) - Add Nth accessor functions to the Flink API
  * [SEDONA-160](https://issues.apache.org/jira/browse/SEDONA-160) - Fix geoparquetIOTests.scala to cleanup after test
  * [SEDONA-161](https://issues.apache.org/jira/browse/SEDONA-161) - Add ST_Boundary to the Flink API
  * [SEDONA-162](https://issues.apache.org/jira/browse/SEDONA-162) - Add ST_Envelope to the Flink API
  * [SEDONA-163](https://issues.apache.org/jira/browse/SEDONA-163) - Better handle of unsupported types in shapefile reader
  * [SEDONA-164](https://issues.apache.org/jira/browse/SEDONA-164) - Add geometry count functions to the Flink API
  * [SEDONA-165](https://issues.apache.org/jira/browse/SEDONA-165) - Upgrade Apache Rat to 0.14
  * [SEDONA-170](https://issues.apache.org/jira/browse/SEDONA-170) - Add ST_AddPoint and ST_RemovePoint to the Flink API
  * [SEDONA-172](https://issues.apache.org/jira/browse/SEDONA-172) - Add ST_LineFromMultiPoint to Apache Sedona
  * [SEDONA-176](https://issues.apache.org/jira/browse/SEDONA-176) - Make ST_Contains conform with OGC standard, and add ST_Covers and ST_CoveredBy functions.
  * [SEDONA-177](https://issues.apache.org/jira/browse/SEDONA-177) - Support spatial predicates other than INTERSECTS and COVERS/COVERED_BY in RangeQuery.SpatialRangeQuery and JoinQuery.SpatialJoinQuery
  * [SEDONA-181](https://issues.apache.org/jira/browse/SEDONA-181) - Build fails with java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
  * [SEDONA-189](https://issues.apache.org/jira/browse/SEDONA-189) - Prepare geometries in broadcast join
  * [SEDONA-192](https://issues.apache.org/jira/browse/SEDONA-192) - Null handling in predicates
  * [SEDONA-195](https://issues.apache.org/jira/browse/SEDONA-195) - Add wkt validation and an optional srid to ST_GeomFromWKT/ST_GeomFromText

### Task

* [SEDONA-150](https://issues.apache.org/jira/browse/SEDONA-150) - Drop Spark 2.4 and Scala 2.11 support


## Sedona 1.2.1

This version is a maintenance release on Sedona 1.2.0 line. It includes bug fixes.

Sedona on Spark is now compiled against Spark 3.3, instead of Spark 3.2.


### SQL (for Spark)

Bug fixes:

* [SEDONA-104](https://issues.apache.org/jira/browse/SEDONA-104): Bug in reading band values of GeoTiff images
* [SEDONA-118](https://issues.apache.org/jira/browse/SEDONA-118): Fix the wrong result in ST_Within
* [SEDONA-123](https://issues.apache.org/jira/browse/SEDONA-123): Fix the check for invalid lat/lon in ST_GeoHash

Improvement:

* [SEDONA-96](https://issues.apache.org/jira/browse/SEDONA-96): Refactor ST_MakeValid to use GeometryFixer
* [SEDONA-108](https://issues.apache.org/jira/browse/SEDONA-108): Write support for GeoTiff images
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column
* [SEDONA-127](https://issues.apache.org/jira/browse/SEDONA-127): Add null safety to ST_GeomFromWKT/WKB/Text
* [SEDONA-129](https://issues.apache.org/jira/browse/SEDONA-129): Support Spark 3.3
* [SEDONA-135](https://issues.apache.org/jira/browse/SEDONA-135): Consolidate and upgrade hadoop dependency

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-106](https://issues.apache.org/jira/browse/SEDONA-106): Add ST_LineFromText
* [SEDONA-117](https://issues.apache.org/jira/browse/SEDONA-117): Add RS_AppendNormalizedDifference
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_Ymin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-124](https://issues.apache.org/jira/browse/SEDONA-124): Add ST_CollectionExtract
* [SEDONA-109](https://issues.apache.org/jira/browse/SEDONA-109): Add ST_OrderingEquals

### Flink

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_Ymin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-110](https://issues.apache.org/jira/browse/SEDONA-110): Add ST_GeomFromGeoHash
* [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-124): More ST constructors to Flink
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column


## Sedona 1.2.0

This version is a major release on Sedona 1.2.0 line. It includes bug fixes and new features: Sedona with Apache Flink.

### RDD

Bug fix:

* [SEDONA-18](https://issues.apache.org/jira/browse/SEDONA-18): Fix an error reading Shapefile
* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Exclude scala-library from scala-collection-compat

Improvement:

* [SEDONA-77](https://issues.apache.org/jira/browse/SEDONA-77): Refactor Format readers and spatial partitioning functions to be standalone libraries. So they can be used by Flink and others.

### SQL

New features:

* [SEDONA-4](https://issues.apache.org/jira/browse/SEDONA-4): Handle nulls in SQL functions
* [SEDONA-65](https://issues.apache.org/jira/browse/SEDONA-65): Create ST_Difference function
* [SEDONA-68](https://issues.apache.org/jira/browse/SEDONA-68) Add St_Collect function.
* [SEDONA-82](https://issues.apache.org/jira/browse/SEDONA-82): Create ST_SymmDifference function
* [SEDONA-75](https://issues.apache.org/jira/browse/SEDONA-75): Add support for "3D" geometries: Preserve Z coordinates on geometries when serializing, ST_AsText , ST_Z, ST_3DDistance
* [SEDONA-86](https://issues.apache.org/jira/browse/SEDONA-86): Support empty geometries in ST_AsBinary and ST_AsEWKB
* [SEDONA-90](https://issues.apache.org/jira/browse/SEDONA-90): Add ST_Union
* [SEDONA-100](https://issues.apache.org/jira/browse/SEDONA-100): Add st_multi function

Bug fix:

* [SEDONA-89](https://issues.apache.org/jira/browse/SEDONA-89): GeometryUDT equals should test equivalence of the other object

### Flink

Major update:

* [SEDONA-80](https://issues.apache.org/jira/browse/SEDONA-80): Geospatial stream processing support in Flink Table API
* [SEDONA-85](https://issues.apache.org/jira/browse/SEDONA-85): ST_Geohash function in Flink
* [SEDONA-87](https://issues.apache.org/jira/browse/SEDONA-87): Support Flink Table and DataStream conversion
* [SEDONA-93](https://issues.apache.org/jira/browse/SEDONA-93): Add ST_GeomFromGeoJSON


## Sedona 1.1.1

This version is a maintenance release on Sedona 1.1.X line. It includes bug fixes and a few new functions.

### Global

New feature:

* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Scala source code supports Scala 2.13

### SQL

Bug fix:

* [SEDONA-67](https://issues.apache.org/jira/browse/SEDONA-67): Support Spark 3.2

New features:

* [SEDONA-43](https://issues.apache.org/jira/browse/SEDONA-43): Add ST_GeoHash and ST_GeomFromGeoHash
* [SEDONA-45](https://issues.apache.org/jira/browse/SEDONA-45): Add ST_MakePolygon
* [SEDONA-71](https://issues.apache.org/jira/browse/SEDONA-71): Add ST_AsBinary, ST_AsEWKB, ST_SRID, ST_SetSRID



## Sedona 1.1.0

This version is a major release on Sedona 1.1.0 line. It includes bug fixes and new features: R language API, Raster data and Map algebra support

### Global

Dependency upgrade: 

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Use Geotools-wrapper 1.1.0-24.1 to include geotools GeoTiff libraries.

Improvement on join queries in core and SQL:

* [SEDONA-63](https://issues.apache.org/jira/browse/SEDONA-63): Skip empty partitions in NestedLoopJudgement
* [SEDONA-64](https://issues.apache.org/jira/browse/SEDONA-64): Broadcast dedupParams to improve performance

Behavior change:

* [SEDONA-62](https://issues.apache.org/jira/browse/SEDONA-62): Ignore HDF test in order to avoid NASA copyright issue

### Core

Bug fix:

* [SEDONA-41](https://issues.apache.org/jira/browse/SEDONA-41): Fix rangeFilter bug when the leftCoveredByRight para is false
* [SEDONA-53](https://issues.apache.org/jira/browse/SEDONA-53): Fix SpatialKnnQuery NullPointerException

### SQL

Major update:

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Add GeoTiff raster I/O and Map Algebra function

New function:

* [SEDONA-27](https://issues.apache.org/jira/browse/SEDONA-27): Add ST_Subdivide and ST_SubdivideExplode functions

Bug fix:

* [SEDONA-56](https://issues.apache.org/jira/browse/SEDONA-56): Fix broadcast join with Adapter Query Engine enabled
* [SEDONA-22](https://issues.apache.org/jira/browse/SEDONA-22), [SEDONA-60](https://issues.apache.org/jira/browse/SEDONA-60): Fix join queries in SparkSQL when one side has no rows or only one row

### Viz

N/A

### Python

Improvement:

* [SEDONA-59](https://issues.apache.org/jira/browse/SEDONA-59): Make pyspark dependency of Sedona Python optional

Bug fix:

* [SEDONA-50](https://issues.apache.org/jira/browse/SEDONA-50): Remove problematic logging conf that leads to errors on Databricks
* Fix the issue: Spark dependency in setup.py was configured to be < v3.1.0 by mistake.

### R

Major update:

* [SEDONA-31](https://issues.apache.org/jira/browse/SEDONA-31): Add R interface for Sedona
 
## Sedona 1.0.1

This version is a maintenance release on Sedona 1.0.0 line. It includes bug fixes, some new features, one ==API change==

### Known issue

In Sedona v1.0.1 and earlier versions, the Spark dependency in setup.py was configured to be ==< v3.1.0== [by mistake](https://github.com/apache/sedona/blob/8235924ac80939cbf2ce562b0209b71833ed9429/python/setup.py#L39). When you install Sedona Python (apache-sedona v1.0.1) from PyPI, pip might uninstall PySpark 3.1.1 and install PySpark 3.0.2 on your machine.

Three ways to fix this:

1. After install apache-sedona v1.0.1, uninstall PySpark 3.0.2 and reinstall PySpark 3.1.1

2. Ask pip not to install Sedona dependencies: `pip install --no-deps apache-sedona`

3. Install Sedona from the latest setup.py (on GitHub) manually.

### Global

Dependency upgrade:

* [SEDONA-16](https://issues.apache.org/jira/browse/SEDONA-16): Use a GeoTools Maven Central wrapper to fix failed Jupyter notebook examples
* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): upgrade to Spark 3.1.1
* [SEDONA-33](https://issues.apache.org/jira/browse/SEDONA-33): jts2geojson version from 0.14.3 to 0.16.1

### Core

Bug fix:

* [SEDONA-35](https://issues.apache.org/jira/browse/SEDONA-35): Address user-data mutability issue with Adapter.toDF()

### SQL

Bug fix:

* [SEDONA-14](https://issues.apache.org/jira/browse/SEDONA-14): Saving dataframe to CSV or Parquet fails due to unknown type
* [SEDONA-15](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_MinimumBoundingRadius and ST_MinimumBoundingCircle functions
* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins
* [SEDONA-20](https://issues.apache.org/jira/browse/SEDONA-20): Case object GeometryUDT and GeometryUDT instance not equal in Spark 3.0.2

New function:

* [SEDONA-21](https://issues.apache.org/jira/browse/SEDONA-21): allows Sedona to be used in pure SQL environment
* [SEDONA-24](https://issues.apache.org/jira/browse/SEDONA-24): Add ST_LineSubString and ST_LineInterpolatePoint
* [SEDONA-26](https://issues.apache.org/jira/browse/SEDONA-26): Add broadcast join support


### Viz

Improvement:

* [SEDONA-32](https://issues.apache.org/jira/browse/SEDONA-32): Speed up ST_Render

API change:

* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): Upgrade to Spark 3.1.1 and fix ST_Pixelize

### Python

Bug fix:

* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins

## Sedona 1.0.0

This version is the first Sedona release since it joins the Apache Incubator. It includes new functions, bug fixes, and ==API changes==.

### Global

Key dependency upgrade:

* [SEDONA-1](https://issues.apache.org/jira/browse/SEDONA-1): upgrade to JTS 1.18
* upgrade to GeoTools 24.0
* upgrade to jts2geojson 0.14.3

Key dependency packaging strategy change:

* JTS, GeoTools, jts2geojson are no longer packaged in Sedona jars. End users need to add them manually. See [here](../maven-coordinates).

Key compilation target change:

* [SEDONA-3](https://issues.apache.org/jira/browse/SEDONA-3): Paths and class names have been changed to Apache Sedona
* [SEDONA-7](https://issues.apache.org/jira/browse/SEDONA-7): build the source code for Spark 2.4, 3.0, Scala 2.11, 2.12, Python 3.7, 3.8, 3.9. See [here](../compile).


### Sedona-core

Bug fix:

* PR [443](https://github.com/apache/sedona/pull/443): read multiple Shape Files by multiPartitions
* PR [451](https://github.com/apache/sedona/pull/451) (==API change==): modify CRSTransform to ignore datum shift

New function:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): spatialRDD.flipCoordinates()

API / behavior change:

* PR [488](https://github.com/apache/sedona/pull/488): JoinQuery.SpatialJoinQuery/DistanceJoinQuery now returns `<Geometry, List>` instead of `<Geometry, HashSet>` because we can no longer use HashSet in Sedona for duplicates removal. All original duplicates in both input RDDs will be preserved in the output.

### Sedona-sql

Bug fix:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8) (==API change==): ST_Transform slow due to lock contention.
* PR [427](https://github.com/apache/sedona/pull/427): ST_Point and ST_PolygonFromEnvelope now allows Double type

New function:

* PR [499](https://github.com/apache/sedona/pull/449): ST_Azimuth, ST_X, ST_Y, ST_StartPoint, ST_Boundary, ST_EndPoint, ST_ExteriorRing, ST_GeometryN, ST_InteriorRingN, ST_Dump, ST_DumpPoints, ST_IsClosed, ST_NumInteriorRings, ST_AddPoint, ST_RemovePoint, ST_IsRing
* PR [459](https://github.com/apache/sedona/pull/459): ST_LineMerge
* PR [460](https://github.com/apache/sedona/pull/460): ST_NumGeometries
* PR [469](https://github.com/apache/sedona/pull/469): ST_AsGeoJSON
* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): ST_FlipCoordinates

Behavior change:

* PR [480](https://github.com/apache/sedona/pull/480): Aggregate Functions rewrite for new Aggregator API. The functions can be used as typed functions in code and enable compilation-time type check.

API change:

* [SEDONA-11](https://issues.apache.org/jira/browse/SEDONA-11): Adapter.toDf() will directly generate a geometry type column. ST_GeomFromWKT is no longer needed.

### Sedona-viz

API change: Drop the function which can generate SVG vector images because the required library has an incompatible license and the SVG image is not good at plotting big data

### Sedona Python

API/Behavior change:

* Python-to-Sedona adapter is moved to a separate module. To use Sedona Python, see [here](../overview/#prepare-python-adapter-jar)

New function:

* PR [448](https://github.com/apache/sedona/pull/448): Add support for partition number in spatialPartitioning function `spatial_rdd.spatialPartitioning(grid_type, NUM_PARTITION)`
