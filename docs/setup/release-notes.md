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

In Sedona v1.0.1 and eariler versions, the Spark dependency in setup.py was configured to be ==< v3.1.0== [by mistake](https://github.com/apache/incubator-sedona/blob/8235924ac80939cbf2ce562b0209b71833ed9429/python/setup.py#L39). When you install  Sedona Python (apache-sedona v1.0.1) from Pypi, pip might unstall PySpark 3.1.1 and install PySpark 3.0.2 on your machine.

Three ways to fix this:

1. After install apache-sedona v1.0.1, unstall PySpark 3.0.2 and reinstall PySpark 3.1.1

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

* PR [443](https://github.com/apache/incubator-sedona/pull/443): read multiple Shape Files by multiPartitions
* PR [451](https://github.com/apache/incubator-sedona/pull/451) (==API change==): modify CRSTransform to ignore datum shift

New function:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): spatialRDD.flipCoordinates()

API / behavior change:

* PR [488](https://github.com/apache/incubator-sedona/pull/488): JoinQuery.SpatialJoinQuery/DistanceJoinQuery now returns `<Geometry, List>` instead of `<Geometry, HashSet>` because we can no longer use HashSet in Sedona for duplicates removal. All original duplicates in both input RDDs will be preserved in the output.

### Sedona-sql

Bug fix:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8) (==API change==): ST_Transform slow due to lock contention. See [here](/api/sql/GeoSparkSQL-Function/#st_transform)
* PR [427](https://github.com/apache/incubator-sedona/pull/427): ST_Point and ST_PolygonFromEnvelope now allows Double type

New function:

* PR [499](https://github.com/apache/incubator-sedona/pull/449): ST_Azimuth, ST_X, ST_Y, ST_StartPoint, ST_Boundary, ST_EndPoint, ST_ExteriorRing, ST_GeometryN, ST_InteriorRingN, ST_Dump, ST_DumpPoints, ST_IsClosed, ST_NumInteriorRings, ST_AddPoint, ST_RemovePoint, ST_IsRing
* PR [459](https://github.com/apache/incubator-sedona/pull/459): ST_LineMerge
* PR [460](https://github.com/apache/incubator-sedona/pull/460): ST_NumGeometries
* PR [469](https://github.com/apache/incubator-sedona/pull/469): ST_AsGeoJSON
* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): ST_FlipCoordinates

Behavior change:

* PR [480](https://github.com/apache/incubator-sedona/pull/480): Aggregate Functions rewrite for new Aggregator API. The functions can be used as typed functions in code and enable compilation-time type check.

API change:

* [SEDONA-11](https://issues.apache.org/jira/browse/SEDONA-11): Adapter.toDf() will directly generate a geometry type column. ST_GeomFromWKT is no longer needed.

### Sedona-viz

API change: Drop the function which can generate SVG vector images because the required library has an incompatible license and the SVG image is not good at plotting big data

### Sedona Python

API/Behavior change:

* Python-to-Sedona adapter is moved to a separate module. To use Sedona Python, see [here](../overview/#prepare-python-adapter-jar)

New function:

* PR [448](https://github.com/apache/incubator-sedona/pull/448): Add support for partition number in spatialPartitioning function `spatial_rdd.spatialPartitioning(grid_type, NUM_PARTITION)`


## GeoSpark legacy release notes

### v1.3.1

This version includes the official release of GeoSpark Python wrapper. It also contains a number of bug fixes and new functions. The tutorial section provides some articles to explain the usage of GeoSpark Python wrapper.

**GeoSpark Core**

Bug fix:

* Issue #[344](https://github.com/DataSystemsLab/GeoSpark/issues/344) and PR #[365](https://github.com/DataSystemsLab/GeoSpark/pull/365): GeoJSON reader cannot handle "id"
* Issue #[420](https://github.com/DataSystemsLab/GeoSpark/issues/420) and PR #[421](https://github.com/DataSystemsLab/GeoSpark/pull/421): Cannot handle null value in geojson properties
* PR #[422](https://github.com/DataSystemsLab/GeoSpark/pull/422): Use HTTPS to resolve dependencies in Maven Build

New functions:

* Issue #[399](https://github.com/DataSystemsLab/GeoSpark/issues/399) and PR #[401](https://github.com/DataSystemsLab/GeoSpark/pull/401): saveAsWKB
* PR #[402](https://github.com/DataSystemsLab/GeoSpark/pull/402): saveAsWKT

**GeoSpark SQL**

New functions:

* PR #[359](https://github.com/DataSystemsLab/GeoSpark/pull/359): ST_NPoints
* PR #[373](https://github.com/DataSystemsLab/GeoSpark/pull/373): ST_GeometryType
* PR #[398](https://github.com/DataSystemsLab/GeoSpark/pull/398): ST_SimplifyPreserveTopology
* PR #[406](https://github.com/DataSystemsLab/GeoSpark/pull/406): ST_MakeValid
* PR #[416](https://github.com/DataSystemsLab/GeoSpark/pull/416): ST\_Intersection\_aggr

Performance:

* Issue #[345](https://github.com/DataSystemsLab/GeoSpark/issues/345) and PR #[346](https://github.com/DataSystemsLab/GeoSpark/pull/346): the performance issue of Adapter.toDF() function

Bug fix:

* Issue #[395](https://github.com/DataSystemsLab/GeoSpark/issues/395) and PR #[396](https://github.com/DataSystemsLab/GeoSpark/pull/396): Fix the geometry col bug in Adapter

**GeoSpark Viz**

Bug fix:

* Issue #[378](https://github.com/DataSystemsLab/GeoSpark/issues/378) and PR #[379](https://github.com/DataSystemsLab/GeoSpark/pull/379): Classpath issue when integrating GeoSparkViz with s3

**GeoSpark Python**

Add new GeoSpark python wrapper for RDD and SQL APIs

**Contributors (12)**

* Mariano Gonzalez
* Paweł Kociński
* Semen Komissarov
* Jonathan Leitschuh
* Netanel Malka
* Keivan Shahida
* Sachio Wakai
* Hui Wang
* Wrussia
* Jia Yu
* Harry Zhu
* Ilya Zverev


### v1.3.0

This release has been skipped due to a bug in GeoSpark Python wrapper.

### v1.2.0

This version contains numerous bug fixes, new functions, and new GeoSpark module.

**License change**

From MIT to Apache License 2.0

**GeoSpark Core**

Bug fix:

* Issue #[224](https://github.com/DataSystemsLab/GeoSpark/issues/224) load GeoJSON non-spatial attributes.
* Issue #[228](https://github.com/DataSystemsLab/GeoSpark/issues/228) Shapefiel Reader fails to handle UNDEFINED type.
* Issue #[320](https://github.com/DataSystemsLab/GeoSpark/issues/320) Read CSV ArrayIndexOutOfBoundsException

New functions:

* PR #[270](https://github.com/DataSystemsLab/GeoSpark/pull/270) #[298](https://github.com/DataSystemsLab/GeoSpark/pull/298) Add GeoJSON Reader to load GeoJSON with all attributes. See [GeoSpark doc](../tutorial/rdd/#from-geojson) for an example.
* PR #[314](https://github.com/DataSystemsLab/GeoSpark/pull/314) Add WktReader and WkbReader. Their usage is simialr to GeoJSON reader.

**GeoSpark SQL**

Bug fix:

* Issue #[244](https://github.com/DataSystemsLab/GeoSpark/issues/244) JTS side location conflict
* Issue #[245](https://github.com/DataSystemsLab/GeoSpark/issues/245) Drop ST_Circle in 1.2.0
* Issue #[288](https://github.com/DataSystemsLab/GeoSpark/issues/288) ST_isValid fails
* Issue #[321](https://github.com/DataSystemsLab/GeoSpark/issues/321) ST_Point doesn't accept null user data
* PR #[284](https://github.com/DataSystemsLab/GeoSpark/pull/284) ST_Union_Aggr bug
* PR #[331](https://github.com/DataSystemsLab/GeoSpark/pull/331) Adapter doesn't handle null values

New SQL functions:

* ST_IsValid
* ST_PrecisionReduce
* ST_Touches
* ST_Overlaps
* ST_Equals
* ST_Crosses
* ST_IsSimple
* ST_AsText

Behavior / API change:

* GeoSpark Adapter will automatically carry all attributes between DataFrame and RDD. No need to use UUID in SQL ST functions to pass values. Please read [GeoSpark doc](../tutorial/sql/#dataframe-to-spatialrdd).

**GeoSpark Viz**

Bug fix:

* Issue #[231](https://github.com/DataSystemsLab/GeoSpark/issues/231) Pixel NullPointException
* Issue #[234](https://github.com/DataSystemsLab/GeoSpark/issues/234) OutOfMemory for large images

New functions

* Add the DataFrame support. Please read [GeoSpark doc](../tutorial/viz)
* ST_Pixelize
* ST_TileName
* ST_Colorize
* ST_EncodeImage
* ST_Render

Behavior / API change

* GeoSparkViz Maven coordinate changed. You need to specify Spark version. Please read [GeoSpark Maven coordinate](GeoSpark-All-Modules-Maven-Central-Coordinates/#geospark-viz-120-and-later)

**GeoSpark-Zeppelin**

New functions

* Add the support of connecting GeoSpark and Zeppelin
* Add the support of connecting GeoSparkViz and Zeppelin

**Contributors (13)**

Anton Peniaziev, Avshalom Orenstein, Jia Yu, Jordan Perr-Sauer, JulienPeloton, Sergii Mikhtoniuk, Netanel Malka, Rishabh Mishra, sagar1993, Shi-Hao Liu, Serhuela, tociek, Wrussia

### v1.1.3

This version contains a critical bug fix for GeoSpark-core RDD API.

**GeoSpark Core**

* Fixed Issue #[222](https://github.com/DataSystemsLab/GeoSpark/issues/222): geometry toString() method has cumulative non-spatial attributes. See PR #[223](https://github.com/DataSystemsLab/GeoSpark/pull/223)

**GeoSpark SQL**

None

**GeoSpark Viz**

None

### v1.1.2

This version contains several bug fixes and several small improvements.

**GeoSpark Core**

* Added WKB input format support (Issue #[2](https://github.com/DataSystemsLab/GeoSpark/issues/2), [213](https://github.com/DataSystemsLab/GeoSpark/issues/213)): See PR #[203](https://github.com/DataSystemsLab/GeoSpark/pull/203), [216](https://github.com/DataSystemsLab/GeoSpark/pull/216). Thanks for the patch from Lucas C.!
* Added empty constructors for typed SpatialRDDs. This is especially useful when the users want to load a persisted RDD from disk and assemble a typed SpatialRDD by themselves. See PR #[211](https://github.com/DataSystemsLab/GeoSpark/pull/211)
* Fixed Issue #[214](https://github.com/DataSystemsLab/GeoSpark/issues/214): duplicated geometry parts when print each Geometry in a SpatialRDD to a String using toString() method. See PR #[216](https://github.com/DataSystemsLab/GeoSpark/pull/216)

**GeoSpark SQL**

* Added ST_GeomFromWKB expression (Issue #[2](https://github.com/DataSystemsLab/GeoSpark/issues/2)): See PR #[203](https://github.com/DataSystemsLab/GeoSpark/pull/203). Thanks for the patch from Lucas C.!
* Fixed Issue #[193](https://github.com/DataSystemsLab/GeoSpark/issues/193): IllegalArgumentException in RangeJoin: Number of partitions must be >= 0. See PR #[207](https://github.com/DataSystemsLab/GeoSpark/pull/207)
* Fixed Issue #[204](https://github.com/DataSystemsLab/GeoSpark/issues/204): Wrong ST_Intersection result. See PR #[205](https://github.com/DataSystemsLab/GeoSpark/pull/205)
* [For Developer] Separate the expression catalog and the udf registrator to simplify the steps of merging patches among different Spark versions. See PR #[209](https://github.com/DataSystemsLab/GeoSpark/pull/209)

**GeoSpark Viz**

None

### v1.1.1

This release has been skipped due to wrong Maven Central configuration.

### v1.1.0

This version adds very efficient R-Tree and Quad-Tree index serializers and supports Apache Spark and  SparkSQL 2.3. See [Maven Central coordinate](./GeoSpark-All-Modules-Maven-Central-Coordinates) to locate the particular version.

**GeoSpark Core**

* Fixed Issue #[185](https://github.com/DataSystemsLab/GeoSpark/issues/185): CRStransform throws Exception for Bursa wolf parameters. See PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).
* Fixed Issue #[190](https://github.com/DataSystemsLab/GeoSpark/issues/190): Shapefile reader doesn't support Chinese characters (中文字符). See PR #[192](https://github.com/DataSystemsLab/GeoSpark/pull/192).
* Add R-Tree and Quad-Tree index serializer. GeoSpark custom index serializer has around 2 times smaller index size and faster serialization than Apache Spark kryo serializer. See PR #[177](https://github.com/DataSystemsLab/GeoSpark/pull/177).

**GeoSpark SQL**

* Fixed Issue #[194](https://github.com/DataSystemsLab/GeoSpark/issues/194): doesn't support Spark 2.3.
* Fixed Issue #[188](https://github.com/DataSystemsLab/GeoSpark/issues/188):ST_ConvexHull should accept any type of geometry as an input. See PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).
* Add ST_Intersection function. See Issue #[110](https://github.com/DataSystemsLab/GeoSpark/issues/110) and PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).

**GeoSpark Viz**

* Fixed Issue #[154](https://github.com/DataSystemsLab/GeoSpark/issues/154): GeoSpark kryp serializer and GeoSparkViz conflict. See PR #[178](https://github.com/DataSystemsLab/GeoSpark/pull/178)

### v1.0.1
**GeoSpark Core**

* Fixed Issue #[170](https://github.com/DataSystemsLab/GeoSpark/issues/170)

**GeoSpark SQL**

* Fixed Issue #[171](https://github.com/DataSystemsLab/GeoSpark/issues/171)
* Added the support of SparkSQL 2.2. GeoSpark-SQL for Spark 2.1 is published separately ([Maven Coordinates](./GeoSpark-All-Modules-Maven-Central-Coordinates)).

**GeoSpark Viz**
None

---
### v1.0.0
**GeoSpark Core**

* Add GeoSparkConf class to read GeoSparkConf from SparkConf

**GeoSpark SQL**

* Initial release: fully supports SQL/MM-Part3 Spatial SQL standard

**GeoSpark Viz**

* Republish GeoSpark Viz under "GeoSparkViz" folder. All "Babylon" strings have been replaced to "GeoSparkViz"

---

---

---
### v0.9.1 (GeoSpark-core)
* **Bug fixes**: Fixed "Missing values when reading Shapefile": [Issue #141](https://github.com/DataSystemsLab/GeoSpark/issues/141)
* **Performance improvement**: Solved Issue [#91](https://github.com/DataSystemsLab/GeoSpark/issues/91), [#103](https://github.com/DataSystemsLab/GeoSpark/issues/103), [#104](https://github.com/DataSystemsLab/GeoSpark/issues/104), [#125](https://github.com/DataSystemsLab/GeoSpark/issues/125), [#150](https://github.com/DataSystemsLab/GeoSpark/issues/150).
    * Add GeoSpark customized Kryo Serializer to significantly reduce memory footprint. This serializer which follows Shapefile compression rule takes less memory than the default Kryo. See [PR 139](https://github.com/DataSystemsLab/GeoSpark/pull/139).
    * Delete the duplicate removal by using Reference Point concept. This eliminates one data shuffle but still guarantees the accuracy. See [PR 131](https://github.com/DataSystemsLab/GeoSpark/pull/131).
* **New Functionalities added**:
    * **SpatialJoinQueryFlat/DistanceJoinQueryFlat** returns the join query in a flat way following database iteration model: Each row has fixed two members [Polygon, Point]. This API is more efficient for unbalanced length of join results. 
    * The left and right shapes in Range query, Distance query, Range join query, Distance join query can be switched.
    * The index side in Range query, Distance query, Range join query, Distance join query can be switched.
    * The generic SpatialRdd supports heterogenous geometries
    * Add KDB-Tree spatial partitioning method which is more balanced than Quad-Tree
    * Range query, Distance query, Range join query, Distance join query, KNN query supports heterogenous inputs.
### v0.8.2 (GeoSpark-core)
* **Bug fixes**: Fix the shapefile RDD null pointer bug when running in cluster mode. See Issue https://github.com/DataSystemsLab/GeoSpark/issues/115
* **New function added**: Provide granular control to SpatialRDD sampling utils. SpatialRDD has a setter and getter for a parameter called "sampleNumber". The user can manually specify the sample size for spatial partitioning.
### v0.8.1 (GeoSpark-core)
* **Bug fixes**: (1) Fix the blank DBF attribute error when load DBF along with SHX file. (2) Allow user to call CRS transformation function at any time. Previously, it was only allowed in GeoSpark constructors
### v0.8.0 (GeoSpark-core)
* **New input format added**: GeoSpark is able to load and query ESRI ShapeFile (.shp, .shx, .dbf) from local disk and HDFS! Users first need to build a Shapefile RDD by giving Spark Context and an input path then call ShapefileRDD.getSpatialRDD to retrieve Spatial RDD. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase), [Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase))
* **Join Query Performance enhancement 1**: GeoSpark provides a new Quad-Tree Spatial Partitioning method to speed up Join Query. Users need to pass GridType.QUADTREE parameter to RDD1.spatialPartitioning() function. Then users need to use RDD1.partitionTree in RDD2.spatialPartitioning() function. This Quad-Tree partitioning method (1) avoids overflowed spatial objects when partitioning spatial objects. (2) checking a spatial object against the Quad-Tree grids is completed in a log complexity tree search. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase), [Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase))
* **Join Query Performance enhancement 2**: Internally, GeoSpark uses zipPartitions instead of CoGroup to join two Spatial RDD so that the incurred shuffle overhead decreases.
* **SpatialRDD Initialization Performance enhancement**: GeoSpark uses mapPartition instead of flatMapToPair to generate Spatial Objects. This will speed up the calculation.
* **API changed**: Since it chooses mapPartition API in mappers, GeoSpark no longer supports the old user supplified format mapper. However, if you are using your own format mapper for old GeoSpark version, you just need to add one more loop to fit in GeoSpark 0.8.0. Please see [GeoSpark user supplied format mapper examples](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase)
* **Alternative SpatialRDD constructor added**: GeoSpark no longer forces users to provide StorageLevel parameter in their SpatialRDD constructors. This will siginicantly accelerate all Spatial RDD initialization.
 1. If he only needs Spatial Range Query and KNN query, the user can totally remove this parameter from their constructors.
 2. If he needs Spatial Join Query or Distance Join Query but he knows his dataset boundary and approximate total count, the user can also remove StorageLevel parameter and append a Envelope type dataset boundary and an approxmiate total count as additional parameters.
 3. **If he needs Spatial Join Query or Distance Join Query but knows nothing about his dataset**, the user still has to pass StorageLevel parameter.
* **Bug fix**: Fix bug [Issue #97](https://github.com/DataSystemsLab/GeoSpark/issues/97) and [Issue #100](https://github.com/DataSystemsLab/GeoSpark/issues/100).

### v0.1 - v0.7
|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.7.0| **Coordinate Reference System (CRS) Transformation (aka. Coordinate projection) added:** GeoSpark allows users to transform the original CRS (e.g., degree based coordinates such as EPSG:4326 and WGS84) to any other CRS (e.g., meter based coordinates such as EPSG:3857) so that it can accurately process both geographic data and geometrical data. Please specify your desired CRS in GeoSpark Spatial RDD constructor ([Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/core/src/main/scala/org/datasyslab/geospark/showcase/ScalaExample.scala#L221)); **Unnecessary dependencies removed**: NetCDF/HDF support depends on [SerNetCDF](https://github.com/jiayuasu/SerNetCDF). SetNetCDF becomes optional dependency to reduce fat jar size; **Default JDK/JRE change to JDK/JRE 1.8**: To satisfy CRS transformation requirement, GeoSpark is compiled by JDK 1.8 by default; **Bug fix**: fix a small format bug when output spatial RDD to disk.|
|0.6.2| **New input format added:** Add a new input format mapper called EarthdataHDFPointMapper so that GeoSpark can load, query and save NASA Petabytes NetCDF/HDF Earth Data ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase)); **Bug fix:** Print UserData attribute when output Spatial RDDs as GeoJSON or regular text file|
|0.6.1| **Bug fixes:** Fix typos LineString DistanceJoin API|
|0.6.0| **Major updates:** (1) DistanceJoin is merged into JoinQuery. GeoSpark now supports complete DistanceJoin between Points, Polygons, and LineStrings. (2) Add Refine Phase to Spatial Range and Join Query. Use real polygon coordinates instead of its MBR to filter the final results. **API changes:** All spatial range and join  queries now take a parameter called *ConsiderBoundaryIntersection*. This will tell GeoSpark whether returns the objects intersect with windows.|
|0.5.3| **Bug fix:** Fix [Issue #69](https://github.com/DataSystemsLab/GeoSpark/issues/69): Now, if two objects have the same coordinates but different non-spatial attributes (UserData), GeoSpark treats them as different objects.|
|0.5.2| **Bug fix:** Fix [Issue #58](https://github.com/DataSystemsLab/GeoSpark/issues/58) and [Issue #60](https://github.com/DataSystemsLab/GeoSpark/issues/60); **Performance enhancement:** (1) Deprecate all old Spatial RDD constructors. See the JavaDoc [here](http://www.public.asu.edu/~jiayu2/geospark/javadoc/0.5.2/). (2) Recommend the new SRDD constructors which take an additional RDD storage level and automatically cache rawSpatialRDD to accelerate internal SRDD analyze step|
|0.5.1| **Bug fix:** (1) GeoSpark: Fix inaccurate KNN result when K is large (2) GeoSpark: Replace incompatible Spark API call [Issue #55](https://github.com/DataSystemsLab/GeoSpark/issues/55); (3) Babylon: Remove JPG output format temporarily due to the lack of OpenJDK support|
| 0.5.0| **Major updates:** We are pleased to announce the initial version of [Babylon](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon) a large-scale in-memory geospatial visualization system extending GeoSpark. Babylon and GeoSpark are integrated together. You can just import GeoSpark and enjoy! More details are available here: [Babylon GeoSpatial Visualization](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon)|
| 0.4.0| **Major updates:** ([Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/geospark/showcase/Example.java)) 1. Refactor constrcutor API usage. 2. Simplify Spatial Join Query API. 3. Add native support for LineStringRDD; **Functionality enhancement:** 1. Release the persist function back to users. 2. Add more exception explanations.
|       0.3.2      	| Functionality enhancement: 1. [JTSplus Spatial Objects](https://github.com/jiayuasu/JTSplus) now carry the original input data. Each object stores "UserData" and provides getter and setter. 2. Add a new SpatialRDD constructor to transform a regular data RDD to a spatial partitioned SpatialRDD.                                                                             	|
|       0.3.1      	| Bug fix: Support Apache Spark 2.X version, fix a bug which results in inaccurate results when doing join query, add more unit test cases                                                                              	|
|        0.3       	| Major updates: Significantly shorten query time on spatial join for skewed data; Support load balanced spatial partitioning methods (also serve as the global index); Optimize code for iterative spatial data mining 	|
|        0.2       	| Improve code structure and refactor API                                                    																																|
|        0.1       	| Support spatial range, join and Knn               |

### GeoSpark-Viz (old)
|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.2.2| **Add the support of new output storage**: Now the user is able to output gigapixel or megapixel resolution images (image tiles or stitched single image) to HDFS and Amazon S3. Please use the new ImageGenerator not the BabylonImageGenerator class.|
|0.2.1| **Performance enhancement**: significantly accelerate single image generation pipeline. **Bug fix**:fix a bug in scatter plot parallel rendering.|
|0.2.0| **API updates for [Issue #80](https://github.com/DataSystemsLab/GeoSpark/issues/80):** 1. Babylon now has two different OverlayOperators for raster image and vector image: RasterOverlayOperator and VectorOverlayOperator; 2. Babylon merged old SparkImageGenerator and NativeJavaGenerator into a new BabylonImageGenerator which has neat APIs; **New feature:** Babylon can use Scatter Plot to visualize NASA Petabytes NetCDF/HDF format Earth Data. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/java/org/datasyslab/babylon/showcase))
|0.1.1| **Major updates:** Babylon supports vector image and outputs SVG image format|
|0.1.0| **Major updates:** Babylon initial version supports raster images|