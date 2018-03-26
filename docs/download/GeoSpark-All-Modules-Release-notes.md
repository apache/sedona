## v1.1.0

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

## v1.0.1
**GeoSpark Core**

* Fixed Issue #[170](https://github.com/DataSystemsLab/GeoSpark/issues/170)

**GeoSpark SQL**

* Fixed Issue #[171](https://github.com/DataSystemsLab/GeoSpark/issues/171)
* Added the support of SparkSQL 2.2. GeoSpark-SQL for Spark 2.1 is published separately ([Maven Coordinates](./GeoSpark-All-Modules-Maven-Central-Coordinates)).

**GeoSpark Viz**
None

---
## v1.0.0
**GeoSpark Core**

* Add GeoSparkConf class to read GeoSparkConf from SparkConf

**GeoSpark SQL**

* Initial release: fully supports SQL/MM-Part3 Spatial SQL standard

**GeoSpark Viz**

* Republish GeoSpark Viz under "GeoSparkViz" folder. All "Babylon" strings have been replaced to "GeoSparkViz"

---

---

---
## v0.9.1 (GeoSpark-core)
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
## v0.8.2 (GeoSpark-core)
* **Bug fixes**: Fix the shapefile RDD null pointer bug when running in cluster mode. See Issue https://github.com/DataSystemsLab/GeoSpark/issues/115
* **New function added**: Provide granular control to SpatialRDD sampling utils. SpatialRDD has a setter and getter for a parameter called "sampleNumber". The user can manually specify the sample size for spatial partitioning.
## v0.8.1 (GeoSpark-core)
* **Bug fixes**: (1) Fix the blank DBF attribute error when load DBF along with SHX file. (2) Allow user to call CRS transformation function at any time. Previously, it was only allowed in GeoSpark constructors
## v0.8.0 (GeoSpark-core)
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

## v0.1 - v0.7
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

## GeoSpark-Viz (old)
|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.2.2| **Add the support of new output storage**: Now the user is able to output gigapixel or megapixel resolution images (image tiles or stitched single image) to HDFS and Amazon S3. Please use the new ImageGenerator not the BabylonImageGenerator class.|
|0.2.1| **Performance enhancement**: significantly accelerate single image generation pipeline. **Bug fix**:fix a bug in scatter plot parallel rendering.|
|0.2.0| **API updates for [Issue #80](https://github.com/DataSystemsLab/GeoSpark/issues/80):** 1. Babylon now has two different OverlayOperators for raster image and vector image: RasterOverlayOperator and VectorOverlayOperator; 2. Babylon merged old SparkImageGenerator and NativeJavaGenerator into a new BabylonImageGenerator which has neat APIs; **New feature:** Babylon can use Scatter Plot to visualize NASA Petabytes NetCDF/HDF format Earth Data. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/java/org/datasyslab/babylon/showcase))
|0.1.1| **Major updates:** Babylon supports vector image and outputs SVG image format|
|0.1.0| **Major updates:** Babylon initial version supports raster images|