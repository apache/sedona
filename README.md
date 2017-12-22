![GeoSpark Logo](http://www.public.asu.edu/~jiayu2/geospark/logo.png)


| Status   |      Stable    | Latest | Source code|Spark compatibility|
|:----------:|:-------------:|:------|:------:|:------|
| GeoSpark |  [![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|Spark 2.X, 1.X|
| GeoSparkSQL |  [![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark-sql.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark-sql.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)| Spark SQL 2.1, 2.2|
| [GeoSparkViz](https://github.com/DataSystemsLab/GeoSpark/tree/master/viz) |   [![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.datasyslab/geospark-viz.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.datasyslab/geospark-viz.svg)](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates) | [![Build Status](https://travis-ci.org/DataSystemsLab/GeoSpark.svg?branch=master)](https://travis-ci.org/DataSystemsLab/GeoSpark)|Spark 2.X, 1.X|

[GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU)||[GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board)||[![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

GeoSpark is listed as **Infrastructure Project** on [**Apache Spark Official Third Party Project Page**](http://spark.apache.org/third-party-projects.html)

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs) that efficiently load, process, and analyze large-scale spatial data across machines. GeoSpark provides APIs for Apache Spark programmer to easily develop their spatial analysis programs with Spatial Resilient Distributed Datasets (SRDDs) which have in house support for geometrical and Spatial Queries (Range, K Nearest Neighbors, Join).



GeoSpark artifacts are hosted in Maven Central: [**Maven Central Coordinates**](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates)



##  Version release notes: [click here](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Release-notes)

## News!
* GeoSpark 1.0.1 is release. This release added the SQL support of SparkSQL 2.2 and includes two minor bug fixes. Now GeoSpark-SQL fully supports SparkSQL 2.1 and 2.2 ([Maven coordinates](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-All-Modules-Maven-Central-Coordinates)).
* GeoSpark 1.0 is released. This release mainly includes a complete version of **GeoSparkSQL**. Look at these exciting features! Documents are at [GeoSpark Wiki](https://github.com/DataSystemsLab/GeoSpark/wiki) 
  * Supports SQL/MM-Part3, Spatial SQL standard 
  * Supports pure Spark SQL statement. No DSL style any more!
  * Supports Spark query optimizer: the beloved GeoSpark Spatial Join / predicate pushdown!
  * Supports multiple GeoSpark parameters: take the control of your own program!
  * Supports constructors, functions, aggregate functions, and predicates!
* GeoSparkSQL 1.0 is released. This module contains contributions from Jia Yu, Masha Basmanova, Mohamed Sarwat and Zongsi Zhang. Especially, we want to thank Masha for her great patch on designing spatial join strategy and optimization.
* GeoSpark 0.9.1 is released (more details in [Release notes](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Full-Version-Release-notes))
* Welcome GeoSpark new contributor, Masha Basmanova (@mbasmanova) from Facebook!
* Welcome GeoSpark new contributor, Zongsi Zhang (@zongsizhang) from Arizona State University!


# Important features ([more](https://github.com/DataSystemsLab/GeoSpark/wiki))

## Spatial SQL
GeoSparkSQL fully supports Apache Spark SQL. Features are as follows:

  * Supports SQL/MM-Part3, Spatial SQL standard 
  * Supports pure Spark SQL statement. No DSL style any more!
  * Supports Spark query optimizer: the beloved GeoSpark Spatial Join / predicate pushdown!
  * Supports multiple GeoSpark parameters: take the control of your own program!
  * Supports constructors, functions, aggregate functions, and predicates!

## Spatial Resilient Distributed Datasets (SRDDs)
Supported Spatial RDDs: PointRDD, RectangleRDD, PolygonRDD, LineStringRDD

The generic SpatialRDD supports heterogenous geometries:

* Point
* Polygon
* Line string
* Multi-point
* Multi-polygon
* Multi-line string
* GeometryCollection
* Circle


## Supported input data format

**Native input format support**: 

* CSV
* TSV
* WKT
* GeoJSON (single-line compact format)
* NASA Earth Data NetCDF/HDF
* ESRI ShapeFile(.shp, .shx, .dbf)

**User-supplied input format mapper**: Any single-line input formats

## Spatial Partitioning
Supported Spatial Partitioning techniques: Quad-Tree (recommend), KDB-Tree (recommend), R-Tree, Voronoi diagram, Uniform grids (Experimental), Hilbert Curve (Experimental)

## Spatial Index
Supported Spatial Indexes: Quad-Tree and R-Tree. R-Tree supports Spatial K Nearest Neighbors query.

## Geometrical operation
DatasetBoundary, Minimum Bounding Rectangle, Polygon Union

## Spatial Operation
Spatial Range Query, Distance Join Query, Spatial Join Query (Inside and Overlap), and Spatial K Nearest Neighbors Query.

## Coordinate Reference System (CRS) Transformation (aka. Coordinate projection)

GeoSpark allows users to transform the original CRS (e.g., degree based coordinates such as EPSG:4326 and WGS84) to any other CRS (e.g., meter based coordinates such as EPSG:3857) so that it can accurately process both geographic data and geometrical data. Please specify your desired CRS in GeoSpark Spatial RDD constructor ([Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/core/src/main/scala/org/datasyslab/geospark/showcase/ScalaExample.scala#L221)).

## Users

### Companies that are using GeoSpark (incomplete list)

[<img src="https://www.bluedme.com/wp-content/uploads/2015/10/cropped-LOGO-Blue-DME-PNG-3.png" width="150">](https://www.bluedme.com/)

[<img src="https://retailrecharged.com/wp-content/uploads/2017/10/logo.png" width="150">](https://www.gyana.co.uk/)

Please make a Pull Request to add yourself!

# GeoSpark Tutorial ([more](https://github.com/DataSystemsLab/GeoSpark/wiki))
GeoSpark full tutorial is available at GeoSpark GitHub Wiki: [GeoSpark GitHub Wiki](https://github.com/DataSystemsLab/GeoSpark/wiki)

GeoSpark Scala and Java template project is available here: [Template Project](https://github.com/jiayuasu/GeoSparkTemplateProject)

GeoSpark Function Use Cases: [Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase), [Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase)

# GeoSpark Visualization Extension (GeoSparkViz)
GeoSparkViz is a large-scale in-memory geospatial visualization system.

GeoSparkViz provides native support for general cartographic design by extending GeoSpark to process large-scale spatial data. It can visulize Spatial RDD and Spatial Queries and render super high resolution image in parallel.

More details are available here: [GeoSpark Visualization Extension](https://github.com/DataSystemsLab/GeoSpark/tree/master/viz) 

## GeoSparkViz Gallery
<img style="float: left;" src="http://www.public.asu.edu/~jiayu2/geospark/picture/usrail.png" width="250">

[Watch High Resolution on a real map](http://www.public.asu.edu/~jiayu2/geospark/picture/overlay.html)

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/heatmapnycsmall.png" width="500">

<img src="http://www.public.asu.edu/~jiayu2/geospark/picture/ustweet.png" width="250">


# Publication

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["A Demonstration of GeoSpark: A Cluster Computing Framework for Processing Big Spatial Data"](). (demo paper) In Proceeding of IEEE International Conference on Data Engineering ICDE 2016, Helsinki, FI, May 2016

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["GeoSpark: A Cluster Computing Framework for Processing Large-Scale Spatial Data"](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_ShortPaper.pdf). (short paper) In Proceeding of the ACM International Conference on Advances in Geographic Information Systems ACM SIGSPATIAL GIS 2015, Seattle, WA, USA November 2015

# Statement from our team

## Benchmark
We welcome people to use GeoSpark for benchmark purpose. To achieve the best performance or enjoy all features of GeoSpark, 

* Please always use the latest version or state the version used in your benchmark so that we can trace back to the issues.
* Please consider using GeoSpark core instead of GeoSparkSQL. Due to the limitation of SparkSQL (for instance, not support clustered index), we are not able to expose all features to SparkSQL.
* Please open GeoSpark kryo serializer to reduce the memory footprint.

## Paper citation
Currently, we have published two papers about GeoSpark. Only these two papers are associated with GeoSpark Development Team.


# Contact

## Questions

* [GeoSpark@Twitter](https://twitter.com/GeoSpark_ASU)
* [GeoSpark Discussion Board](https://groups.google.com/forum/#!forum/geospark-discussion-board)
* [![Join the chat at https://gitter.im/geospark-datasys/Lobby](https://badges.gitter.im/geospark-datasys/Lobby.svg)](https://gitter.im/geospark-datasys/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* Email us!

## Contact
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

## Project website
Please visit [GeoSpark project wesbite](http://geospark.datasyslab.org) for latest news and releases.

## Data Systems Lab
GeoSpark is one of the projects initiated by [Data Systems Lab](https://www.datasyslab.net/) at Arizona State University. The mission of Data Systems Lab is designing and developing experimental data management systems (e.g., database systems).
