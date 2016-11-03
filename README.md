![GeoSpark Logo](http://www.public.asu.edu/~jiayu2/geospark/logo.png)

[![Build Status](https://travis-ci.org/jiayuasu/GeoSpark.svg?branch=master)](https://travis-ci.org/jiayuasu/GeoSpark) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.datasyslab/geospark/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.datasyslab/geospark)


GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs) that efficiently load, process, and analyze large-scale spatial data across machines. GeoSpark provides APIs for Apache Spark programmer to easily develop their spatial analysis programs with Spatial Resilient Distributed Datasets (SRDDs) which have in house support for geometrical and distance operations.

GeoSpark is listed as **Infrastructure Project** in **Apache Spark  Third Party Project Wiki Page** ([Link](https://cwiki.apache.org/confluence/display/SPARK/Third+Party+Projects))

GeoSpark artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:

```
groupId: org.datasyslab
artifactId: geospark
version: 0.3.2
```

The following artifact supports Apache Spark 1.X versions:

```
groupId: org.datasyslab
artifactId: geospark
version: 0.3.2-spark-1.x
```



##  Version information ([Full List](https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Full-Version-Release-notes))


|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|       0.3.2      	| Functionality enhancement: 1. [JTSplus Spatial Objects](https://github.com/jiayuasu/JTSplus) now carry the original input data. Each object stores "UserData" and provides getter and setter. 2. Add a new SpatialRDD constructor to transform a regular data RDD to a spatial partitioned SpatialRDD.                                                                             	|
|       0.3.1      	| Bug fix: Support Apache Spark 2.X version, fix a bug which results in inaccurate results when doing join query, add more unit test cases                                                                              	|
|        0.3       	| Major updates: Significantly shorten query time on spatial join for skewed data; Support load balanced spatial partitioning methods (also serve as the global index); Optimize code for iterative spatial data mining 	|
|   Master branch  	| even with 0.3.2                                                                                                                                                                                                     	 	|
| Spark 1.X branch 	| even with 0.3.2 but only supports Apache Spark 1.X        																																								|

## How to get started (For Scala and Java developers)



### Prerequisites

1. Apache Spark 2.X releases (Apache Spark 1.X releases support available in GeoSpark for Spark 1.X branch)
2. JDK 1.7
3. You might need to modify the dependencies in "POM.xml" and make it consistent with your environment.

Note: GeoSpark Master branch supports Apache Spark 2.X releases and GeoSpark for Spark 1.X branch supports Apache Spark 1.X releases. Please refer to the proper branch you need.

### How to use GeoSpark APIs in an interactive Spark shell (Scala)

1. Have your Spark cluster ready.
2. Download [pre-compiled GeoSpark jar](https://github.com/DataSystemsLab/GeoSpark/releases) under "Release" tag.
3. Run Spark shell with GeoSpark as a dependency.

  `
  ./bin/spark-shell --jars GeoSpark_COMPILED.jar
  `

3. You can now call GeoSpark APIs directly in your Spark shell!

### How to use GeoSpark APIs in a self-contained Spark application (Scala and Java)

1. Create your own Apache Spark project in Scala or Java
2. Add GeoSpark Maven coordinates into your project dependencies.
4. You can now use GeoSpark APIs in your Spark program!
5. Use spark-submit to submit your compiled self-contained Spark program.

### GeoSpark Programming Examples (Scala)

[GeoSpark Scala Example](https://gist.github.com/jiayuasu/bcecaa2e9e6f280a0f9a72bb7549ffaa)

[Test Data](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/test/resources)

### GeoSpark Programming Examples (Java)

[GeoSpark Java Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/geospark/showcase/Example.java)

[Test Data](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/test/resources)

## Scala and Java API usage

Please refer [GeoSpark Scala and Java API Usage](http://www.public.asu.edu/~jiayu2/geospark/javadoc/index.html)


## Spatial Resilient Distributed Datasets (SRDDs)

GeoSpark extends RDDs to form Spatial RDDs (SRDDs) and efficiently partitions SRDD data elements across machines and introduces novel parallelized spatial (geometric operations that follows the Open Geosptial Consortium (OGC) standard) transformations and actions (for SRDD) that provide a more intuitive interface for users to write spatial data analytics programs. Moreover, GeoSpark extends the SRDD layer to execute spatial queries (e.g., Range query, KNN query, and Join query) on large-scale spatial datasets. After geometrical objects are retrieved in the Spatial RDD layer, users can invoke spatial query processing operations provided in the Spatial Query Processing Layer of GeoSpark which runs over the in-memory cluster, decides how spatial object-relational tuples could be stored, indexed, and accessed using SRDDs, and returns the spatial query results required by user.



### PointRDD

(column, column,..., Longitude, Latitude, column, column,...)

### RectangleRDD

(column, column,...,Longitude 1, Longitude 2, Latitude 1, Latitude 2,column, column,...)

Two pairs of longitude and latitude present the vertexes lie on the diagonal of one rectangle.

### PolygonRDD

(column, column,...,Longitude 1, Latitude 1, Longitude 2, Latitude 2, ...)

Each tuple contains unlimited points.

## Supported data format
GeoSpark supports Comma-Separated Values ("csv"), Tab-separated values ("tsv"), Well-Known Text ("wkt"), and  GeoJSON ("geojson") as the input formats. Users only need to specify input format as Splitter and the start column (if necessary) of spatial info in one tuple as Offset when call Constructors.

## Important features

### Spatial partitioning

GeoSpark supports equal size ("equalgrid"), R-Tree ("rtree") and Voronoi diagram ("voronoi") spatial partitioning methods. Spatial partitioning is to repartition RDD according to objects' spatial locations. Spatial join on spatial paritioned RDD will be very fast.

### Spatial Index

GeoSpark supports two Spatial Indexes, Quad-Tree and R-Tree. 

### Geometrical operation

GeoSpark currently provides native support for Inside, Overlap, DatasetBoundary, Minimum Bounding Rectangle and Polygon Union in SRDDS following [Open Geospatial Consortium (OGC) standard](http://www.opengeospatial.org/standards).

### Spatial Operation

GeoSpark so far provides spatial range query, join query and KNN query in SRDDs.


## Publication

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["A Demonstration of GeoSpark: A Cluster Computing Framework for Processing Big Spatial Data"](). (demo paper) In Proceeding of IEEE International Conference on Data Engineering ICDE 2016, Helsinki, FI, May 2016

Jia Yu, Jinxuan Wu, Mohamed Sarwat. ["GeoSpark: A Cluster Computing Framework for Processing Large-Scale Spatial Data"](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_ShortPaper.pdf). (short paper) In Proceeding of the ACM International Conference on Advances in Geographic Information Systems ACM SIGSPATIAL GIS 2015, Seattle, WA, USA November 2015


## Acknowledgement

GeoSaprk makes use of JTS Plus (An extended JTS Topology Suite Version 1.14) for some geometrical computations.

Please refer to [JTS Topology Suite website](http://tsusiatsoftware.net/jts/main.html) and [JTS Plus](https://github.com/jiayuasu/JTSplus) for more details.

## Thanks for the help from GeoSpark community
We appreciate the help and suggestions from the following GeoSpark users (List is increasing..):

* @gaufung
* @lrojas94
* @mdespriee
* @sabman
* @samchorlton
* @Tsarazin
* @TBuc
* ...



## Contact

### Contributors
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Jinxuan Wu](http://www.public.asu.edu/~jinxuanw/) (Email: jinxuanw@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

### Project website
Please visit [GeoSpark project wesbite](http://geospark.datasyslab.org) for latest news and releases.

### DataSys Lab
GeoSpark is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).
