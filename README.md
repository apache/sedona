![GeoSpark Logo](http://datasyslab.ghost.io/content/images/2015/06/GeoSpark.png) current version: v0.2

[![Build Status](https://travis-ci.org/jinxuan/GeoSpark.svg)](https://travis-ci.org/jinxuan/GeoSpark) 
[![codecov.io](https://codecov.io/github/jinxuan/GeoSpark/coverage.svg?branch=master)](https://codecov.io/github/jinxuan/GeoSpark?branch=master)
GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs) that efficiently load, process, and analyze large-scale spatial data across machines. This problem is quite challenging due to the fact that (1) spatial data may be quite complex, e.g., rivers' and cities' geometrical boundaries, (2) spatial (and geometric) operations (e.g., Overlap, Intersect, Convex Hull, Cartographic Distances) cannot be easily and efficiently expressed using regular RDD transformations and actions. eoSpark provides APIs for Apache Spark programmer to easily develop their spatial analysis programs with Spatial Resilient Distributed Datasets (SRDDs) which have in house support for geometrical and distance operations. Experiments show that GeoSpark is scalable and exhibits faster run-time performance than Hadoop-based systems in spatial analysis applications like spatial join, spatial aggregation, spatial autocorrelation analysis and spatial co-location pattern recognition.


## How to get started (For Scala and Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7

Note: GeoSpark has been tested on Apache Spark 1.2, 1.3, 1.4 and Apache Hadoop 2.4, 2.6.

### Steps

1. Create your own Apache Spark project
2. Add GeoSpark.jar into your Apache Spark build environment
3. You can now use GeoSpark spatial RDDs in your Apache Spark program to store spatial data and call needed functions!

### GeoSpark Programming Examples (In Java)
1. Spatial queries in "app" folder: Spatial range, join and KNN.
2. Spatial analysis examples in "app" foler: Spatial aggregation, spatial autocorrelation and spatial co-location


## Java API usage

Please refer [GeoSpark Java API Usage](http://www.public.asu.edu/~jiayu2/geospark/javadoc/index.html)


## Spatial Resilient Distributed Datasets (SRDDs)

GeoSpark extends RDDs to form Spatial RDDs (SRDDs) and efficiently partitions SRDD data elements across machines and introduces novel parallelized spatial (geometric operations that follows the Open Geosptial Consortium (OGC) standard) transformations and actions (for SRDD) that provide a more intuitive interface for users to write spatial data analytics programs. Moreover, GeoSpark extends the SRDD layer to execute spatial queries (e.g., Range query, KNN query, and Join query) on large-scale spatial datasets. After geometrical objects are retrieved in the Spatial RDD layer, users can invoke spatial query processing operations provided in the Spatial Query Processing Layer of GeoSpark which runs over the in-memory cluster, decides how spatial object-relational tuples could be stored, indexed, and accessed using SRDDs, and returns the spatial query results required by user.

GeoSpark supports either Comma-Separated Values (CSV) or Tab-separated values (TSV) as the input format. Users only need to specify input format as Splitter and the start column of spatial info in one tuple as Offset when call Constructors.

### PointRDD

(column, column,..., Longitude, Latitude, column, column,...)

### RectangleRDD

(column, column,...,Longitude 1, Longitude 2, Latitude 1, Latitude 2,column, column,...)

Two pairs of longitude and latitude present the vertexes lie on the diagonal of one rectangle.

### PolygonRDD

(column, column,...,Longitude 1, Latitude 1, Longitude 2, Latitude 2, ...)

Each tuple contains unlimited points.

### Spatial Index

GeoSpark supports two Spatial Indexes, Quad-Tree and R-Tree. There are two methods in GeoSpark can create a desired Spatial Index.

1. Instantialize SpatialIndexRDDs like PointIndexRDD, RectangleIndexRDD or PolygonIndexRDD with "rtree" or "quadtree". GeoSpark will create corresponding local Spatial Index on each machine and cache it in memory automatically.
2. Call SpatialJoinQueryWithIndex in SpatialRDDs like PointRDD, RectangleRDD or PolygonRDD and specify IndexName with "rtree" or "quadtree". GeoSpark will create corresponding local Spatial Index on each machine on-the-fly when it does spatial join query.

### Geometrical operation

GeoSpark currently provides native support for Inside, Overlap, DatasetBoundary, Minimum Bounding Rectangle and Polygon Union in SRDDS following [Open Geospatial Consortium (OGC) standard](http://www.opengeospatial.org/standards).

### Spatial Operation

GeoSpark so far provides spatial range query, join query and KNN query in SRDDs.

## How to modify GeoSpark source code (For Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7
4. Maven 2
5. JTS Topology Suite version 1.13

### Steps

1. Create Java Maven project
2. Add the dependecies of Apache Hadoop, Spark and JTS Topology Suite
3. Put the java files in your your Maven project
4. Go ahead to modify GeoSpark source code!


## Publication

GeoSpark: A Cluster Computing Framework for Processing Large-Scale Spatial Data [[PDF](http://www.public.asu.edu/~jiayu2/geospark/publication/GeoSpark_ShortPaper.pdf)]

Jia Yu, Jinxuan Wu, Mohamed Sarwat

To appear at ACM International Conference on Advances in Geographic Information Systems ACM SIGSPATIAL GIS 2015, Seattle, WA, USA November 2015


## Acknowledgement

GeoSaprk makes use of JTS Topology Suite Version 1.13 for some geometrical computations.

Please refer [JTS Topology Suite website](http://tsusiatsoftware.net/jts/main.html) for more details.
## Contact

### Contributors
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Jinxuan Wu](http://www.public.asu.edu/~jinxuanw/) (Email: jinxuanw@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

###Project website
Please visit [GeoSpark project wesbite](http://www.public.asu.edu/~jiayu2/geospark/index.html) for latest news and releases.

### DataSys Lab
GeoSpark is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).
