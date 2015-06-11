# GeoSpark
A Cluster Computing System for Processing Large-Scale Spatial and Spatio-Temporal Data

![GeoSpark Logo](http://datasyslab.ghost.io/content/images/2015/06/GeoSpark.png)

## Introduction
GeoSpark consists of three layers: Apache Spark Layer, Spatial RDD Layer and Spatial Query Processing Layer. Apache Spark Layer provides basic Apache Spark functionalities that include loading / storing data to disk as well as regular RDD operations. Spatial RDD Layer consists of three novel Spatial Resilient Distributed Datasets (SRDDs) which extend regular Apache Spark RDD to support geometrical and spatial objects. GeoSpark provides a geometrical operations library that access Spatial RDDs to perform basic geometrical operations. The Spatial Query Processing Layer executes spatial queries (e.g., Spatial Join) on Spatial RDDs.

## How to process spatial data with GeoSpark (For general users) 

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JRE 1.7

### Steps

1. Setup Apache Hadoop Distributed File System and Apache Spark cluster
2. Load spatail data into Hadoop Distributed File System
3. Call "java -jar GeoSpark.jar" in GeoSpark.jar folder
4. Follow the instruction on the command window to use needed spatial functions as well as specified parameters
5. GeoSpark will submit Spark tasks and persist the result to HDFS

## How to make use of GeoSpark in your own program (For Scala and Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7

### Steps

1. Create your own project
2. Add GeoSpark.jar into your build environment
3. Go ahead to use GeoSpark spatial RDDs to store spatial data and call needed functions!

### One quick start program (In Java)
Please check out the "QuickStartProgram.java" in GeoSpark root folder for a sample program with GeoSpark.


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

## Required input spatial dataset schema

### Point

(Longitude, Latitude)

### Rectangle

(Longitude 1, Longitude 2, Latitude 1, Latitude 2)

Two pairs of longitude and latitude present the vertexes lie on the diagonal of one rectangle.

### Polygon

(Longitude 1, Latitude 1, Longitude 2, Latitude 2, ...)

Each tuple contains unlimited points.

## Main functionality checklist

### PointRDD

  * `Constructor: PointRDD(JavaSparkContext spark, String InputLocation)`
 
  * `Constructor: PointRDD(JavaRDD<point> pointRDD)`

 
  * `void rePartition(Integer partitions)`


  * `PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 

  * `PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  
  * `Double[] boundary()` 
 
  * `SpatialPairRDD<Point,ArrayList<Point>> SpatialJoinQuery(CircleRDD circleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`


  * `SpatialPairRDD<Envelope,ArrayList<Point>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 

### RectangleRDD

  * `Constructor: RectangleRDD(JavaRDD<Envelope> rectangleRDD)`
 

  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation)`
 

  * `void rePartition(Integer partitions)`
  
 
  * `RectangleRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 

  * `RectangleRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  
  * `Double[] boundary()` 
 
  * `SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`


  * `SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`


  * `SpatialPairRDD<Polygon,ArrayList<Envelope>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
   

### PolygonRDD

  * `Constructor: PolygonRDD(JavaRDD<Polygon> polygonRDD)`


  * `Constructor: PolygonRDD(JavaSparkContext spark, String InputLocation)`


  * `void rePartition(Integer partitions)`

  * `RectangleRDD MinimumBoundingRectangle()`
 

  * `PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 

  * `PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQuery(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQueryWithMBR(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

  * `Union PolygonUnion()`

### CircleRDD

* `Constructor: CircleRDD(PointRDD pointRDD, Double Radius)`
* `Constructor: CircleRDD(Double x, Double y, Double Radius)`
* `RectangleRDD MinimumBoundingRectangle()`
* `PointRDD Centre()`

### Circle

* `Constructor: Circle(Double x, Double y, Double radius)`
* `Constructor: Circle(Point point, Double radius)`
* `Envelope getMBR()`
* `static Circle MBRtoCircle(Envelope mbr)()`

### SpatialPairRDD

* `Constructor: SpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD)`

* `SpatialPairRDD<T1, Integer> countByKey()`

* `SpatialPairRDD<T1, Point> FlatMapToPoint()`

* `SpatialPairRDD<T1, Envelope> FlatMapToRectangle()`

* `SpatialPairRDD<T1, Polygon> FlatMapToPolygon()`
