# GeoSpark
A Cluster Computing System for Processing Large-Scale Spatial and Spatio-Temporal Data

## Introduction
GeoSpark consists of three layers: Apache Spark Layer, Spatial RDD Layer and Spatial Query Processing Layer. Apache Spark Layer provides basic Apache Spark functionalities that include loading / storing data to disk as well as regular RDD operations. Spatial RDD Layer consists of three novel Spatial Resilient Distributed Datasets (SRDDs) which extend regular Apache Spark RDD to support geometrical and spatial objects. GeoSpark provides a geometrical operations library that access Spatial RDDs to perform basic geometrical operations. The Spatial Query Processing Layer executes spatial queries (e.g., Spatial Join) on Spatial RDDs.

## How to process spatial data with GeoSpark (For general users) 

### Prerequisites

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
3. JDK 1.7

### Steps

1. Setup Apache Hadoop Distributed File System and Apache Spark cluster
2. Load spatail data into Hadoop Distributed File System
3. Call "java -jar GeoSpark.jar" in GeoSpark.jar folder
4. Follow the instruction on the command window to use needed spatial functions as well as specified parameters
5. GeoSpark will submit Spark tasks and persist the result to HDFS

## How to make use of GeoSpark in your own program (For Scala and Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
3. JDK 1.7

### Steps

1. Create your own project
2. Add GeoSpark.jar into your build environment
3. Go ahead to use GeoSpark spatial RDDs to store spatial data and call needed functions!

### One quick start program (In Java)
Please check out the "QuickStartProgram.java" in GeoSpark root folder for a sample program with GeoSpark.


## How to modify GeoSpark source code (For Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
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

 * `Constructor: PointRDD(JavaRDD<Point> pointRDD)`

This function creates a new PointRDD instance from an existing PointRDD.

  * `Constructor: PointRDD(JavaSparkContext spark, String InputLocation)`

This function creates a new PointRDD instance from an input file.
  
  * `void rePartition(Integer partitions)`

This function repartitions the PointRDD according to the parameter "partitions".

  * `PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 
This function executes a spatial range query on the PointRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one PointRDD. Note: The query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).

  * `PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)`

This function executes a spatial range query on the PointRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one PointRDD. Note: The query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `SpatialPairRDD<Envelope,ArrayList<Point>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PointRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one RectangleRDD which contains a set of rectangle query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one RectanglePairRDD which can be parsed or persisted on files. Note: Every query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).

  * `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PointRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one PolygonPairRDD which can be parsed or persisted on files. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PointRDD. This function firstly uses the Minimum Bounding Rectangle set of the first parameter to call SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical). Then this function uses the first parameter instead its MBR to refine the spatial join query result. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).   


### RectangleRDD

  * `Constructor: RectangleRDD(JavaRDD<Envelope> rectangleRDD)`
 
This function creates a new RectangleRDD instance from an existing RectangleRDD.

  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation)`
 
This function creates a new RectangleRDD instance from an input file.  

  * `void rePartition(Integer partitions)`
  
This function repartitions the PointRDD according to the parameter "partitions". 
  
  * `RectangleRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 
This function executes a spatial range query on the RectangleRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one RectangleRDD. Note: The query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `RectangleRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
 
This function executes a spatial range query on the PointRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one RectangleRDD. Note: The query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).    

  * `SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * 
This function executes a spatial join query on the RectangleRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one RectangleRDD which contains a set of rectangle query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one RectanglePairRDD which can be parsed or persisted on files. Note: Every query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `SpatialPairRDD<Polygon,ArrayList<Envelope>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
   
  This function executes a spatial join query on the RectangleRDD. This function firstly uses the Minimum Bounding Rectangle set of the first parameter to call SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical). Then this function uses the first parameter instead its MBR to refine the spatial join query result. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one PolygonPairRDD which can be parsed or persisted on files. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).

### PolygonRDD

  * `Constructor: PolygonRDD(JavaRDD<Polygon> polygonRDD)`

This function creates a new PolygonRDD instance from an existing PolygonRDD.

  * `Constructor: PolygonRDD(JavaSparkContext spark, String InputLocation)`

This function creates a new PolygonRDD instance from an input file.

  * `void rePartition(Integer partitions)`

This function repartitions the PointRDD according to the parameter "partitions".

  * `RectangleRDD MinimumBoundingRectangle()`
 
This function returns a RectangleRDD which contains the minimum bounding rectangles for all of the polygons in this PolygonRDD.

  * `PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 
This function executes a spatial range query on the PolygonRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one PolygonRDD. Note: The query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
 
This function executes a spatial range query on the PointRDD. The first parameter stands for the query window and the second one stands for the spatial predicate. The query result is one PolygonRDD. Note: The query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).    

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PolygonRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. Note: Every query window is a Polygon which is one type in JTS topology suite. The query result is one PolygonPairRDD which can be parsed or persisted on files. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PolygonRDD. This function firstly uses the Minimum Bounding Rectangle sets of the first parameter and this PolygonRDD to call SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical). Then this function uses the first parameter and this PolygonRDD instead their MBR to refine the spatial join query result. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one PolygonPairRDD which can be parsed or persisted on files. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `Union PolygonUnion()`

This function unions all of the polygons in this PolygonRDD and returns the result as one Polygon. Note: The result is one Polygon which is one type in JTS topology suite.

### SpatialPairRDD

* `Constructor: SpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD)`
* `SpatialPairRDD<T1, Integer> countByKey()`
