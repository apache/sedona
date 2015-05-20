# GeoSpark
A Cluster Computing System for Processing Large-Scale Spatial and Spatio-Temporal Data

## Introduction
GeoSpark consists of three layers: Apache Spark Layer, Spatial RDD Layer and Spatial Query Processing Layer. Apache Spark Layer provides basic Apache Spark functionalities that include loading / storing data to disk as well as regular RDD operations. Spatial RDD Layer consists of three novel Spatial Resilient Distributed Datasets (SRDDs) which extend regular Apache Spark RDD to support geometrical and spatial objects. GeoSpark provides a geometrical operations library that access Spatial RDDs to perform basic geometrical operations. The Spatial Query Processing Layer executes spatial queries (e.g., Spatial Join) on Spatial RDDs.

## How to get started

### Prerequisites (For Java version)

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
3. JDK 1.7
4. Maven 2
5. JTS Topology Suite version 1.13

### Steps
1. Create Java Maven project
2. Add the dependecies of Apache Hadoop, Spark and JTS Topology Suite
3. Put the java files in your project or add the jar package into your Maven project
4. Use spatial RDDs to store spatial data and call needed functions

### One quick start program
Please check out the "QuickStartProgram.java" in GeoSpark root folder for a sample program with GeoSpark.

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

  * `RectanglePairRDD SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PointRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one RectangleRDD which contains a set of rectangle query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one RectanglePairRDD which can be parsed or persisted on files. Note: Every query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).

  * `PolygonPairRDD SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PointRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one PolygonPairRDD which can be parsed or persisted on files. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `PolygonPairRDD SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
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

  * `RectanglePairRDD SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * 
This function executes a spatial join query on the RectangleRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one RectangleRDD which contains a set of rectangle query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one RectanglePairRDD which can be parsed or persisted on files. Note: Every query window is a rectangle which is called Envelope type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `PolygonPairRDD SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
   
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

  * `PolygonPairRDD SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PolygonRDD. This function firstly creates one grid file based on spatial coordinates of the two input datasets and each element from both of the two input datasets is assigned to one grid cell. Then this functions joins the elements, which lie inside the same grid, from two input datasets. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. Note: Every query window is a Polygon which is one type in JTS topology suite. The query result is one PolygonPairRDD which can be parsed or persisted on files. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `PolygonPairRDD SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
This function executes a spatial join query on the PolygonRDD. This function firstly uses the Minimum Bounding Rectangle sets of the first parameter and this PolygonRDD to call SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical). Then this function uses the first parameter and this PolygonRDD instead their MBR to refine the spatial join query result. The first parameter is one PolygonRDD which contains a set of polygon query windows, the second one stands for the spatial predicate, the third one is the granularity of the grid file on horizontal direction and the fourth one is the granularity of the grid file on vertical direction. The query result is one PolygonPairRDD which can be parsed or persisted on files. Note: Every query window is a Polygon which is one type in JTS topology suite. The spatial predicate can be either "fully contain" (0) or "overlap and fully contain" (1).  

  * `Union PolygonUnion()`

This function unions all of the polygons in this PolygonRDD and returns the result as one Polygon. Note: The result is one Polygon which is one type in JTS topology suite.

### RectanglePairRDD

  * `JavaPairRDD<Envelope,ArrayList<Point>> ParseToPoint()`

This function parses the result of join queries to a regular formats for the ease of use. The result of this function contains an Envelope which is the key of this RDD and a ArrayList<Point> which is the value. The Points in ArrayList satisfy the particular spatial predicate with the key Envelope. Note: Envelope and Point are types in JTS Topology Suite which present rectangle and point.

  * `JavaPairRDD<Envelope,ArrayList<Envelope>> ParseToRectangle()`
 
This function parses the result of join queries to a regular formats for the ease of use. The result of this function contains an Envelope which is the key of this RDD and a ArrayList<Envelope> which is the value. The Envelopes in ArrayList satisfy the particular spatial predicate with the key Envelope. Note: Envelope is one type in JTS Topology Suite which presents rectangle.

  * `void PersistOnFile(String OutpuLocation)`

This function persists this RectanglePairRDD to one particular OutputLocation.

### PolygonPairRDD
  
  * `JavaPairRDD<Polygon,ArrayList<Point>> ParseToPoint()`
 
This function parses the result of join queries to a regular formats for the ease of use. The result of this function contains a Polygon which is the key of this RDD and a ArrayList<Point> which is the value. The Points in ArrayList satisfy the particular spatial predicate with the key Polygon. Note: Polygon and Point are types in JTS Topology Suite which present polygon and point.  

  * `JavaPairRDD<Polygon,ArrayList<Envelope>> ParseToRectangle()`
 
This function parses the result of join queries to a regular formats for the ease of use. The result of this function contains a Polygon which is the key of this RDD and a ArrayList<Envelope> which is the value. The Envelopes in ArrayList satisfy the particular spatial predicate with the key Polygon. Note: Polygon and Envelope are types in JTS Topology Suite which present polygon and rectangle.    

  * `JavaPairRDD<Polygon,ArrayList<Polygon>> ParseToRectangle()`
 
This function parses the result of join queries to a regular formats for the ease of use. The result of this function contains an Polygon which is the key of this RDD and a ArrayList<Polygon> which is the value. The Polygons in ArrayList satisfy the particular spatial predicate with the key Polygon. Note: Polygon is one type in JTS Topology Suite which presents polygon.

  * `void PersistOnFile(String OutputLocation)`
 
This function persists this PolygonPairRDD to one particular OutputLocation.  
