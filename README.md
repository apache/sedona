![GeoSpark Logo](http://datasyslab.ghost.io/content/images/2015/06/GeoSpark.png)

GeoSpark is an in-memory cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark to support spatial data types and operations. In other words, the system extends the resilient distributed datasets (RDDs) concept to support spatial data. This problem is quite challenging due to the fact that (1) spatial data may be quite complex, e.g., rivers' and cities' geometrical boundaries, (2) spatial (and geometric) operations (e.g., Overlap, Intersect, Convex Hull, Cartographic Distances) cannot be easily and efficiently expressed using regular RDD transformations and actions. GeoSpark extends RDDs to form Spatial RDDs (SRDDs) and efficiently partitions SRDD data elements across machines and introduces novel parallelized spatial (geometric operations that follows the Open Geosptial Consortium (OGC) standard) transformations and actions (for SRDD) that provide a more intuitive interface for users to write spatial data analytics programs. Moreover, GeoSpark extends the SRDD layer to execute spatial queries (e.g., Range query, KNN query, and Join query) on large-scale spatial datasets. After geometrical objects are retrieved in the Spatial RDD layer, users can invoke spatial query processing operations provided in the Spatial Query Processing Layer of GeoSpark which runs over the in-memory cluster, decides how spatial object-relational tuples could be stored, indexed, and accessed using SRDDs, and returns the spatial query results required by user.


## How to get started (For Scala and Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7

### Steps

1. Create your own project
2. Add GeoSpark.jar into your build environment
3. Go ahead to use GeoSpark spatial RDDs to store spatial data and call needed functions!

### GeoSpark Programming Examples (In Java)
Please check out the "test case" folder for two programming examples with GeoSpark. One is spatial join and the other one is spatial aggregation. 


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

GeoSpark supports either Comma-Separated Values (CSV) or Tab-separated values (TSV) as the input format. Users only need to specify input format as Splitter and the start column of spatial info in one tuple as Offset when call Constructors.

### Point

Tuple(column, column,..., Longitude, Latitude, column, column,...)

### Rectangle

(column, column,...,Longitude 1, Longitude 2, Latitude 1, Latitude 2,column, column,...)

Two pairs of longitude and latitude present the vertexes lie on the diagonal of one rectangle.

### Polygon

(column, column,...,Longitude 1, Latitude 1, Longitude 2, Latitude 2, ...)

Each tuple contains unlimited points.

## Main functionality checklist

### PointRDD

* `Constructor: PointRDD(JavaRDD<point> pointRDD)`

* `Constructor: PointRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)`  

* `Constructor: PointRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)`
* `Envelope boundary()`
* `PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 
* `PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)`



* `SpatialPairRDD<Point,ArrayList<Point>> SpatialJoinQuery(CircleRDD circleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

* `SpatialPairRDD<Envelope,ArrayList<Point>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

* `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

* `SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

### RectangleRDD

  * `Constructor: RectangleRDD(JavaRDD<Envelope> rectangleRDD)`
  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)`
  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)`
 
  * `Envelope boundary()` 
  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation)`
 
  * `RectangleRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 

  * `RectangleRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  

 
  * `SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`


  * `SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`


  * `SpatialPairRDD<Polygon,ArrayList<Envelope>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
   

### PolygonRDD

  * `Constructor: PolygonRDD(JavaRDD<Envelope> rectangleRDD)`
  * `Constructor: PolygonRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)`
  * `Constructor: PolygonRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)`

  * `RectangleRDD MinimumBoundingRectangle()`
  * `Envelope boundary()`
  * `Union PolygonUnion()`
  * `PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
 

  * `PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQuery(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 

  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
 
  * `SpatialPairRDD<Polygon,ArrayList<Polygon>> SpatialJoinQueryWithMBR(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`



### CircleRDD

* `Constructor: CircleRDD(PointRDD pointRDD, Double Radius)`
* `Constructor: CircleRDD(Double x, Double y, Double Radius)`
* `RectangleRDD MinimumBoundingRectangle()`
* `PointRDD Centre()`
* `Envelope boundary()`

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

## Contributor

Jia Yu (jiayu2@asu.edu)

Mohamed Sarwat (msarwat@asu.edu)

## Acknowledgement

GeoSaprk makes used of JTS Topology Suite Version 1.13 for some geometrical computations.

JTS Topology Suite website: http://tsusiatsoftware.net/jts/main.html
