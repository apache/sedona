![GeoSpark Logo](http://www.public.asu.edu/~jiayu2/geospark/logo.png)

[![Build Status](https://travis-ci.org/jinxuan/GeoSpark.svg)](https://travis-ci.org/jiayuasu/GeoSpark) 

##  Version information

| Version     | Summary |
|-------------|---------------|
| 0.1       |  Support spatial range, join and Knn         |
| 0.2       | Improve code structure and refactor API         |
| 0.3       | Support load balanced spatial partitioning methods (also serve as the global index); Optimize code for iterative spatial data mining|
| master    | even with 0.3         |

GeoSpark is a cluster computing system for processing large-scale spatial data. GeoSpark extends Apache Spark with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs) that efficiently load, process, and analyze large-scale spatial data across machines. This problem is quite challenging due to the fact that (1) spatial data may be quite complex, e.g., rivers' and cities' geometrical boundaries, (2) spatial (and geometric) operations (e.g., Overlap, Intersect, Convex Hull, Cartographic Distances) cannot be easily and efficiently expressed using regular RDD transformations and actions. GeoSpark provides APIs for Apache Spark programmer to easily develop their spatial analysis programs with Spatial Resilient Distributed Datasets (SRDDs) which have in house support for geometrical and distance operations. Experiments show that GeoSpark is scalable and exhibits faster run-time performance than Hadoop-based systems in spatial analysis applications like spatial join, spatial aggregation, spatial autocorrelation analysis and spatial co-location pattern recognition.


## How to get started (For Scala and Java developers)

### Prerequisites

1. Apache Hadoop 2.4.0 and later
2. Apache Spark 1.2.1 and later
3. JDK 1.7

Note: GeoSpark has been tested on Apache Spark 1.2, 1.3, 1.4, 1.5 and Apache Hadoop 2.4, 2.6.

### How to use GeoSpark APIs in an interactive Spark shell (Scala)

1. Have your Spark cluster ready.
2. Run Spark shell with GeoSpark as a dependency.

  `
  ./bin/spark-shell --jars GeoSpark_Precompile_0.3_WithDependencies.jar
  `

3. You can now call GeoSpark APIs directly in your Spark shell!

### How to use GeoSpark APIs in a self-contained Spark application (Scala and Java)

1. Create your own Apache Spark project in Scala or Java
2. Download GeoSpark source code or pre-compiled binary jar.
3. Put GeoSpark source code with your own code and compile together. Or add GeoSpark.jar into your local compilation dependency (GeoSpark will be added to Maven central soon).
4. You can now use GeoSpark APIs in your Spark program!
5. Use spark-submit to submit your compiled self-contained Spark program.

### GeoSpark Programming Examples (Java)

Spatial queries Java example in "org.datasyslab.geospark.showcase" folder: Spatial range, join and KNN.


### GeoSpark Programming Examples (Scala)

####Spatial range query

`
val objectRDD = new RectangleRDD(sc, inputLocation, offset, "csv");
`

`
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawRectangleRDD().count();
`

####Spatial KNN query

`
val objectRDD = new RectangleRDD(sc, inputLocation, offset, "csv");
`

`
val result = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 1000);
`
####Spatial join query with index
`
val rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, "csv");
`

`
val objectRDD = new RectangleRDD(sc, inputLocation, offset ,"wkt","rtree",numPartitions);
`

`
objectRDD.buildIndex("rtree");
`

`
val joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD); 
`

`
val resultSize = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,rectangleRDD).count();
`

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

##Supported data format
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
## Contact

### Contributors
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Jinxuan Wu](http://www.public.asu.edu/~jinxuanw/) (Email: jinxuanw@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

###Project website
Please visit [GeoSpark project wesbite](http://geospark.datasyslab.org) for latest news and releases.

### DataSys Lab
GeoSpark is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).
