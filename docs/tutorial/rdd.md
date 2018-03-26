
The page outlines the steps to create Spatial RDDs and run spatial queries using GeoSpark-core. ==The example code is written in Scala but also works for Java==.

## Set up dependencies

1. Read [GeoSpark Maven Central coordinates](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md)
2. Select ==the minimum dependencies==: Add Apache Spark (only the Spark core) and GeoSpark (core).
3. Add the dependencies in build.sbt or pom.xml.

!!!note
	To enjoy the full functions of GeoSpark, we suggest you include ==the full dependencies==: [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), [GeoSpark core](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-core), [GeoSparkSQL](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-sql), [GeoSparkViz](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-viz)

## Initiate SparkContext

```Scala
val conf = new SparkConf()
	conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
	conf.setMaster("local[*]") // Delete this if run in cluster mode
	// Enable GeoSpark custom Kryo serializer
	conf.set("spark.serializer", classOf[KryoSerializer].getName)
	conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
val sc = new SparkContext(conf)
```

!!!warning
	GeoSpark has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

If you add ==the GeoSpark full dependencies== as suggested above, please use the following two lines to enable GeoSpark Kryo serializer instead:
```Scala
	conf.set("spark.serializer", classOf[KryoSerializer].getName)
	conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
```

## Create a SpatialRDD

### Create a typed SpatialRDD
GeoSpark-core provdies three special SpatialRDDs: ==PointRDD, PolygonRDD, and LineStringRDD==. They can be loaded from CSV, TSV, WKT, WKB, Shapefiles, GeoJSON and NetCDF/HDF format.

#### PointRDD from CSV/TSV
Suppose we have a `checkin.csv` CSV file at Path `/Download/checkin.csv` as follows:
```
-88.331492,32.324142,hotel
-88.175933,32.360763,gas
-88.388954,32.357073,bar
-88.221102,32.35078,restaurant
```
This file has three columns and corresponding ==offsets==(Column IDs) are 0, 1, 2.
Use the following code to create a PointRDD

```Scala
val pointRDDInputLocation = "/Download/checkin.csv"
val pointRDDOffset = 0 // The point long/lat starts from Column 0
val pointRDDSplitter = FileDataSplitter.CSV
val carryOtherAttributes = true // Carry Column 2 (hotel, gas, bar...)
var objectRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
```

If the data file is in TSV format, just simply use the following line to replace the old FileDataSplitter:
```Scala
val pointRDDSplitter = FileDataSplitter.TSV
```

#### PolygonRDD/LineStringRDD from CSV/TSV

#### Typed SpatialRDD from WKT/WKB/GeoJSON



### Create a generic SpatialRDD

A generic SpatialRDD is not typed to a certain geometry type and not limited to a certain scenarios. It allows an input data file contains mixed types of geometries. For instace, a WKT file contains three types gemetries ==LineString==, ==Polygon== and ==MultiPolygon==.

#### From GeoSparkSQL

Make sure you include ==the full dependencies== of GeoSpark. Read [GeoSparkSQL API](../api/GeoSparkSQL-Overview.md). GeoSparkSQL allows CSV, TSV, WKT, WKB and GeoJSON input format.

We use [checkin.csv CSV file](#pointrdd-from-csvtsv) as the example. You can create a generic SpatialRDD using the following steps:

1. Load data in GeoSparkSQL.
```Scala
var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(csvPointInputLocation)
df.createOrReplaceTempView("inputtable")
```
2. Create a Geometry type column in GeoSparkSQL
```Scala
var spatialDf = sparkSession.sql(
	"""
   		|SELECT ST_Point(CAST(inputtable._c0 AS Decimal(24,20)),CAST(inputtable._c1 AS Decimal(24,20))) AS checkin
   		|FROM inputtable
   	""".stripMargin)
```
3. Use GeoSpark SQL-RDD Adapter to convert a DataFrame to an SpatialRDD
```Scala
var spatialRDD = new SpatialRDD[Geometry]
spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
```
#### From Shapefile

```Scala
val shapefileInputLocation="/Download/myshapefile"
var spatialRDD = new SpatialRDD[Geometry]
spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
```

!!!note
	The file extensions of .shp, .shx, .dbf must be in lowercase. Assume you have a shape file called ==myShapefile==, the file structure should be like this:
	```
	- shapefile1
	- shapefile2
	- myshapefile
		- myshapefile.shp
		- myshapefile.shx
		- myshapefile.dbf
		- myshapefile...
		- ...
	```
## Write a Spatial Range Query

## Write a Spatial KNN Query

## Write a Spatial Join Query

## Write a Distance Join Query

## Transform the Coordinate Reference System

## Read other attributes in a SpatialRDD

## Run geometrical operations