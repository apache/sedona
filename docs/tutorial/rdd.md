
The page outlines the steps to create Spatial RDDs and run spatial queries using Sedona-core.

## Set up dependencies

=== "Scala/Java"

	1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md) and add Sedona dependencies in build.sbt or pom.xml.
	2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql) in build.sbt or pom.xml.
	3. Please see [RDD example project](../demo/)

=== "Python"

	1. Please read [Quick start](../../setup/install-python) to install Sedona Python.
	2. This tutorial is based on [Sedona Core Jupyter Notebook example](../jupyter-notebook). You can interact with Sedona Python Jupyter notebook immediately on Binder. Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) to interact with Sedona Python Jupyter notebook immediately on Binder.

## Initiate SparkContext

=== "Scala"

	```scala
	val conf = new SparkConf()
	conf.setAppName("SedonaRunnableExample") // Change this to a proper name
	conf.setMaster("local[*]") // Delete this if run in cluster mode
	// Enable Sedona custom Kryo serializer
	conf.set("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
	conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName) // org.apache.sedona.core.serde.SedonaKryoRegistrator
	val sc = new SparkContext(conf)
	```
	
	If you add ==the Sedona full dependencies== as suggested above, please use the following two lines to enable Sedona Kryo serializer instead:
	```scala
	conf.set("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
	conf.set("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```

=== "Java"

	```java
	SparkConf conf = new SparkConf()
	conf.setAppName("SedonaRunnableExample") // Change this to a proper name
	conf.setMaster("local[*]") // Delete this if run in cluster mode
	// Enable Sedona custom Kryo serializer
	conf.set("spark.serializer", KryoSerializer.class.getName) // org.apache.spark.serializer.KryoSerializer
	conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName) // org.apache.sedona.core.serde.SedonaKryoRegistrator
	SparkContext sc = new SparkContext(conf)
	```
	
	If you use SedonaViz with SedonaRDD, please use the following two lines to enable Sedona Kryo serializer instead:
	```scala
	conf.set("spark.serializer", KryoSerializer.class.getName) // org.apache.spark.serializer.KryoSerializer
	conf.set("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```

=== "Python"

```python
conf.set("spark.serializer", KryoSerializer.getName)
conf.set("spark.kryo.registrator", SedonaKryoRegistrator.getName)
sc = SparkContext(conf=conf)
```

!!!warning
	Sedona has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

## Create a SpatialRDD

### Create a typed SpatialRDD
Sedona-core provides three special SpatialRDDs: PointRDD, PolygonRDD, and LineStringRDD.

!!!warning
	Typed SpatialRDD has been deprecated for a long time. We do NOT recommend it anymore.

### Create a generic SpatialRDD

A generic SpatialRDD is not typed to a certain geometry type and open to more scenarios. It allows an input data file contains mixed types of geometries. For instance, a WKT file contains three types gemetries ==LineString==, ==Polygon== and ==MultiPolygon==.

#### From WKT/WKB

Geometries in a WKT and WKB file always occupy a single column no matter how many coordinates they have. Sedona provides `WktReader ` and `WkbReader` to create generic SpatialRDD.

Suppose we have a `checkin.tsv` WKT TSV file at Path `/Download/checkin.tsv` as follows:
```
POINT (-88.331492 32.324142)	hotel
POINT (-88.175933 32.360763)	gas
POINT (-88.388954 32.357073)	bar
POINT (-88.221102 32.35078)	restaurant
```
This file has two columns and corresponding ==offsets==(Column IDs) are 0, 1. Column 0 is the WKT string and Column 1 is the checkin business type.

Use the following code to create a SpatialRDD

=== "Scala"

	```scala
	val inputLocation = "/Download/checkin.tsv"
	val wktColumn = 0 // The WKT string starts from Column 0
	val allowTopologyInvalidGeometries = true // Optional
	val skipSyntaxInvalidGeometries = false // Optional
	val spatialRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	```

=== "Java"

	```java
	String inputLocation = "/Download/checkin.tsv"
	int wktColumn = 0 // The WKT string starts from Column 0
	boolean allowTopologyInvalidGeometries = true // Optional
	boolean skipSyntaxInvalidGeometries = false // Optional
	SpatialRDD spatialRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	```

=== "Python"

	```python
	from sedona.core.formatMapper import WktReader
	from sedona.core.formatMapper import WkbReader
	
	WktReader.readToGeometryRDD(sc, wkt_geometries_location, 0, True, False)
	
	WkbReader.readToGeometryRDD(sc, wkb_geometries_location, 0, True, False)
	```


#### From GeoJSON

Geometries in GeoJSON is similar to WKT/WKB. However, a GeoJSON file must be beaked into multiple lines.

Suppose we have a `polygon.json` GeoJSON file at Path `/Download/polygon.json` as follows:

```
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "077", "TRACTCE": "011501", "BLKGRPCE": "5", "AFFGEOID": "1500000US010770115015", "GEOID": "010770115015", "NAME": "5", "LSAD": "BG", "ALAND": 6844991, "AWATER": 32636 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -87.621765, 34.873444 ], [ -87.617535, 34.873369 ], [ -87.6123, 34.873337 ], [ -87.604049, 34.873303 ], [ -87.604033, 34.872316 ], [ -87.60415, 34.867502 ], [ -87.604218, 34.865687 ], [ -87.604409, 34.858537 ], [ -87.604018, 34.851336 ], [ -87.603716, 34.844829 ], [ -87.603696, 34.844307 ], [ -87.603673, 34.841884 ], [ -87.60372, 34.841003 ], [ -87.603879, 34.838423 ], [ -87.603888, 34.837682 ], [ -87.603889, 34.83763 ], [ -87.613127, 34.833938 ], [ -87.616451, 34.832699 ], [ -87.621041, 34.831431 ], [ -87.621056, 34.831526 ], [ -87.62112, 34.831925 ], [ -87.621603, 34.8352 ], [ -87.62158, 34.836087 ], [ -87.621383, 34.84329 ], [ -87.621359, 34.844438 ], [ -87.62129, 34.846387 ], [ -87.62119, 34.85053 ], [ -87.62144, 34.865379 ], [ -87.621765, 34.873444 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "045", "TRACTCE": "021102", "BLKGRPCE": "4", "AFFGEOID": "1500000US010450211024", "GEOID": "010450211024", "NAME": "4", "LSAD": "BG", "ALAND": 11360854, "AWATER": 0 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -85.719017, 31.297901 ], [ -85.715626, 31.305203 ], [ -85.714271, 31.307096 ], [ -85.69999, 31.307552 ], [ -85.697419, 31.307951 ], [ -85.675603, 31.31218 ], [ -85.672733, 31.312876 ], [ -85.672275, 31.311977 ], [ -85.67145, 31.310988 ], [ -85.670622, 31.309524 ], [ -85.670729, 31.307622 ], [ -85.669876, 31.30666 ], [ -85.669796, 31.306224 ], [ -85.670356, 31.306178 ], [ -85.671664, 31.305583 ], [ -85.67177, 31.305299 ], [ -85.671878, 31.302764 ], [ -85.671344, 31.302123 ], [ -85.668276, 31.302076 ], [ -85.66566, 31.30093 ], [ -85.665687, 31.30022 ], [ -85.669183, 31.297677 ], [ -85.668703, 31.295638 ], [ -85.671985, 31.29314 ], [ -85.677177, 31.288211 ], [ -85.678452, 31.286376 ], [ -85.679236, 31.28285 ], [ -85.679195, 31.281426 ], [ -85.676865, 31.281049 ], [ -85.674661, 31.28008 ], [ -85.674377, 31.27935 ], [ -85.675714, 31.276882 ], [ -85.677938, 31.275168 ], [ -85.680348, 31.276814 ], [ -85.684032, 31.278848 ], [ -85.684387, 31.279082 ], [ -85.692398, 31.283499 ], [ -85.705032, 31.289718 ], [ -85.706755, 31.290476 ], [ -85.718102, 31.295204 ], [ -85.719132, 31.29689 ], [ -85.719017, 31.297901 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "055", "TRACTCE": "001300", "BLKGRPCE": "3", "AFFGEOID": "1500000US010550013003", "GEOID": "010550013003", "NAME": "3", "LSAD": "BG", "ALAND": 1378742, "AWATER": 247387 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -86.000685, 34.00537 ], [ -85.998837, 34.009768 ], [ -85.998012, 34.010398 ], [ -85.987865, 34.005426 ], [ -85.986656, 34.004552 ], [ -85.985, 34.002659 ], [ -85.98851, 34.001502 ], [ -85.987567, 33.999488 ], [ -85.988666, 33.99913 ], [ -85.992568, 33.999131 ], [ -85.993144, 33.999714 ], [ -85.994876, 33.995153 ], [ -85.998823, 33.989548 ], [ -85.999925, 33.994237 ], [ -86.000616, 34.000028 ], [ -86.000685, 34.00537 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "089", "TRACTCE": "001700", "BLKGRPCE": "2", "AFFGEOID": "1500000US010890017002", "GEOID": "010890017002", "NAME": "2", "LSAD": "BG", "ALAND": 1040641, "AWATER": 0 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -86.574172, 34.727375 ], [ -86.562684, 34.727131 ], [ -86.562797, 34.723865 ], [ -86.562957, 34.723168 ], [ -86.562336, 34.719766 ], [ -86.557381, 34.719143 ], [ -86.557352, 34.718322 ], [ -86.559921, 34.717363 ], [ -86.564827, 34.718513 ], [ -86.567582, 34.718565 ], [ -86.570572, 34.718577 ], [ -86.573618, 34.719377 ], [ -86.574172, 34.727375 ] ] ] } },

```

Use the following code to create a generic SpatialRDD:

=== "Scala"

	```scala
	val inputLocation = "/Download/polygon.json"
	val allowTopologyInvalidGeometries = true // Optional
	val skipSyntaxInvalidGeometries = false // Optional
	val spatialRDD = GeoJsonReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	```

=== "Java"

	```java
	String inputLocation = "/Download/polygon.json"
	boolean allowTopologyInvalidGeometries = true // Optional
	boolean skipSyntaxInvalidGeometries = false // Optional
	SpatialRDD spatialRDD = GeoJsonReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	```
	
=== "Python"

	```python
	from sedona.core.formatMapper import GeoJsonReader
	
	GeoJsonReader.readToGeometryRDD(sc, geo_json_file_location)
	```

!!!warning
	The way that Sedona reads JSON file is different from SparkSQL
	
#### From Shapefile

=== "Scala"

	```scala
	val shapefileInputLocation="/Download/myshapefile"
	val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
	```

=== "Java"

	```java
	String shapefileInputLocation="/Download/myshapefile"
	SpatialRDD spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
	```

=== "Python"

	```python
	from sedona.core.formatMapper.shapefileParser import ShapefileReader
	
	ShapefileReader.readToGeometryRDD(sc, shape_file_location)
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

If the file you are reading contains non-ASCII characters you'll need to explicitly set the encoding
via `sedona.global.charset` system property before creating your Spark context.

Example:

```scala
System.setProperty("sedona.global.charset", "utf8")

val sc = new SparkContext(...)
```

#### From SedonaSQL DataFrame

!!!note
	More details about SedonaSQL, please read the SedonaSQL tutorial.

To create a generic SpatialRDD from CSV, TSV, WKT, WKB and GeoJSON input formats, you can use SedonaSQL.

We use checkin.csv CSV file as the example. You can create a generic SpatialRDD using the following steps:

1. Load data in SedonaSQL.
```scala
var df = sparkSession.read.format("csv").option("header", "false").load(csvPointInputLocation)
df.createOrReplaceTempView("inputtable")
```
2. Create a Geometry type column in SedonaSQL
```scala
var spatialDf = sparkSession.sql(
	"""
   		|SELECT ST_Point(CAST(inputtable._c0 AS Decimal(24,20)),CAST(inputtable._c1 AS Decimal(24,20))) AS checkin
   		|FROM inputtable
   	""".stripMargin)
```
3. Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
```scala
var spatialRDD = Adapter.toSpatialRdd(spatialDf, "checkin")
```

"checkin" is the name of the geometry column

For WKT/WKB/GeoJSON data, please use ==ST_GeomFromWKT / ST_GeomFromWKB / ST_GeomFromGeoJSON== instead.
	
## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in an SpatialRDD. The unit of all related distances in Sedona is same as the unit of all geometries in an SpatialRDD.

To convert Coordinate Reference System of an SpatialRDD, use the following code:

=== "Scala"

	```scala
	val sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	val targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Java"

	```java
	String sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	String targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Python"

	```python
	sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, False)
	```

`false` in CRSTransform(sourceCrsCode, targetCrsCode, false) means that it will not tolerate Datum shift. If you want it to be lenient, use `true` instead.

!!!warning
	CRS transformation should be done right after creating each SpatialRDD, otherwise it will lead to wrong query results. For instance, use something like this:


=== "Scala"

	```scala
	val objectRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", false)
	```

=== "Java"

	```java
	SpatialRDD objectRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", false)
	```

=== "Python"

	```python
	objectRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", False)
	```

The details CRS information can be found on [EPSG.io](https://epsg.io/)

## Read other attributes in an SpatialRDD

Each SpatialRDD can carry non-spatial attributes such as price, age and name.

The other attributes are combined together to a string and stored in ==UserData== field of each geometry.

To retrieve the UserData field, use the following code:

=== "Scala"

	```scala
	val rddWithOtherAttributes = objectRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
	```

=== "Java"

	```java
	SpatialRDD<Geometry> spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark");
	spatialRDD.rawSpatialRDD.map(obj -> {return obj.getUserData();});
	```

=== "Python"

	```python
	rdd_with_other_attributes = object_rdd.rawSpatialRDD.map(lambda x: x.getUserData())
	```

## Write a Spatial Range Query

A spatial range query takes as input a range query window and an SpatialRDD and returns all geometries that have specified relationship with the query window.

Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial Range Query on it.

==spatialPredicate== can be set to `SpatialPredicate.INTERSECTS` to return all geometries intersect with query window. Supported spatial predicates are:

* `CONTAINS`: geometry is completely inside the query window
* `INTERSECTS`: geometry have at least one point in common with the query window
* `WITHIN`: geometry is completely within the query window (no touching edges)
* `COVERS`: query window has no point outside of the geometry
* `COVERED_BY`: geometry has no point outside of the query window
* `OVERLAPS`: geometry and the query window spatially overlap
* `CROSSES`: geometry and the query window spatially cross
* `TOUCHES`: the only points shared between geometry and the query window are on the boundary of geometry and the query window
* `EQUALS`: geometry and the query window are spatially equal

!!!note
	Spatial range query is equivalent with a SELECT query with spatial predicate as search condition in Spatial SQL. An example query is as follows:
	```sql
	SELECT *
	FROM checkin
	WHERE ST_Intersects(checkin.location, queryWindow)
	```

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	val usingIndex = false
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	boolean usingIndex = false
	JavaRDD queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Python"

	```python
	from sedona.core.geom.envelope import Envelope
	from sedona.core.spatialOperator import RangeQuery
	
	range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
	consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
	using_index = False
	query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
	```

!!!note
    Sedona Python users: Please use RangeQueryRaw from the same module if you want to avoid jvm python serde while converting to Spatial DataFrame. It takes the same parameters as RangeQuery but returns reference to jvm rdd which can be converted to dataframe without python - jvm serde using Adapter.
    
    Example:
    ```python
    from sedona.core.geom.envelope import Envelope
    from sedona.core.spatialOperator import RangeQueryRaw
    from sedona.utils.adapter import Adapter
    
    range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
    consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
    using_index = False
    query_result = RangeQueryRaw.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
    gdf = Adapter.toDf(query_result, spark, ["col1", ..., "coln"])
    ```


### Range query window

Besides the rectangle (Envelope) type range query window, Sedona range query window can be Point/Polygon/LineString.

The code to create a point, linestring (4 vertexes) and polygon (4 vertexes) is as follows:

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	
	val geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](5)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
	val polygonObject = geometryFactory.createPolygon(coordinates)
	
	val geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](4)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	val linestringObject = geometryFactory.createLineString(coordinates)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	
	GeometryFactory geometryFactory = new GeometryFactory()
	Coordinate[] coordinates = new Array[Coordinate](5)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
	Polygon polygonObject = geometryFactory.createPolygon(coordinates)
	
	GeometryFactory geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](4)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	LineString linestringObject = geometryFactory.createLineString(coordinates)
	```

=== "Python"

	A Shapely geometry can be used as a query window. To create shapely geometries, please follow [Shapely official docs](https://shapely.readthedocs.io/en/stable/manual.html)

### Use spatial indexes

Sedona provides two types of spatial indexes, Quad-Tree and R-Tree. Once you specify an index type, Sedona will build a local tree index on each of the SpatialRDD partition.

To utilize a spatial index in a spatial range query, use the following code:

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	
	val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
	
	val usingIndex = true
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	
	boolean buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
	
	boolean usingIndex = true
	JavaRDD queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Python"

	```python
	from sedona.core.geom.envelope import Envelope
	from sedona.core.enums import IndexType
	from sedona.core.spatialOperator import RangeQuery
	
	range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
	consider_boundary_intersection = False ## Only return gemeotries fully covered by the window
	
	build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
	spatial_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)
	
	using_index = True
	
	query_result = RangeQuery.SpatialRangeQuery(
	    spatial_rdd,
	    range_query_window,
	    consider_boundary_intersection,
	    using_index
	)
	```

!!!tip
	Using an index might not be the best choice all the time because building index also takes time. A spatial index is very useful when your data is complex polygons and line strings.

### Output format

=== "Scala/Java"

	The output format of the spatial range query is another SpatialRDD.

=== "Python"

	The output format of the spatial range query is another RDD which consists of GeoData objects.
	
	SpatialRangeQuery result can be used as RDD with map or other spark RDD functions. Also it can be used as 
	Python objects when using collect method.
	Example:
	
	```python
	query_result.map(lambda x: x.geom.length).collect()
	```
	
	```
	[
	 1.5900840000000045,
	 1.5906639999999896,
	 1.1110299999999995,
	 1.1096700000000084,
	 1.1415619999999933,
	 1.1386399999999952,
	 1.1415619999999933,
	 1.1418860000000137,
	 1.1392780000000045,
	 ...
	]
	```
	
	Or transformed to GeoPandas GeoDataFrame
	
	```python
	import geopandas as gpd
	gpd.GeoDataFrame(
	    query_result.map(lambda x: [x.geom, x.userData]).collect(),
	    columns=["geom", "user_data"],
	    geometry="geom"
	)
	```

## Write a Spatial KNN Query

A spatial K Nearnest Neighbor query takes as input a K, a query point and an SpatialRDD and finds the K geometries in the RDD which are the closest to he query point.

Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial KNN Query on it.

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors
	val usingIndex = false
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	int K = 1000 // K Nearest Neighbors
	boolean usingIndex = false
	JavaRDD result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Python"

	```python
	from sedona.core.spatialOperator import KNNQuery
	from shapely.geometry import Point
	
	point = Point(-84.01, 34.01)
	k = 1000 ## K Nearest Neighbors
	using_index = False
	result = KNNQuery.SpatialKnnQuery(object_rdd, point, k, using_index)
	```

!!!note
	Spatial KNN query that returns 5 Nearest Neighbors is equal to the following statement in Spatial SQL
	```sql
	SELECT ck.name, ck.rating, ST_Distance(ck.location, myLocation) AS distance
	FROM checkins ck
	ORDER BY distance DESC
	LIMIT 5
	```

### Query center geometry

Besides the Point type, Sedona KNN query center can be Polygon and LineString.

=== "Scala/Java"

	To learn how to create Polygon and LineString object, see [Range query window](#range-query-window).

=== "Python"

	To create Polygon or Linestring object please follow [Shapely official docs](https://shapely.readthedocs.io/en/stable/manual.html)




### Use spatial indexes

To utilize a spatial index in a spatial KNN query, use the following code:

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors
	
	
	val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	objectRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
	
	val usingIndex = true
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors
	
	
	boolean buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	objectRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)
	
	boolean usingIndex = true
	JavaRDD result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Python"

	```python
	from sedona.core.spatialOperator import KNNQuery
	from sedona.core.enums import IndexType
	from shapely.geometry import Point
	
	point = Point(-84.01, 34.01)
	k = 5 ## K Nearest Neighbors
	
	build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
	spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)
	
	using_index = True
	result = KNNQuery.SpatialKnnQuery(spatial_rdd, point, k, using_index)
	```


!!!warning
	Only R-Tree index supports Spatial KNN query

### Output format

=== "Scala/Java"

	The output format of the spatial KNN query is a list of geometries. The list has K geometry objects.

=== "Python"

	The output format of the spatial KNN query is a list of GeoData objects. 
	The list has K GeoData objects.
	
	Example:
	```python
	>> result
	
	[GeoData, GeoData, GeoData, GeoData, GeoData]
	```

## Write a Spatial Join Query


A spatial join query takes as input two Spatial RDD A and B. For each geometry in A, finds the geometries (from B) covered/intersected by it. A and B can be any geometry type and are not necessary to have the same geometry type.

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue an Spatial Join Query on them.

=== "Scala"

	```scala
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	val usingIndex = false
	
	objectRDD.analyze()
	
	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	
	val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	val usingIndex = false
	
	objectRDD.analyze()
	
	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	
	JavaPairRDD result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.core.enums import GridType
	from sedona.core.spatialOperator import JoinQuery
	
	consider_boundary_intersection = False ## Only return geometries fully covered by each query window in queryWindowRDD
	using_index = False
	
	object_rdd.analyze()
	
	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())
	
	result = JoinQuery.SpatialJoinQuery(object_rdd, query_window_rdd, using_index, consider_boundary_intersection)
	```

!!!note
	Spatial join query is equal to the following query in Spatial SQL:
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Contains(city.geom, superhero.geom);
	```
	Find the superheroes in each city

### Use spatial partitioning

Sedona spatial partitioning method can significantly speed up the join query. Three spatial partitioning methods are available: KDB-Tree, Quad-Tree and R-Tree. Two SpatialRDD must be partitioned by the same way.

If you first partition SpatialRDD A, then you must use the partitioner of A to partition B.

=== "Scala/Java"

	```scala
	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	```

=== "Python"

	```python
	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())
	```

Or 

=== "Scala/Java"

	```scala
	queryWindowRDD.spatialPartitioning(GridType.KDBTREE)
	objectRDD.spatialPartitioning(queryWindowRDD.getPartitioner)
	```

=== "Python"

	```python
	query_window_rdd.spatialPartitioning(GridType.KDBTREE)
	object_rdd.spatialPartitioning(query_window_rdd.getPartitioner())
	```


### Use spatial indexes

To utilize a spatial index in a spatial join query, use the following code:

=== "Scala"

	```scala
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	
	val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
	val usingIndex = true
	queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
	
	val result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	
	boolean buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
	boolean usingIndex = true
	queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
	
	JavaPairRDD result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.core.enums import GridType
	from sedona.core.enums import IndexType
	from sedona.core.spatialOperator import JoinQuery
	
	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())
	
	build_on_spatial_partitioned_rdd = True ## Set to TRUE only if run join query
	using_index = True
	query_window_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)
	
	result = JoinQuery.SpatialJoinQueryFlat(object_rdd, query_window_rdd, using_index, True)
	```

The index should be built on either one of two SpatialRDDs. In general, you should build it on the larger SpatialRDD.

### Output format

=== "Scala/Java"

	The output format of the spatial join query is a PairRDD. In this PairRDD, each object is a pair of two geometries. The left one is the geometry from objectRDD and the right one is the geometry from the queryWindowRDD.
	
	```
	Point,Polygon
	Point,Polygon
	Point,Polygon
	Polygon,Polygon
	LineString,LineString
	Polygon,LineString
	...
	```
	
	Each object on the left is covered/intersected by the object on the right.

=== "Python"

	Result for this query is RDD which holds two GeoData objects within list of lists.
	Example:
	```python
	result.collect()
	```
	
	```
	[[GeoData, GeoData], [GeoData, GeoData] ...]
	```
	
	It is possible to do some RDD operation on result data ex. Getting polygon centroid.
	```python
	result.map(lambda x: x[0].geom.centroid).collect()
	```

!!!note
    Sedona Python users: Please use JoinQueryRaw from the same module for methods 
    
    - spatialJoin
    
    - DistanceJoinQueryFlat

    - SpatialJoinQueryFlat

    For better performance while converting to dataframe with adapter. 
    That approach allows to avoid costly serialization between Python 
    and jvm and in result operating on python object instead of native geometries.
    
    Example:
    ```python
    from sedona.core.SpatialRDD import CircleRDD
    from sedona.core.enums import GridType
    from sedona.core.spatialOperator import JoinQueryRaw
    
    object_rdd.analyze()
    
    circle_rdd = CircleRDD(object_rdd, 0.1) ## Create a CircleRDD using the given distance
    circle_rdd.analyze()
    
    circle_rdd.spatialPartitioning(GridType.KDBTREE)
    spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())
    
    consider_boundary_intersection = False ## Only return gemeotries fully covered by each query window in queryWindowRDD
    using_index = False
    
    result = JoinQueryRaw.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
    
    gdf = Adapter.toDf(result, ["left_col1", ..., "lefcoln"], ["rightcol1", ..., "rightcol2"], spark)
    ```


## Write a Distance Join Query

!!!warning
	RDD distance joins are only reliable for points. For other geometry types, please use Spatial SQL.

A distance join query takes as input two Spatial RDD A and B and a distance. For each geometry in A, finds the geometries (from B) are within the given distance to it. A and B can be any geometry type and are not necessary to have the same geometry type. The unit of the distance is explained [here](#transform-the-coordinate-reference-system).

If you don't want to transform your data and are ok with sacrificing the query accuracy, you can use an approximate degree value for distance. Please use [this calculator](https://lucidar.me/en/online-unit-converter-length-to-angle/convert-degrees-to-meters/#online-converter).

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue an Distance Join Query on them.

=== "Scala"

	```scala
	objectRddA.analyze()
	
	val circleRDD = new CircleRDD(objectRddA, 0.1) // Create a CircleRDD using the given distance
	
	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)
	
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	val usingIndex = false
	
	val result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRddA.analyze()
	
	CircleRDD circleRDD = new CircleRDD(objectRddA, 0.1) // Create a CircleRDD using the given distance
	
	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)
	
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	boolean usingIndex = false
	
	JavaPairRDD result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.core.SpatialRDD import CircleRDD
	from sedona.core.enums import GridType
	from sedona.core.spatialOperator import JoinQuery
	
	object_rdd.analyze()
	
	circle_rdd = CircleRDD(object_rdd, 0.1) ## Create a CircleRDD using the given distance
	circle_rdd.analyze()
	
	circle_rdd.spatialPartitioning(GridType.KDBTREE)
	spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())
	
	consider_boundary_intersection = False ## Only return gemeotries fully covered by each query window in queryWindowRDD
	using_index = False
	
	result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
	```

Distance join can only accept `COVERED_BY` and `INTERSECTS` as spatial predicates. The rest part of the join query is same as the spatial join query.

The details of spatial partitioning in join query is [here](#use-spatial-partitioning).

The details of using spatial indexes in join query is [here](#use-spatial-indexes_2).

The output format of the distance join query is [here](#output-format_2).

!!!note
	Distance join query is equal to the following query in Spatial SQL:
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Distance(city.geom, superhero.geom) <= 10;
	```
	Find the superheroes within 10 miles of each city
	
## Save to permanent storage

You can always save an SpatialRDD back to some permanent storage such as HDFS and Amazon S3. You can save distributed SpatialRDD to WKT, GeoJSON and object files.

!!!note
	Non-spatial attributes such as price, age and name will also be stored to permanent storage.

### Save an SpatialRDD (not indexed)

Typed SpatialRDD and generic SpatialRDD can be saved to permanent storage.

#### Save to distributed WKT text file

Use the following code to save an SpatialRDD as a distributed WKT text file:

```scala
objectRDD.rawSpatialRDD.saveAsTextFile("hdfs://PATH")
objectRDD.saveAsWKT("hdfs://PATH")
```

#### Save to distributed WKB text file

Use the following code to save an SpatialRDD as a distributed WKB text file:

```scala
objectRDD.saveAsWKB("hdfs://PATH")
```

#### Save to distributed GeoJSON text file

Use the following code to save an SpatialRDD as a distributed GeoJSON text file:

```scala
objectRDD.saveAsGeoJSON("hdfs://PATH")
```


#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

=== "Scala/Java"

	```scala
	objectRDD.rawSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

=== "Python"

	```python
	object_rdd.rawJvmSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

!!!note
	Each object in a distributed object file is a byte array (not human-readable). This byte array is the serialized format of a Geometry or a SpatialIndex.

### Save an SpatialRDD (indexed)

Indexed typed SpatialRDD and generic SpatialRDD can be saved to permanent storage. However, the indexed SpatialRDD has to be stored as a distributed object file.

#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```
objectRDD.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### Save an SpatialRDD (spatialPartitioned W/O indexed)

A spatial partitioned RDD can be saved to permanent storage but Spark is not able to maintain the same RDD partition Id of the original RDD. This will lead to wrong join query results. We are working on some solutions. Stay tuned!

### Reload a saved SpatialRDD

You can easily reload an SpatialRDD that has been saved to ==a distributed object file==.

#### Load to a typed SpatialRDD

!!!warning
	Typed SpatialRDD has been deprecated for a long time. We do NOT recommend it anymore.

#### Load to a generic SpatialRDD

Use the following code to reload the SpatialRDD:

=== "Scala"

	```scala
	var savedRDD = new SpatialRDD[Geometry]
	savedRDD.rawSpatialRDD = sc.objectFile[Geometry]("hdfs://PATH")
	```

=== "Java"

	```java
	SpatialRDD savedRDD = new SpatialRDD<Geometry>
	savedRDD.rawSpatialRDD = sc.objectFile<Geometry>("hdfs://PATH")
	```

=== "Python"

	```python
	saved_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.GEOMETRY)
	```

Use the following code to reload the indexed SpatialRDD:

=== "Scala"

	```scala
	var savedRDD = new SpatialRDD[Geometry]
	savedRDD.indexedRawRDD = sc.objectFile[SpatialIndex]("hdfs://PATH")
	```

=== "Java"

	```java
	SpatialRDD savedRDD = new SpatialRDD<Geometry>
	savedRDD.indexedRawRDD = sc.objectFile<SpatialIndex>("hdfs://PATH")
	```

=== "Python"

	```python
	saved_rdd = SpatialRDD()
	saved_rdd.indexedRawRDD = load_spatial_index_rdd_from_disc(sc, "hdfs://PATH")
	```