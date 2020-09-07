The page outlines the steps to manage spatial data using GeoSparkSQL. ==The example code is written in Scala but also works for Java==.

GeoSparkSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:
```Scala
var myDataFrame = sparkSession.sql("YOUR_SQL")
```

Detailed GeoSparkSQL APIs are available here: [GeoSparkSQL API](../api/sql/GeoSparkSQL-Overview.md)

## Set up dependencies

1. Read [GeoSpark Maven Central coordinates](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md)
2. Select ==the minimum dependencies==: Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), [GeoSpark core](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-core), [GeoSparkSQL](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-sql)
3. Add the dependencies in build.sbt or pom.xml.

!!!note
	To enjoy the full functions of GeoSpark, we suggest you include ==the full dependencies==: [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), [GeoSpark core](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-core), [GeoSparkSQL](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-sql), [GeoSparkViz](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md#geospark-viz)


## Initiate SparkSession
Use the following code to initiate your SparkSession at the beginning:
```Scala
var sparkSession = SparkSession.builder()
.master("local[*]") // Delete this if run in cluster mode
.appName("readTestScala") // Change this to a proper name
// Enable GeoSpark custom Kryo serializer
.config("spark.serializer", classOf[KryoSerializer].getName)
.config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
.getOrCreate()
```

!!!warning
	GeoSpark has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

If you add ==the GeoSpark full dependencies== as suggested above, please use the following two lines to enable GeoSpark Kryo serializer instead:
```Scala
.config("spark.serializer", classOf[KryoSerializer].getName)
.config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
```

## Register GeoSparkSQL

Add the following line after your SparkSession declaration

```Scala
GeoSparkSQLRegistrator.registerAll(sparkSession)
```

This function will register GeoSpark User Defined Type, User Defined Function and optimized join query strategy.

## Load data from files

Assume we have a WKT file, namely `usa-county.tsv`, at Path `/Download/usa-county.tsv` as follows:

```
POLYGON (..., ...)	Cuming County	
POLYGON (..., ...)	Wahkiakum County
POLYGON (..., ...)	De Baca County
POLYGON (..., ...)	Lancaster County
```
The file may have many other columns.

Use the following code to load the data and create a raw DataFrame:

```Scala
var rawDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
rawDf.createOrReplaceTempView("rawdf")
rawDf.show()
```

The output will be like this:

```
|                 _c0|_c1|_c2|     _c3|  _c4|        _c5|                 _c6|_c7|_c8|  _c9|_c10| _c11|_c12|_c13|      _c14|    _c15|       _c16|        _c17|
+--------------------+---+---+--------+-----+-----------+--------------------+---+---+-----+----+-----+----+----+----------+--------+-----------+------------+
|POLYGON ((-97.019...| 31|039|00835841|31039|     Cuming|       Cuming County| 06| H1|G4020|null| null|null|   A|1477895811|10447360|+41.9158651|-096.7885168|
|POLYGON ((-123.43...| 53|069|01513275|53069|  Wahkiakum|    Wahkiakum County| 06| H1|G4020|null| null|null|   A| 682138871|61658258|+46.2946377|-123.4244583|
|POLYGON ((-104.56...| 35|011|00933054|35011|    De Baca|      De Baca County| 06| H1|G4020|null| null|null|   A|6015539696|29159492|+34.3592729|-104.3686961|
|POLYGON ((-96.910...| 31|109|00835876|31109|  Lancaster|    Lancaster County| 06| H1|G4020| 339|30700|null|   A|2169240202|22877180|+40.7835474|-096.6886584|
```

## Create a Geometry type column

All geometrical operations in GeoSparkSQL are on Geometry type objects. Therefore, before any kind of queries, you need to create a Geometry type column on a DataFrame.


```Scala
var spatialDf = sparkSession.sql(
  """
    |SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2
    |FROM rawdf
  """.stripMargin)
spatialDf.createOrReplaceTempView("spatialdf")
spatialDf.show()
```

You can select many other attributes to compose this `spatialdDf`. The output will be something like this:

```
|                 countyshape|_c1|_c2|     _c3|  _c4|        _c5|                 _c6|_c7|_c8|  _c9|_c10| _c11|_c12|_c13|      _c14|    _c15|       _c16|        _c17|
+--------------------+---+---+--------+-----+-----------+--------------------+---+---+-----+----+-----+----+----+----------+--------+-----------+------------+
|POLYGON ((-97.019...| 31|039|00835841|31039|     Cuming|       Cuming County| 06| H1|G4020|null| null|null|   A|1477895811|10447360|+41.9158651|-096.7885168|
|POLYGON ((-123.43...| 53|069|01513275|53069|  Wahkiakum|    Wahkiakum County| 06| H1|G4020|null| null|null|   A| 682138871|61658258|+46.2946377|-123.4244583|
|POLYGON ((-104.56...| 35|011|00933054|35011|    De Baca|      De Baca County| 06| H1|G4020|null| null|null|   A|6015539696|29159492|+34.3592729|-104.3686961|
|POLYGON ((-96.910...| 31|109|00835876|31109|  Lancaster|    Lancaster County| 06| H1|G4020| 339|30700|null|   A|2169240202|22877180|+40.7835474|-096.6886584|
```

Although it looks same with the input, but actually the type of column countyshape has been changed to ==Geometry== type.

To verify this, use the following code to print the schema of the DataFrame:

```Scala
spatialDf.printSchema()
```

The output will be like this:

```
root
 |-- countyshape: geometry (nullable = false)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)
```

!!!note
	GeoSparkSQL provides more than 10 different functions to create a Geometry column, please read [GeoSparkSQL constructor API](../api/sql/GeoSparkSQL-Constructor.md).
	
## Load Shapefile and GeoJSON

Shapefile and GeoJSON must be loaded by SpatialRDD and converted to DataFrame using Adapter. Please read [Load SpatialRDD](rdd/#create-a-generic-spatialrdd-behavoir-changed-in-v120) and [DataFrame <-> RDD](sql/#convert-between-dataframe-and-spatialrdd).


## Transform the Coordinate Reference System

GeoSpark doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in GeoSparkSQL is same as the unit of all geometries in a Geometry column.

To convert Coordinate Reference System of the Geometry column created before, use the following code:

```Scala
spatialDf = sparkSession.sql(
  """
    |SELECT ST_Transform(countyshape, "epsg:4326", "epsg:3857") AS newcountyshape, _c1, _c2, _c3, _c4, _c5, _c6, _c7
    |FROM spatialdf
  """.stripMargin)
spatialDf.createOrReplaceTempView("spatialdf")
spatialDf.show()
```

The first EPSG code EPSG:4326 in `ST_Transform` is the source CRS of the geometries. It is WGS84, the most common degree-based CRS.

The second EPSG code EPSG:3857 in `ST_Transform` is the target CRS of the geometries. It is the most common meter-based CRS.

This `ST_Transform` transform the CRS of these geomtries from EPSG:4326 to EPSG:3857. The details CRS information can be found on [EPSG.io](https://epsg.io/.)

The coordinates of polygons have been changed. The output will be like this:

```
+--------------------+---+---+--------+-----+-----------+--------------------+---+
|      newcountyshape|_c1|_c2|     _c3|  _c4|        _c5|                 _c6|_c7|
+--------------------+---+---+--------+-----+-----------+--------------------+---+
|POLYGON ((-108001...| 31|039|00835841|31039|     Cuming|       Cuming County| 06|
|POLYGON ((-137408...| 53|069|01513275|53069|  Wahkiakum|    Wahkiakum County| 06|
|POLYGON ((-116403...| 35|011|00933054|35011|    De Baca|      De Baca County| 06|
|POLYGON ((-107880...| 31|109|00835876|31109|  Lancaster|    Lancaster County| 06|

```


## Run spatial queries

After creating a Geometry type column, you are able to run spatial queries.

### Range query

Use ==ST_Contains==, ==ST_Intersects==, ==ST_Within== to run a range query over a single column.

The following example finds all counties that are within the given polygon:

```Scala
spatialDf = sparkSession.sql(
  """
    |SELECT *
    |FROM spatialdf
    |WHERE ST_Contains (ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape)
  """.stripMargin)
spatialDf.createOrReplaceTempView("spatialdf")
spatialDf.show()
```

!!!note
	Read [GeoSparkSQL constructor API](../api/sql/GeoSparkSQL-Constructor.md) to learn how to create a Geometry type query window
### KNN query

Use ==ST_Distance== to calculate the distance and rank the distance.

The following code returns the 5 nearest neighbor of the given polygon.

```Scala
spatialDf = sparkSession.sql(
  """
    |SELECT countyname, ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape) AS distance
    |FROM spatialdf
    |ORDER BY distance DESC
    |LIMIT 5
  """.stripMargin)
spatialDf.createOrReplaceTempView("spatialdf")
spatialDf.show()
```

### Join query

The details of a join query is available here [Join query](../api/sql/GeoSparkSQL-Optimizer.md).

### Other queries

There are lots of other functions can be combined with these queries. Please read [GeoSparkSQL functions](../api/sql/GeoSparkSQL-Function.md) and [GeoSparkSQL aggregate functions](../api/sql/GeoSparkSQL-AggregateFunction.md).

## Save to permanent storage

To save a Spatial DataFrame to some permanent storage such as Hive tables and HDFS, you can simply convert each geometry in the Geometry type column back to a plain String and save the plain DataFrame to wherever you want.


Use the following code to convert the Geometry column in a DataFrame back to a WKT string column:
```Scala
sparkSession.udf.register("ST_SaveAsWKT", (geometry: Geometry) => (geometry.toText))
var stringDf = sparkSession.sql(
  """
    |SELECT ST_SaveAsWKT(countyshape)
    |FROM polygondf
  """.stripMargin)
```

!!!note
	We are working on providing more user-friendly output functions such as ==ST_SaveAsWKT== and ==ST_SaveAsWKB==. Stay tuned!

To load the DataFrame back, you first use the regular method to load the saved string DataFrame from the permanent storage and use ==ST_GeomFromWKT== to re-build the Geometry type column.


## Convert between DataFrame and SpatialRDD

### DataFrame to SpatialRDD

Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD

**GeoSpark 1.2.0+**
```Scala
var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
```

"usacounty" is the name of the geometry column

**Before GeoSpark 1.2.0**
```Scala
var spatialRDD = new SpatialRDD[Geometry]
spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
```

Geometry must be the first column in the DataFrame

!!!warning
	Only one Geometry type column is allowed per DataFrame.

!!!note
	Before GeoSpark 1.2.0, other non-spatial columns need be brought to SpatialRDD using the UUIDs. Please read [GeoSparkSQL constructor API](../api/sql/GeoSparkSQL-Constructor.md). In GeoSpark 1.2.0+, all other non-spatial columns are automatically kept in SpatialRDD.
	
### SpatialRDD to DataFrame

Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
```Scala
var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](./rdd#read-other-attributes-in-an-spatialrdd)).

### SpatialPairRDD to DataFrame

PairRDD is the result of a spatial join query or distance join query. GeoSparkSQL DataFrame-RDD Adapter can convert the result to a DataFrame:

```Scala
var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](./rdd#read-other-attributes-in-an-spatialrdd)).
