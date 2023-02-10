The page outlines the steps to manage spatial data using SedonaSQL. ==The example code is written in Scala but also works for Java==.

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:
```Scala
var myDataFrame = sparkSession.sql("YOUR_SQL")
```

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md)

## Set up dependencies

1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md)
2. Select ==the minimum dependencies==: Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), Sedona-core and Sedona-SQL
3. Add the dependencies in build.sbt or pom.xml.

!!!note
	To enjoy the full functions of Sedona, we suggest you include ==the full dependencies==: [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), Sedona-core, Sedona-SQL, Sedona-Viz. Please see [SQL example project](../demo/)


## Initiate SparkSession
Use the following code to initiate your SparkSession at the beginning:
```Scala
var sparkSession = SparkSession.builder()
.master("local[*]") // Delete this if run in cluster mode
.appName("readTestScala") // Change this to a proper name
// Enable Sedona custom Kryo serializer
.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
.config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
.getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
```

!!!warning
	Sedona has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

If you add ==the Sedona full dependencies== as suggested above, please use the following two lines to enable Sedona Kryo serializer instead:
```Scala
.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
```

## Register SedonaSQL

Add the following line after your SparkSession declaration

```Scala
SedonaSQLRegistrator.registerAll(sparkSession)
```

This function will register Sedona User Defined Type, User Defined Function and optimized join query strategy.

You can also register everything by passing `--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions` to `spark-submit` or `spark-shell`.

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

All geometrical operations in SedonaSQL are on Geometry type objects. Therefore, before any kind of queries, you need to create a Geometry type column on a DataFrame.


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
	SedonaSQL provides lots of functions to create a Geometry column, please read [SedonaSQL constructor API](../api/sql/Constructor.md).
	
## Load Shapefile and GeoJSON

Shapefile and GeoJSON must be loaded by SpatialRDD and converted to DataFrame using Adapter. Please read [Load SpatialRDD](../rdd/#create-a-generic-spatialrdd) and [DataFrame <-> RDD](#convert-between-dataframe-and-spatialrdd).

## Load GeoParquet

Since v`1.3.0`, Sedona natively supports loading GeoParquet file. Sedona will infer geometry fields using the "geo" metadata in GeoParquet files.

```Scala
val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
df.printSchema()
```
The output will be as follows:

```
root
 |-- pop_est: long (nullable = true)
 |-- continent: string (nullable = true)
 |-- name: string (nullable = true)
 |-- iso_a3: string (nullable = true)
 |-- gdp_md_est: double (nullable = true)
 |-- geometry: geometry (nullable = true)
```

Sedona supports spatial predicate push-down for GeoParquet files, please refer to the [SedonaSQL query optimizer](../api/sql/Optimizer.md) documentation for details.

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in SedonaSQL is same as the unit of all geometries in a Geometry column.

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

This `ST_Transform` transform the CRS of these geometries from EPSG:4326 to EPSG:3857. The details CRS information can be found on [EPSG.io](https://epsg.io/)

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
	Read [SedonaSQL constructor API](../api/sql/Constructor.md) to learn how to create a Geometry type query window
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

The details of a join query is available here [Join query](../api/sql/Optimizer.md).

### Other queries

There are lots of other functions can be combined with these queries. Please read [SedonaSQL functions](../api/sql/Function.md) and [SedonaSQL aggregate functions](../api/sql/AggregateFunction.md).

## Save to permanent storage

To save a Spatial DataFrame to some permanent storage such as Hive tables and HDFS, you can simply convert each geometry in the Geometry type column back to a plain String and save the plain DataFrame to wherever you want.


Use the following code to convert the Geometry column in a DataFrame back to a WKT string column:
```Scala
var stringDf = sparkSession.sql(
  """
    |SELECT ST_AsText(countyshape)
    |FROM polygondf
  """.stripMargin)
```

!!!note
	ST_AsGeoJSON is also available. We would like to invite you to contribute more functions


## Save GeoParquet

Since v`1.3.0`, Sedona natively supports writing GeoParquet file. GeoParquet can be saved as follows:

```Scala
df.write.format("geoparquet").save(geoparquetoutputlocation + "/GeoParquet_File_Name.parquet")
```

## Convert between DataFrame and SpatialRDD

### DataFrame to SpatialRDD

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html)

```Scala
var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
```

"usacounty" is the name of the geometry column

!!!warning
	Only one Geometry type column is allowed per DataFrame.
	
### SpatialRDD to DataFrame

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html)

```Scala
var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](../rdd#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
case. At least one column for the user data must be provided.

```Scala
val schema = StructType(Array(
  StructField("county", GeometryUDT, nullable = true),
  StructField("name", StringType, nullable = true),
  StructField("price", DoubleType, nullable = true),
  StructField("age", IntegerType, nullable = true)
))
val spatialDf = Adapter.toDf(spatialRDD, schema, sparkSession)
```

### SpatialPairRDD to DataFrame

PairRDD is the result of a spatial join query or distance join query. SedonaSQL DataFrame-RDD Adapter can convert the result to a DataFrame. But you need to provide the name of other attributes.

```Scala
var joinResultDf = Adapter.toDf(joinResultPairRDD, Seq("left_attribute1", "left_attribute2"), Seq("right_attribute1", "right_attribute2"), sparkSession)
```

or you can use the attribute names directly from the input RDD

```Scala
import scala.collection.JavaConversions._
var joinResultDf = Adapter.toDf(joinResultPairRDD, leftRdd.fieldNames, rightRdd.fieldNames, sparkSession)
```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](../rdd#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
case. Columns for the left and right user data must be provided.

```Scala
val schema = StructType(Array(
  StructField("leftGeometry", GeometryUDT, nullable = true),
  StructField("name", StringType, nullable = true),
  StructField("price", DoubleType, nullable = true),
  StructField("age", IntegerType, nullable = true),
  StructField("rightGeometry", GeometryUDT, nullable = true),
  StructField("category", StringType, nullable = true)
))
val joinResultDf = Adapter.toDf(joinResultPairRDD, schema, sparkSession)
```

## DataFrame Style API

Sedona functions can be used in a DataFrame style API similar to Spark functions.
The following objects contain the exposed functions: `org.apache.spark.sql.sedona_sql.expressions.st_functions`, `org.apache.spark.sql.sedona_sql.expressions.st_constructors`, `org.apache.spark.sql.sedona_sql.expressions.st_predicates`, and `org.apache.spark.sql.sedona_sql.expressions.st_aggregates`.
Every functions can take all `Column` arguments.
Additionally, overloaded forms can commonly take a mix of `String` and other Scala types (such as `Double`) as arguments.
In general the following rules apply (although check the documentation of specific functions for any exceptions):
1. Every function returns a `Column` so that it can be used interchangeably with Spark functions as well as `DataFrame` methods such as `DataFrame.select` or `DataFrame.join`.
1. Every function has a form that takes all `Column` arguments.
These are the most versatile of the forms.
1. Most functions have a form that takes a mix of `String` arguments with other Scala types.
The exact mixture of argument types allowed is function specific.
However, in these instances, all `String` arguments are assumed to be the names of columns and will be wrapped in a `Column` automatically.
Non-`String` arguments are assumed to be literals that are passed to the sedona function. If you need to pass a `String` literal then you should use the all `Column` form of the sedona function and wrap the `String` literal in a `Column` with the `lit` Spark function.

A short example of using this API (uses the `array_min` and `array_max` Spark functions):

```Scala
val values_df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
val min_value = array_min("values")
val max_value = array_max("values")
val point_df = values_df.select(ST_Point(min_value, max_value).as("point"))
```
