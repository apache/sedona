# Write a Sedona SQL application

The page outlines the steps to manage spatial data using SedonaSQL.


SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:

=== "Scala"

	```scala
	var myDataFrame = sparkSession.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```

=== "Java"

	```java
	Dataset<Row> myDataFrame = sparkSession.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```
	
=== "Python"

	```python
	myDataFrame = sparkSession.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md). You can find example county data (i.e., `county_small.tsv`) in [Sedona GitHub repo](https://github.com/apache/sedona/tree/master/core/src/test/resources).

## Set up dependencies

=== "Scala/Java"

	1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md) and add Sedona dependencies in build.sbt or pom.xml.
	2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql) in build.sbt or pom.xml.
	3. Please see [SQL example project](../demo/)

=== "Python"

	1. Please read [Quick start](../../setup/install-python) to install Sedona Python.
	2. This tutorial is based on [Sedona SQL Jupyter Notebook example](../jupyter-notebook). You can interact with Sedona Python Jupyter notebook immediately on Binder. Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) to interact with Sedona Python Jupyter notebook immediately on Binder.

## Initiate SparkSession
Use the following code to initiate your SparkSession at the beginning:

=== "Scala"

	```scala
	var sparkSession = SparkSession.builder()
	.master("local[*]") // Delete this if run in cluster mode
	.appName("readTestScala") // Change this to a proper name
	// Enable Sedona custom Kryo serializer
	.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
	.config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
	.getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
	```
	If you use SedonaViz together with SedonaSQL, please use the following two lines to enable Sedona Kryo serializer instead:
	```scala
	.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
	.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```

=== "Java"

	```java
	SparkSession sparkSession = SparkSession.builder()
	.master("local[*]") // Delete this if run in cluster mode
	.appName("readTestScala") // Change this to a proper name
	// Enable Sedona custom Kryo serializer
	.config("spark.serializer", KryoSerializer.class.getName) // org.apache.spark.serializer.KryoSerializer
	.config("spark.kryo.registrator", SedonaKryoRegistrator.class.getName)
	.getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
	```
	If you use SedonaViz together with SedonaSQL, please use the following two lines to enable Sedona Kryo serializer instead:
	```scala
	.config("spark.serializer", KryoSerializer.class.getName) // org.apache.spark.serializer.KryoSerializer
	.config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```
	
=== "Python"

	```python
	sparkSession = SparkSession. \
	    builder. \
	    appName('appName'). \
	    config("spark.serializer", KryoSerializer.getName). \
	    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
	    config('spark.jars.packages',
	           'org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},'
	           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
	    getOrCreate()
	```

!!!warning
	Sedona has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption and slow performance.



## Register SedonaSQL

Add the following line after your SparkSession declaration

=== "Scala"

	```scala
	SedonaSQLRegistrator.registerAll(sparkSession)
	```

=== "Java"

	```java
	SedonaSQLRegistrator.registerAll(sparkSession)
	```
	
=== "Python"

	```python
	from sedona.register import SedonaRegistrator
	
	SedonaRegistrator.registerAll(spark)
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

=== "Scala/Java"
	```scala
	var rawDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	```

=== "Java"
	```java
	Dataset<Row> rawDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	```

=== "Python"
	```python
	rawDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
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

```sql
SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2
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

```scala
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

=== "Scala/Java"

	```scala
	val df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Java"

	```java
	Dataset<Row> df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Python"

	```python
	df = sparkSession.read.format("geoparquet").load(geoparquetdatalocation1)
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

```sql
SELECT ST_Transform(countyshape, "epsg:4326", "epsg:3857") AS newcountyshape, _c1, _c2, _c3, _c4, _c5, _c6, _c7
FROM spatialdf
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

```sql
SELECT *
FROM spatialdf
WHERE ST_Contains (ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape)
```


!!!note
	Read [SedonaSQL constructor API](../api/sql/Constructor.md) to learn how to create a Geometry type query window

### KNN query

Use ==ST_Distance== to calculate the distance and rank the distance.

The following code returns the 5 nearest neighbor of the given polygon.

```sql
SELECT countyname, ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape) AS distance
FROM spatialdf
ORDER BY distance DESC
LIMIT 5
```

### Join query

The details of a join query is available here [Join query](../api/sql/Optimizer.md).

### Other queries

There are lots of other functions can be combined with these queries. Please read [SedonaSQL functions](../api/sql/Function.md) and [SedonaSQL aggregate functions](../api/sql/AggregateFunction.md).

## Save to permanent storage

To save a Spatial DataFrame to some permanent storage such as Hive tables and HDFS, you can simply convert each geometry in the Geometry type column back to a plain String and save the plain DataFrame to wherever you want.


Use the following code to convert the Geometry column in a DataFrame back to a WKT string column:

```sql
SELECT ST_AsText(countyshape)
FROM polygondf
```

!!!note
	ST_AsGeoJSON is also available. We would like to invite you to contribute more functions


## Save GeoParquet

Since v`1.3.0`, Sedona natively supports writing GeoParquet file. GeoParquet can be saved as follows:

```scala
df.write.format("geoparquet").save(geoparquetoutputlocation + "/GeoParquet_File_Name.parquet")
```

## Sort then Save GeoParquet

To maximize the performance of Sedona GeoParquet filter pushdown, we suggest that you sort the data by their geohash values (see [ST_GeoHash](../../api/sql/Function/#st_geohash)) and then save as a GeoParquet file. An example is as follows:

```
SELECT col1, col2, geom, ST_GeoHash(geom, 5) as geohash
FROM spatialDf
ORDER BY geohash
```


## Convert between DataFrame and SpatialRDD

### DataFrame to SpatialRDD

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html)

=== "Scala"

	```scala
	var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
	```
	
=== "Java"

	```java
	SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
	```

=== "Python"

	```python
	from sedona.utils.adapter import Adapter

	spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
	```

"usacounty" is the name of the geometry column

!!!warning
	Only one Geometry type column is allowed per DataFrame.
	
### SpatialRDD to DataFrame

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html)

=== "Scala"

	```scala
	var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
	```

=== "Java"

	```java
	Dataset<Row> spatialDf = Adapter.toDf(spatialRDD, sparkSession)
	```
	
=== "Python"

	```python
	from sedona.utils.adapter import Adapter
	
	spatialDf = Adapter.toDf(spatialRDD, sparkSession)
	```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](../rdd#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
case. At least one column for the user data must be provided.

=== "Scala"

	```scala
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

=== "Scala"

	```scala
	var joinResultDf = Adapter.toDf(joinResultPairRDD, Seq("left_attribute1", "left_attribute2"), Seq("right_attribute1", "right_attribute2"), sparkSession)
	```

=== "Java"

	```java
	import scala.collection.JavaConverters;	
	
	List leftFields = new ArrayList<>(Arrays.asList("c1", "c2", "c3"));
	List rightFields = new ArrayList<>(Arrays.asList("c4", "c5", "c6"));
	Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, JavaConverters.asScalaBuffer(leftFields).toSeq(), JavaConverters.asScalaBuffer(rightFields).toSeq(), sparkSession);
	```

=== "Python"

	```python
	from sedona.utils.adapter import Adapter

	joinResultDf = Adapter.toDf(jvm_sedona_rdd, ["poi_from_id", "poi_from_name"], ["poi_to_id", "poi_to_name"], spark))
	```
or you can use the attribute names directly from the input RDD

=== "Scala"

	```scala
	import scala.collection.JavaConversions._
	var joinResultDf = Adapter.toDf(joinResultPairRDD, leftRdd.fieldNames, rightRdd.fieldNames, sparkSession)
	```

=== "Java"

	```java
	import scala.collection.JavaConverters;	
	Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, JavaConverters.asScalaBuffer(leftRdd.fieldNames).toSeq(), JavaConverters.asScalaBuffer(rightRdd.fieldNames).toSeq(), sparkSession);
	```
=== "Python"

	```python
	from sedona.utils.adapter import Adapter

	joinResultDf = Adapter.toDf(result_pair_rdd, leftRdd.fieldNames, rightRdd.fieldNames, spark)
	```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](../rdd#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
case. Columns for the left and right user data must be provided.

=== "Scala"

	```scala
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

Every functions can take all `Column` arguments. Additionally, overloaded forms can commonly take a mix of `String` and other Scala types (such as `Double`) as arguments.

In general the following rules apply (although check the documentation of specific functions for any exceptions):

=== "Scala"
	1. Every function returns a `Column` so that it can be used interchangeably with Spark functions as well as `DataFrame` methods such as `DataFrame.select` or `DataFrame.join`.
	2. Every function has a form that takes all `Column` arguments.
	These are the most versatile of the forms.
	3. Most functions have a form that takes a mix of `String` arguments with other Scala types.

=== "Python"

	1. `Column` type arguments are passed straight through and are always accepted.
	2. `str` type arguments are always assumed to be names of columns and are wrapped in a `Column` to support that.
	If an actual string literal needs to be passed then it will need to be wrapped in a `Column` using `pyspark.sql.functions.lit`.
	3. Any other types of arguments are checked on a per function basis.
	Generally, arguments that could reasonably support a python native type are accepted and passed through.
	Check the specific docstring of the function to be sure.
	4. Shapely `Geometry` objects are not currently accepted in any of the functions.

The exact mixture of argument types allowed is function specific.
However, in these instances, all `String` arguments are assumed to be the names of columns and will be wrapped in a `Column` automatically.
Non-`String` arguments are assumed to be literals that are passed to the sedona function. If you need to pass a `String` literal then you should use the all `Column` form of the sedona function and wrap the `String` literal in a `Column` with the `lit` Spark function.

A short example of using this API (uses the `array_min` and `array_max` Spark functions):

=== "Scala"

	```scala
	val values_df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
	val min_value = array_min("values")
	val max_value = array_max("values")
	val point_df = values_df.select(ST_Point(min_value, max_value).as("point"))
	```

=== "Python"

	```python3
	from pyspark.sql import functions as f
	
	from sedona.sql import st_constructors as stc
	
	df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
	
	min_value = f.array_min("values")
	max_value = f.array_max("values")
	
	df = df.select(stc.ST_Point(min_value, max_value).alias("point"))
	```
	
The above code will generate the following dataframe:
```
+-----------+
|point      |
+-----------+
|POINT (0 2)|
+-----------+
```

Some functions will take native python values and infer them as literals.
For example:

```python3
df = df.select(stc.ST_Point(1.0, 3.0).alias("point"))
```

This will generate a dataframe with a constant point in a column:
```
+-----------+
|point      |
+-----------+
|POINT (1 3)|
+-----------+
```

## Interoperate with GeoPandas

Sedona Python has implemented serializers and deserializers which allows to convert Sedona Geometry objects into Shapely BaseGeometry objects. Based on that it is possible to load the data with geopandas from file (look at Fiona possible drivers) and create Spark DataFrame based on GeoDataFrame object.

### From GeoPandas to Sedona DataFrame

Loading the data from shapefile using geopandas read_file method and create Spark DataFrame based on GeoDataFrame:

```python

import geopandas as gpd
from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator

spark = SparkSession.builder.\
      getOrCreate()

SedonaRegistrator.registerAll(spark)

gdf = gpd.read_file("gis_osm_pois_free_1.shp")

spark.createDataFrame(
  gdf
).show()

```

This query will show the following outputs:

```

+---------+----+-----------+--------------------+--------------------+
|   osm_id|code|     fclass|                name|            geometry|
+---------+----+-----------+--------------------+--------------------+
| 26860257|2422|  camp_site|            de Kroon|POINT (15.3393145...|
| 26860294|2406|     chalet|      Leśne Ustronie|POINT (14.8709625...|
| 29947493|2402|      motel|                null|POINT (15.0946636...|
| 29947498|2602|        atm|                null|POINT (15.0732014...|
| 29947499|2401|      hotel|                null|POINT (15.0696777...|
| 29947505|2401|      hotel|                null|POINT (15.0155749...|
+---------+----+-----------+--------------------+--------------------+

```

### From Sedona DataFrame to GeoPandas

Reading data with Spark and converting to GeoPandas

```python

import geopandas as gpd
from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator

spark = SparkSession.builder.\
    getOrCreate()

SedonaRegistrator.registerAll(spark)

counties = spark.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv("counties.csv")

counties.createOrReplaceTempView("county")

counties_geom = spark.sql(
    "SELECT *, st_geomFromWKT(geom) as geometry from county"
)

df = counties_geom.toPandas()
gdf = gpd.GeoDataFrame(df, geometry="geometry")

gdf.plot(
    figsize=(10, 8),
    column="value",
    legend=True,
    cmap='YlOrBr',
    scheme='quantiles',
    edgecolor='lightgray'
)

```
<br>
<br>

![poland_image](https://user-images.githubusercontent.com/22958216/67603296-c08b4680-f778-11e9-8cde-d2e14ffbba3b.png)

<br>
<br>


## Interoperate with shapely objects

### Supported Shapely objects

| shapely object  | Available          |
|-----------------|--------------------|
| Point           | :heavy_check_mark: |
| MultiPoint      | :heavy_check_mark: |
| LineString      | :heavy_check_mark: |
| MultiLinestring | :heavy_check_mark: |
| Polygon         | :heavy_check_mark: |
| MultiPolygon    | :heavy_check_mark: |

To create Spark DataFrame based on mentioned Geometry types, please use <b> GeometryType </b> from  <b> sedona.sql.types </b> module. Converting works for list or tuple with shapely objects.

Schema for target table with integer id and geometry type can be defined as follow:

```python

from pyspark.sql.types import IntegerType, StructField, StructType

from sedona.sql.types import GeometryType

schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("geom", GeometryType(), False)
    ]
)

```

Also Spark DataFrame with geometry type can be converted to list of shapely objects with <b> collect </b> method.

### Point example

```python
from shapely.geometry import Point

data = [
    [1, Point(21.0, 52.0)],
    [1, Point(23.0, 42.0)],
    [1, Point(26.0, 32.0)]
]


gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show()

```

```
+---+-------------+
| id|         geom|
+---+-------------+
|  1|POINT (21 52)|
|  1|POINT (23 42)|
|  1|POINT (26 32)|
+---+-------------+
```

```python
gdf.printSchema()
```

```
root
 |-- id: integer (nullable = false)
 |-- geom: geometry (nullable = false)
```

### MultiPoint example

```python3

from shapely.geometry import MultiPoint

data = [
    [1, MultiPoint([[19.511463, 51.765158], [19.446408, 51.779752]])]
]

gdf = spark.createDataFrame(
    data,
    schema
).show(1, False)

```

```

+---+---------------------------------------------------------+
|id |geom                                                     |
+---+---------------------------------------------------------+
|1  |MULTIPOINT ((19.511463 51.765158), (19.446408 51.779752))|
+---+---------------------------------------------------------+


```

### LineString example

```python3

from shapely.geometry import LineString

line = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, LineString(line)]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+--------------------------------+
|id |geom                            |
+---+--------------------------------+
|1  |LINESTRING (10 10, 20 20, 10 40)|
+---+--------------------------------+

```

### MultiLineString example

```python3

from shapely.geometry import MultiLineString

line1 = [(10, 10), (20, 20), (10, 40)]
line2 = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, MultiLineString([line1, line2])]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+---------------------------------------------------------------------+
|id |geom                                                                 |
+---+---------------------------------------------------------------------+
|1  |MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))|
+---+---------------------------------------------------------------------+

```

### Polygon example

```python3

from shapely.geometry import Polygon

polygon = Polygon(
    [
         [19.51121, 51.76426],
         [19.51056, 51.76583],
         [19.51216, 51.76599],
         [19.51280, 51.76448],
         [19.51121, 51.76426]
    ]
)

data = [
    [1, polygon]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```


```

+---+--------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                    |
+---+--------------------------------------------------------------------------------------------------------+
|1  |POLYGON ((19.51121 51.76426, 19.51056 51.76583, 19.51216 51.76599, 19.5128 51.76448, 19.51121 51.76426))|
+---+--------------------------------------------------------------------------------------------------------+

```

### MultiPolygon example

```python3

from shapely.geometry import MultiPolygon

exterior_p1 = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
interior_p1 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

exterior_p2 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]

polygons = [
    Polygon(exterior_p1, [interior_p1]),
    Polygon(exterior_p2)
]

data = [
    [1, MultiPolygon(polygons)]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+----------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                      |
+---+----------------------------------------------------------------------------------------------------------+
|1  |MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1.5 1, 1.5 1.5, 1 1.5, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))|
+---+----------------------------------------------------------------------------------------------------------+

```

