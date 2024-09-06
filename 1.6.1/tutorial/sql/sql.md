The page outlines the steps to manage spatial data using SedonaSQL.

!!!note
    Since v`1.5.0`, Sedona assumes geographic coordinates to be in longitude/latitude order. If your data is lat/lon order, please use `ST_FlipCoordinates` to swap X and Y.

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:

=== "Scala"

	```scala
	var myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```

=== "Java"

	```java
	Dataset<Row> myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```

=== "Python"

	```python
	myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("spatialDf")
	```

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md). You can find example county data (i.e., `county_small.tsv`) in [Sedona GitHub repo](https://github.com/apache/sedona/tree/master/core/src/test/resources).

## Set up dependencies

=== "Scala/Java"

	1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md) and add Sedona dependencies in build.sbt or pom.xml.
	2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql) in build.sbt or pom.xml.
	3. Please see [SQL example project](demo.md)

=== "Python"

	1. Please read [Quick start](../setup/install-python.md) to install Sedona Python.
	2. This tutorial is based on [Sedona SQL Jupyter Notebook example](jupyter-notebook.md). You can interact with Sedona Python Jupyter notebook immediately on Binder. Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=docs/usecases) to interact with Sedona Python Jupyter notebook immediately on Binder.

## Create Sedona config

Use the following code to create your Sedona config at the beginning. If you already have a SparkSession (usually named `spark`) created by AWS EMR/Databricks/Microsoft Fabric, please ==skip this step==.

==Sedona >= 1.4.1==

You can add additional Spark runtime config to the config builder. For example, `SedonaContext.builder().config("spark.sql.autoBroadcastJoinThreshold", "10485760")`

=== "Scala"

	```scala
	import org.apache.sedona.spark.SedonaContext

	val config = SedonaContext.builder()
	.master("local[*]") // Delete this if run in cluster mode
	.appName("readTestScala") // Change this to a proper name
	.getOrCreate()
	```
	If you use SedonaViz together with SedonaSQL, please add the following line after `SedonaContext.builder()` to enable Sedona Kryo serializer:
	```scala
	.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```

=== "Java"

	```java
	import org.apache.sedona.spark.SedonaContext;

	SparkSession config = SedonaContext.builder()
	.master("local[*]") // Delete this if run in cluster mode
	.appName("readTestScala") // Change this to a proper name
	.getOrCreate()
	```
	If you use SedonaViz together with SedonaSQL, please add the following line after `SedonaContext.builder()` to enable Sedona Kryo serializer:
	```scala
	.config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
	```

=== "Python"

	```python
	from sedona.spark import *

	config = SedonaContext.builder() .\
	    config('spark.jars.packages',
	           'org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},'
	           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
	    getOrCreate()
	```
    If you are using Spark versions >= 3.4, please replace the `3.0` in package name of sedona-spark-shaded with the corresponding major.minor version of Spark, such as `sedona-spark-shaded-3.4_2.12:{{ sedona.current_version }}`.

==Sedona < 1.4.1==

The following method has been deprecated since Sedona 1.4.1. Please use the method above to create your Sedona config.

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
    If you are using Spark versions >= 3.4, please replace the `3.0` in package name of sedona-spark-shaded with the corresponding major.minor version of Spark, such as `sedona-spark-shaded-3.4_2.12:{{ sedona.current_version }}`.

## Initiate SedonaContext

Add the following line after creating Sedona config. If you already have a SparkSession (usually named `spark`) created by AWS EMR/Databricks/Microsoft Fabric, please call `sedona = SedonaContext.create(spark)` instead. For ==Databricks==, the situation is more complicated, please refer to [Databricks setup guide](../setup/databricks.md), but generally you don't need to create SedonaContext.

==Sedona >= 1.4.1==

=== "Scala"

	```scala
	import org.apache.sedona.spark.SedonaContext

	val sedona = SedonaContext.create(config)
	```

=== "Java"

	```java
	import org.apache.sedona.spark.SedonaContext;

	SparkSession sedona = SedonaContext.create(config)
	```

=== "Python"

	```python
	from sedona.spark import *

	sedona = SedonaContext.create(config)
	```

==Sedona < 1.4.1==

The following method has been deprecated since Sedona 1.4.1. Please use the method above to create your SedonaContext.

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

=== "Scala"
	```scala
	var rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	```

=== "Java"
	```java
	Dataset<Row> rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	```

=== "Python"
	```python
	rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
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

## Load GeoJSON Data

Since `v1.6.1`, Sedona supports reading GeoJSON files using the `geojson` data source. It is designed to handle JSON files that use [GeoJSON format](https://datatracker.ietf.org/doc/html/rfc7946) for their geometries.

This includes SpatioTemporal Asset Catalog (STAC) files, GeoJSON features, GeoJSON feature collections and other variations.
The key functionality lies in the way 'geometry' fields are processed: these are specifically read as Sedona's `GeometryUDT` type, ensuring integration with Sedona's suite of spatial functions.

### Key features

- Broad Support: The reader and writer are versatile, supporting all GeoJSON-formatted files, including STAC files, feature collections, and more.
- Geometry Transformation: When reading, fields named 'geometry' are automatically converted from GeoJSON format to Sedona's `GeometryUDT` type and vice versa when writing.

### Load MultiLine GeoJSON FeatureCollection

Suppose we have a GeoJSON FeatureCollection file as follows.
This entire file is considered as a single GeoJSON FeatureCollection object.
Multiline format is preferable for scenarios where files need to be human-readable or manually edited.

```json
{ "type": "FeatureCollection",
    "features": [
      { "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
        "properties": {"prop0": "value0"}
        },
      { "type": "Feature",
        "geometry": {
          "type": "LineString",
          "coordinates": [
            [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]
            ]
          },
        "properties": {
          "prop0": "value1",
          "prop1": 0.0
          }
        },
      { "type": "Feature",
         "geometry": {
           "type": "Polygon",
           "coordinates": [
             [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
               [100.0, 1.0], [100.0, 0.0] ]
             ]
         },
         "properties": {
           "prop0": "value2",
           "prop1": {"this": "that"}
           }
         }
       ]
}
```

Set the `multiLine` option to `True` to read multiline GeoJSON files.

=== "Python"

    ```python
    df = sedona.read.format("geojson").option("multiLine", "true").load("PATH/TO/MYFILE.json")
     .selectExpr("explode(features) as features") # Explode the envelope to get one feature per row.
     .select("features.*") # Unpack the features struct.
     .withColumn("prop0", f.expr("properties['prop0']")).drop("properties").drop("type")

    df.show()
    df.printSchema()
    ```

=== "Scala"

    ```scala
    val df = sedona.read.format("geojson").option("multiLine", "true").load("PATH/TO/MYFILE.json")
    val parsedDf = df.selectExpr("explode(features) as features").select("features.*")
            .withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")

    parsedDf.show()
    parsedDf.printSchema()
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read.format("geojson").option("multiLine", "true").load("PATH/TO/MYFILE.json")
     .selectExpr("explode(features) as features") // Explode the envelope to get one feature per row.
     .select("features.*") // Unpack the features struct.
     .withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")

    df.show();
    df.printSchema();
    ```

The output is as follows:

```
+--------------------+------+
|            geometry| prop0|
+--------------------+------+
|     POINT (102 0.5)|value0|
|LINESTRING (102 0...|value1|
|POLYGON ((100 0, ...|value2|
+--------------------+------+

root
 |-- geometry: geometry (nullable = false)
 |-- prop0: string (nullable = true)

```

### Load Single Line GeoJSON Features

Suppose we have a single-line GeoJSON Features dataset as follows. Each line is a single GeoJSON Feature.
This format is efficient for processing large datasets where each line is a separate, self-contained GeoJSON object.

```json
{"type":"Feature","geometry":{"type":"Point","coordinates":[102.0,0.5]},"properties":{"prop0":"value0"}}
{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102.0,0.0],[103.0,1.0],[104.0,0.0],[105.0,1.0]]},"properties":{"prop0":"value1"}}
{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]},"properties":{"prop0":"value2"}}
```

By default, when `option` is not specified, Sedona reads a GeoJSON file as a single line GeoJSON.

=== "Python"

	```python
	df = sedona.read.format("geojson").load("PATH/TO/MYFILE.json")
	   .withColumn("prop0", f.expr("properties['prop0']")).drop("properties").drop("type")

	df.show()
	df.printSchema()
	```

=== "Scala"

	```scala
	val df = sedona.read.format("geojson").load("PATH/TO/MYFILE.json")
	   .withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")

	df.show()
	df.printSchema()
	```

=== "Java"

	```java
	Dataset<Row> df = sedona.read.format("geojson").load("PATH/TO/MYFILE.json")
	   .withColumn("prop0", expr("properties['prop0']")).drop("properties").drop("type")

	df.show()
	df.printSchema()
	```

The output is as follows:

```
+--------------------+------+
|            geometry| prop0|
+--------------------+------+
|     POINT (102 0.5)|value0|
|LINESTRING (102 0...|value1|
|POLYGON ((100 0, ...|value2|
+--------------------+------+

root
 |-- geometry: geometry (nullable = false)
 |-- prop0: string (nullable = true)
```

## Load Shapefile using SpatialRDD

Shapefile can be loaded by SpatialRDD and converted to DataFrame using Adapter. Please read [Load SpatialRDD](rdd.md#create-a-generic-spatialrdd) and [DataFrame <-> RDD](#convert-between-dataframe-and-spatialrdd).

## Load GeoParquet

Since v`1.3.0`, Sedona natively supports loading GeoParquet file. Sedona will infer geometry fields using the "geo" metadata in GeoParquet files.

=== "Scala/Java"

	```scala
	val df = sedona.read.format("geoparquet").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Java"

	```java
	Dataset<Row> df = sedona.read.format("geoparquet").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Python"

	```python
	df = sedona.read.format("geoparquet").load(geoparquetdatalocation1)
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

GeoParquet file reader can also be used to read legacy Parquet files written by Apache Sedona 1.3.1-incubating or earlier.
Please refer to [Reading Legacy Parquet Files](../api/sql/Reading-legacy-parquet.md) for details.

!!!warning
	GeoParquet file reader does not work on Databricks runtime when Photon is enabled. Please disable Photon when using
	GeoParquet file reader on Databricks runtime.

### Inspect GeoParquet metadata

Since v`1.5.1`, Sedona provides a Spark SQL data source `"geoparquet.metadata"` for inspecting GeoParquet metadata. The resulting dataframe contains
the "geo" metadata for each input file.

=== "Scala/Java"

	```scala
	val df = sedona.read.format("geoparquet.metadata").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Java"

	```java
	Dataset<Row> df = sedona.read.format("geoparquet.metadata").load(geoparquetdatalocation1)
	df.printSchema()
	```

=== "Python"

	```python
	df = sedona.read.format("geoparquet.metadata").load(geoparquetdatalocation1)
	df.printSchema()
	```

The output will be as follows:

```
root
 |-- path: string (nullable = true)
 |-- version: string (nullable = true)
 |-- primary_column: string (nullable = true)
 |-- columns: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- encoding: string (nullable = true)
 |    |    |-- geometry_types: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- bbox: array (nullable = true)
 |    |    |    |-- element: double (containsNull = true)
 |    |    |-- crs: string (nullable = true)
```

If the input Parquet file does not have GeoParquet metadata, the values of `version`, `primary_column` and `columns` fields of the resulting dataframe will be `null`.

!!! note
	`geoparquet.metadata` only supports reading GeoParquet specific metadata. Users can use [G-Research/spark-extension](https://github.com/G-Research/spark-extension/blob/13109b8e43dfba9272c85896ba5e30cfe280426f/PARQUET.md) to read comprehensive metadata of generic Parquet files.

## Load data from JDBC data sources

The 'query' option in Spark SQL's JDBC data source can be used to convert geometry columns to a format that Sedona can interpret.
This should work for most spatial JDBC data sources.
For Postgis there is no need to add a query to convert geometry types since it's already using EWKB as it's wire format.

=== "Scala"

	```scala
	// For any JDBC data source, including Postgis.
	val df = sedona.read.format("jdbc")
		// Other options.
		.option("query", "SELECT id, ST_AsBinary(geom) as geom FROM my_table")
		.load()
		.withColumn("geom", expr("ST_GeomFromWKB(geom)"))

	// This is a simplified version that works for Postgis.
	val df = sedona.read.format("jdbc")
		// Other options.
		.option("dbtable", "my_table")
		.load()
		.withColumn("geom", expr("ST_GeomFromWKB(geom)"))
	```

=== "Java"

	```java
	// For any JDBC data source, including Postgis.
	Dataset<Row> df = sedona.read().format("jdbc")
		// Other options.
		.option("query", "SELECT id, ST_AsBinary(geom) as geom FROM my_table")
		.load()
		.withColumn("geom", expr("ST_GeomFromWKB(geom)"))

	// This is a simplified version that works for Postgis.
	Dataset<Row> df = sedona.read().format("jdbc")
		// Other options.
		.option("dbtable", "my_table")
		.load()
		.withColumn("geom", expr("ST_GeomFromWKB(geom)"))
	```

=== "Python"

	```python
	# For any JDBC data source, including Postgis.
	df = (sedona.read.format("jdbc")
		# Other options.
		.option("query", "SELECT id, ST_AsBinary(geom) as geom FROM my_table")
		.load()
		.withColumn("geom", f.expr("ST_GeomFromWKB(geom)")))

	# This is a simplified version that works for Postgis.
	df = (sedona.read.format("jdbc")
		# Other options.
		.option("dbtable", "my_table")
		.load()
		.withColumn("geom", f.expr("ST_GeomFromWKB(geom)")))
	```

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in a Geometry column. The unit of all related distances in SedonaSQL is same as the unit of all geometries in a Geometry column.

By default, this function uses lon/lat order since `v1.5.0`. Before, it used lat/lon order. You can use ==ST_FlipCoordinates== to swap X and Y.

For more details, please read the `ST_Transform` section in Sedona API References.

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

## Visualize query results

Sedona provides `SedonaPyDeck` and `SedonaKepler` wrappers, both of which expose APIs to create interactive map visualizations from SedonaDataFrames in a Jupyter environment.

!!!Note
	Both SedonaPyDeck and SedonaKepler expect the default geometry order to be lon-lat. If your dataframe has geometries in the lat-lon order, please check out [ST_FlipCoordinates](https://sedona.apache.org/latest-snapshot/api/sql/Function/#st_flipcoordinates)

!!!Note
	Both SedonaPyDeck and SedonaKepler are designed to work with SedonaDataFrames containing only 1 geometry column. Passing dataframes with multiple geometry columns will cause errors.

### SedonaPyDeck

Spatial query results can be visualized in a Jupyter lab/notebook environment using SedonaPyDeck.

SedonaPyDeck exposes APIs to create interactive map visualizations using [pydeck](https://pydeck.gl/index.html#) based on [deck.gl](https://deck.gl/)

!!!Note
	To use SedonaPyDeck, install sedona with the `pydeck-map` extra:
	```
	pip install sedona[pydeck-map]
	```

The following tutorial showcases the various maps that can be created using SedonaPyDeck, the datasets used to create these maps are publicly available.

Each API exposed by SedonaPyDeck offers customization via optional arguments, details on all possible arguments can be found in the [API docs of SedonaPyDeck](../api/sql/Visualization_SedonaPyDeck.md).

#### Creating a Choropleth map using SedonaPyDeck

SedonaPyDeck exposes a `create_choropleth_map` API which can be used to visualize a choropleth map out of the passed SedonaDataFrame containing polygons with an observation:

Example (referenced from example notebook available via binder):

```python
SedonaPyDeck.create_choropleth_map(df=groupedresult, plot_col='AirportCount')
```

!!!Note
	`plot_col` is a required argument informing SedonaPyDeck of the column name used to render the choropleth effect.

![](../image/choropleth.gif)

The dataset used is available [here](https://github.com/apache/sedona/tree/4c5fa8333b2c61850d5664b878df9493c7915066/binder/data/ne_50m_airports) and
can also be found in the example notebook available [here](https://github.com/apache/sedona/blob/4c5fa8333b2c61850d5664b878df9493c7915066/binder/ApacheSedonaSQL_SpatialJoin_AirportsPerCountry.ipynb)

#### Creating a Geometry map using SedonaPyDeck

SedonaPyDeck exposes a create_geometry_map API which can be used to visualize a passed SedonaDataFrame containing any type of geometries:

Example (referenced from overture notebook available via binder):

```python
SedonaPyDeck.create_geometry_map(df_building, elevation_col='height')
```

![](../image/buildings.gif)

!!!Tip
	`elevation_col` is an optional argument which can be used to render a 3D map. Pass the column with 'elevation' values for the geometries here.

#### Creating a Scatterplot map using SedonaPyDeck

SedonaPyDeck exposes a create_scatterplot_map API which can be used to visualize a scatterplot out of the passed SedonaDataFrame containing points:

Example:

```python
SedonaPyDeck.create_scatterplot_map(df=crimes_df)
```

![](../image/points.gif)

The dataset used here is the Chicago crimes dataset, available [here](https://github.com/apache/sedona/blob/sedona-1.5.0/spark/common/src/test/resources/Chicago_Crimes.csv)

#### Creating a heatmap using SedonaPyDeck

SedonaPyDeck exposes a create_heatmap API which can be used to visualize a heatmap out of the passed SedonaDataFrame containing points:

Example:

```python
SedonaPyDeck.create_heatmap(df=crimes_df)
```

![](../image/heatmap.gif)

The dataset used here is the Chicago crimes dataset, available [here](https://github.com/apache/sedona/blob/sedona-1.5.0/spark/common/src/test/resources/Chicago_Crimes.csv)

### SedonaKepler

Spatial query results can be visualized in a Jupyter lab/notebook environment using SedonaKepler.

SedonaKepler exposes APIs to create interactive and customizable map visualizations using [KeplerGl](https://kepler.gl/).

!!!Note
	To use SedonaKepler, install sedona with the `kepler-map` extra:
	```
	pip install sedona[kepler-map]
	```

This tutorial showcases how simple it is to instantly visualize geospatial data using SedonaKepler.

Example (referenced from an example notebook via the binder):

```python
SedonaKepler.create_map(df=groupedresult, name="AirportCount")
```

![](../image/sedona_customization.gif)

The dataset used is available [here](https://github.com/apache/sedona/tree/4c5fa8333b2c61850d5664b878df9493c7915066/binder/data/ne_50m_airports) and
can also be found in the example notebook available [here](https://github.com/apache/sedona/blob/4c5fa8333b2c61850d5664b878df9493c7915066/binder/ApacheSedonaSQL_SpatialJoin_AirportsPerCountry.ipynb)

Details on all the APIs available by SedonaKepler are listed in the [SedonaKepler API docs](../api/sql/Visualization_SedonaKepler.md)

## Create a User-Defined Function (UDF)

User-Defined Functions (UDFs) are user-created procedures that can perform operations on a single row of information. To cover almost all use cases, we will showcase 4 types of UDFs for a better understanding of how to use geometry with UDFs. Sedona's serializer deserializes the SQL geometry type to [JTS Geometry](https://locationtech.github.io/jts/javadoc-1.18.0/org/locationtech/jts/geom/Geometry.html) (Scala/Java) or [Shapely Geometry](https://shapely.readthedocs.io/en/stable/geometry.html) (Python). You can implement any custom logic using the rich ecosystem around these two libraries.

### Geometry to primitive

This UDF example takes a geometry type input and returns a primitive type output:

=== "Scala"

	```scala
	import org.locationtech.jts.geom.Geometry
	import org.apache.spark.sql.types._

	def lengthPoly(geom: Geometry): Double = {
        geom.getLength
	}

	sedona.udf.register("udf_lengthPoly", lengthPoly _)

	df.selectExpr("udf_lengthPoly(geom)").show()
	```

=== "Java"

	```java
	import org.apache.spark.sql.api.java.UDF1;
	import org.apache.spark.sql.types.DataTypes;

	// using lambda function to register the UDF
	sparkSession.udf().register(
			"udf_lengthPoly",
			(UDF1<Geometry, Double>) Geometry::getLength,
			DataTypes.DoubleType);

	df.selectExpr("udf_lengthPoly(geom)").show()
	```

=== "Python"

	```python
	from sedona.sql.types import GeometryType
	from pyspark.sql.types import DoubleType

	def lengthPoly(geom: GeometryType()):
		return geom.length

	sedona.udf.register("udf_lengthPoly", lengthPoly, DoubleType())

	df.selectExpr("udf_lengthPoly(geom)").show()
	```

Output:

```
+--------------------+
|udf_lengthPoly(geom)|
+--------------------+
|   3.414213562373095|
+--------------------+
```

### Geometry to Geometry

This UDF example takes a geometry type input and returns a geometry type output:

=== "Scala"

	```scala
	import org.locationtech.jts.geom.Geometry
	import org.apache.spark.sql.types._

	def bufferFixed(geom: Geometry): Geometry = {
        geom.buffer(5.5)
	}

	sedona.udf.register("udf_bufferFixed", bufferFixed _)

	df.selectExpr("udf_bufferFixed(geom)").show()
	```

=== "Java"

	```java
	import org.apache.spark.sql.api.java.UDF1;
	import org.apache.spark.sql.types.DataTypes;

	// using lambda function to register the UDF
	sparkSession.udf().register(
			"udf_bufferFixed",
			(UDF1<Geometry, Geometry>) geom ->
                geom.buffer(5.5),
			new GeometryUDT());

	df.selectExpr("udf_bufferFixed(geom)").show()
	```

=== "Python"

	```python
	from sedona.sql.types import GeometryType
	from pyspark.sql.types import DoubleType

	def bufferFixed(geom: GeometryType()):
    	return geom.buffer(5.5)

	sedona.udf.register("udf_bufferFixed", bufferFixed, GeometryType())

	df.selectExpr("udf_bufferFixed(geom)").show()
	```

Output:

```
+--------------------------------------------------+
|                             udf_bufferFixed(geom)|
+--------------------------------------------------+
|POLYGON ((1 -4.5, -0.0729967710887076 -4.394319...|
+--------------------------------------------------+
```

### Geometry, primitive to geometry

This UDF example takes a geometry type input and a primitive type input and returns a geometry type output:

=== "Scala"

	```scala
	import org.locationtech.jts.geom.Geometry
	import org.apache.spark.sql.types._

	def bufferIt(geom: Geometry, distance: Double): Geometry = {
        geom.buffer(distance)
	}

	sedona.udf.register("udf_buffer", bufferIt _)

	df.selectExpr("udf_buffer(geom, distance)").show()
	```

=== "Java"

	```java
	import org.apache.spark.sql.api.java.UDF2;
	import org.apache.spark.sql.types.DataTypes;

	// using lambda function to register the UDF
	sparkSession.udf().register(
			"udf_buffer",
			(UDF2<Geometry, Double, Geometry>) Geometry::buffer,
			new GeometryUDT());

	df.selectExpr("udf_buffer(geom, distance)").show()
	```

=== "Python"

	```python
	from sedona.sql.types import GeometryType
	from pyspark.sql.types import DoubleType

	def bufferIt(geom: GeometryType(), distance: DoubleType()):
    	return geom.buffer(distance)

	sedona.udf.register("udf_buffer", bufferIt, GeometryType())

	df.selectExpr("udf_buffer(geom, distance)").show()
	```

Output:

```
+--------------------------------------------------+
|                        udf_buffer(geom, distance)|
+--------------------------------------------------+
|POLYGON ((1 -9, -0.9509032201612866 -8.80785280...|
+--------------------------------------------------+
```

### Geometry, primitive to Geometry, primitive

This UDF example takes a geometry type input and a primitive type input and returns a geometry type and a primitive type output:

=== "Scala"

	```scala
	import org.locationtech.jts.geom.Geometry
	import org.apache.spark.sql.types._
	import org.apache.spark.sql.api.java.UDF2

	val schemaUDF = StructType(Array(
		StructField("buffed", GeometryUDT),
		StructField("length", DoubleType)
	))

	val udf_bufferLength = udf(
		new UDF2[Geometry, Double, (Geometry, Double)] {
			def call(geom: Geometry, distance: Double): (Geometry, Double) = {
				val buffed = geom.buffer(distance)
				val length = geom.getLength
				(buffed, length)
			}
		}, schemaUDF)

	sedona.udf.register("udf_bufferLength", udf_bufferLength)

	data.withColumn("bufferLength", expr("udf_bufferLengths(geom, distance)"))
        .select("geom", "distance", "bufferLength.*")
		.show()
	```

=== "Java"

	```java
	import org.apache.spark.sql.api.java.UDF2;
	import org.apache.spark.sql.types.DataTypes;
	import org.apache.spark.sql.types.StructType;
	import scala.Tuple2;

	StructType schemaUDF = new StructType()
                .add("buffedGeom", new GeometryUDT())
                .add("length", DataTypes.DoubleType);

	// using lambda function to register the UDF
	sparkSession.udf().register("udf_bufferLength",
                (UDF2<Geometry, Double, Tuple2<Geometry, Double>>) (geom, distance) -> {
                    Geometry buffed = geom.buffer(distance);
                    Double length = buffed.getLength();
                    return new Tuple2<>(buffed, length);
                },
                schemaUDF);

	df.withColumn("bufferLength", functions.expr("udf_bufferLength(geom, distance)"))
                .select("geom", "distance", "bufferLength.*")
				.show();
	```

=== "Python"

	```python
	from sedona.sql.types import GeometryType
	from pyspark.sql.types import *

	schemaUDF = StructType([
        StructField("buffed", GeometryType()),
        StructField("length", DoubleType())
    ])

	def bufferAndLength(geom: GeometryType(), distance: DoubleType()):
		buffed = geom.buffer(distance)
		length = buffed.length
		return [buffed, length]

	sedona.udf.register("udf_bufferLength", bufferAndLength, schemaUDF)

	df.withColumn("bufferLength", expr("udf_bufferLength(geom, buffer)")) \
				.select("geom", "buffer", "bufferLength.*") \
				.show()
	```

Output:

```
+------------------------------+--------+--------------------------------------------------+-----------------+
|                          geom|distance|                                        buffedGeom|           length|
+------------------------------+--------+--------------------------------------------------+-----------------+
|POLYGON ((1 1, 1 2, 2 1, 1 1))|    10.0|POLYGON ((1 -9, -0.9509032201612866 -8.80785280...|66.14518337329191|
+------------------------------+--------+--------------------------------------------------+-----------------+
```

## Save to permanent storage

To save a Spatial DataFrame to some permanent storage such as Hive tables and HDFS, you can simply convert each geometry in the Geometry type column back to a plain String and save the plain DataFrame to wherever you want.

Use the following code to convert the Geometry column in a DataFrame back to a WKT string column:

```sql
SELECT ST_AsText(countyshape)
FROM polygondf
```

## Save as GeoJSON

Since `v1.6.1`, the GeoJSON data source in Sedona can be used to save a Spatial DataFrame to a single-line JSON file, with geometries written in GeoJSON format.

```sparksql
df.write.format("geojson").save("YOUR/PATH.json")
```

The structure of the generated file will be like this:

```json
{"type":"Feature","geometry":{"type":"Point","coordinates":[102.0,0.5]},"properties":{"prop0":"value0"}}
{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102.0,0.0],[103.0,1.0],[104.0,0.0],[105.0,1.0]]},"properties":{"prop0":"value1"}}
{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]},"properties":{"prop0":"value2"}}
```

## Save GeoParquet

Since v`1.3.0`, Sedona natively supports writing GeoParquet file. GeoParquet can be saved as follows:

```scala
df.write.format("geoparquet").save(geoparquetoutputlocation + "/GeoParquet_File_Name.parquet")
```

### CRS Metadata

Since v`1.5.1`, Sedona supports writing GeoParquet files with custom GeoParquet spec version and crs.
The default GeoParquet spec version is `1.0.0` and the default crs is `null`. You can specify the GeoParquet spec version and crs as follows:

```scala
val projjson = "{...}" // PROJJSON string for all geometry columns
df.write.format("geoparquet")
		.option("geoparquet.version", "1.0.0")
		.option("geoparquet.crs", projjson)
		.save(geoparquetoutputlocation + "/GeoParquet_File_Name.parquet")
```

If you have multiple geometry columns written to the GeoParquet file, you can specify the CRS for each column.
For example, `g0` and `g1` are two geometry columns in the DataFrame `df`, and you want to specify the CRS for each column as follows:

```scala
val projjson_g0 = "{...}" // PROJJSON string for g0
val projjson_g1 = "{...}" // PROJJSON string for g1
df.write.format("geoparquet")
		.option("geoparquet.version", "1.0.0")
		.option("geoparquet.crs.g0", projjson_g0)
		.option("geoparquet.crs.g1", projjson_g1)
		.save(geoparquetoutputlocation + "/GeoParquet_File_Name.parquet")
```

The value of `geoparquet.crs` and `geoparquet.crs.<column_name>` can be one of the following:

- `"null"`: Explicitly setting `crs` field to `null`. This is the default behavior.
- `""` (empty string): Omit the `crs` field. This implies that the CRS is [OGC:CRS84](https://www.opengis.net/def/crs/OGC/1.3/CRS84) for CRS-aware implementations.
- `"{...}"` (PROJJSON string): The `crs` field will be set as the PROJJSON object representing the Coordinate Reference System (CRS) of the geometry. You can find the PROJJSON string of a specific CRS from here: https://epsg.io/ (click the JSON option at the bottom of the page). You can also customize your PROJJSON string as needed.

Please note that Sedona currently cannot set/get a projjson string to/from a CRS. Its geoparquet reader will ignore the projjson metadata and you will have to set your CRS via [`ST_SetSRID`](../api/sql/Function.md#st_setsrid) after reading the file.
Its geoparquet writer will not leverage the SRID field of a geometry so you will have to always set the `geoparquet.crs` option manually when writing the file, if you want to write a meaningful CRS field.

Due to the same reason, Sedona geoparquet reader and writer do NOT check the axis order (lon/lat or lat/lon) and assume they are handled by the users themselves when writing / reading the files. You can always use [`ST_FlipCoordinates`](../api/sql/Function.md#st_flipcoordinates) to swap the axis order of your geometries.

### Covering Metadata

Since `v1.6.1`, Sedona supports writing the [`covering` field](https://github.com/opengeospatial/geoparquet/blob/v1.1.0/format-specs/geoparquet.md#covering) to geometry column metadata. The `covering` field specifies a bounding box column to help accelerate spatial data retrieval. The bounding box column should be a top-level struct column containing `xmin`, `ymin`, `xmax`, `ymax` columns. If the DataFrame you are writing contains such columns, you can specify `.option("geoparquet.covering.<geometryColumnName>", "<coveringColumnName>")` option to write `covering` metadata to GeoParquet files:

```scala
df.write.format("geoparquet")
		.option("geoparquet.covering.geometry", "bbox")
		.save("/path/to/saved_geoparquet.parquet")
```

If the DataFrame has only one geometry column, you can simply specify the `geoparquet.covering` option and omit the geometry column name:

```scala
df.write.format("geoparquet")
		.option("geoparquet.covering", "bbox")
		.save("/path/to/saved_geoparquet.parquet")
```

If the DataFrame does not have a covering column, you can construct one using Sedona's SQL functions:

```scala
val df_bbox = df.withColumn("bbox", expr("struct(ST_XMin(geometry) AS xmin, ST_YMin(geometry) AS ymin, ST_XMax(geometry) AS xmax, ST_YMax(geometry) AS ymax)"))
df_bbox.write.format("geoparquet").option("geoparquet.covering.geometry", "bbox").save("/path/to/saved_geoparquet.parquet")
```

## Sort then Save GeoParquet

To maximize the performance of Sedona GeoParquet filter pushdown, we suggest that you sort the data by their geohash values (see [ST_GeoHash](../api/sql/Function.md#st_geohash)) and then save as a GeoParquet file. An example is as follows:

```
SELECT col1, col2, geom, ST_GeoHash(geom, 5) as geohash
FROM spatialDf
ORDER BY geohash
```

## Save to Postgis

Unfortunately, the Spark SQL JDBC data source doesn't support creating geometry types in PostGIS using the 'createTableColumnTypes' option.
Only the Spark built-in types are recognized.
This means that you'll need to manage your PostGIS schema separately from Spark.
One way to do this is to create the table with the correct geometry column before writing data to it with Spark.
Alternatively, you can write your data to the table using Spark and then manually alter the column to be a geometry type afterward.

Postgis uses EWKB to serialize geometries.
If you convert your geometries to EWKB format in Sedona you don't have to do any additional conversion in Postgis.

```
my_postgis_db# create table my_table (id int8, geom geometry);

df.withColumn("geom", expr("ST_AsEWKB(geom)")
	.write.format("jdbc")
	.option("truncate","true") // Don't let Spark recreate the table.
	// Other options.
	.save()

// If you didn't create the table before writing you can change the type afterward.
my_postgis_db# alter table my_table alter column geom type geometry;

```

## Convert between DataFrame and SpatialRDD

### DataFrame to SpatialRDD

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../api/scaladoc/spark/org/apache/sedona/sql/utils/index.html)

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

Use SedonaSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD. Please read [Adapter Scaladoc](../api/javadoc/sql/org/apache/sedona/sql/utils/index.html)

=== "Scala"

	```scala
	var spatialDf = Adapter.toDf(spatialRDD, sedona)
	```

=== "Java"

	```java
	Dataset<Row> spatialDf = Adapter.toDf(spatialRDD, sedona)
	```

=== "Python"

	```python
	from sedona.utils.adapter import Adapter

	spatialDf = Adapter.toDf(spatialRDD, sedona)
	```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](rdd.md#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
case. At least one column for the user data must be provided.

=== "Scala"

	```scala
	val schema = StructType(Array(
	  StructField("county", GeometryUDT, nullable = true),
	  StructField("name", StringType, nullable = true),
	  StructField("price", DoubleType, nullable = true),
	  StructField("age", IntegerType, nullable = true)
	))
	val spatialDf = Adapter.toDf(spatialRDD, schema, sedona)
	```

### SpatialPairRDD to DataFrame

PairRDD is the result of a spatial join query or distance join query. SedonaSQL DataFrame-RDD Adapter can convert the result to a DataFrame. But you need to provide the name of other attributes.

=== "Scala"

	```scala
	var joinResultDf = Adapter.toDf(joinResultPairRDD, Seq("left_attribute1", "left_attribute2"), Seq("right_attribute1", "right_attribute2"), sedona)
	```

=== "Java"

	```java
	import scala.collection.JavaConverters;

	List leftFields = new ArrayList<>(Arrays.asList("c1", "c2", "c3"));
	List rightFields = new ArrayList<>(Arrays.asList("c4", "c5", "c6"));
	Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, JavaConverters.asScalaBuffer(leftFields).toSeq(), JavaConverters.asScalaBuffer(rightFields).toSeq(), sedona);
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
	var joinResultDf = Adapter.toDf(joinResultPairRDD, leftRdd.fieldNames, rightRdd.fieldNames, sedona)
	```

=== "Java"

	```java
	import scala.collection.JavaConverters;
	Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, JavaConverters.asScalaBuffer(leftRdd.fieldNames).toSeq(), JavaConverters.asScalaBuffer(rightRdd.fieldNames).toSeq(), sedona);
	```
=== "Python"

	```python
	from sedona.utils.adapter import Adapter

	joinResultDf = Adapter.toDf(result_pair_rdd, leftRdd.fieldNames, rightRdd.fieldNames, spark)
	```

All other attributes such as price and age will be also brought to the DataFrame as long as you specify ==carryOtherAttributes== (see [Read other attributes in an SpatialRDD](rdd.md#read-other-attributes-in-an-spatialrdd)).

You may also manually specify a schema for the resulting DataFrame in case you require different column names or data
types. Note that string schemas and not all data types are supported&mdash;please check the
[Adapter Scaladoc](../api/javadoc/sql/org/apache/sedona/sql/utils/index.html) to confirm what is supported for your use
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
	val joinResultDf = Adapter.toDf(joinResultPairRDD, schema, sedona)
	```
