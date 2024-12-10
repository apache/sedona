<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

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

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md). You can find example county data (i.e., `county_small.tsv`) in [Sedona GitHub repo](https://github.com/apache/sedona/tree/master/spark/common/src/test/resources).

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
    .appName("readTestJava") // Change this to a proper name
    .getOrCreate()
    ```
    If you use SedonaViz together with SedonaSQL, please add the following line after `SedonaContext.builder()` to enable Sedona Kryo serializer:
    ```java
    .config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName()) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
    ```

=== "Python"

    ```python
    from sedona.spark import *

    config = SedonaContext.builder() .\
        config('spark.jars.packages',
               'org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},'
               'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
        getOrCreate()
    ```
    If you are using a different Spark version, please replace the `3.3` in package name of sedona-spark-shaded with the corresponding major.minor version of Spark, such as `sedona-spark-shaded-3.4_2.12:{{ sedona.current_version }}`.

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
    .appName("readTestJava") // Change this to a proper name
    // Enable Sedona custom Kryo serializer
    .config("spark.serializer", KryoSerializer.class.getName()) // org.apache.spark.serializer.KryoSerializer
    .config("spark.kryo.registrator", SedonaKryoRegistrator.class.getName())
    .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
    ```
    If you use SedonaViz together with SedonaSQL, please use the following two lines to enable Sedona Kryo serializer instead:
    ```java
    .config("spark.serializer", KryoSerializer.class.getName()) // org.apache.spark.serializer.KryoSerializer
    .config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName()) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
    ```

=== "Python"

    ```python
    sparkSession = SparkSession. \
        builder. \
        appName('readTestPython'). \
        config("spark.serializer", KryoSerializer.getName()). \
        config("spark.kryo.registrator", SedonaKryoRegistrator.getName()). \
        config('spark.jars.packages',
               'org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},'
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
`scala
	var rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	`

=== "Java"
`java
	Dataset<Row> rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	`

=== "Python"
`python
	rawDf = sedona.read.format("csv").option("delimiter", "\t").option("header", "false").load("/Download/usa-county.tsv")
	rawDf.createOrReplaceTempView("rawdf")
	rawDf.show()
	`

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
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
      "properties": {"prop0": "value0"}
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [102.0, 0.0],
          [103.0, 1.0],
          [104.0, 0.0],
          [105.0, 1.0]
        ]
      },
      "properties": {
        "prop0": "value1",
        "prop1": 0.0
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [100.0, 0.0],
            [101.0, 0.0],
            [101.0, 1.0],
            [100.0, 1.0],
            [100.0, 0.0]
          ]
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

## Load Shapefile

Since v`1.7.0`, Sedona supports loading Shapefile as a DataFrame.

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = sedona.read.format("shapefile").load("/path/to/shapefile")
    ```

The input path can be a directory containing one or multiple shapefiles, or path to a `.shp` file.

- When the input path is a directory, all shapefiles directly under the directory will be loaded. If you want to load all shapefiles in subdirectories, please specify `.option("recursiveFileLookup", "true")`.
- When the input path is a `.shp` file, that shapefile will be loaded. Sedona will look for sibling files (`.dbf`, `.shx`, etc.) with the same main file name and load them automatically.

The name of the geometry column is `geometry` by default. You can change the name of the geometry column using the `geometry.name` option. If one of the non-spatial attributes is named "geometry", `geometry.name` must be configured to avoid conflict.

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").option("geometry.name", "geom").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").option("geometry.name", "geom").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = sedona.read.format("shapefile").option("geometry.name", "geom").load("/path/to/shapefile")
    ```

Each record in shapefile has a unique record number, that record number is not loaded by default. If you want to include record number in the loaded DataFrame, you can set the `key.name` option to the name of the record number column:

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").option("key.name", "FID").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").option("key.name", "FID").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = sedona.read.format("shapefile").option("key.name", "FID").load("/path/to/shapefile")
    ```

The character encoding of string attributes are inferred from the `.cpg` file. If you see garbled values in string fields, you can manually specify the correct charset using the `charset` option. For example:

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = sedona.read.format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

### (Deprecated) Loading Shapefile using SpatialRDD

If you are using Sedona earlier than v`1.7.0`, you can load shapefiles as SpatialRDD and converted to DataFrame using Adapter. Please read [Load SpatialRDD](rdd.md#create-a-generic-spatialrdd) and [DataFrame <-> RDD](#convert-between-dataframe-and-spatialrdd).

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

## Load from geopackage

Since v1.7.0, Sedona supports loading Geopackage file format as a DataFrame.

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("geopackage").option("tableName", "tab").load("/path/to/geopackage")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("geopackage").option("tableName", "tab").load("/path/to/geopackage")
    ```

=== "Python"

    ```python
    df = sedona.read.format("geopackage").option("tableName", "tab").load("/path/to/geopackage")
    ```

Geopackage files can contain vector data and raster data. To show the possible options from a file you can
look into the metadata table by adding parameter showMetadata and set its value as true.

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("geopackage").option("showMetadata", "true").load("/path/to/geopackage")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("geopackage").option("showMetadata", "true").load("/path/to/geopackage")
    ```

=== "Python"

    ```python
    df = sedona.read.format("geopackage").option("showMetadata", "true").load("/path/to/geopackage")

Then you can see the metadata of the geopackage file like below.

```
+--------------------+---------+--------------------+-----------+--------------------+----------+-----------------+----------+----------+------+
|          table_name|data_type|          identifier|description|         last_change|     min_x|            min_y|     max_x|     max_y|srs_id|
+--------------------+---------+--------------------+-----------+--------------------+----------+-----------------+----------+----------+------+
|gis_osm_water_a_f...| features|gis_osm_water_a_f...|           |2024-09-30 23:07:...|-9.0257084|57.96814069999999|33.4866675|80.4291867|  4326|
+--------------------+---------+--------------------+-----------+--------------------+----------+-----------------+----------+----------+------+
```

You can also load data from raster tables in the geopackage file. To load raster data, you can use the following code.

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("geopackage").option("tableName", "raster_table").load("/path/to/geopackage")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("geopackage").option("tableName", "raster_table").load("/path/to/geopackage")
    ```

=== "Python"

    ```python
    df = sedona.read.format("geopackage").option("tableName", "raster_table").load("/path/to/geopackage")
    ```

```
+---+----------+-----------+--------+--------------------+
| id|zoom_level|tile_column|tile_row|           tile_data|
+---+----------+-----------+--------+--------------------+
|  1|        11|        428|     778|GridCoverage2D["c...|
|  2|        11|        429|     778|GridCoverage2D["c...|
|  3|        11|        428|     779|GridCoverage2D["c...|
|  4|        11|        429|     779|GridCoverage2D["c...|
|  5|        11|        427|     777|GridCoverage2D["c...|
+---+----------+-----------+--------+--------------------+
```

Known limitations (v1.7.0):

- webp rasters are not supported
- ewkb geometries are not supported
- filtering based on geometries envelopes are not supported

All points above should be resolved soon, stay tuned !

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

## Cluster with DBSCAN

Sedona provides an implementation of the [DBSCAN](https://en.wikipedia.org/wiki/Dbscan) algorithm to cluster spatial data.

The algorithm is available as a Scala and Python function called on a spatial dataframe. The returned dataframe has an additional column added containing the unique identifier of the cluster that record is a member of and a boolean column indicating if the record is a core point.

The first parameter is the dataframe, the next two are the epsilon and min_points parameters of the DBSCAN algorithm.

=== "Scala"

    ```scala
    import org.apache.sedona.stats.clustering.DBSCAN.dbscan

    dbscan(df, 0.1, 5).show()
    ```

=== "Java"

    ```java
    import org.apache.sedona.stats.clustering.DBSCAN;

    DBSCAN.dbscan(df, 0.1, 5).show();
    ```

=== "Python"

    ```python
    from sedona.stats.clustering.dbscan import dbscan

    dbscan(df, 0.1, 5).show()
    ```

The output will look like this:

```
+----------------+---+------+-------+
|        geometry| id|isCore|cluster|
+----------------+---+------+-------+
|   POINT (2.5 4)|  3| false|      1|
|     POINT (3 4)|  2| false|      1|
|     POINT (3 5)|  5| false|      1|
|     POINT (1 3)|  9|  true|      0|
| POINT (2.5 4.5)|  7|  true|      1|
|     POINT (1 2)|  1|  true|      0|
| POINT (1.5 2.5)|  4|  true|      0|
| POINT (1.2 2.5)|  8|  true|      0|
|   POINT (1 2.5)| 11|  true|      0|
|     POINT (1 5)| 10| false|     -1|
|     POINT (5 6)| 12| false|     -1|
|POINT (12.8 4.5)|  6| false|     -1|
|     POINT (4 3)| 13| false|     -1|
+----------------+---+------+-------+
```

## Calculate the Local Outlier Factor (LOF)

Sedona provides an implementation of the [Local Outlier Factor](https://en.wikipedia.org/wiki/Local_outlier_factor) algorithm to identify anomalous data.

The algorithm is available as a Scala and Python function called on a spatial dataframe. The returned dataframe has an additional column added containing the local outlier factor.

The first parameter is the dataframe, the next is the number of nearest neighbors to consider use in calculating the score.

=== "Scala"

    ```scala
    import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor.localOutlierFactor

    localOutlierFactor(df, 20).show()
    ```

=== "Java"

    ```java
    import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor;

    LocalOutlierFactor.localOutlierFactor(df, 20).show();
    ```

=== "Python"

    ```python
    from sedona.stats.outlier_detection.local_outlier_factor import local_outlier_factor

    local_outlier_factor(df, 20).show()
    ```

The output will look like this:

```
+--------------------+------------------+
|            geometry|               lof|
+--------------------+------------------+
|POINT (-2.0231305...| 0.952098153363662|
|POINT (-2.0346944...|0.9975325496668104|
|POINT (-2.2040074...|1.0825843906411081|
|POINT (1.61573501...|1.7367129352162634|
|POINT (-2.1176324...|1.5714144683150393|
|POINT (-2.2349759...|0.9167275845938276|
|POINT (1.65470192...| 1.046231536764447|
|POINT (0.62624112...|1.1988700676990034|
|POINT (2.01746261...|1.1060219481067417|
|POINT (-2.0483857...|1.0775553430145446|
|POINT (2.43969463...|1.1129132178576646|
|POINT (-2.2425480...| 1.104108012697006|
|POINT (-2.7859235...|  2.86371824574529|
|POINT (-1.9738858...|1.0398822680356794|
|POINT (2.00153403...| 0.927409656346015|
|POINT (2.06422812...|0.9222203762264445|
|POINT (-1.7533819...|1.0273650471626696|
|POINT (-2.2030766...| 0.964744555830738|
|POINT (-1.8509857...|1.0375927869698574|
|POINT (2.10849080...|1.0753419197322656|
+--------------------+------------------+
```

## Perform Getis-Ord Gi(\*) Hot Spot Analysis

Sedona provides an implementation of the [Gi and Gi\*](https://en.wikipedia.org/wiki/Getis%E2%80%93Ord_statistics) algorithms to identify local hotspots in spatial data

The algorithm is available as a Scala and Python function called on a spatial dataframe. The returned dataframe has additional columns added containing G statistic, E[G], V[G], the Z score, and the p-value.

Using Gi involves first generating the neighbors list for each record, then calling the g_local function.
=== "Scala"

    ```scala
    import org.apache.sedona.stats.Weighting.addBinaryDistanceBandColumn
    import org.apache.sedona.stats.hotspotDetection.GetisOrd.gLocal

    val distanceRadius = 1.0
    val weightedDf = addBinaryDistanceBandColumn(df, distanceRadius)
    gLocal(weightedDf, "val").show()
    ```

=== "Java"

    ```java
    import org.apache.sedona.stats.Weighting;
    import org.apache.sedona.stats.hotspotDetection.GetisOrd;
    import org.apache.spark.sql.DataFrame;

    double distanceRadius = 1.0;
    DataFrame weightedDf = Weighting.addBinaryDistanceBandColumn(df, distanceRadius);
    GetisOrd.gLocal(weightedDf, "val").show();
    ```

=== "Python"

    ```python
    from sedona.stats.weighting import add_binary_distance_band_column
    from sedona.stats.hotspot_detection.getis_ord import g_local

    distance_radius = 1.0
    weighted_df = addBinaryDistanceBandColumn(df, distance_radius)
    g_local(weightedDf, "val").show()
    ```

The output will look like this:

<pre>
<code>
+-----------+---+--------------------+-------------------+-------------------+--------------------+--------------------+--------------------+
|   geometry|val|             weights|                  G|                 EG|                  VG|                   Z|                   P|
+-----------+---+--------------------+-------------------+-------------------+--------------------+--------------------+--------------------+
|POINT (2 2)|0.9|[&#123;&#123;POINT (2 3), 1...| 0.4488188976377953|0.45454545454545453| 0.00356321373799772|-0.09593402008347063|  0.4617864875295957|
|POINT (2 3)|1.2|[&#123;&#123;POINT (2 2), 0...|0.35433070866141736|0.36363636363636365|0.003325666155464539|-0.16136436037034918|  0.4359032175415549|
|POINT (3 3)|1.2|[&#123;&#123;POINT (2 3), 1...|0.28346456692913385| 0.2727272727272727|0.002850570990398176| 0.20110780337013057| 0.42030714022155924|
|POINT (3 2)|1.2|[&#123;&#123;POINT (2 2), 0...| 0.4488188976377953|0.45454545454545453| 0.00356321373799772|-0.09593402008347063|  0.4617864875295957|
|POINT (3 1)|1.2|[&#123;&#123;POINT (3 2), 3...| 0.3622047244094489| 0.2727272727272727|0.002850570990398176|  1.6758983614177538| 0.04687905137429871|
|POINT (2 1)|2.2|[&#123;&#123;POINT (2 2), 0...| 0.4330708661417323|0.36363636363636365|0.003325666155464539|  1.2040263812249166| 0.11428969105925013|
|POINT (1 1)|1.2|[&#123;&#123;POINT (2 1), 5...| 0.2834645669291339| 0.2727272727272727|0.002850570990398176|  0.2011078033701316|  0.4203071402215588|
|POINT (1 2)|0.2|[&#123;&#123;POINT (2 2), 0...|0.35433070866141736|0.45454545454545453| 0.00356321373799772|   -1.67884535146075|0.046591093685710794|
|POINT (1 3)|1.2|[&#123;&#123;POINT (2 3), 1...| 0.2047244094488189| 0.2727272727272727|0.002850570990398176| -1.2736827546774914| 0.10138793530151635|
|POINT (0 2)|1.0|[&#123;&#123;POINT (1 2), 7...|0.09448818897637795|0.18181818181818182|0.002137928242798632| -1.8887168824332323|0.029464887612748458|
|POINT (4 2)|1.2|[&#123;&#123;POINT (3 2), 3...| 0.1889763779527559|0.18181818181818182|0.002137928242798632| 0.15481285921583854| 0.43848442662481324|
+-----------+---+--------------------+-------------------+-------------------+--------------------+--------------------+--------------------+
</code>
</pre>

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

### KNN join query

The details of a KNN join query is available here [KNN join query](../api/sql/NearestNeighbourSearching.md).

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
`
	pip install sedona[pydeck-map]
	`

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

![Creating a Choropleth map using SedonaPyDeck](../image/choropleth.gif)

The dataset used is available [here](https://github.com/apache/sedona/tree/b66e768155866a38ba2e3404f1151cac14fad5ea/docs/usecases/data/ne_50m_airports) and
can also be found in the example notebook available [here](https://github.com/apache/sedona/blob/master/docs/usecases/ApacheSedonaSQL_SpatialJoin_AirportsPerCountry.ipynb)

#### Creating a Geometry map using SedonaPyDeck

SedonaPyDeck exposes a create_geometry_map API which can be used to visualize a passed SedonaDataFrame containing any type of geometries:

Example (referenced from overture notebook available via binder):

```python
SedonaPyDeck.create_geometry_map(df_building, elevation_col='height')
```

![Creating a Geometry map using SedonaPyDeck](../image/buildings.gif)

!!!Tip
`elevation_col` is an optional argument which can be used to render a 3D map. Pass the column with 'elevation' values for the geometries here.

#### Creating a Scatterplot map using SedonaPyDeck

SedonaPyDeck exposes a create_scatterplot_map API which can be used to visualize a scatterplot out of the passed SedonaDataFrame containing points:

Example:

```python
SedonaPyDeck.create_scatterplot_map(df=crimes_df)
```

![Creating a Scatterplot map using SedonaPyDeck](../image/points.gif)

The dataset used here is the Chicago crimes dataset, available [here](https://github.com/apache/sedona/blob/sedona-1.5.0/spark/common/src/test/resources/Chicago_Crimes.csv)

#### Creating a heatmap using SedonaPyDeck

SedonaPyDeck exposes a create_heatmap API which can be used to visualize a heatmap out of the passed SedonaDataFrame containing points:

Example:

```python
SedonaPyDeck.create_heatmap(df=crimes_df)
```

![Creating a heatmap using SedonaPyDeck](../image/heatmap.gif)

The dataset used here is the Chicago crimes dataset, available [here](https://github.com/apache/sedona/blob/sedona-1.5.0/spark/common/src/test/resources/Chicago_Crimes.csv)

### SedonaKepler

Spatial query results can be visualized in a Jupyter lab/notebook environment using SedonaKepler.

SedonaKepler exposes APIs to create interactive and customizable map visualizations using [KeplerGl](https://kepler.gl/).

!!!Note
To use SedonaKepler, install sedona with the `kepler-map` extra:
`
	pip install sedona[kepler-map]
	`

This tutorial showcases how simple it is to instantly visualize geospatial data using SedonaKepler.

Example (referenced from an example notebook via the binder):

```python
SedonaKepler.create_map(df=groupedresult, name="AirportCount")
```

![Visualize geospatial data using SedonaKepler](../image/sedona_customization.gif)

The dataset used is available [here](https://github.com/apache/sedona/tree/b66e768155866a38ba2e3404f1151cac14fad5ea/docs/usecases/data/ne_50m_airports) and
can also be found in the example notebook available [here](https://github.com/apache/sedona/blob/master/docs/usecases/ApacheSedonaSQL_SpatialJoin_AirportsPerCountry.ipynb)

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
