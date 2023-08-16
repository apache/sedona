The page outlines the steps to manage spatial data using SedonaSQL.


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
	3. Please see [SQL example project](../demo/)

=== "Python"

	1. Please read [Quick start](../../setup/install-python) to install Sedona Python.
	2. This tutorial is based on [Sedona SQL Jupyter Notebook example](../jupyter-notebook). You can interact with Sedona Python Jupyter notebook immediately on Binder. Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) to interact with Sedona Python Jupyter notebook immediately on Binder.

## Create Sedona config

Use the following code to create your Sedona config at the beginning. If you already have a SparkSession (usually named `spark`) created by Wherobots/AWS EMR/Databricks, please skip this step and can use `spark` directly.


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

Add the following line after creating Sedona config. If you already have a SparkSession (usually named `spark`) created by Wherobots/AWS EMR/Databricks, please call `SedonaContext.create(spark)` instead.

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

## Load GeoJSON using Spark JSON Data Source

Spark SQL's built-in JSON data source supports reading GeoJSON data.
To ensure proper parsing of the geometry property, we can define a schema with the geometry property set to type 'string'.
This prevents Spark from interpreting the property and allows us to use the ST_GeomFromGeoJSON function for accurate geometry parsing.

=== "Scala"

	```scala
	val schema = "type string, crs string, totalFeatures long, features array<struct<type string, geometry string, properties map<string, string>>>"
	sedona.read.schema(schema).json(geojson_path)
		.selectExpr("explode(features) as features") // Explode the envelope to get one feature per row.
		.select("features.*") // Unpack the features struct.
		.withColumn("geometry", expr("ST_GeomFromGeoJSON(geometry)")) // Convert the geometry string.
		.printSchema()
	```

=== "Java"

	```java
	String schema = "type string, crs string, totalFeatures long, features array<struct<type string, geometry string, properties map<string, string>>>";
	sedona.read.schema(schema).json(geojson_path)
		.selectExpr("explode(features) as features") // Explode the envelope to get one feature per row.
		.select("features.*") // Unpack the features struct.
		.withColumn("geometry", expr("ST_GeomFromGeoJSON(geometry)")) // Convert the geometry string.
		.printSchema();
	```

=== "Python"

	```python
	schema = "type string, crs string, totalFeatures long, features array<struct<type string, geometry string, properties map<string, string>>>";
	(sedona.read.json(geojson_path, schema=schema) 
		.selectExpr("explode(features) as features") # Explode the envelope to get one feature per row.
		.select("features.*") # Unpack the features struct.
		.withColumn("geometry", f.expr("ST_GeomFromGeoJSON(geometry)")) # Convert the geometry string.
		.printSchema())
	```

	
## Load Shapefile and GeoJSON using SpatialRDD

Shapefile and GeoJSON can be loaded by SpatialRDD and converted to DataFrame using Adapter. Please read [Load SpatialRDD](../rdd/#create-a-generic-spatialrdd) and [DataFrame <-> RDD](#convert-between-dataframe-and-spatialrdd).

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
Spatial query results can be visualized in Jupyter lab/notebook using SedonaPyDeck.

SedonaPyDeck exposes APIs to create interactive map visualizations using [pydeck](https://pydeck.gl/index.html#) based on [deck.gl](https://deck.gl/)

!!!Note
	To use SedonaPyDeck, GeoPandas and PyDeck must be installed. We recommend the following installation commands:
	```
	pip install 'pandas<=1.3.5'
	pip install 'geopandas<=0.10.2'
	pip install pydeck==0.8.0
	```

#### Creating a Choropleth map using SedonaPyDeck

SedonaPyDeck exposes a create_choropleth_map API which can be used to visualize a choropleth map out of the passed SedonaDataFrame containing polygons with an observation:

```python
def create_choropleth_map(cls, df, fill_color=None, plot_col=None, initial_view_state=None, map_style=None,
						  map_provider=None, elevation_col=0)
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column.

For example, all these are valid values of fill_color:
```python
fill_color=[255, 12, 250]
fill_color=[0, 12, 250, 255]
fill_color='[0, 12, 240, AirportCount * 10]' ## AirportCount is a column in the passed df
```

Instead of giving a `fill_color` parameter, a 'plot_col' can be passed which specifies the column to decide the choropleth. 
SedonaPyDeck then creates a default color scheme based on the values of the column passed.

The parameter `elevation_col` can be given a numeric or a string value (containing the column with/without operations on it) to set a 3D elevation to the plotted polygons if any.


Optionally, parameters `initial_view_state`, `map_style`, `map_provider` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the pydeck website.

#### Creating a Geometry map using SedonaPyDeck

SedonaPyDeck exposes a create_geometry_map API which can be used to visualize a passed SedonaDataFrame containing any type of geometries:

```python
def create_geometry_map(cls, df, fill_color="[85, 183, 177, 255]", line_color="[85, 183, 177, 255]",
						elevation_col=0, initial_view_state=None,
						map_style=None, map_provider=None):
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column, and is used to color polygons or point geometries in the map

The parameter `line_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column, and is used to color the line geometries in the map.

The parameter `elevation_col` can be given a static elevation or elevation based on column values like `fill_color`, this only works for the polygon geometries in the map.

Optionally, parameters `initial_view_state`, `map_style`, `map_provider` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the pydeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/geojson-layer.md)

#### Creating a Scatterplot map using SedonaPyDeck

SedonaPyDeck exposes a create_scatterplot_map API which can be used to visualize a scatterplot out of the passed SedonaDataFrame containing points:

```python
def create_scatterplot_map(cls, df, fill_color="[255, 140, 0]", radius_col=1, radius_min_pixels = 1, radius_max_pixels = 10, radius_scale=1, initial_view_state=None, map_style=None, map_provider=None)
```

The parameter `fill_color` can be given a list of RGB/RGBA values, or a string that contains RGB/RGBA values based on a column.

The parameter `radius_col` can be given a numeric value or a string value consisting of any operations on the column, in order to specify the radius of the plotted point.

The parameter `radius_min_pixels` can be given a numeric value that would set the minimum radius in pixels. This can be used to prevent the plotted circle from getting too small when zoomed out.

The parameter `radius_max_pixels` can be given a numeric value that would set the maximum radius in pixels. This can be used to prevent the circle from getting too big when zoomed in.

The parameter `radius_scale` can be given a numeric value that sets a global radius multiplier for all points. 

Optionally, parameters `initial_view_state`, `map_style`, `map_provider` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the pydeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/layers/scatterplot-layer.md)


#### Creating a heatmap using SedonaPyDeck 

SedonaPyDeck exposes a create_heatmap API which can be used to visualize a heatmap out of the passed SedonaDataFrame containing points:
```python
def create_heatmap(cls, df, color_range=None, weight=1, aggregation="SUM", initial_view_state=None, map_style=None,
				   map_provider=None)
```

The parameter `color_range` can be optionally given a list of RGB values, SedonaPyDeck by default uses `6-class YlOrRd` as color_range.
More examples can be found on [colorbrewer](https://colorbrewer2.org/#type=sequential&scheme=YlOrRd&n=6)

The parameter `weight` can be given a numeric value or a string with column and operations on it to determine weight of each point while plotting a heatmap.
By default, SedonaPyDeck assigns a weight of 1 to each point

The parameter `aggregation` can be used to define aggregation strategy to use when aggregating heatmap to a lower resolution (zooming out). 
One of "MEAN" or "SUM" can be provided. By default, SedonaPyDeck uses "MEAN" as the aggregation strategy.

Optionally, parameters `initial_view_state`, `map_style`, `map_provider` can be passed to configure the map as per user's liking.
More details on the parameters and their default values can be found on the pydeck website as well by deck.gl [here](https://github.com/visgl/deck.gl/blob/8.9-release/docs/api-reference/aggregation-layers/heatmap-layer.md)


### SedonaKepler

Spatial query results can be visualized in Jupyter lab/notebook using SedonaKepler. 

SedonaKepler exposes APIs to create interactive and customizable map visualizations using [KeplerGl](https://kepler.gl/).

!!!Note
	To use SedonaKepler, GeoPandas and KeplerGL must be installed. We recommend the following installation commands:
	```
	pip install 'pandas<=1.3.5'
	pip install 'geopandas<=0.10.2'
	pip install keplergl==0.3.2
	```

#### Creating a map object using SedonaKepler.create_map

SedonaKepler exposes a create_map API with the following signature:

```python
create_map(df: SedonaDataFrame=None, name: str='unnamed', config: dict=None) -> map
```

The parameter 'name' is used to associate the passed SedonaDataFrame in the map object and any config applied to the map is linked to this name. It is recommended you pass a unique identifier to the dataframe here.

If no SedonaDataFrame object is passed, an empty map (with config applied if passed) is returned. A SedonaDataFrame can be added later using the method `add_df`

A map config can be passed optionally to apply pre-apply customizations to the map.

!!!Note 
	The map config references every customization with the name assigned to the SedonaDataFrame being displayed, if there is a mismatch in the name, the config will not be applied to the map object.


!!! abstract "Example usage (Referenced from Sedona Jupyter examples)"

	=== "Python"
		```python
		map = SedonaKepler.create_map(df=groupedresult, name="AirportCount")
		map
		```

#### Adding SedonaDataFrame to a map object using SedonaKepler.add_df
SedonaKepler exposes a add_df API with the following signature:

```python
add_df(map, df: SedonaDataFrame, name: str='unnamed')
```

This API can be used to add a SedonaDataFrame to an already created map object. The map object passed is directly mutated and nothing is returned.

The parameters name has the same conditions as 'create_map'

!!!Tip
	This method can be used to add multiple dataframes to a map object to be able to visualize them together.

!!! abstract "Example usage (Referenced from Sedona Jupyter examples)"
	=== "Python"
		```python
		map = SedonaKepler.create_map()
		SedonaKepler.add_df(map, groupedresult, name="AirportCount")
		map
		```

#### Setting a config via the map 
A map rendered by accessing the map object created by SedonaKepler includes a config panel which can be used to customize the map

<img src="../../image/sedona_customization.gif" width="1000">


#### Saving and setting config

A map object's current config can be accessed by accessing its 'config' attribute like `map.config`. This config can be saved for future use or use across notebooks if the exact same map is to be rendered everytime.

!!!Note
	The map config references each applied customization with the name given to the dataframe and hence will work only on maps with the same name of dataframe supplied.
	For more details refer to keplerGl documentation [here](https://docs.kepler.gl/docs/keplergl-jupyter#6.-match-config-with-data)
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
	val joinResultDf = Adapter.toDf(joinResultPairRDD, schema, sedona)
	```
