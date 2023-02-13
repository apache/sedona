The page outlines the steps to visualize spatial data using SedonaViz. ==The example code is written in Scala but also works for Java==.

SedonaViz provides native support for general cartographic design by extending Sedona to process large-scale spatial data. It can visulize Spatial RDD and Spatial Queries and render super high resolution image in parallel.

SedonaViz offers Map Visualization SQL. This gives users a more flexible way to design beautiful map visualization effects including scatter plots and heat maps. SedonaViz RDD API is also available.

!!!note
	All SedonaViz SQL/DataFrame APIs are explained in [SedonaViz API](../../api/viz/sql). Please see [Viz example project](https://github.com/apache/sedona/tree/master/examples/viz)

## Why scalable map visualization?

Data visualization allows users to summarize, analyze and reason about data. Guaranteeing detailed and accurate geospatial map visualization (e.g., at multiple zoom levels) requires extremely high-resolution maps. Classic visualization solutions such as Google Maps, MapBox and ArcGIS suffer from limited computation resources and hence take a tremendous amount of time to generate maps for large-scale geospatial data. In big spatial data scenarios, these tools just crash or run forever.

SedonaViz encapsulates the main steps of map visualization process, e.g., pixelize, aggregate, and render, into a set of massively parallelized GeoViz operators and the user can assemble any customized styles.

## Visualize SpatialRDD
This tutorial mainly focuses on explaining SQL/DataFrame API. SedonaViz RDD example can be found in Please see [Viz example project](https://github.com/apache/sedona/tree/master/examples/viz)

## Set up dependencies
1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md)
2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), Sedona-core, Sedona-SQL, Sedona-Viz

## Initiate SparkSession

Use the following code to initiate your SparkSession at the beginning:
This will register SedonaViz Kryo serializer.

```scala
var sparkSession = SparkSession.builder()
.master("local[*]") // Delete this if run in cluster mode
.appName("Sedona Viz") // Change this to a proper name
// Enable Sedona custom Kryo serializer
.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
.getOrCreate()
```

## Register SedonaSQL and SedonaViz

Add the following line after your SparkSession declaration

```scala
SedonaSQLRegistrator.registerAll(sparkSession)
SedonaVizRegistrator.registerAll(sparkSession)
```

This will register all User Defined Tyeps, functions and optimizations in SedonaSQL and SedonaViz.

You can also register everything by passing `--conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions` to `spark-submit` or `spark-shell`.

## Create Spatial DataFrame

There is a DataFrame as follows:

```
+----------+---------+
|       _c0|      _c1|
+----------+---------+
|-88.331492|32.324142|
|-88.175933|32.360763|
|-88.388954|32.357073|
|-88.221102| 32.35078|
```

You first need to create a Geometry type column.

```sql
CREATE OR REPLACE TEMP VIEW pointtable AS
SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
FROM pointtable
```

As you know, Sedona provides many different methods to load various spatial data formats. Please read [Write an Spatial DataFrame application](../sql).

## Generate a single image

In most cases, you just want to see a single image out of your spatial dataset.

### Pixelize spatial objects

To put spatial objects on a map image, you first need to convert them to pixels.

First, compute the spatial boundary of this column.

```sql
CREATE OR REPLACE TEMP VIEW boundtable AS
SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable
```

Then use ST_Pixelize to convert them to pixels.

This example is for Sedona before v1.0.1. ST_Pixelize extends Generator so it can directly flatten the array without the **explode** function.

```sql
CREATE OR REPLACE TEMP VIEW pixels AS
SELECT pixel, shape FROM pointtable
LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel
```

This example is for Sedona on and after v1.0.1. ST_Pixelize returns an array of pixels. You need to use **explode** to flatten it.

```sql
CREATE OR REPLACE TEMP VIEW pixels AS
SELECT pixel, shape FROM pointtable
LATERAL VIEW explode(ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable))) AS pixel
```

This will give you a 256*256 resolution image after you run ST_Render at the end of this tutorial.

!!!warning
	We highly suggest that you should use ST_Transform to transform coordinates to a visualization-specific coordinate system such as epsg:3857. Otherwise you map may look distorted.
	
### Aggregate pixels

Many objects may be pixelized to the same pixel locations. You now need to aggregate them based on either their spatial aggregation or spatial observations such as temperature or humidity.

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, count(*) as weight
FROM pixels
GROUP BY pixel
```

The weight indicates the degree of spatial aggregation or spatial observations. Later on, it will determine the color of this pixel.

### Colorize pixels

Run the following command to assign colors for pixels based on their weights.

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)) as color
FROM pixelaggregates
```

Please read [ST_Colorize](../../api/viz/sql/#st_colorize) for a detailed API description.

### Render the image

Use ST_Render to plot all pixels on a single image.

```sql
CREATE OR REPLACE TEMP VIEW images AS
SELECT ST_Render(pixel, color) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary
FROM pixelaggregates
```

This DataFrame will contain a Image type column which has only one image.


### Store the image on disk

Fetch the image from the previous DataFrame

```
var image = sparkSession.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
```

Use Sedona Viz ImageGenerator to store this image on disk.

```scala
var imageGenerator = new ImageGenerator
imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)
```

## Generate map tiles

If you are a map professional, you may need to generate map tiles for different zoom levels and eventually create the map tile layer.


### Pixelization and pixel aggregation

Please first do pixelization and pixel aggregation using the same commands in single image generation. In ST_Pixelize, you need specify a very high resolution, such as 1000*1000. Note that, each dimension should be divisible by 2^zoom-level 

### Create tile name

Run the following command to compute the tile name for every pixels

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, weight, ST_TileName(pixel, 3) AS pid
FROM pixelaggregates
```

"3" is the zoom level for these map tiles.

### Colorize pixels

Use the same command explained in single image generation to assign colors.

### Render map tiles

You now need to group pixels by tiles and then render map tile images in parallel.

```sql
CREATE OR REPLACE TEMP VIEW images AS
SELECT ST_Render(pixel, color, 3) AS image
FROM pixelaggregates
GROUP BY pid
```

"3" is the zoom level for these map tiles.

### Store map tiles on disk

You can use the same commands in single image generation to fetch all map tiles and store them one by one.
