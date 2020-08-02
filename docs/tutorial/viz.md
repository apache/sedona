The page outlines the steps to visualize spatial data using GeoSparkViz. ==The example code is written in Scala but also works for Java==.

Starting from 1.2.0, GeoSparkViz provides the DataFrame support. This offers users a more flexible way to design beautiful map visualization effects including scatter plots and heat maps. In the meantime, GeoSparkViz RDD API remains the same.

!!!note
	All GeoSparkViz SQL/DataFrame APIs are explained in [GeoSparkViz API](../api/viz/sql).

## Why scalable map visualization?

Data visualization allows users to summarize, analyze and reason about data. Guaranteeing detailed and accurate geospatial map visualization (e.g., at multiple zoom levels) requires extremely high-resolution maps. Classic visualization solutions such as Google Maps, MapBox and ArcGIS suffer from limited computation resources and hence take a tremendous amount of time to generate maps for large-scale geospatial data. In big spatial data scenarios, these tools just crash or run forever.

GeoSparkViz encapsulates the main steps of map visualization process, e.g., pixelize, aggregate, and render, into a set of massively parallelized GeoViz operators and the user can assemble any customized styles.

## Visualize SpatialRDD
This tutorial mainly focuses on explaining SQL/DataFrame API. GeoSparkViz RDD example can be found in [GeoSpark template project](https://github.com/jiayuasu/GeoSparkTemplateProject/tree/master/geospark-viz).

## Set up dependencies
1. Read [GeoSpark Maven Central coordinates](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md)
2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql), [GeoSpark core](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md), [GeoSparkSQL](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md), [GeoSparkViz](../download/GeoSpark-All-Modules-Maven-Central-Coordinates.md)

## Initiate SparkSession

Use the following code to initiate your SparkSession at the beginning:
This will register GeoSparkVizKryo serializer.

```scala
var sparkSession = SparkSession.builder()
.master("local[*]") // Delete this if run in cluster mode
.appName("readTestScala") // Change this to a proper name
// Enable GeoSpark custom Kryo serializer
.config("spark.serializer", classOf[KryoSerializer].getName)
.config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
.getOrCreate()
```

## Register GeoSparkSQL and GeoSparkViz

Add the following line after your SparkSession declaration

```scala
GeoSparkSQLRegistrator.registerAll(sparkSession)
GeoSparkVizRegistrator.registerAll(sparkSession)
```

This will register all User Defined Tyeps, functions and optimizations in GeoSparkSQL and GeoSparkViz.

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

As you know, GeoSpark provides many different methods to load various spatial data formats. Please read [Write an Spatial DataFrame application](sql).

## Generate a single image

In most cases, you just want to see a single image out of your spatial dataset.

### Pixelize spatial objects

To put spatial objects on a map image, you first need to convert them to pixels.

First, compute the spatial boundary of this column.

```sql
CREATE OR REPLACE TEMP VIEW boundtable AS
SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable
```

Then use ST_Pixelize to conver them to pixels.

```sql
CREATE OR REPLACE TEMP VIEW pixels AS
SELECT pixel, shape FROM pointtable
LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel
```

This will give you a 256*256 resolution image after you run ST_Render at the end of this tutorial.

!!!warning
	We highly suggest that you should use ST_Transform to transfrom coordiantes to a visualization-specific coordinate sysmte such as epsg:3857. Otherwise you map may look distorted.
	
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

Please read [ST_Colorize](../api/viz/sql/#st_colorize) for a detailed API description.

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

Use GeoSparkViz ImageGenerator to store this image on disk.

```scala
var imageGenerator = new ImageGenerator
imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)
```

## Generate map tiles

If you are a map tile professional, you may need to generate map tiles for different zoom levels and eventually create the map tile layer.


### Pixelization and pixel aggregation

Please first do pixelization and pixel aggregation using the same commands in single image generation. In ST_Pixelize, you need specify a very high resolution.

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
SELECT ST_Render(pixel, color) AS image
FROM pixelaggregates
GROUP BY pid
```

### Store map tiles on disk

You can use the same commands in single image generation to fetch all map tiles and store them one by one.
