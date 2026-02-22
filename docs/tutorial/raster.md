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

!!!note
    Sedona uses 1-based indexing for all raster functions except [map algebra function](../api/sql/Raster-map-algebra.md), which uses 0-based indexing.

!!!note
    Sedona assumes geographic coordinates to be in longitude/latitude order. If your data is lat/lon order, please use `ST_FlipCoordinates` to swap X and Y.

Starting from `v1.1.0`, Sedona SQL supports raster data sources and raster operators in DataFrame and SQL. Raster support is available in all Sedona language bindings including ==Scala, Java, Python, and R==.

This page outlines the steps to manage raster data using SedonaSQL.

=== "Scala"

	```scala
	var myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("rasterDf")
	```

=== "Java"

	```java
	Dataset<Row> myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("rasterDf")
	```

=== "Python"

	```python
	myDataFrame = sedona.sql("YOUR_SQL")
	myDataFrame.createOrReplaceTempView("rasterDf")
	```

Detailed SedonaSQL APIs are available here: [SedonaSQL API](../api/sql/Overview.md). You can find example raster data in [Sedona GitHub repo](https://github.com/apache/sedona/blob/0eae42576c2588fe278f75cef3b17fee600eac90/spark/common/src/test/resources/raster/raster_with_no_data/test5.tiff).

## Set up dependencies

=== "Scala/Java"

	1. Read [Sedona Maven Central coordinates](../setup/maven-coordinates.md) and add Sedona dependencies in build.sbt or pom.xml.
	2. Add [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core), [Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql) in build.sbt or pom.xml.
	3. Please see [SQL example project](demo.md)

=== "Python"

	1. Please read [Quick start](../setup/install-python.md) to install Sedona Python.
	2. This tutorial is based on [Sedona SQL Jupyter Notebook example](jupyter-notebook.md).

## Create Sedona config

Use the following code to create your Sedona config at the beginning. If you already have a SparkSession (usually named `spark`) created by Wherobots/AWS EMR/Databricks, please skip this step and use `spark` directly.

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
	           'org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},'
	           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
	    getOrCreate()
	```
    Please replace the `3.3` in the package name of sedona-spark-shaded with the corresponding major.minor version of Spark, such as `sedona-spark-shaded-3.4_2.12:{{ sedona.current_version }}`.

## Initiate SedonaContext

Add the following line after creating the Sedona config. If you already have a SparkSession (usually named `spark`) created by Wherobots/AWS EMR/Databricks, please call `SedonaContext.create(spark)` instead.

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

You can also register everything by passing `--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions` to `spark-submit` or `spark-shell`.

## Load data from files

Assume we have a single raster data file called rasterData.tiff, [at Path](https://github.com/apache/sedona/blob/0eae42576c2588fe278f75cef3b17fee600eac90/spark/common/src/test/resources/raster/raster_with_no_data/test5.tiff).

Use the following code to load the data and create a raw Dataframe.

=== "Scala"
    ```scala
    var rawDf = sedona.read.format("binaryFile").load(path_to_raster_data)
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

=== "Java"
    ```java
    Dataset<Row> rawDf = sedona.read.format("binaryFile").load(path_to_raster_data)
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

=== "Python"
    ```python
    rawDf = sedona.read.format("binaryFile").load(path_to_raster_data)
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

The output will look like this:

```
|                path|    modificationTime|length|             content|
+--------------------+--------------------+------+--------------------+
|file:/Download/ra...|2023-09-06 16:24:...|174803|[49 49 2A 00 08 0...|
```

For multiple raster data files use the following code to load the data [from path](https://github.com/apache/sedona/blob/0eae42576c2588fe278f75cef3b17fee600eac90/spark/common/src/test/resources/raster/) and create raw DataFrame.

!!!note
    The above code works too for loading multiple raster data files. If the raster files are in separate directories and the option also makes sure that only `.tif` or `.tiff` files are being loaded.

=== "Scala"
    ```scala
    var rawDf = sedona.read.format("binaryFile").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.tif*").load(path_to_raster_data_folder)
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

=== "Java"
    ```java
    Dataset<Row> rawDf = sedona.read.format("binaryFile").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.tif*").load(path_to_raster_data_folder);
    rawDf.createOrReplaceTempView("rawdf");
    rawDf.show();
    ```

=== "Python"
    ```python
    rawDf = (
        sedona.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.tif*")
        .load(path_to_raster_data_folder)
    )
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

The output will look like this:

```
|                path|    modificationTime|length|             content|
+--------------------+--------------------+------+--------------------+
|file:/Download/ra...|2023-09-06 16:24:...|209199|[4D 4D 00 2A 00 0...|
|file:/Download/ra...|2023-09-06 16:24:...|174803|[49 49 2A 00 08 0...|
|file:/Download/ra...|2023-09-06 16:24:...|174803|[49 49 2A 00 08 0...|
|file:/Download/ra...|2023-09-06 16:24:...|  6619|[49 49 2A 00 08 0...|
```

The content column in the raster table is still in the raw form, binary form.

## Create a Raster type column

All raster operations in SedonaSQL require Raster type objects. Therefore, this should be the next step after loading the data.

### From Geotiff

```sql
SELECT RS_FromGeoTiff(content) AS rast, modificationTime, length, path FROM rawdf
```

To verify this, use the following code to print the schema of the DataFrame:

```sql
rasterDf.printSchema()
```

The output will be like this:

```
root
 |-- rast: raster (nullable = true)
 |-- modificationTime: timestamp (nullable = true)
 |-- length: long (nullable = true)
 |-- path: string (nullable = true)
```

### From Arc Grid

The raster data is loaded the same way as `tiff` file, but the raster data is stored with the extension `.asc`, ASCII format. The following code creates a Raster type objects from binary data:

```sql
SELECT RS_FromArcInfoAsciiGrid(content) AS rast, modificationTime, length, path FROM rawdf
```

## Raster's metadata

Sedona has a function to get the metadata for the raster, and also a function to get the world file of the raster.

### Metadata

This function will return an array of metadata, it will have all the necessary information about the raster, Please refer to [RS_MetaData](../api/sql/Raster-operators.md#rs_metadata).

```sql
SELECT RS_MetaData(rast) FROM rasterDf
```

Output for the following function will be:

```
[-1.3095817809482181E7, 4021262.7487925636, 512.0, 517.0, 72.32861272132695, -72.32861272132695, 0.0, 0.0, 3857.0, 1.0]
```

The first two elements of the array represent the real-world geographic coordinates (like longitude/latitude) of the raster image's top left pixel, while the next two elements represent the pixel dimensions of the raster.

### World File

There are two kinds of georeferences, GDAL and ESRI seen in [world files](https://en.wikipedia.org/wiki/World_file). For more information please refer to [RS_GeoReference](../api/sql/Raster-operators.md#rs_georeference).

```sql
SELECT RS_GeoReference(rast, "ESRI") FROM rasterDf
```

The Output will be as follows:

```
72.328613
0.000000
0.000000
-72.328613
-13095781.645176
4021226.584486
```

World files are used to georeference and geolocate images by establishing an image-to-world coordinate transformation that assigns real-world geographic coordinates to the pixels of the image.

## Raster Manipulation

Since `v1.5.0` there have been many additions to manipulate raster data, we will show you a few example queries.

!!!note
    Read [SedonaSQL Raster operators](../api/sql/Raster-operators.md) to learn how you can use Sedona for raster manipulation.

### Coordinate translation

Sedona allows you to translate coordinates as per your needs. It can translate pixel locations to world coordinates and vice versa.

#### PixelAsPoint

Use [RS_PixelAsPoint](../api/sql/Raster-operators.md#rs_pixelaspoint) to translate pixel coordinates to world location.

```sql
SELECT RS_PixelAsPoint(rast, 450, 400) FROM rasterDf
```

Output:

```
POINT (-13063342 3992403.75)
```

#### World to Raster Coordinate

Use [RS_WorldToRasterCoord](../api/sql/Raster-operators.md#rs_worldtorastercoord) to translate world location to pixel coordinates. To just get X coordinate use [RS_WorldToRasterCoordX](../api/sql/Raster-operators.md#rs_worldtorastercoordx) and for just Y coordinate use [RS_WorldToRasterCoordY](../api/sql/Raster-operators.md#rs_worldtorastercoordy).

```sql
SELECT RS_WorldToRasterCoord(rast, -1.3063342E7, 3992403.75)
```

Output:

```
POINT (450 400)
```

### Pixel Manipulation

Use [RS_Values](../api/sql/Raster-operators.md#rs_values) to fetch values for a specified array of Point Geometries. The coordinates in the point geometry are indicative of real-world location.

```sql
SELECT RS_Values(rast, Array(ST_Point(-13063342, 3992403.75), ST_Point(-13074192, 3996020)))
```

Output:

```
[132.0, 148.0]
```

To change values over a grid or area defined by geometry, we will use [RS_SetValues](../api/sql/Raster-operators.md#rs_setvalues).

```sql
SELECT RS_SetValues(
        rast, 1, 250, 260, 3, 3,
        Array(10, 12, 17, 26, 28, 37, 43, 64, 66)
    )
```

Follow the links to get more information on how to use the functions appropriately.

### Band Manipulation

Sedona provides APIs to select specific bands from a raster image and create a new raster. For example, to select 2 bands from a raster, you can use the [RS_Band](../api/sql/Raster-operators.md#rs_band) API to retrieve the desired multi-band raster.

Let's use a [multi-band raster](https://github.com/apache/sedona/blob/2a0b36989aa895c0781f9a10c907dd726506d0b7/spark/common/src/test/resources/raster_geotiff_color/FAA_UTM18N_NAD83.tif) for this example. The process of loading and converting it to raster type is the same.

```sql
SELECT RS_Band(colorRaster, Array(1, 2))
```

Let's say you have many single-banded rasters and want to add a band to the raster to perform [map algebra operations](#execute-map-algebra-operations). You can do so using [RS_AddBand](../api/sql/Raster-operators.md#rs_addband) Sedona function.

```sql
SELECT RS_AddBand(raster1, raster2, 1, 2)
```

This will result in `raster1` having `raster2`'s specified band.

### Resample raster data

Sedona allows you to resample raster data using different interpolation methods like the nearest neighbor, bilinear, and bicubic to change the cell size or align raster grids, using [RS_Resample](../api/sql/Raster-operators.md#rs_resample).

```sql
SELECT RS_Resample(rast, 50, -50, -13063342, 3992403.75, true, "bicubic")
```

For more information please follow the link.

## Execute map algebra operations

Map algebra is a way to perform raster calculations using mathematical expressions. The expression can be a simple arithmetic operation or a complex combination of multiple operations.

The Normalized Difference Vegetation Index (NDVI) is a simple graphical indicator that can be used to analyze remote sensing measurements from a space platform and assess whether the target being observed contains live green vegetation or not.

```
NDVI = (NIR - Red) / (NIR + Red)
```

where NIR is the near-infrared band and Red is the red band.

```sql
SELECT RS_MapAlgebra(raster, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') as ndvi FROM raster_table
```

For more information please refer to [Map Algebra API](../api/sql/Raster-map-algebra.md).

## Interoperability between raster and vector data

### Geometry As Raster

Sedona allows you to rasterize a geometry by using [RS_AsRaster](../api/sql/Raster-writer.md#rs_asraster).

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((150 150, 220 260, 190 300, 300 220, 150 150))'),
        RS_MakeEmptyRaster(1, 'b', 4, 6, 1, -1, 1),
        'b', 230
    )
```

The image created is as below for the vector:

![Rasterized vector](../image/rasterized-image.png)

!!!note
    The vector coordinates are buffed up to showcase the output, the real use case, may or may not match the example.

### Spatial range query

Sedona provides raster predicates to do a range query using a geometry window, for example, let's use [RS_Intersects](../api/sql/Raster-operators.md#rs_intersects).

```sql
SELECT rast FROM rasterDf WHERE RS_Intersect(rast, ST_GeomFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))
```

### Spatial join query

Sedona's raster predicates also can do a spatial join using the raster column and geometry column, using the same function as above.

```sql
SELECT r.rast, g.geom FROM rasterDf r, geomDf g WHERE RS_Interest(r.rast, g.geom)
```

!!!note
    These range and join queries will filter rasters using the provided geometric boundary and the spatial boundary of the raster.

    Sedona offers more raster predicates to do spatial range queries and spatial join queries. Please refer to [raster predicates docs](../api/sql/Raster-operators.md#raster-predicates).

## Visualize raster images

Sedona provides APIs to visualize raster data in an image form.

### Base64 String

The [RS_AsBase64](../api/sql/Raster-visualizer.md#rs_asbase64) encodes the raster data as a Base64 string and can be visualized using [online decoder](https://base64-viewer.onrender.com/).

```sql
SELECT RS_AsBase64(rast) FROM rasterDf
```

### HTML Image

The [RS_AsImage](../api/sql/Raster-visualizer.md#rs_asimage) returns an HTML image tag, that can be visualized using an HTML viewer or in Jupyter Notebook. For more information please click on the link.

```sql
SELECT RS_AsImage(rast, 500) FROM rasterDf
```

The output looks like this:

![Output](../image/DisplayImage.png)

!!!tip
    In a Jupyter notebook, use `SedonaUtils.display_image` to render rasters directly — no need to call RS_AsImage manually:

    ```python
    from sedona.spark import SedonaUtils

    SedonaUtils.display_image(rasterDf)
    ```

    See [Display raster in Jupyter](../api/sql/Raster-visualizer.md#display-raster-in-jupyter) for details.

### 2-D Matrix

Sedona offers an API to visualize raster data that is not sufficient for the other APIs mentioned above.

```sql
SELECT RS_AsMatrix(rast) FROM rasterDf
```

Output will be as follows:

```sql
| 1   3   4   0|
| 2   9  10  11|
| 3   4   5   6|
```

Please refer to [Raster visualizer docs](../api/sql/Raster-visualizer.md) to learn how to make the most of the visualizing APIs.

## Save to permanent storage

Sedona has APIs that can save an entire raster column to files in a specified location. Before saving, the raster type column needs to be converted to a binary format. Sedona provides several functions to convert a raster column into a binary column suitable for file storage. Once in binary format, the raster data can then be written to files on disk using the Sedona file storage APIs.

```sparksql
rasterDf.write.format("raster").option("rasterField", "raster").option("fileExtension", ".tiff").mode(SaveMode.Overwrite).save(dirPath)
```

Sedona has a few writer functions that create the binary DataFrame necessary for saving the raster images.

### As Arc Grid

Use [RS_AsArcGrid](../api/sql/Raster-writer.md#rs_asarcgrid) to get the binary Dataframe of the raster in Arc Grid format.

```sql
SELECT RS_AsArcGrid(raster)
```

### As GeoTiff

Use [RS_AsGeoTiff](../api/sql/Raster-writer.md#rs_asgeotiff) to get the binary Dataframe of the raster in GeoTiff format.

```sql
SELECT RS_AsGeoTiff(raster)
```

### As Cloud Optimized GeoTiff

Use [RS_AsCOG](../api/sql/Raster-writer.md#rs_ascog) to get the binary Dataframe of the raster in [Cloud Optimized GeoTiff](https://www.cogeo.org/) (COG) format. COG is ideal for cloud-hosted raster data because it supports efficient range-read access over HTTP.

```sql
SELECT RS_AsCOG(raster)
```

### As PNG

Use [RS_AsPNG](../api/sql/Raster-writer.md#rs_aspng) to get the binary Dataframe of the raster in PNG format.

```sql
SELECT RS_AsPNG(raster)
```

Please refer to [Raster writer docs](../api/sql/Raster-writer.md) for more details.

## Collecting raster Dataframes and working with them locally in Python

Sedona allows collecting Dataframes with raster columns and working with them locally in Python since `v1.6.0`.
The raster objects are represented as `SedonaRaster` objects in Python, which can be used to perform raster operations.

!!!tip
    If you just want to quickly visualize a raster in Jupyter, use `SedonaUtils.display_image(df)` instead of collecting the DataFrame:

    ```python
    from sedona.spark import SedonaUtils

    SedonaUtils.display_image(df_raster)
    ```

```python
df_raster = (
    sedona.read.format("binaryFile")
    .load("/path/to/raster.tif")
    .selectExpr("RS_FromGeoTiff(content) as rast")
)
rows = df_raster.collect()
raster = rows[0].rast
raster  # <sedona.raster.sedona_raster.InDbSedonaRaster at 0x1618fb1f0>
```

You can retrieve the metadata of the raster by accessing the properties of the `SedonaRaster` object.

```python
raster.width  # width of the raster
raster.height  # height of the raster
raster.affine_trans  # affine transformation matrix
raster.crs_wkt  # coordinate reference system as WKT
```

You can get a numpy array containing the band data of the raster using the `as_numpy` or `as_numpy_masked` method. The
band data is organized in CHW order.

```python
raster.as_numpy()  # numpy array of the raster
raster.as_numpy_masked()  # numpy array with nodata values masked as nan
```

If you want to work with the raster data using `rasterio`, you can retrieve a `rasterio.DatasetReader` object using the
`as_rasterio` method.

!!!note
    You need to have the `rasterio` package installed (version >= 1.2.10) to use this method. You can install it using `pip install rasterio`.

```python
ds = raster.as_rasterio()  # rasterio.DatasetReader object
# Work with the raster using rasterio
band1 = ds.read(1)  # read the first band
```

## Writing Python UDF to work with raster data

You can write Python UDFs to work with raster data in Python. The UDFs can take `SedonaRaster` objects as input and
return any Spark data type as output. This is an example of a Python UDF that calculates the mean of the raster data.

```python
from pyspark.sql.types import DoubleType


def mean_udf(raster):
    return float(raster.as_numpy().mean())


sedona.udf.register("mean_udf", mean_udf, DoubleType())
df_raster.withColumn("mean", expr("mean_udf(rast)")).show()
```

```
+--------------------+------------------+
|                rast|              mean|
+--------------------+------------------+
|GridCoverage2D["g...|1542.8092886117788|
+--------------------+------------------+
```

It is much trickier to write an UDF that returns a raster object, since Sedona does not support serializing Python raster
objects yet. However, you can write a UDF that returns the band data as an array and then construct the raster object using
`RS_MakeRaster`. This is an example of a Python UDF that creates a mask raster based on the first band of the input raster.

```python
from pyspark.sql.types import ArrayType, DoubleType
import numpy as np


def mask_udf(raster):
    band1 = raster.as_numpy()[0, :, :]
    mask = (band1 < 1400).astype(np.float64)
    return mask.flatten().tolist()


sedona.udf.register("mask_udf", band_udf, ArrayType(DoubleType()))
df_raster.withColumn("mask", expr("mask_udf(rast)")).withColumn(
    "mask_rast", expr("RS_MakeRaster(rast, 'I', mask)")
).show()
```

```
+--------------------+--------------------+--------------------+
|                rast|                mask|           mask_rast|
+--------------------+--------------------+--------------------+
|GridCoverage2D["g...|[0.0, 0.0, 0.0, 0...|GridCoverage2D["g...|
+--------------------+--------------------+--------------------+
```

## Performance optimization

When working with large raster datasets, refer to the [documentation on storing raster geometries in Parquet format](storing-blobs-in-parquet.md) for recommendations to optimize performance.
