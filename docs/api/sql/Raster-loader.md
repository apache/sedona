!!!note
	Sedona loader are available in Scala, Java and Python and have the same APIs.

Sedona provides two types of raster DataFrame loaders. They both use Sedona built-in data source but load raster images to different internal formats.

## Load any raster to RasterUDT format

The raster loader of Sedona leverages Spark built-in binary data source and works with several RS RasterUDT constructors to produce RasterUDT type. Each raster is a row in the resulting DataFrame and stored in a `RasterUDT` format.

### Load raster to a binary DataFrame

You can load any type of raster data using the code below. Then use the RS constructors below to create RasterUDT.

```scala
spark.read.format("binaryFile").load("/some/path/*.asc")
```


### RS_FromArcInfoAsciiGrid

Introduction: Returns a raster geometry from an Arc Info Ascii Grid file.

Format: `RS_FromArcInfoAsciiGrid(asc: Array[Byte])`

Since: `v1.4.0`

Spark SQL example:

```scala
var df = spark.read.format("binaryFile").load("/some/path/*.asc")
df = df.withColumn("raster", f.expr("RS_FromArcInfoAsciiGrid(content)"))
```


### RS_FromGeoTiff

Introduction: Returns a raster geometry from a GeoTiff file.

Format: `RS_FromGeoTiff(asc: Array[Byte])`

Since: `v1.4.0`

Spark SQL example:

```scala
var df = spark.read.format("binaryFile").load("/some/path/*.tiff")
df = df.withColumn("raster", f.expr("RS_FromGeoTiff(content)"))
```

## Load GeoTiff to Array[Double] format

The `geotiff` loader of Sedona is a Spark built-in data source. It can read a single geotiff image or a number of geotiff images into a DataFrame. Each geotiff is a row in the resulting DataFrame and stored in an array of Double type format.

Since: `v1.1.0`

Spark SQL example:

The input path could be a path to a single GeoTiff image or a directory of GeoTiff images.
 You can optionally append an option to drop invalid images. The geometry bound of each image is automatically loaded
as a Sedona geometry and is transformed to WGS84 (EPSG:4326) reference system.

```scala
var geotiffDF = sparkSession.read.format("geotiff").option("dropInvalid", true).load("YOUR_PATH")
geotiffDF.printSchema()
```

Output:

```html
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- Geometry: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nBands: integer (nullable = true)
 |    |-- data: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
```

There are three more optional parameters for reading GeoTiff:

```html
 |-- readfromCRS: Coordinate reference system of the geometry coordinates representing the location of the Geotiff. An example value of readfromCRS is EPSG:4326.
 |-- readToCRS: If you want to transform the Geotiff location geometry coordinates to a different coordinate reference system, you can define the target coordinate reference system with this option.
 |-- disableErrorInCRS: (Default value false) => Indicates whether to ignore errors in CRS transformation.
```

An example with all GeoTiff read options:

```scala
var geotiffDF = sparkSession.read.format("geotiff").option("dropInvalid", true).option("readFromCRS", "EPSG:4499").option("readToCRS", "EPSG:4326").option("disableErrorInCRS", true).load("YOUR_PATH")
geotiffDF.printSchema()
```

Output:

```html
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- Geometry: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nBands: integer (nullable = true)
 |    |-- data: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
```

You can also select sub-attributes individually to construct a new DataFrame

```scala
geotiffDF = geotiffDF.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
geotiffDF.createOrReplaceTempView("GeotiffDataframe")
geotiffDF.show()
```

Output:

```html
+--------------------+--------------------+------+-----+--------------------+-----+
|              origin|                Geom|height|width|                data|bands|
+--------------------+--------------------+------+-----+--------------------+-----+
|file:///home/hp/D...|POLYGON ((-58.699...|    32|   32|[1058.0, 1039.0, ...|    4|
|file:///home/hp/D...|POLYGON ((-58.297...|    32|   32|[1258.0, 1298.0, ...|    4|
+--------------------+--------------------+------+-----+--------------------+-----+
```

### RS_Array

Introduction: Create an array that is filled by the given value

Format: `RS_Array(length:Int, value: Decimal)`

Since: `v1.1.0`

Spark SQL example:

```scala
SELECT RS_Array(height * width, 0.0)
```

### RS_GetBand

Introduction: Return a particular band from Geotiff Dataframe

The number of total bands can be obtained from the GeoTiff loader

Format: `RS_GetBand (allBandValues: Array[Double], targetBand:Int, totalBands:Int)`

Since: `v1.1.0`

!!!note
	Index of targetBand starts from 1 (instead of 0). Index of the first band is 1.

Spark SQL example:

```scala
val BandDF = spark.sql("select RS_GetBand(data, 2, Band) as targetBand from GeotiffDataframe")
BandDF.show()
```

Output:

```html
+--------------------+
|          targetBand|
+--------------------+
|[1058.0, 1039.0, ...|
|[1258.0, 1298.0, ...|
+--------------------+
```
