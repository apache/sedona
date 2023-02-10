## Geotiff Dataframe Loader

Introduction: The GeoTiff loader of Sedona is a Spark built-in data source. It can read a single geotiff image or 
a number of geotiff images into a DataFrame.

Since: `v1.1.0`

Spark SQL example:

The input path could be a path to a single GeoTiff image or a directory of GeoTiff images.
 You can optionally append an option to drop invalid images. The geometry bound of each image is automatically loaded
as a Sedona geometry and is transformed to WGS84 (EPSG:4326) reference system.

```Scala
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

```Scala
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

```Scala
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

## Geotiff Dataframe Writer

Introduction: You can write a GeoTiff dataframe as GeoTiff images using the spark `write` feature with the format `geotiff`.

Since: `v1.2.1`

Spark SQL example:

The schema of the GeoTiff dataframe to be written can be one of the following two schemas:

```html
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- Geometry: geometry (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nBands: integer (nullable = true)
 |    |-- data: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
```

or

```html
 |-- origin: string (nullable = true)
 |-- Geometry: geometry (nullable = true)
 |-- height: integer (nullable = true)
 |-- width: integer (nullable = true)
 |-- nBands: integer (nullable = true)
 |-- data: array (nullable = true)
 |    |-- element: double (containsNull = true)
```

Field names can be renamed, but schema should exactly match with one of the above two schemas. The output path could be a path to a directory where GeoTiff images will be saved. If the directory already exists, `write` should be called in `overwrite` mode.

```Scala
var dfToWrite = sparkSession.read.format("geotiff").option("dropInvalid", true).option("readToCRS", "EPSG:4326").load("PATH_TO_INPUT_GEOTIFF_IMAGES")
dfToWrite.write.format("geotiff").save("DESTINATION_PATH")
```

You can override an existing path with the following approach:

```Scala
dfToWrite.write.mode("overwrite").format("geotiff").save("DESTINATION_PATH")
```

You can also extract the columns nested within `image` column and write the dataframe as GeoTiff image.

```Scala
dfToWrite = dfToWrite.selectExpr("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands")
dfToWrite.write.mode("overwrite").format("geotiff").save("DESTINATION_PATH")
```

If you want the saved GeoTiff images not to be distributed into multiple partitions, you can call coalesce to merge all files in a single partition.

```Scala
dfToWrite.coalesce(1).write.mode("overwrite").format("geotiff").save("DESTINATION_PATH")
```

In case, you rename the columns of GeoTiff dataframe, you can set the corresponding column names with the `option` parameter. All available optional parameters are listed below:

```html
 |-- writeToCRS: (Default value "EPSG:4326") => Coordinate reference system of the geometry coordinates representing the location of the Geotiff.
 |-- fieldImage: (Default value "image") => Indicates the image column of GeoTiff DataFrame.
 |-- fieldOrigin: (Default value "origin") => Indicates the origin column of GeoTiff DataFrame.
 |-- fieldNBands: (Default value "nBands") => Indicates the nBands column of GeoTiff DataFrame.
 |-- fieldWidth: (Default value "width") => Indicates the width column of GeoTiff DataFrame.
 |-- fieldHeight: (Default value "height") => Indicates the height column of GeoTiff DataFrame.
 |-- fieldGeometry: (Default value "geometry") => Indicates the geometry column of GeoTiff DataFrame.
 |-- fieldData: (Default value "data") => Indicates the data column of GeoTiff DataFrame.
```

An example:

```Scala
dfToWrite = sparkSession.read.format("geotiff").option("dropInvalid", true).option("readToCRS", "EPSG:4326").load("PATH_TO_INPUT_GEOTIFF_IMAGES")
dfToWrite = dfToWrite.selectExpr("image.origin as source","ST_GeomFromWkt(image.geometry) as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
dfToWrite.write.mode("overwrite").format("geotiff").option("writeToCRS", "EPSG:4326").option("fieldOrigin", "source").option("fieldGeometry", "geom").option("fieldNBands", "bands").save("DESTINATION_PATH")
```

## RS_Array

Introduction: Create an array that is filled by the given value

Format: `RS_Array(length:Int, value: Decimal)`

Since: `v1.1.0`

Spark SQL example:

```Scala
SELECT RS_Array(height * width, 0.0)
```

## RS_Base64

Introduction: Return a Base64 String from a geotiff image

Format: `RS_Base64 (height:Int, width:Int, redBand: Array[Double], greenBand: Array[Double], blackBand: Array[Double],
optional: alphaBand: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala
val BandDF = spark.sql("select RS_Base64(h, w, band1, band2, RS_Array(h*w, 0)) as baseString from dataframe")
BandDF.show()
```

Output:

```html
+--------------------+
|          baseString|
+--------------------+
|QJCIAAAAAABAkDwAA...|
|QJOoAAAAAABAlEgAA...|
+--------------------+
```

!!!note
    Although the 3 RGB bands are mandatory, you can use [RS_Array(h*w, 0.0)](#rs_array) to create an array (zeroed out, size = h * w) as input.

## RS_GetBand

Introduction: Return a particular band from Geotiff Dataframe

The number of total bands can be obtained from the GeoTiff loader

Format: `RS_GetBand (allBandValues: Array[Double], targetBand:Int, totalBands:Int)`

Since: `v1.1.0`

!!!note
	Index of targetBand starts from 1 (instead of 0). Index of the first band is 1.

Spark SQL example:

```Scala
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

## RS_HTML

Introduction: Return a html img tag with the base64 string embedded

Format: `RS_HTML(base64:String, optional: width_in_px:String)`

Spark SQL example:

```Scala
df.selectExpr("RS_HTML(encodedstring, '300') as htmlstring" ).show()
```

Output:

```html
+--------------------+
|          htmlstring|
+--------------------+
|<img src="data:im...|
|<img src="data:im...|
+--------------------+
```
