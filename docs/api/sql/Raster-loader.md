### Geotiff Dataframe Loader

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
 |    |-- Geometry: geometry (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nBands: integer (nullable = true)
 |    |-- data: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
```

You can also select sub-attributes individually to construct a new DataFrame

```Scala
geotiffDF = geotiffDF.selectExpr("image.origin as origin","ST_GeomFromWkt(image.wkt) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
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

## RS_GetBand

Introduction: Return a particular band from Geotiff Dataframe

The number of total bands can be obtained from the GeoTiff loader

Format: `RS_GetBand (allBandValues: Array[Double], targetBand:Int, totalBands:Int)`

Since: `v1.1.0`

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

