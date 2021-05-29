## Geotiff Dataframe Loader

Introduction: The GeoTiff loader of Sedona is a Spark built-in data source. It can read a single geotiff image or 
a number of geotiff images into a DataFrame.

Since: `v1.1.0`

Spark SQL example:

The input path could be a path to a single GeoTiff image or a directory of GeoTiff images.
 You can optionally append an option to drop invalid images. The geometry bound of each image is automatically loaded
as a Sedona geometry and is transformed to WGS84 (EPSG:4326) reference system.

```SQL 
var geotiffDF = sparkSession.read.format("geotiff").option("dropInvalid", true).load("YOUR_PATH")
geotiffDF.printSchema()
```

Output

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

```SQL
geotiffDF = geotiffDF.selectExpr("image.origin as origin","image.Geometry as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
geotiffDF.createOrReplaceTempView("GeotiffDataframe")
geotiffDF.show()
```

Output

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
```SQL

val BandDF = spark.sql("select RS_GetBand(data, 2, Band) as targetBand from GeotiffDataframe")
BandDF.show()

+--------------------+
|          targetBand|
+--------------------+
|[1058.0, 1039.0, ...|
|[1258.0, 1298.0, ...|
+--------------------+
```

## RS_Base64

Introduction: Return a Base64 String from a geotiff image

Format: `RS_Base64 (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```SQL

val BandDF = spark.sql("select RS_Base64(band) as baseString from dataframe")
BandDF.show()

+--------------------+
|          baseString|
+--------------------+
|QJCIAAAAAABAkDwAA...|
|QJOoAAAAAABAlEgAA...|
+--------------------+

```
