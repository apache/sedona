!!!note
	Sedona loader are available in Scala, Java and Python and have the same APIs.

Sedona provides two types of raster DataFrame loaders. They both use Sedona built-in data source but load raster images to different internal formats.

## Load any raster to RasterUDT format

The raster loader of Sedona leverages Spark built-in binary data source and works with several RS RasterUDT constructors to produce RasterUDT type. Each raster is a row in the resulting DataFrame and stored in a `RasterUDT` format.

By default, these functions uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

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

### RS_MakeEmptyRaster

Introduction: Returns an empty raster geometry. Every band in the raster is initialized to `0.0`.

Since: `v1.5.0`

Format: `RS_MakeEmptyRaster(numBands:Int, bandDataType:String = 'D', width: Int, height: Int, upperleftX: Double, upperleftY: Double, cellSize:Double)`

* NumBands: The number of bands in the raster. If not specified, the raster will have a single band.
* BandDataType: Optional parameter specifying the data types of all the bands in the created raster.
Accepts one of: 
    1. "D" - 64 bits Double
    2. "F" - 32 bits Float
    3. "I" - 32 bits signed Integer
    4. "S" - 16 bits signed Short
    5. "US" - 16 bits unsigned Short
    6. "B" - 8 bits Byte
* Width: The width of the raster in pixels.
* Height: The height of the raster in pixels.
* UpperleftX: The X coordinate of the upper left corner of the raster, in terms of the CRS units.
* UpperleftY: The Y coordinate of the upper left corner of the raster, in terms of the CRS units.
* Cell Size (pixel size): The size of the cells in the raster, in terms of the CRS units.

It uses the default Cartesian coordinate system.

Format: `RS_MakeEmptyRaster(numBands:Int, bandDataType = 'D', width: Int, height: Int, upperleftX: Double, upperleftY: Double, scaleX:Double, scaleY:Double, skewX:Double, skewY:Double, srid: Int)`

* NumBands: The number of bands in the raster. If not specified, the raster will have a single band.
* BandDataType: Optional parameter specifying the data types of all the bands in the created raster.
Accepts one of:
    1. "D" - 64 bits Double
    2. "F" - 32 bits Float
    3. "I" - 32 bits signed Integer
    4. "S" - 16 bits signed Short
    5. "US" - 16 bits unsigned Short
    6. "B" - 8 bits Byte
* Width: The width of the raster in pixels.
* Height: The height of the raster in pixels.
* UpperleftX: The X coordinate of the upper left corner of the raster, in terms of the CRS units.
* UpperleftY: The Y coordinate of the upper left corner of the raster, in terms of the CRS units.
* ScaleX (pixel size on X): The size of the cells on the X axis, in terms of the CRS units.
* ScaleY (pixel size on Y): The size of the cells on the Y axis, in terms of the CRS units.
* SkewX: The skew of the raster on the X axis, in terms of the CRS units.
* SkewY: The skew of the raster on the Y axis, in terms of the CRS units.
* SRID: The SRID of the raster. Use 0 if you want to use the default Cartesian coordinate system. Use 4326 if you want to use WGS84.


!!!Note
  If any other value than the accepted values for the bandDataType is provided, RS_MakeEmptyRaster defaults to double as the data type for the raster.

SQL example 1 (with 2 bands):

```sql
SELECT RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0)
```

Output:
```
+--------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0)|
+--------------------------------------------+
|                        GridCoverage2D["g...|
+--------------------------------------------+
```

SQL example 2 (with 2 bands and dataType):

```sql
SELECT RS_MakeEmptyRaster(2, 'I', 10, 10, 0.0, 0.0, 1.0) - Create a raster with integer datatype
```

Output:
```
+--------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0)|
+--------------------------------------------+
|                        GridCoverage2D["g...|
+--------------------------------------------+
```


SQL example 3 (with 2 bands, scale, skew, and SRID):

```sql
SELECT RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)
```

Output:
```
+------------------------------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)|
+------------------------------------------------------------------+
|                                              GridCoverage2D["g...|
+------------------------------------------------------------------+
```


SQL example 4 (with 2 bands, scale, skew, and SRID):

```sql
SELECT RS_MakeEmptyRaster(2, 'F', 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326) - Create a raster with float datatype
```

Output:
```
+------------------------------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)|
+------------------------------------------------------------------+
|                                              GridCoverage2D["g...|
+------------------------------------------------------------------+
```

## Load GeoTiff to Array[Double] format

!!!warning
	This function has been deprecated since v1.4.1. Please use `RS_FromGeoTiff` instead and `binaryFile` data source to read GeoTiff files.

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

Format: `RS_Array(length:Int, value: Double)`

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
