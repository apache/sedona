!!!note
	Sedona writers are available in Scala, Java and Python and have the same APIs.
	
## Write Raster DataFrame to raster files

To write a Sedona Raster DataFrame to raster files, you need to (1) first convert the Raster DataFrame to a binary DataFrame using `RS_AsXXX` functions and (2) then write the binary DataFrame to raster files using Sedona's built-in `raster` data source.

### Write raster DataFrame to a binary DataFrame

You can use the following RS output functions (`RS_AsXXX`) to convert a Raster DataFrame to a binary DataFrame. Generally the output format of a raster can be different from the original input format. For example, you can use `RS_FromGeoTiff` to create rasters and save them using `RS_AsArcInfoAsciiGrid`.

#### RS_AsGeoTiff

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is a GeoTiff image in binary format.

Since: `v1.4.1`

Format 1: `RS_AsGeoTiff(raster: Raster)`

Format 2: `RS_AsGeoTiff(raster: Raster, compressionType:String, imageQuality:Integer/Decimal)`

Possible values for `compressionType`: `None`, `PackBits`, `Deflate`, `Huffman`, `LZW` and `JPEG`

Possible values for `imageQuality`: any decimal number between 0 and 1. 0 means the lowest quality and 1 means the highest quality.

SQL example 1:

```sql
SELECT RS_AsGeoTiff(raster) FROM my_raster_table
```

SQL example 2:

```sql
SELECT RS_AsGeoTiff(raster, 'LZW', '0.75') FROM my_raster_table
```

Output:

```html
+--------------------+
|             geotiff|
+--------------------+
|[4D 4D 00 2A 00 0...|
+--------------------+
```

Output schema:

```sql
root
 |-- geotiff: binary (nullable = true)
```

#### RS_AsArcGrid

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is an ArcGrid image in binary format. ArcGrid only takes 1 source band. If your raster has multiple bands, you need to specify which band you want to use as the source.

Since: `v1.4.1`

Format 1: `RS_AsArcGrid(raster: Raster)`

Format 2: `RS_AsArcGrid(raster: Raster, sourceBand:Integer)`

Possible values for `sourceBand `: any non-negative value (>=0). If not given, it will use Band 0.

SQL example 1:

```sql
SELECT RS_AsArcGrid(raster) FROM my_raster_table
```

SQL example 2:

```sql
SELECT RS_AsArcGrid(raster, 1) FROM my_raster_table
```

Output:

```html
+--------------------+
|             arcgrid|
+--------------------+
|[4D 4D 00 2A 00 0...|
+--------------------+
```

Output schema:

```sql
root
 |-- arcgrid: binary (nullable = true)
```

### Write a binary DataFrame to raster files

Introduction: You can write a Sedona binary DataFrame to external storage using Sedona's built-in `raster` data source. Note that: `raster` data source does not support reading rasters. Please use Spark built-in `binaryFile` and Sedona RS constructors together to read rasters.

Since: `v1.4.1`

Available options:

* rasterField:
	* Default value: the `binary` type column in the DataFrame. If the input DataFrame has several binary columns, please specify which column you want to use.
	* Allowed values: the name of the to-be-saved binary type column
* fileExtension
	* Default value: `.tiff`
	* Allowed values: any string values such as `.png`, `.jpeg`, `.asc`
* pathField
	* No defaulut value. If you use this option, then the column specified in this option must exist in the DataFrame schema. If this option is not used, each produced raster image will have a random UUID file name.
	* Allowed values: any column name that indicates the paths of each raster file

The schema of the Raster dataframe to be written can be one of the following two schemas:

```html
root
 |-- rs_asgeotiff(raster): binary (nullable = true)
```

or

```html
root
 |-- rs_asgeotiff(raster): binary (nullable = true)
 |-- path: string (nullable = true)
```

Spark SQL example 1:

```scala
sparkSession.write.format("raster").mode(SaveMode.Overwrite).save("my_raster_file")
```

Spark SQL example 2:

```scala
sparkSession.write.format("raster").option("rasterField", "raster").option("pathField", "path").option("fileExtension", ".tiff").mode(SaveMode.Overwrite).save("my_raster_file")
```

The produced file structure will look like this:

```html
my_raster_file
- part-00000-6c7af016-c371-4564-886d-1690f3b27ca8-c000
	- test1.tiff
	- .test1.tiff.crc
- part-00001-6c7af016-c371-4564-886d-1690f3b27ca8-c000
	- test2.tiff
	- .test2.tiff.crc
- part-00002-6c7af016-c371-4564-886d-1690f3b27ca8-c000
	- test3.tiff
	- .test3.tiff.crc
- _SUCCESS
```

To read it back to Sedona Raster DataFrame, you can use the following command (note the `*` in the path):

```scala
sparkSession.read.format("binaryFile").load("my_raster_file/*")
```

Then you can create Raster type in Sedona like this `RS_FromGeoTiff(content)` (if the written data was in GeoTiff format).

The newly created DataFrame can be written to disk again but must be under a different name such as `my_raster_file_modified`


## Write Array[Double] to GeoTiff files

Introduction: You can write a GeoTiff dataframe as GeoTiff images using the spark `write` feature with the format `geotiff`. The geotiff raster column needs to be an array of double type data.

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

```scala
var dfToWrite = sparkSession.read.format("geotiff").option("dropInvalid", true).option("readToCRS", "EPSG:4326").load("PATH_TO_INPUT_GEOTIFF_IMAGES")
dfToWrite.write.format("geotiff").save("DESTINATION_PATH")
```

You can override an existing path with the following approach:

```scala
dfToWrite.write.mode("overwrite").format("geotiff").save("DESTINATION_PATH")
```

You can also extract the columns nested within `image` column and write the dataframe as GeoTiff image.

```scala
dfToWrite = dfToWrite.selectExpr("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands")
dfToWrite.write.mode("overwrite").format("geotiff").save("DESTINATION_PATH")
```

If you want the saved GeoTiff images not to be distributed into multiple partitions, you can call coalesce to merge all files in a single partition.

```scala
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

```scala
dfToWrite = sparkSession.read.format("geotiff").option("dropInvalid", true).option("readToCRS", "EPSG:4326").load("PATH_TO_INPUT_GEOTIFF_IMAGES")
dfToWrite = dfToWrite.selectExpr("image.origin as source","ST_GeomFromWkt(image.geometry) as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
dfToWrite.write.mode("overwrite").format("geotiff").option("writeToCRS", "EPSG:4326").option("fieldOrigin", "source").option("fieldGeometry", "geom").option("fieldNBands", "bands").save("DESTINATION_PATH")
```

## Write Array[Double] to other formats

### RS_Base64

Introduction: Return a Base64 String from a geotiff image

Format: `RS_Base64 (height:Int, width:Int, redBand: Array[Double], greenBand: Array[Double], blackBand: Array[Double],
optional: alphaBand: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala
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


### RS_HTML

Introduction: Return a html img tag with the base64 string embedded

Format: `RS_HTML(base64:String, optional: width_in_px:String)`

Spark SQL example:

```scala
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
