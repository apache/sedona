!!!note
	Sedona writers are available in Scala, Java and Python and have the same APIs.
	
## Write RasterUDT to raster files

Introduction: You can write a Sedona Raster DataFrame to any raster formats using Sedona's built-in `raster` data source. With this, you can even read GeoTiff rasters and write them to ArcGrid rasters. Note that: `raster` data source does not support reading rasters. Please use Spark built-in `binaryFile` and Sedona RS constructors together to read rasters.

Since: `v1.4.1`

Available options:

* rasterType
	* mandatory
	* Allowed values: `geotiff`, `arcgrid`
* pathField
	* optional. If you use this option, then the column specified in this option must exist in the DataFrame schema. If this option is not used, each produced raster image will have a random UUID file name.
	* Allowed values: any column name that indicates the paths of each raster file

The schema of the Raster dataframe to be written can be one of the following two schemas:

```html
root
 |-- rs_fromgeotiff(content): raster (nullable = true)
```

or

```html
root
 |-- rs_fromgeotiff(content): raster (nullable = true)
 |-- path: string (nullable = true)
```

Spark SQL example 1:

```scala
sparkSession.write.format("raster").option("rasterType", "geotiff").mode(SaveMode.Overwrite).save("my_raster_file")
```

Spark SQL example 2:

```scala
sparkSession.write.format("raster").option("rasterType", "geotiff").option("pathField", "path").mode(SaveMode.Overwrite).save("my_raster_file")
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
