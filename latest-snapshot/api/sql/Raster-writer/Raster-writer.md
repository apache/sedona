!!!note
	Sedona writers are available in Scala, Java and Python and have the same APIs.

## Write Raster DataFrame to raster files

To write a Sedona Raster DataFrame to raster files, you need to (1) first convert the Raster DataFrame to a binary DataFrame using `RS_AsXXX` functions and (2) then write the binary DataFrame to raster files using Sedona's built-in `raster` data source.

### Write raster DataFrame to a binary DataFrame

You can use the following RS output functions (`RS_AsXXX`) to convert a Raster DataFrame to a binary DataFrame. Generally the output format of a raster can be different from the original input format. For example, you can use `RS_FromGeoTiff` to create rasters and save them using `RS_AsArcInfoAsciiGrid`.

#### RS_AsArcGrid

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is an ArcGrid image in binary format. ArcGrid only takes 1 source band. If your raster has multiple bands, you need to specify which band you want to use as the source.

Possible values for `sourceBand`: any non-negative value (>=0). If not given, it will use Band 0.

Format:

`RS_AsArcGrid(raster: Raster)`

`RS_AsArcGrid(raster: Raster, sourceBand: Integer)`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_AsArcGrid(raster) FROM my_raster_table
```

SQL Example

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

#### RS_AsGeoTiff

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is a GeoTiff image in binary format.

Possible values for `compressionType`: `None`, `PackBits`, `Deflate`, `Huffman`, `LZW` and `JPEG`

Possible values for `imageQuality`: any decimal number between 0 and 1. 0 means the lowest quality and 1 means the highest quality.

Format:

`RS_AsGeoTiff(raster: Raster)`

`RS_AsGeoTiff(raster: Raster, compressionType: String, imageQuality: Double)`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_AsGeoTiff(raster) FROM my_raster_table
```

SQL Example

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

#### RS_AsPNG

Introduction: Returns a PNG byte array, that can be written to raster files as PNGs using the [sedona function](#write-a-binary-dataframe-to-raster-files). This function can only accept pixel data type of unsigned integer. PNG can accept 1 or 3 bands of data from the raster, refer to [RS_Band](Raster-operators.md#rs_band) for more details.

!!!Note
	Raster having `UNSIGNED_8BITS` pixel data type will have range of `0 - 255`, whereas rasters having `UNSIGNED_16BITS` pixel data type will have range of `0 - 65535`. If provided pixel value is greater than either `255` for `UNSIGNED_8BITS` or `65535` for `UNSIGNED_16BITS`, then the extra bit will be truncated.

!!!Note
	Raster that have float or double values will result in an empty byte array. PNG only accepts Integer values, if you want to write your raster to an image file, please refer to [RS_AsGeoTiff](#rs_asgeotiff).

Format:

`RS_AsPNG(raster: Raster, maxWidth: Integer)`

`RS_AsPNG(raster: Raster)`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_AsPNG(raster) FROM Rasters
```

Output:

```
[-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73...]
```

SQL Example

```sql
SELECT RS_AsPNG(RS_Band(raster, Array(3, 1, 2)))
```

Output:

```
[-103, 78, 94, -26, 61, -16, -91, -103, -65, -116...]
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
	* No default value. If you use this option, then the column specified in this option must exist in the DataFrame schema. If this option is not used, each produced raster image will have a random UUID file name.
	* Allowed values: any column name that indicates the paths of each raster file
* useDirectCommitter (Since: `v1.6.1`)
	* Default value: `true`. If set to `true`, the output files will be written directly to the target location. If set to `false`, the output files will be written to a temporary location and finally be committed to their target location. It is usually slower to write large amount of raster files with `useDirectCommitter` set to `false`, especially when writing to object stores such as S3.
	* Allowed values: `true` or `false`

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

### Write Geometry to Raster dataframe

#### RS_AsRaster

Introduction: Converts a Geometry to a Raster dataset. Defaults to using `1.0` for cell `value` and `null` for `noDataValue` if not provided. Supports all geometry types.
The `pixelType` argument defines data type of the output raster. This can be one of the following, D (double), F (float), I (integer), S (short), US (unsigned short) or B (byte).
The `useGeometryExtent` argument defines the extent of the resultant raster. When set to `true`, it corresponds to the extent of `geom`, and when set to false, it corresponds to the extent of `raster`. Default value is `true` if not set.
Format:

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, value: Double, noDataValue: Double, useGeometryExtent: Boolean)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, value: Double, noDataValue: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, value: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String)
```

Since: `v1.5.0`

!!!note
	The function doesn't support rasters that have any one of the following properties:
	```
	ScaleX < 0
	ScaleY > 0
	SkewX != 0
	SkewY != 0
	```
	If a raster is provided with anyone of these properties then IllegalArgumentException is thrown.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](Raster-affine-transformation.md) section.

SQL Example

```sql
SELECT RS_AsRaster(
		ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
    	RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326),
    	'D', 255.0, 0d
	)
```

Output:

```
GridCoverage2D["g...
```

SQL Example

```sql
SELECT RS_AsRaster(
		ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
    	RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326),
    	'D'
	)
```

Output:

```
GridCoverage2D["g...
```

```sql
SELECT RS_AsRaster(
		ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
		RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 0),
       'D',255, 0d, false
)
```

Output:

```
GridCoverage2D["g...
```
