!!!note
	Sedona loader are available in Scala, Java and Python and have the same APIs.

The raster loader of Sedona leverages Spark built-in binary data source and works with several RS constructors to produce Raster type. Each raster is a row in the resulting DataFrame and stored in a `Raster` format.

By default, these functions uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

## Step 1: Load raster to a binary DataFrame

You can load any type of raster data using the code below. Then use the RS constructors below to create a Raster DataFrame.

```scala
sedona.read.format("binaryFile").load("/some/path/*.asc")
```

## Step 2: Create a raster type column

### RS_FromArcInfoAsciiGrid

Introduction: Returns a raster geometry from an Arc Info Ascii Grid file.

Format: `RS_FromArcInfoAsciiGrid(asc: ARRAY[Byte])`

Since: `v1.4.0`

SQL Example

```scala
var df = sedona.read.format("binaryFile").load("/some/path/*.asc")
df = df.withColumn("raster", f.expr("RS_FromArcInfoAsciiGrid(content)"))
```

### RS_FromGeoTiff

Introduction: Returns a raster geometry from a GeoTiff file.

Format: `RS_FromGeoTiff(asc: ARRAY[Byte])`

Since: `v1.4.0`

SQL Example

```scala
var df = sedona.read.format("binaryFile").load("/some/path/*.tiff")
df = df.withColumn("raster", f.expr("RS_FromGeoTiff(content)"))
```

### RS_MakeEmptyRaster

Introduction: Returns an empty raster geometry. Every band in the raster is initialized to `0.0`.

Since: `v1.5.0`

Format:

```
RS_MakeEmptyRaster(numBands: Integer, bandDataType: String = 'D', width: Integer, height: Integer, upperleftX: Double, upperleftY: Double, cellSize: Double)
```

* NumBands: The number of bands in the raster. If not specified, the raster will have a single band.
* BandDataType: Optional parameter specifying the data types of all the bands in the created raster.
Accepts one of:
    1. "D" - 64 bits Double
    2. "F" - 32 bits Float
    3. "I" - 32 bits signed Integer
    4. "S" - 16 bits signed Short
    5. "US" - 16 bits unsigned Short
    6. "B" - 8 bits unsigned Byte
* Width: The width of the raster in pixels.
* Height: The height of the raster in pixels.
* UpperleftX: The X coordinate of the upper left corner of the raster, in terms of the CRS units.
* UpperleftY: The Y coordinate of the upper left corner of the raster, in terms of the CRS units.
* Cell Size (pixel size): The size of the cells in the raster, in terms of the CRS units.

It uses the default Cartesian coordinate system.

Format:

```
RS_MakeEmptyRaster(numBands: Integer, bandDataType: String = 'D', width: Integer, height: Integer, upperleftX: Double, upperleftY: Double, scaleX: Double, scaleY: Double, skewX: Double, skewY: Double, srid: Integer)
```

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
* ScaleX: The scaling factor of the cells on the X axis
* ScaleY: The scaling factor of the cells on the Y axis
* SkewX: The skew of the raster on the X axis, effectively tilting them in the horizontal direction
* SkewY: The skew of the raster on the Y axis, effectively tilting them in the vertical direction
* SRID: The SRID of the raster. Use 0 if you want to use the default Cartesian coordinate system. Use 4326 if you want to use WGS84.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](Raster-affine-transformation.md) section.

!!!Note
    If any other value than the accepted values for the bandDataType is provided, RS_MakeEmptyRaster defaults to double as the data type for the raster.

Spark SQL example 1 (with 2 bands):

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

Spark SQL example 2 (with 2 bands and dataType):

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

Spark SQL example 3 (with 2 bands, scale, skew, and SRID):

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

Spark SQL example 4 (with 2 bands, scale, skew, and SRID):

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

### RS_MakeRaster

Introduction: Creates a raster from the given array of pixel values. The width, height, geo-reference information, and
the CRS will be taken from the given reference raster. The data type of the resulting raster will be DOUBLE and the
number of bands of the resulting raster will be `data.length / (refRaster.width * refRaster.height)`.

Since: `v1.6.0`

Format: `RS_MakeRaster(refRaster: Raster, bandDataType: String, data: ARRAY[Double])`

* refRaster: The reference raster from which the width, height, geo-reference information, and the CRS will be taken.
* bandDataType: The data type of the bands in the resulting raster. Please refer to the `RS_MakeEmptyRaster` function for the accepted values.
* data: The array of pixel values. The size of the array cannot be 0, and should be multiple of width * height of the reference raster.

SQL example:

```sql
WITH r AS (SELECT RS_MakeEmptyRaster(2, 3, 2, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326) AS rast)
SELECT RS_AsMatrix(RS_MakeRaster(rast, 'D', ARRAY(1, 2, 3, 4, 5, 6))) FROM r
```

Output:

```
+------------------------------------------------------------+
|rs_asmatrix(rs_makeraster(rast, D, array(1, 2, 3, 4, 5, 6)))|
+------------------------------------------------------------+
||1.0  2.0  3.0|\n|4.0  5.0  6.0|\n                          |
+------------------------------------------------------------+
```

### RS_FromNetCDF

Introduction: Returns a raster geometry representing the given record variable short name from a NetCDF file.
This API reads the array data of the record variable *in memory* along with all its dimensions
Since the netCDF format has many variants, the reader might not work for your test case, if that is so, please report this using the public forums.

This API has been tested for netCDF classic (NetCDF 1, 2, 5) and netCDF4/HDF5 files.

This API requires the name of the record variable. It is assumed that a variable of the given name exists, and its last 2 dimensions are 'lat' and 'lon' dimensions *respectively*.

If this assumption does not hold true for your case, you can choose to pass the lonDimensionName and latDimensionName explicitly.

You can use [RS_NetCDFInfo](#rs_netcdfinfo) to get the details of the passed netCDF file (variables and its dimensions).

Format 1: `RS_FromNetCDF(netCDF: ARRAY[Byte], recordVariableName: String)`

Format 2: `RS_FromNetCDF(netCDF: ARRAY[Byte], recordVariableName: String, lonDimensionName: String, latDimensionName: String)`

Since: `v1.5.1`

Spark Example:

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
df = df.withColumn("raster", f.expr("RS_FromNetCDF(content, 'O3')"))
```

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
df = df.withColumn("raster", f.expr("RS_FromNetCDF(content, 'O3', 'lon', 'lat')"))
```

### RS_NetCDFInfo

Introduction: Returns a string containing names of the variables in a given netCDF file along with its dimensions.

Format: `RS_NetCDFInfo(netCDF: ARRAY[Byte])`

Since: `1.5.1`

Spark Example:

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
recordInfo = df.selectExpr("RS_NetCDFInfo(content) as record_info").first().getString(0)
print(recordInfo)
```

Output:

```text
O3(time=2, z=2, lat=48, lon=80)

NO2(time=2, z=2, lat=48, lon=80)
```
