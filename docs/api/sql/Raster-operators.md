## Pixel Functions

### RS_PixelAsPoint

Introduction: Returns a point geometry of the specified pixel's upper-left corner. The pixel coordinates specified are 1-indexed.

!!!Note
    If the pixel coordinates specified do not exist in the raster (out of bounds), RS_PixelAsPoint throws an IndexOutOfBoundsException.


Format: `RS_PixelAsPoint(raster: Raster, colX: int, rowY: int)`

Since: `1.5.0`

Spark SQL examples:

```sql
SELECT ST_AsText(RS_PixelAsPoint(raster, 2, 1)) from rasters
```

Output: 
```
POINT (123.19, -12)
```

```sql
SELECT ST_AsText(RS_PixelAsPoint(raster, 6, 2)) from rasters
```

Output:
```
IndexOutOfBoundsException: Specified pixel coordinates (6, 2) do not lie in the raster
```

### RS_PixelAsPolygon

Introduction: Returns a polygon geometry that bounds the specified pixel.
The pixel coordinates specified are 1-indexed. 
If `colX` and `rowY` are out of bounds for the raster, they are interpolated assuming the same skew and translate values.

Format: `RS_PixelAsPolygon(raster: Raster, colX:int, rowY:int)`

Since: `v1.5.0`

Spark SQL Example:

```sql
SELECT ST_AsText(RS_PixelAsPolygon(RS_MakeEmptyRaster(1, 5, 10, 123, -230, 8), 2, 3)) FROM rasters
```

Output:

```
POLYGON ((131 -246, 139 -246, 139 -254, 131 -254, 131 -246))
```

## Geometry Functions

### RS_Envelope

Introduction: Returns the envelope of the raster as a Geometry.

Format: `RS_Envelope (raster: Raster)`

Since: `v1.4.0`

Spark SQL example:
```sql
SELECT RS_Envelope(raster) FROM raster_table
```
Output:
```
POLYGON ((0 0,20 0,20 60,0 60,0 0))
```

### RS_ConvexHull

Introduction: Return the convex hull geometry of the raster including the NoDataBandValue band pixels. 
For regular shaped and non-skewed rasters, this gives more or less the same result as RS_ConvexHull and hence is only useful for irregularly shaped or skewed rasters.

Format: `RS_ConvexHull(raster: Raster)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_ConvexHull(RS_MakeEmptyRaster(1, 5, 10, 156, -132, 5, 10, 3, 5, 0));
```

Output:
```
POLYGON ((156 -132, 181 -107, 211 -7, 186 -32, 156 -132))
```

### RS_MinConvexHull

Introduction: Returns the min convex hull geometry of the raster **excluding** the NoDataBandValue band pixels, in the given band.
If no band is specified, all the bands are considered when creating the min convex hull of the raster. 
The created geometry representing the min convex hull has world coordinates of the raster in its CRS as the corner coordinates. 

!!!Note
    If the specified band does not exist in the raster, RS_MinConvexHull throws an IllegalArgumentException

Format: `RS_MinConvexHull(raster: Raster) | RS_MinConvexHull(raster: Raster, band: Int)`

Since: `1.5.0`

Spark SQL example:


```scala
val inputDf = Seq((Seq(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
        Seq(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0))).toDF("values2", "values1")
inputDf.selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
        "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0))) as minConvexHullAll").show()
```

Output:
```sql
+----------------------------------------+
|minConvexHullAll                        |
+----------------------------------------+
|POLYGON ((0 -1, 4 -1, 4 -5, 0 -5, 0 -1))|
+----------------------------------------+
```

```scala
val inputDf = Seq((Seq(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
        Seq(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0))).toDF("values2", "values1")
inputDf.selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
  "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0), 1)) as minConvexHull1").show()
```

Output:
```sql
+----------------------------------------+
|minConvexHull1                          |
+----------------------------------------+
|POLYGON ((1 -1, 4 -1, 4 -5, 1 -5, 1 -1))|
+----------------------------------------+
```

```sql
SELECT RS_MinConvexHull(raster, 3) from rasters;
```

Output:
```sql
Provided band index 3 does not lie in the raster
```

## Raster Accessors

### RS_GeoReference

Introduction: Returns the georeference metadata of raster as a string in GDAL or ESRI format. Default is GDAL if not specified.

Format: `RS_GeoReference(raster: Raster, format:string)`

Difference between format representation is as follows:

`GDAL`

```
ScaleX 
SkewY 
SkewX 
ScaleY 
UpperLeftX
UpperLeftY
```

`ESRI`

```
ScaleX 
SkewY 
SkewX 
ScaleY 
UpperLeftX + ScaleX * 0.5
UpperLeftY + ScaleY * 0.5
```

Spark SQL Example:

```sql
SELECT RS_GeoReference(ST_MakeEmptyRaster(1, 100, 100, -53, 51, 2, -2, 4, 5, 4326))
```

Output:

```
2.000000 
5.000000 
4.000000 
-2.000000 
-53.000000 
51.000000
```

Spark SQL Example:

```sql
SELECT RS_GeoReferrence(ST_MakeEmptyRaster(1, 3, 4, 100.0, 200.0,2.0, -3.0, 0.1, 0.2, 0), "GDAL")
```

Output:

```
2.000000 
0.200000 
0.100000 
-3.000000 
100.000000 
200.000000
```

Spark SQL Example:

```sql
SELECT RS_GeoReferrence(ST_MakeEmptyRaster(1, 3, 4, 100.0, 200.0,2.0, -3.0, 0.1, 0.2, 0), "ERSI")
```

```
2.000000 
0.200000 
0.100000 
-3.000000 
101.000000 
198.500000
```

### RS_Height

Introduction: Returns the height of the raster.

Format: `RS_Height(raster: Raster)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_Height(raster) FROM rasters
```

Output:
```
512
```

### RS_RasterToWorldCoordX

Introduction: Returns the upper left X coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the X coordinate of the assumed point considering existing raster pixel size and skew values will be returned.

Format: `RS_RasterToWorldCoordX(raster: Raster, colX: int, rowY: int)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_RasterToWorldCoordX(ST_MakeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326), 1, 1) from rasters
```

Output:
```
-123
```

### RS_RasterToWorldCoordY

Introduction: Returns the upper left Y coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the Y coordinate of the assumed point considering existing raster pixel size and skew values will be returned.

Format: `RS_RasterToWorldCoordY(raster: Raster, colX: int, rowY: int)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_RasterToWorldCoordY(ST_MakeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326), 1, 1) from rasters
```

Output:
```
54
```

### RS_ScaleX

Introduction: Returns the pixel width of the raster in CRS units.
!!!Note
    RS_ScaleX attempts to get an Affine transform on the grid in order to return scaleX (See [World File](https://en.wikipedia.org/wiki/World_file) for more details). If the transform on the geometry is not an Affine transform, RS_ScaleX will throw an UnsupportedException:
    ```
    UnsupportedOperationException("Only AffineTransform2D is supported")
    ```

Format: `RS_ScaleX(raster: Raster)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_ScaleX(raster) FROM rasters
```

Output:
```
1
```

### RS_ScaleY

Introduction: Returns the pixel height of the raster in CRS units.
!!!Note
    RS_ScaleY attempts to get an Affine transform on the grid in order to return scaleX (See [World File](https://en.wikipedia.org/wiki/World_file) for more details). If the transform on the geometry is not an Affine transform, RS_ScaleY will throw an UnsupportedException:
    ```
    UnsupportedOperationException("Only AffineTransform2D is supported")
    ```

Format: `RS_ScaleY(raster: Raster)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_ScaleY(raster) FROM rasters
```

Output:
```
-2
```

### RS_SkewX

Introduction: Returns the X skew or rotation parameter.

Format: `RS_SkewX(raster: Raster)`

Since: `v1.5.0`

Spark SQL Example:

```sql
SELECT RS_SkewX(RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.1, 0.2, 4326))
```

Output:

```
0.1
```

### RS_SkewY

Introduction: Returns the Y skew or rotation parameter.

Format: `RS_SkewY(raster: Raster)`

Since: `v1.5.0`

Spark SQL Example:

```sql
SELECT RS_SkewY(RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.1, 0.2, 4326))
```

Output:

```
0.2
```

### RS_UpperLeftX

Introduction: Returns the X coordinate of the upper-left corner of the raster.

Format: `RS_UpperLeftX(raster: Raster)`

Since: `v1.5.0`

Spark SQL Example:

```sql
SELECT RS_UpperLeftX(raster) FROM rasters
```

Output:

```
5
```

### RS_UpperLeftY

Introduction: Returns the Y coordinate of the upper-left corner of the raster.

Format: `RS_UpperLeftY(raster: Raster)`

Since: `v1.5.0`

Spark SQL Example:

```sql
SELECT RS_UpperLeftY(raster) FROM rasters
```

Output:

```
6
```

### RS_Width

Introduction: Returns the width of the raster.

Format: `RS_Width(raster: Raster)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_Width(raster) FROM rasters
```

Output:
```
517
```

### RS_WorldToRasterCoord

Introduction: Returns the grid coordinate of the given world coordinates as a Point.

Format: `RS_WorldToRasterCoord(raster: Raster, point: Geometry)`
        `RS_WorldToRasterCoord(raster: Raster, x: double, y: point)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_WorldToRasterCoord(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326), -53, 51) from rasters;
SELECT RS_WorldToRasterCoord(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326), ST_GeomFromText('POINT (-52 51)')) from rasters;
```

Output:
`POINT (1 1)`
`POINT (2 1)`

!!!Note
    If the given geometry point is not in the same CRS as the given raster, the given geometry will be transformed to the given raster's CRS. You can use [ST_Transform](../Function/#st_transform) to transform the geometry beforehand.



### RS_WorldToRasterCoordX

Introduction: Returns the X coordinate of the grid coordinate of the given world coordinates as an integer.

Format: `RS_WorldToRasterCoord(raster: Raster, point: Geometry)`
        `RS_WorldToRasterCoord(raster: Raster, x: double, y: double)`


Since: `1.5.0`

Spark SQL example:

```sql
SELECT RS_WorldToRasterCoordX(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), -53, 51) from rasters;
```

Output: `1`

```sql
SELECT RS_WorldToRasterCoordX(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), ST_GeomFromText('POINT (-53 51)')) from rasters;
```

Output: `1`

!!!Tip
    For non-skewed rasters, you can provide any value for latitude and the intended value of world longitude, to get the desired answer


### RS_WorldToRasterCoordY

Introduction: Returns the Y coordinate of the grid coordinate of the given world coordinates as an integer.

Format: `RS_WorldToRasterCoordY(raster: Raster, point: Geometry)`
        `RS_WorldToRasterCoordY(raster: Raster, x: double, y: double)`

Since: `1.5.0`

Spark SQL example:

```sql
SELECT RS_WorldToRasterCoordY(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), ST_GeomFromText('POINT (-50 50)'));
```

Output: `2`

```sql
SELECT RS_WorldToRasterCoordY(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), -50, 49);
```

Output: `3`


!!!Tip
    For non-skewed rasters, you can provide any value for longitude and the intended value of world latitude, to get the desired answer

## Raster Band Accessors

### RS_BandNoDataValue

Introduction: Returns the no data value of the given band of the given raster. If no band is given, band 1 is assumed. The band parameter is 1-indexed. If there is no no data value associated with the given band, RS_BandNoDataValue returns null.

!!!Note
    If the given band does not lie in the raster, RS_BandNoDataValue throws an IllegalArgumentException

Format: `RS_BandNoDataValue (raster: Raster, band: Int = 1)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_BandNoDataValue(raster, 1) from rasters;
```

Output: `0.0`

```sql
SELECT RS_BandNoDataValue(raster) from rasters_without_nodata;
```

Output: `null`

```sql
SELECT RS_BandNoDataValue(raster, 3) from rasters;
```

Output: `IllegalArgumentException: Provided band index 3 is not present in the raster.`

### RS_BandPixelType

Introduction: Returns the datatype of each pixel in the given band of the given raster in string format. The band parameter is 1-indexed. If no band is specified, band 1 is assumed.
!!!Note
    If the given band index does not exist in the given raster, RS_BandPixelType throws an IllegalArgumentException.
Following are the possible values returned by RS_BandPixelType:


1. `REAL_64BITS` - For Double values
2. `REAL_32BITS` - For Float values
3. `SIGNED_32BITS` - For Integer values
4. `SIGNED_16BITS` - For Short values
5. `UNSIGNED_16BITS` - For unsigned Short values
6. `UNSIGNED_8BITS` - For Byte values

    
Format: `RS_BandPixelType(rast: Raster, band: Int = 1)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "D", 5, 5, 53, 51, 1, 1, 0, 0, 0), 2);
```

Output: `REAL_64BITS`

```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "I", 5, 5, 53, 51, 1, 1, 0, 0, 0));
```

Output: `SIGNED_32BITS`

```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "I", 5, 5, 53, 51, 1, 1, 0, 0, 0), 3);
```

Output: `IllegalArgumentException: Provided band index 3 is not present in the raster`


## Raster based operators

### RS_Intersects

Introduction: Returns true if the envelope of the raster intersects the given geometry. If the geometry does not have a
defined SRID, it is considered to be in the same CRS with the raster. If the geometry has a defined SRID, the geometry
will be transformed to the CRS of the raster before the intersection test.

Format: `RS_Intersects (raster: Raster, geom: Geometry)`

Since: `v1.5.0`

Spark SQL example:
```sql
SELECT RS_Intersects(raster, ST_SetSRID(ST_PolygonFromEnvelope(0, 0, 10, 10), 4326)) FROM raster_table
```
Output:
```
true
```

### RS_Within

Introduction: Returns true if the envelope of the raster is within the given geometry. If the geometry does not have a
defined SRID, it is considered to be in the same CRS with the raster. If the geometry has a defined SRID, the geometry
will be transformed to the CRS of the raster before checking the within relationship.

Format: `RS_Within(raster: Raster, geom: Geometry)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_Within(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((0 0, 0 50, 100 50, 100 0, 0 0))'));
```

Output:
```
true
```

### RS_Contains

Introduction: Returns true if the envelope of the raster contains the given geometry. If the geometry does not have a
defined SRID, it is considered to be in the same CRS with the raster. If the geometry has a defined SRID, the geometry
will be transformed to the CRS of the raster before checking the contains relationship.

Format: `RS_Contains(raster: Raster, geom: Geometry)`

Since: `1.5.0`

Spark SQL example:
```sql
SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((5 5, 5 10, 10 10, 10 5, 5 5))'));
```

Output:
```
true
```

### RS_MetaData

Introduction: Returns the metadata of the raster as an array of double. The array contains the following values:

- 0: upper left x coordinate of the raster, in terms of CRS units
- 1: upper left y coordinate of the raster, in terms of CRS units
- 2: width of the raster, in terms of pixels
- 3: height of the raster, in terms of pixels
- 4: width of a pixel, in terms of CRS units (scaleX)
- 5: height of a pixel, in terms of CRS units (scaleY), may be negative
- 6: skew in x direction (rotation x)
- 7: skew in y direction (rotation y)
- 8: srid of the raster
- 9: number of bands

Format: `RS_MetaData (raster: Raster)`

Since: `v1.4.1`

SQL example:
```sql
SELECT RS_MetaData(raster) FROM raster_table
```

Output:
```
+-----------------------------------------------------------------------------------------------------------------------+
|rs_metadata(raster)                                                                                                    |
+-----------------------------------------------------------------------------------------------------------------------+
|[-1.3095817809482181E7, 4021262.7487925636, 512.0, 517.0, 72.32861272132695, -72.32861272132695, 0.0, 0.0, 3857.0, 1.0]|
+-----------------------------------------------------------------------------------------------------------------------+
```

### RS_NumBands

Introduction: Returns the number of the bands in the raster.

Format: `RS_NumBands (raster: Raster)`

Since: `v1.4.0`

Spark SQL example:
```sql
SELECT RS_NumBands(raster) FROM raster_table
```

Output:
```
4
```

### RS_SetSRID

Introduction: Sets the spatial reference system identifier (SRID) of the raster geometry.

Format: `RS_SetSRID (raster: Raster, srid: Integer)`

Since: `v1.4.1`

Spark SQL example:
```sql
SELECT RS_SetSRID(raster, 4326)
FROM raster_table
```

### RS_SRID

Introduction: Returns the spatial reference system identifier (SRID) of the raster geometry.

Format: `RS_SRID (raster: Raster)`

Since: `v1.4.1`

Spark SQL example:
```sql
SELECT RS_SRID(raster) FROM raster_table
```

Output:
```
3857
```

### RS_Value

Introduction: Returns the value at the given point in the raster.
If no band number is specified it defaults to 1. 

Format: `RS_Value (raster: Raster, point: Geometry)`

Format: `RS_Value (raster: Raster, point: Geometry, band: Int)`

Since: `v1.4.0`

Spark SQL example:
```sql
SELECT RS_Value(raster, ST_Point(-13077301.685, 4002565.802)) FROM raster_table
```

Output:
```
5.0
```

### RS_Values

Introduction: Returns the values at the given points in the raster.
If no band number is specified it defaults to 1.

RS_Values is similar to RS_Value but operates on an array of points.
RS_Values can be significantly faster since a raster only has to be loaded once for several points.

Format: `RS_Values (raster: Raster, points: Array[Geometry])`

Format: `RS_Values (raster: Raster, points: Array[Geometry], band: Int)`

Since: `v1.4.0`

Spark SQL example:
```sql
SELECT RS_Values(raster, Array(ST_Point(-1307.5, 400.8), ST_Point(-1403.3, 399.1)))
FROM raster_table
```

Output:
```
Array(5.0, 3.0)
```

Spark SQL example for joining a point dataset with a raster dataset:
```scala
val pointDf = spark.read...
val rasterDf = spark.read.format("binaryFile").load("/some/path/*.tiff")
  .withColumn("raster", expr("RS_FromGeoTiff(content)"))
  .withColumn("envelope", expr("RS_Envelope(raster)"))

// Join the points with the raster extent and aggregate points to arrays.
// We only use the path and envelope of the raster to keep the shuffle as small as possible.
val df = pointDf.join(rasterDf.select("path", "envelope"), expr("ST_Within(point_geom, envelope)"))
  .groupBy("path")
  .agg(collect_list("point_geom").alias("point"), collect_list("point_id").alias("id"))

df.join(rasterDf, "path")
  .selectExpr("explode(arrays_zip(id, point, RS_Values(raster, point))) as result")
  .selectExpr("result.*")
  .show()
```

Output:
```
+----+------------+-------+
| id | point      | value |
+----+------------+-------+
|  4 | POINT(1 1) |   3.0 |
|  5 | POINT(2 2) |   7.0 |
+----+------------+-------+
```


## Raster to Map Algebra operators

To bridge the gap between the raster and map algebra worlds, the following operators are provided. These operators convert a raster to a map algebra object. The map algebra object can then be used with the map algebra operators described in the next section.

### RS_BandAsArray

Introduction: Extract a band from a raster as an array of doubles.

Format: `RS_BandAsArray (raster: Raster, bandIndex: Int)`.

Since: `v1.4.1`

BandIndex is 1-based and must be between 1 and RS_NumBands(raster). It returns null if the bandIndex is out of range or the raster is null.

SQL example:
```sql
SELECT RS_BandAsArray(raster, 1) FROM raster_table
```

Output:

```
+--------------------+
|                band|
+--------------------+
|[0.0, 0.0, 0.0, 0...|
+--------------------+
```

### RS_AddBandFromArray

Introduction: Add a band to a raster from an array of doubles.

Format: `RS_AddBandFromArray (raster: Raster, band: Array[Double])` | `RS_AddBandFromArray (raster: Raster, band: Array[Double], bandIndex:Int)` | `RS_AddBandFromArray (raster: Raster, band: Array[Double], bandIndex:Int, noDataValue:Double)`

Since: `v1.5.0`

The bandIndex is 1-based and must be between 1 and RS_NumBands(raster) + 1. It throws an exception if the bandIndex is out of range or the raster is null. If not specified, the noDataValue of the band is assumed to be null.

When the bandIndex is RS_NumBands(raster) + 1, it appends the band to the end of the raster. Otherwise, it replaces the existing band at the bandIndex.

If the bandIndex and noDataValue is not given, a convenience implementation adds a new band with a null noDataValue.

Adding a new band with a custom noDataValue requires bandIndex = RS_NumBands(raster) + 1 and non-null noDataValue to be explicitly specified.

Modifying or Adding a customNoDataValue is also possible by giving an existing band in RS_AddBandFromArray

In order to remove an existing noDataValue from an existing band, pass null as the noDataValue in the RS_AddBandFromArray.

Note that: `bandIndex == RS_NumBands(raster) + 1` is an experimental feature and might lead to the loss of raster metadata and properties such as color models.

!!!Note
    RS_AddBandFromArray typecasts the double band values to the given datatype of the raster. This can lead to overflow values if values beyond the range of the raster's datatype are provided.

SQL example:

```sql
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2)) AS raster FROM raster_table
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2), 1) AS raster FROM raster_table
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2), 1, -999) AS raster FROM raster_table
```

Output:
```
+--------------------+
|              raster|
+--------------------+
|GridCoverage2D["g...|
+--------------------+
```

### RS_MapAlgebra

Introduction: Apply a map algebra script on a raster.

Format: `RS_MapAlgebra (raster: Raster, pixelType: String, script: String)`

Format: `RS_MapAlgebra (raster: Raster, pixelType: String, script: String, noDataValue: Double)`

Since: `v1.5.0`

`RS_MapAlgebra` runs a script on a raster. The script is written in a map algebra language called [Jiffle](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle). The script takes a raster
as input and returns a raster of the same size as output. The script can be used to apply a map algebra expression on a raster. The input raster is named `rast` in the Jiffle script, and the output raster is named `out`.

SQL example:

Calculate the NDVI of a raster with 4 bands (R, G, B, NIR):

```sql
-- Assume that the input raster has 4 bands: R, G, B, NIR
-- rast[0] refers to the R band, rast[3] refers to the NIR band.
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') AS ndvi FROM raster_table
```

Output:
```
+--------------------+
|              raster|
+--------------------+
|GridCoverage2D["g...|
+--------------------+
```

For more details and examples about `RS_MapAlgebra`, please refer to the [Map Algebra documentation](../Raster-map-algebra/).
To learn how to write map algebra script, please refer to [Jiffle language summary](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle---language-summary).

## Map Algebra operators

Map algebra operators work on a single band of a raster. Each band is represented as an array of doubles. The operators return an array of doubles.

### RS_Add

Introduction: Add two spectral bands in a Geotiff image 

Format: `RS_Add (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val sumDF = spark.sql("select RS_Add(band1, band2) as sumOfBands from dataframe")

```

### RS_Append

Introduction: Appends a new band to the end of Geotiff image data and returns the new data. The new band to be appended can be a normalized difference index between two bands (example: NBR, NDBI). Normalized difference index between two bands can be calculated with RS_NormalizedDifference operator described earlier in this page. Specific bands can be retrieved using RS_GetBand operator described [here](../Raster-loader/).

Format: `RS_Append(data: Array[Double], newBand: Array[Double], nBands: Int)`

Since: `v1.2.1`

Deprecated since: `v1.4.1`

Spark SQL example:
```scala

val dfAppended = spark.sql("select RS_Append(data, normalizedDifference, nBands) as dataEdited from dataframe")

```

### RS_BitwiseAND

Introduction: Find Bitwise AND between two bands of Geotiff image

Format: `RS_BitwiseAND (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val biwiseandDF = spark.sql("select RS_BitwiseAND(band1, band2) as andvalue from dataframe")

```

### RS_BitwiseOR

Introduction: Find Bitwise OR between two bands of Geotiff image

Format: `RS_BitwiseOR (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val biwiseorDF = spark.sql("select RS_BitwiseOR(band1, band2) as or from dataframe")

```

### RS_CountValue

Introduction: Returns count of a particular value from a spectral band in a raster image

Format: `RS_CountValue (Band1: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val countDF = spark.sql("select RS_CountValue(band1, target) as count from dataframe")

```

### RS_Divide

Introduction: Divide band1 with band2 from a geotiff image

Format: `RS_Divide (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val multiplyDF = spark.sql("select RS_Divide(band1, band2) as divideBands from dataframe")

```

### RS_FetchRegion

Introduction: Fetch a subset of region from given Geotiff image based on minimumX, minimumY, maximumX and maximumY index as well original height and width of image

Format: `RS_FetchRegion (Band: Array[Double], coordinates: Array[Int], dimensions: Array[Int])`

Since: `v1.1.0`

Spark SQL example:
```scala

val region = spark.sql("select RS_FetchRegion(Band,Array(0, 0, 1, 2),Array(3, 3)) as Region from dataframe")
```

### RS_GreaterThan

Introduction: Mask all the values with 1 which are greater than a particular target value

Format: `RS_GreaterThan (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val greaterDF = spark.sql("select RS_GreaterThan(band, target) as maskedvalues from dataframe")

```

### RS_GreaterThanEqual

Introduction: Mask all the values with 1 which are greater than equal to a particular target value

Format: `RS_GreaterThanEqual (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val greaterEqualDF = spark.sql("select RS_GreaterThanEqual(band, target) as maskedvalues from dataframe")

```

### RS_LessThan

Introduction: Mask all the values with 1 which are less than a particular target value

Format: `RS_LessThan (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val lessDF = spark.sql("select RS_LessThan(band, target) as maskedvalues from dataframe")

```

### RS_LessThanEqual

Introduction: Mask all the values with 1 which are less than equal to a particular target value

Format: `RS_LessThanEqual (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val lessEqualDF = spark.sql("select RS_LessThanEqual(band, target) as maskedvalues from dataframe")

```

### RS_LogicalDifference

Introduction: Return value from band 1 if a value in band1 and band2 are different, else return 0

Format: `RS_LogicalDifference (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val logicalDifference = spark.sql("select RS_LogicalDifference(band1, band2) as logdifference from dataframe")

```

### RS_LogicalOver

Introduction: Return value from band1 if it's not equal to 0, else return band2 value

Format: `RS_LogicalOver (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val logicalOver = spark.sql("select RS_LogicalOver(band1, band2) as logover from dataframe")

```

### RS_Mean

Introduction: Returns Mean value for a spectral band in a Geotiff image

Format: `RS_Mean (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val meanDF = spark.sql("select RS_Mean(band) as mean from dataframe")

```

### RS_Mode

Introduction: Returns Mode from a spectral band in a Geotiff image in form of an array

Format: `RS_Mode (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val modeDF = spark.sql("select RS_Mode(band) as mode from dataframe")

```

### RS_Modulo

Introduction: Find modulo of pixels with respect to a particular value

Format: `RS_Modulo (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val moduloDF = spark.sql("select RS_Modulo(band, target) as modulo from dataframe")

```

### RS_Multiply

Introduction: Multiply two spectral bands in a Geotiff image

Format: `RS_Multiply (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val multiplyDF = spark.sql("select RS_Multiply(band1, band2) as multiplyBands from dataframe")

```

### RS_MultiplyFactor

Introduction: Multiply a factor to a spectral band in a geotiff image

Format: `RS_MultiplyFactor (Band1: Array[Double], Factor: Double)`

Since: `v1.1.0`

Spark SQL example:
```scala

val multiplyFactorDF = spark.sql("select RS_MultiplyFactor(band1, 2) as multiplyfactor from dataframe")

```

This function only accepts integer as factor before `v1.5.0`.

### RS_Normalize

Introduction: Normalize the value in the array to [0, 255]

Since: `v1.1.0`

Spark SQL example
```sql
SELECT RS_Normalize(band)
```

### RS_NormalizedDifference

Introduction: Returns Normalized Difference between two bands(band2 and band1) in a Geotiff image(example: NDVI, NDBI)

Format: `RS_NormalizedDifference (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val normalizedDF = spark.sql("select RS_NormalizedDifference(band1, band2) as normdifference from dataframe")

```

### RS_SquareRoot

Introduction: Find Square root of band values in a geotiff image 

Format: `RS_SquareRoot (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val rootDF = spark.sql("select RS_SquareRoot(band) as squareroot from dataframe")

```

### RS_Subtract

Introduction: Subtract two spectral bands in a Geotiff image(band2 - band1)

Format: `RS_Subtract (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```scala

val subtractDF = spark.sql("select RS_Subtract(band1, band2) as differenceOfOfBands from dataframe")

```
