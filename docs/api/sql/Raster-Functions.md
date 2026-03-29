<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Raster Functions

## Raster Constructors

These functions create raster objects from various file formats or from scratch.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_FromArcInfoAsciiGrid](Raster-Constructors/RS_FromArcInfoAsciiGrid.md) | Raster | Returns a raster geometry from an Arc Info Ascii Grid file. | v1.4.0 |
| [RS_FromGeoTiff](Raster-Constructors/RS_FromGeoTiff.md) | Raster | Returns a raster geometry from a GeoTiff file. | v1.4.0 |
| [RS_FromNetCDF](Raster-Constructors/RS_FromNetCDF.md) | Raster | Returns a raster geometry representing the given record variable short name from a NetCDF file. This API reads the array data of the record variable *in memory* along with all its dimensions Since ... | v1.5.1 |
| [RS_MakeEmptyRaster](Raster-Constructors/RS_MakeEmptyRaster.md) | Raster | Returns an empty raster geometry. Every band in the raster is initialized to `0.0`. | v1.5.0 |
| [RS_MakeRaster](Raster-Constructors/RS_MakeRaster.md) | Raster | Creates a raster from the given array of pixel values. The width, height, geo-reference information, and the CRS will be taken from the given reference raster. The data type of the resulting raster... | v1.6.0 |
| [RS_NetCDFInfo](Raster-Constructors/RS_NetCDFInfo.md) | String | Returns a string containing names of the variables in a given netCDF file along with its dimensions. |  |

## Pixel Functions

These functions work with individual pixel geometry representations.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_PixelAsCentroid](Pixel-Functions/RS_PixelAsCentroid.md) | Geometry | Returns the centroid (point geometry) of the specified pixel's area. The pixel coordinates specified are 1-indexed. If `colX` and `rowY` are out of bounds for the raster, they are interpolated assu... | v1.5.0 |
| [RS_PixelAsCentroids](Pixel-Functions/RS_PixelAsCentroids.md) | `Array<Struct>` | Returns a list of the centroid point geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. Each centroid represents the geometric center o... | v1.5.1 |
| [RS_PixelAsPoint](Pixel-Functions/RS_PixelAsPoint.md) | Geometry | Returns a point geometry of the specified pixel's upper-left corner. The pixel coordinates specified are 1-indexed. | v1.5.0 |
| [RS_PixelAsPoints](Pixel-Functions/RS_PixelAsPoints.md) | `Array<Struct>` | Returns a list of the pixel's upper-left corner point geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. | v1.5.1 |
| [RS_PixelAsPolygon](Pixel-Functions/RS_PixelAsPolygon.md) | Geometry | Returns a polygon geometry that bounds the specified pixel. The pixel coordinates specified are 1-indexed. If `colX` and `rowY` are out of bounds for the raster, they are interpolated assuming the ... | v1.5.0 |
| [RS_PixelAsPolygons](Pixel-Functions/RS_PixelAsPolygons.md) | `Array<Struct>` | Returns a list of the polygon geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. | v1.5.1 |

## Raster Geometry Functions

These functions extract geometry representations from rasters.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_ConvexHull](Raster-Geometry-Functions/RS_ConvexHull.md) | Geometry | Return the convex hull geometry of the raster including the NoDataBandValue band pixels. For regular shaped and non-skewed rasters, this gives more or less the same result as RS_Envelope and hence ... | v1.5.0 |
| [RS_Envelope](Raster-Geometry-Functions/RS_Envelope.md) | Geometry | Returns the envelope of the raster as a Geometry. | v1.4.0 |
| [RS_MinConvexHull](Raster-Geometry-Functions/RS_MinConvexHull.md) | Geometry | Returns the min convex hull geometry of the raster **excluding** the NoDataBandValue band pixels, in the given band. If no band is specified, all the bands are considered when creating the min conv... | v1.5.0 |

## Raster Accessors

These functions extract metadata and properties from raster objects.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_GeoReference](Raster-Accessors/RS_GeoReference.md) | String | Returns the georeference metadata of raster as a string in GDAL or ESRI format. Default is GDAL if not specified. | v1.5.0 |
| [RS_GeoTransform](Raster-Accessors/RS_GeoTransform.md) | Struct | Returns a struct of parameters that represent the GeoTransformation of the raster. The struct has the following schema: | v1.5.1 |
| [RS_Height](Raster-Accessors/RS_Height.md) | Integer | Returns the height of the raster. | v1.5.0 |
| [RS_RasterToWorldCoord](Raster-Accessors/RS_RasterToWorldCoord.md) | Geometry | Returns the upper left X and Y coordinates of the given row and column of the given raster geometric units of the geo-referenced raster as a Point geometry. If any out of bounds values are given, t... | v1.5.1 |
| [RS_RasterToWorldCoordX](Raster-Accessors/RS_RasterToWorldCoordX.md) | Double | Returns the upper left X coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the X coordinate of the assu... | v1.5.0 |
| [RS_RasterToWorldCoordY](Raster-Accessors/RS_RasterToWorldCoordY.md) | Double | Returns the upper left Y coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the Y coordinate of the assu... | v1.5.0 |
| [RS_Rotation](Raster-Accessors/RS_Rotation.md) | Double | Returns the uniform rotation of the raster in radian. | v1.5.1 |
| [RS_ScaleX](Raster-Accessors/RS_ScaleX.md) | Double | Returns the pixel width of the raster in CRS units. | v1.5.0 |
| [RS_ScaleY](Raster-Accessors/RS_ScaleY.md) | Double | Returns the pixel height of the raster in CRS units. | v1.5.0 |
| [RS_SkewX](Raster-Accessors/RS_SkewX.md) | Double | Returns the X skew or rotation parameter. | v1.5.0 |
| [RS_SkewY](Raster-Accessors/RS_SkewY.md) | Double | Returns the Y skew or rotation parameter. | v1.5.0 |
| [RS_UpperLeftX](Raster-Accessors/RS_UpperLeftX.md) | Double | Returns the X coordinate of the upper-left corner of the raster. | v1.5.0 |
| [RS_UpperLeftY](Raster-Accessors/RS_UpperLeftY.md) | Double | Returns the Y coordinate of the upper-left corner of the raster. | v1.5.0 |
| [RS_Width](Raster-Accessors/RS_Width.md) | Integer | Returns the width of the raster. | v1.5.0 |
| [RS_WorldToRasterCoord](Raster-Accessors/RS_WorldToRasterCoord.md) | Geometry | Returns the grid coordinate of the given world coordinates as a Point. | v1.5.0 |
| [RS_WorldToRasterCoordX](Raster-Accessors/RS_WorldToRasterCoordX.md) | Integer | Returns the X coordinate of the grid coordinate of the given world coordinates as an integer. | v1.5.0 |
| [RS_WorldToRasterCoordY](Raster-Accessors/RS_WorldToRasterCoordY.md) | Integer | Returns the Y coordinate of the grid coordinate of the given world coordinates as an integer. | v1.5.0 |

## Raster Band Accessors

These functions access band-level properties and statistics of raster objects.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_Band](Raster-Band-Accessors/RS_Band.md) | Raster | Returns a new raster consisting 1 or more bands of an existing raster. It can build new rasters from existing ones, export only selected bands from a multiband raster, or rearrange the order of ban... | v1.5.0 |
| [RS_BandIsNoData](Raster-Band-Accessors/RS_BandIsNoData.md) | Boolean | Returns true if the band is filled with only nodata values. Band 1 is assumed if not specified. | v1.5.0 |
| [RS_BandNoDataValue](Raster-Band-Accessors/RS_BandNoDataValue.md) | Double | Returns the no data value of the given band of the given raster. If no band is given, band 1 is assumed. The band parameter is 1-indexed. If there is no data value associated with the given band, R... | v1.5.0 |
| [RS_BandPixelType](Raster-Band-Accessors/RS_BandPixelType.md) | String | Returns the datatype of each pixel in the given band of the given raster in string format. The band parameter is 1-indexed. If no band is specified, band 1 is assumed. | v1.5.0 |
| [RS_Count](Raster-Band-Accessors/RS_Count.md) | Long | Returns the number of pixels in a given band. If band is not specified then it defaults to `1`. | v1.5.0 |
| [RS_SummaryStats](Raster-Band-Accessors/RS_SummaryStats.md) | Double | Returns summary statistic for a particular band based on the `statType` parameter. The function defaults to band index of `1` when `band` is not specified and excludes noDataValue if `excludeNoData... | v1.6.0 |
| [RS_SummaryStatsAll](Raster-Band-Accessors/RS_SummaryStatsAll.md) | Struct | Returns summary stats struct consisting of count, sum, mean, stddev, min, max for a given band in raster. If band is not specified then it defaults to `1`. | v1.5.0 |
| [RS_ZonalStats](Raster-Band-Accessors/RS_ZonalStats.md) | Double | This returns a statistic value specified by `statType` over the region of interest defined by `zone`. It computes the statistic from the pixel values within the ROI geometry and returns the result.... | v1.5.1 |
| [RS_ZonalStatsAll](Raster-Band-Accessors/RS_ZonalStatsAll.md) | Struct | Returns a struct of statistic values, where each statistic is computed over a region defined by the `zone` geometry. The struct has the following schema: | v1.5.1 |

## Raster Predicates

These functions test spatial relationships involving raster objects.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_Contains](Raster-Predicates/RS_Contains.md) | Boolean | Returns true if the geometry or raster on the left side contains the geometry or raster on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |
| [RS_Intersects](Raster-Predicates/RS_Intersects.md) | Boolean | Returns true if raster or geometry on the left side intersects with the raster or geometry on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |
| [RS_Within](Raster-Predicates/RS_Within.md) | Boolean | Returns true if the geometry or raster on the left side is within the geometry or raster on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |

## Raster Operators

These functions perform operations on raster objects.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_AddBand](Raster-Operators/RS_AddBand.md) | Raster | Adds a new band to a raster `toRaster` at a specified index `toRasterIndex`. The new band's values are copied from `fromRaster` at a specified band index `fromBand`. If no `toRasterIndex` is provided... | v1.5.0 |
| [RS_Clip](Raster-Operators/RS_Clip.md) | Raster | Returns a raster that is clipped by the given geometry. | v1.5.1 |
| [RS_Interpolate](Raster-Operators/RS_Interpolate.md) | Raster | This function performs interpolation on a raster using the Inverse Distance Weighted (IDW) method. This method estimates cell values by averaging the values of sample data points in the vicinity of... | v1.6.0 |
| [RS_MetaData](Raster-Operators/RS_MetaData.md) | `Array<Double>` | Returns the metadata of the raster as a struct. The struct has the following schema: | v1.4.1 |
| [RS_NormalizeAll](Raster-Operators/RS_NormalizeAll.md) | Raster | Normalizes values in all bands of a raster between a given normalization range. The function maintains the data type of the raster values by ensuring that the normalized values are cast back to the... | v1.6.0 |
| [RS_NumBands](Raster-Operators/RS_NumBands.md) | Integer | Returns the number of the bands in the raster. | v1.4.0 |
| [RS_ReprojectMatch](Raster-Operators/RS_ReprojectMatch.md) | Raster | Reproject a raster to match the geo-reference, CRS, and envelope of a reference raster. The output raster always have the same extent and resolution as the reference raster. For pixels not covered ... | v1.6.0 |
| [RS_Resample](Raster-Operators/RS_Resample.md) | Raster | Resamples a raster using a given resampling algorithm and new dimensions (width and height), a new grid corner to pivot the raster at (gridX and gridY) and a set of georeferencing attributes (scale... | v1.5.0 |
| [RS_SetBandNoDataValue](Raster-Operators/RS_SetBandNoDataValue.md) | Raster | This sets the no data value for a specified band in the raster. If the band index is not provided, band 1 is assumed by default. Passing a `null` value for `noDataValue` will remove the no data val... | v1.5.0 |
| [RS_SetGeoReference](Raster-Operators/RS_SetGeoReference.md) | Raster | Sets the Georeference information of an object in a single call. Accepts inputs in `GDAL` and `ESRI` format. Default format is `GDAL`. If all 6 parameters are not provided then will return null. | v1.5.0 |
| [RS_SetPixelType](Raster-Operators/RS_SetPixelType.md) | Raster | Returns a modified raster with the desired pixel data type. | v1.6.0 |
| [RS_SetCRS](Raster-Operators/RS_SetCRS.md) | Raster | Sets the coordinate reference system (CRS) of the raster using a CRS definition string. Accepts EPSG codes, WKT1, WKT2, PROJ strings, and PROJJSON. | v1.9.0 |
| [RS_SetSRID](Raster-Operators/RS_SetSRID.md) | Raster | Sets the spatial reference system identifier (SRID) of the raster geometry. | v1.4.1 |
| [RS_SetValue](Raster-Operators/RS_SetValue.md) | Raster | Returns a raster by replacing the value of pixel specified by `colX` and `rowY`. | v1.5.0 |
| [RS_SetValues](Raster-Operators/RS_SetValues.md) | Raster | Returns a raster by replacing the values of pixels in a specified rectangular region. The top left corner of the region is defined by the `colX` and `rowY` coordinates. The `width` and `height` par... | v1.5.0 |
| [RS_SRID](Raster-Operators/RS_SRID.md) | Integer | Returns the spatial reference system identifier (SRID) of the raster geometry. | v1.4.1 |
| [RS_CRS](Raster-Operators/RS_CRS.md) | String | Returns the coordinate reference system (CRS) of the raster as a string in the specified format (projjson, wkt2, wkt1, proj). Defaults to PROJJSON. | v1.9.0 |
| [RS_Union](Raster-Operators/RS_Union.md) | Raster | Returns a combined multi-band raster from 2 or more input Rasters. The order of bands in the resultant raster will be in the order of the input rasters. For example if `RS_Union` is called on two 2... | v1.6.0 |
| [RS_Value](Raster-Operators/RS_Value.md) | Double | Returns the value at the given point in the raster. If no band number is specified it defaults to 1. | v1.4.0 |
| [RS_Values](Raster-Operators/RS_Values.md) | `Array<Double>` | Returns the values at the given points or grid coordinates in the raster. If no band number is specified it defaults to 1. | v1.4.0 |
| [RS_AsRaster](Raster-Output/RS_AsRaster.md) | Raster | Converts a vector geometry into a raster dataset by assigning a specified value to all pixels covered by the geometry. | v1.5.0 |

## Raster Tiles

These functions split rasters into tiles.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_Tile](Raster-Tiles/RS_Tile.md) | `Array<Raster>` | Returns an array of rasters resulting from the split of the input raster based upon the desired dimensions of the output rasters. | v1.5.1 |
| [RS_TileExplode](Raster-Tiles/RS_TileExplode.md) | Struct | Generates records containing raster tiles resulting from the split of the input raster based upon the desired dimensions of the output rasters. | v1.5.0 |

## Raster Map Algebra Operators

These functions convert between raster bands and arrays for map algebra operations.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_AddBandFromArray](Raster-Map-Algebra-Operators/RS_AddBandFromArray.md) | Raster | Add a band to a raster from an array of doubles. | v1.5.0 |
| [RS_BandAsArray](Raster-Map-Algebra-Operators/RS_BandAsArray.md) | `Array<Double>` | Extract a band from a raster as an array of doubles. | v1.4.1 |
| [RS_MapAlgebra](Raster-Map-Algebra-Operators/RS_MapAlgebra.md) | Raster | Apply a map algebra script on a raster. | v1.5.0 |

## Map Algebra Operators

These functions perform per-pixel mathematical operations on raster band arrays.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_Add](Map-Algebra-Operators/RS_Add.md) | `Array<Double>` | Add two spectral bands in a Geotiff image | v1.1.0 |
| [RS_Array](Map-Algebra-Operators/RS_Array.md) | `Array<Double>` | Create an array that is filled by the given value | v1.1.0 |
| [RS_BitwiseAND](Map-Algebra-Operators/RS_BitwiseAND.md) | `Array<Double>` | Find Bitwise AND between two bands of Geotiff image | v1.1.0 |
| [RS_BitwiseOR](Map-Algebra-Operators/RS_BitwiseOR.md) | `Array<Double>` | Find Bitwise OR between two bands of Geotiff image | v1.1.0 |
| [RS_CountValue](Map-Algebra-Operators/RS_CountValue.md) | Integer | Returns count of a particular value from a spectral band in a raster image | v1.1.0 |
| [RS_Divide](Map-Algebra-Operators/RS_Divide.md) | `Array<Double>` | Divide band1 with band2 from a geotiff image | v1.1.0 |
| [RS_FetchRegion](Map-Algebra-Operators/RS_FetchRegion.md) | `Array<Double>` | Fetch a subset of region from given Geotiff image based on minimumX, minimumY, maximumX and maximumY index as well original height and width of image | v1.1.0 |
| [RS_GreaterThan](Map-Algebra-Operators/RS_GreaterThan.md) | `Array<Double>` | Mask all the values with 1 which are greater than a particular target value | v1.1.0 |
| [RS_GreaterThanEqual](Map-Algebra-Operators/RS_GreaterThanEqual.md) | `Array<Double>` | Mask all the values with 1 which are greater than equal to a particular target value | v1.1.0 |
| [RS_LessThan](Map-Algebra-Operators/RS_LessThan.md) | `Array<Double>` | Mask all the values with 1 which are less than a particular target value | v1.1.0 |
| [RS_LessThanEqual](Map-Algebra-Operators/RS_LessThanEqual.md) | `Array<Double>` | Mask all the values with 1 which are less than equal to a particular target value | v1.1.0 |
| [RS_LogicalDifference](Map-Algebra-Operators/RS_LogicalDifference.md) | `Array<Double>` | Return value from band 1 if a value in band1 and band2 are different, else return 0 | v1.1.0 |
| [RS_LogicalOver](Map-Algebra-Operators/RS_LogicalOver.md) | `Array<Double>` | Return value from band1 if it's not equal to 0, else return band2 value | v1.1.0 |
| [RS_Mean](Map-Algebra-Operators/RS_Mean.md) | Double | Returns Mean value for a spectral band in a Geotiff image | v1.1.0 |
| [RS_Mode](Map-Algebra-Operators/RS_Mode.md) | `Array<Double>` | Returns Mode from a spectral band in a Geotiff image in form of an array | v1.1.0 |
| [RS_Modulo](Map-Algebra-Operators/RS_Modulo.md) | `Array<Double>` | Find modulo of pixels with respect to a particular value | v1.1.0 |
| [RS_Multiply](Map-Algebra-Operators/RS_Multiply.md) | `Array<Double>` | Multiply two spectral bands in a Geotiff image | v1.1.0 |
| [RS_MultiplyFactor](Map-Algebra-Operators/RS_MultiplyFactor.md) | `Array<Double>` | Multiply a factor to a spectral band in a geotiff image | v1.1.0 |
| [RS_Normalize](Map-Algebra-Operators/RS_Normalize.md) | `Array<Double>` | Normalize the value in the array to [0, 255]. Uniform arrays are set to 0 after normalization. | v1.1.0 |
| [RS_NormalizedDifference](Map-Algebra-Operators/RS_NormalizedDifference.md) | `Array<Double>` | Returns Normalized Difference between two bands(band2 and band1) in a Geotiff image(example: NDVI, NDBI) | v1.1.0 |
| [RS_SquareRoot](Map-Algebra-Operators/RS_SquareRoot.md) | `Array<Double>` | Find Square root of band values in a geotiff image | v1.1.0 |
| [RS_Subtract](Map-Algebra-Operators/RS_Subtract.md) | `Array<Double>` | Subtract two spectral bands in a Geotiff image(band2 - band1) | v1.1.0 |

## Raster Aggregate Functions

These functions perform aggregate operations on groups of rasters.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_Union_Aggr](Raster-Aggregate-Functions/RS_Union_Aggr.md) | Raster | This function combines multiple rasters into a single multiband raster by stacking the bands of each input raster sequentially. The function arranges the bands in the output raster according to the... | v1.5.1 |

## Raster Output

These functions convert raster data to various output formats for visualization.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [RS_AsBase64](Raster-Output/RS_AsBase64.md) | String | Returns a base64 encoded string of the given raster. If the datatype is integral then this function internally takes the first 4 bands as RGBA, and converts them to the PNG format, finally produces... | v1.5.0 |
| [RS_AsImage](Raster-Output/RS_AsImage.md) | String | Returns a HTML that when rendered using an HTML viewer or via a Jupyter Notebook, displays the raster as a square image of side length `imageWidth`. Optionally, an imageWidth parameter can be passe... | v1.5.0 |
| [RS_AsMatrix](Raster-Output/RS_AsMatrix.md) | String | Returns a string, that when printed, outputs the raster band as a pretty printed 2D matrix. All the values of the raster are cast to double for the string. RS_AsMatrix allows specifying the number ... |  |
| [RS_AsArcGrid](Raster-Output/RS_AsArcGrid.md) | Binary | Returns a binary DataFrame from a Raster DataFrame. Each raster object is an ArcGrid image in binary format. | v1.4.1 |
| [RS_AsGeoTiff](Raster-Output/RS_AsGeoTiff.md) | Binary | Returns a binary DataFrame from a Raster DataFrame. Each raster object is a GeoTiff image in binary format. | v1.4.1 |
| [RS_AsCOG](Raster-Output/RS_AsCOG.md) | Binary | Returns a binary representation of the raster in Cloud Optimized GeoTiff (COG) format. | v1.9.0 |
| [RS_AsPNG](Raster-Output/RS_AsPNG.md) | Binary | Returns a PNG byte array that can be written to raster files. Only accepts pixel data type of unsigned integer. | v1.5.0 |
