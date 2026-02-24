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

# Raster Operators

These functions perform operations on raster objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_AddBand](RS_AddBand.md) | Adds a new band to a raster `toRaster` at a specified index `toRasterIndex`. The new band's values are copied from `fromRaster` at a specified band index `fromBand`. If no `toRasterIndex` is provid... | v1.5.0 |
| [RS_Clip](RS_Clip.md) | Returns a raster that is clipped by the given geometry. | v1.5.1 |
| [RS_Interpolate](RS_Interpolate.md) | This function performs interpolation on a raster using the Inverse Distance Weighted (IDW) method. This method estimates cell values by averaging the values of sample data points in the vicinity of... | v1.6.0 |
| [RS_MetaData](RS_MetaData.md) | Returns the metadata of the raster as a struct. The struct has the following schema: | v1.4.1 |
| [RS_NormalizeAll](RS_NormalizeAll.md) | Normalizes values in all bands of a raster between a given normalization range. The function maintains the data type of the raster values by ensuring that the normalized values are cast back to the... | v1.6.0 |
| [RS_NumBands](RS_NumBands.md) | Returns the number of the bands in the raster. | v1.4.0 |
| [RS_ReprojectMatch](RS_ReprojectMatch.md) | Reproject a raster to match the geo-reference, CRS, and envelope of a reference raster. The output raster always have the same extent and resolution as the reference raster. For pixels not covered ... | v1.6.0 |
| [RS_Resample](RS_Resample.md) | Resamples a raster using a given resampling algorithm and new dimensions (width and height), a new grid corner to pivot the raster at (gridX and gridY) and a set of georeferencing attributes (scale... | v1.5.0 |
| [RS_SetBandNoDataValue](RS_SetBandNoDataValue.md) | This sets the no data value for a specified band in the raster. If the band index is not provided, band 1 is assumed by default. Passing a `null` value for `noDataValue` will remove the no data val... | v1.5.0 |
| [RS_SetGeoReference](RS_SetGeoReference.md) | Sets the Georeference information of an object in a single call. Accepts inputs in `GDAL` and `ESRI` format. Default format is `GDAL`. If all 6 parameters are not provided then will return null. | v1.5.0 |
| [RS_SetPixelType](RS_SetPixelType.md) | Returns a modified raster with the desired pixel data type. | v1.6.0 |
| [RS_SetSRID](RS_SetSRID.md) | Sets the spatial reference system identifier (SRID) of the raster geometry. | v1.4.1 |
| [RS_SetValue](RS_SetValue.md) | Returns a raster by replacing the value of pixel specified by `colX` and `rowY`. | v1.5.0 |
| [RS_SetValues](RS_SetValues.md) | Returns a raster by replacing the values of pixels in a specified rectangular region. The top left corner of the region is defined by the `colX` and `rowY` coordinates. The `width` and `height` par... | v1.5.0 |
| [RS_SRID](RS_SRID.md) | Returns the spatial reference system identifier (SRID) of the raster geometry. | v1.4.1 |
| [RS_Union](RS_Union.md) | Returns a combined multi-band raster from 2 or more input Rasters. The order of bands in the resultant raster will be in the order of the input rasters. For example if `RS_Union` is called on two 2... | v1.6.0 |
| [RS_Value](RS_Value.md) | Returns the value at the given point in the raster. If no band number is specified it defaults to 1. | v1.4.0 |
| [RS_Values](RS_Values.md) | Returns the values at the given points or grid coordinates in the raster. If no band number is specified it defaults to 1. | v1.4.0 |
