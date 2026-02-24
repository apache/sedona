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

# Raster Accessors

These functions extract metadata and properties from raster objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_GeoReference](RS_GeoReference.md) | Returns the georeference metadata of raster as a string in GDAL or ESRI format. Default is GDAL if not specified. | v1.5.0 |
| [RS_GeoTransform](RS_GeoTransform.md) | Returns a struct of parameters that represent the GeoTransformation of the raster. The struct has the following schema: | v1.5.1 |
| [RS_Height](RS_Height.md) | Returns the height of the raster. | v1.5.0 |
| [RS_RasterToWorldCoord](RS_RasterToWorldCoord.md) | Returns the upper left X and Y coordinates of the given row and column of the given raster geometric units of the geo-referenced raster as a Point geometry. If any out of bounds values are given, t... | v1.5.1 |
| [RS_RasterToWorldCoordX](RS_RasterToWorldCoordX.md) | Returns the upper left X coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the X coordinate of the assu... | v1.5.0 |
| [RS_RasterToWorldCoordY](RS_RasterToWorldCoordY.md) | Returns the upper left Y coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the Y coordinate of the assu... | v1.5.0 |
| [RS_Rotation](RS_Rotation.md) | Returns the uniform rotation of the raster in radian. | v1.5.1 |
| [RS_ScaleX](RS_ScaleX.md) | Returns the pixel width of the raster in CRS units. | v1.5.0 |
| [RS_ScaleY](RS_ScaleY.md) | Returns the pixel height of the raster in CRS units. | v1.5.0 |
| [RS_SkewX](RS_SkewX.md) | Returns the X skew or rotation parameter. | v1.5.0 |
| [RS_SkewY](RS_SkewY.md) | Returns the Y skew or rotation parameter. | v1.5.0 |
| [RS_UpperLeftX](RS_UpperLeftX.md) | Returns the X coordinate of the upper-left corner of the raster. | v1.5.0 |
| [RS_UpperLeftY](RS_UpperLeftY.md) | Returns the Y coordinate of the upper-left corner of the raster. | v1.5.0 |
| [RS_Width](RS_Width.md) | Returns the width of the raster. | v1.5.0 |
| [RS_WorldToRasterCoord](RS_WorldToRasterCoord.md) | Returns the grid coordinate of the given world coordinates as a Point. | v1.5.0 |
| [RS_WorldToRasterCoordX](RS_WorldToRasterCoordX.md) | Returns the X coordinate of the grid coordinate of the given world coordinates as an integer. | v1.5.0 |
| [RS_WorldToRasterCoordY](RS_WorldToRasterCoordY.md) | Returns the Y coordinate of the grid coordinate of the given world coordinates as an integer. | v1.5.0 |
