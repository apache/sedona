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

# Raster Constructors

These functions create raster objects from various file formats or from scratch.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_FromArcInfoAsciiGrid](RS_FromArcInfoAsciiGrid.md) | Returns a raster geometry from an Arc Info Ascii Grid file. | v1.4.0 |
| [RS_FromGeoTiff](RS_FromGeoTiff.md) | Returns a raster geometry from a GeoTiff file. | v1.4.0 |
| [RS_FromNetCDF](RS_FromNetCDF.md) | Returns a raster geometry representing the given record variable short name from a NetCDF file. This API reads the array data of the record variable *in memory* along with all its dimensions Since ... | v1.5.1 |
| [RS_MakeEmptyRaster](RS_MakeEmptyRaster.md) | Returns an empty raster geometry. Every band in the raster is initialized to `0.0`. | v1.5.0 |
| [RS_MakeRaster](RS_MakeRaster.md) | Creates a raster from the given array of pixel values. The width, height, geo-reference information, and the CRS will be taken from the given reference raster. The data type of the resulting raster... | v1.6.0 |
| [RS_NetCDFInfo](RS_NetCDFInfo.md) | Returns a string containing names of the variables in a given netCDF file along with its dimensions. |  |
