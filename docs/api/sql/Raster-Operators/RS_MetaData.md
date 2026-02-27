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

# RS_MetaData

Introduction: Returns the metadata of the raster as a struct. The struct has the following schema:

- upperLeftX: upper left x coordinate of the raster, in terms of CRS units
- upperLeftX: upper left y coordinate of the raster, in terms of CRS units
- gridWidth: width of the raster, in terms of pixels
- gridHeight: height of the raster, in terms of pixels
- scaleX: ScaleX: the scaling factor in the x direction
- scaleY: ScaleY: the scaling factor in the y direction
- skewX: skew in x direction (rotation x)
- skewY: skew in y direction (rotation y)
- srid: srid of the raster
- numSampleDimensions: number of bands
- tileWidth: (Since `v1.6.1`) width of tiles in the raster
- tileHeight: (Since `v1.6.1`) height of tiles in the raster

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

`tileWidth` and `tileHeight` are available since `v1.6.1`, they are the dimensions of the tiles in the raster. For example,
rasters written by `RS_FromGeoTiff` uses the tiling scheme of the loaded GeoTIFF file. For rasters that has only 1 tile,
`tileWidth` and `tileHeight` will be equal to `gridWidth` and `gridHeight` respectively.

Format: `RS_MetaData (raster: Raster)`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_MetaData(raster) FROM raster_table
```

Output:

```
{-1.3095817809482181E7, 4021262.7487925636, 512, 517, 72.32861272132695, -72.32861272132695, 0.0, 0.0, 3857, 1, 256, 256}
```
