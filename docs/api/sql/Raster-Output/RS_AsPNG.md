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

# RS_AsPNG

Introduction: Returns a PNG byte array, that can be written to raster files as PNGs using the Sedona raster data source writer. This function can only accept pixel data type of unsigned integer. PNG can accept 1 or 3 bands of data from the raster, refer to [RS_Band](../Raster-Band-Accessors/RS_Band.md) for more details.

!!!Note
    Raster having `UNSIGNED_8BITS` pixel data type will have range of `0 - 255`, whereas rasters having `UNSIGNED_16BITS` pixel data type will have range of `0 - 65535`. If provided pixel value is greater than either `255` for `UNSIGNED_8BITS` or `65535` for `UNSIGNED_16BITS`, then the extra bit will be truncated.

!!!Note
    Raster that have float or double values will result in an empty byte array. PNG only accepts Integer values, if you want to write your raster to an image file, please refer to [RS_AsGeoTiff](RS_AsGeoTiff.md).

Format:

`RS_AsPNG(raster: Raster)`

`RS_AsPNG(raster: Raster, maxWidth: Integer)`

Return type: `Binary`

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
