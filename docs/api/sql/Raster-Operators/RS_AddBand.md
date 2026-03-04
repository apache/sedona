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

# RS_AddBand

Introduction: Adds a new band to a raster `toRaster` at a specified index `toRasterIndex`. The new band's values are copied from `fromRaster` at a specified band index `fromBand`.
If no `toRasterIndex` is provided, the new band is appended to the end of `toRaster`. If no `fromBand` is specified, band `1` from `fromRaster` is copied by default.

!!!Note
    IllegalArgumentException will be thrown in these cases:

    - The provided Rasters, `toRaster` & `fromRaster` don't have same shape.
    - The provided `fromBand` is not in `fromRaster`.
    - The provided `toRasterIndex` is not in or at end of `toRaster`.

Format:

```
RS_AddBand(toRaster: Raster, fromRaster: Raster, fromBand: Integer = 1, toRasterIndex: Integer = at_end)
```

```
RS_AddBand(toRaster: Raster, fromRaster: Raster, fromBand: Integer = 1)
```

```
RS_AddBand(toRaster: Raster, fromRaster: Raster)
```

Return type: `Raster`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_AddBand(raster1, raster2, 2, 1) FROM rasters
```

Output:

```
GridCoverage2D["g...
```
