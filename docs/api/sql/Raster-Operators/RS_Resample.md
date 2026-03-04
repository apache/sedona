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

# RS_Resample

Introduction:
Resamples a raster using a given resampling algorithm and new dimensions (width and height), a new grid corner to pivot the raster at (gridX and gridY) and a set of
georeferencing attributes (scaleX and scaleY).

RS_Resample also provides an option to pass a reference raster to draw the georeferencing attributes out of. However, the SRIDs of the input and reference raster must be same, otherwise RS_Resample throws an IllegalArgumentException.

For the purpose of resampling, width-height pair and scaleX-scaleY pair are mutually exclusive, meaning any one of them can be used at a time.

The `useScale` parameter controls whether to use width-height or scaleX-scaleY. If `useScale` is false, the provided `widthOrScale` and `heightOrScale` values will be floored to integers and considered as width and height respectively (floating point width and height are not allowed). Otherwise, they are considered as scaleX and scaleY respectively.

Currently, RS_Resample does not support skewed rasters, and hence even if a skewed reference raster is provided, its skew values are ignored. If the input raster is skewed, the output raster geometry and interpolation may be incorrect.

The default algorithm used for resampling is `NearestNeighbor`, and hence if a null, empty or invalid value of algorithm is provided, RS_Resample defaults to using `NearestNeighbor`. However, the algorithm parameter is non-optional.

Following are valid values for the algorithm parameter (Case-insensitive):

1. NearestNeighbor
2. Bilinear
3. Bicubic

!!!Tip
    If you just want to resize or rescale an input raster, you can use RS_Resample(raster: Raster, widthOrScale: Double, heightOrScale: Double, useScale: Boolean, algorithm: String)

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

Format:

```sql
RS_Resample(raster: Raster, widthOrScale: Double, heightOrScale: Double, gridX: Double, gridY: Double, useScale: Boolean, algorithm: String)
```

```sql
RS_Resample(raster: Raster, widthOrScale: Double, heightOrScale: Double, useScale: Boolean, algorithm: String)
```

```sql
RS_Resample(raster: Raster, referenceRaster: Raster, useScale: Boolean, algorithm: String)
```

Return type: `Raster`

Since: `v1.5.0`

SQL Example

```sql
WITH INPUT_RASTER AS (
 SELECT RS_AddBandFromArray(
    RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0),
    ARRAY(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10), 1, null) as rast
),
RESAMPLED_RASTER AS (
 SELECT RS_Resample(rast, 6, 5, 1, -1, false, null) as resample_rast from INPUT_RASTER
)
SELECT RS_AsMatrix(resample_rast) as rast_matrix, RS_Metadata(resample_rast) as rast_metadata from RESAMPLED_RASTER
```

Output:

```sql
| 1.0   1.0   2.0   3.0   3.0   5.0|
| 1.0   1.0   2.0   3.0   3.0   5.0|
| 4.0   4.0   5.0   6.0   6.0   9.0|
| 7.0   7.0   8.0   9.0   9.0  10.0|
| 7.0   7.0   8.0   9.0   9.0  10.0|

(-0.33333333333333326,0.19999999999999996,6,5,1.388888888888889,-1.24,0,0,0,1)
```

SQL Example

```sql
 WITH INPUT_RASTER AS (
   SELECT RS_AddBandFromArray(
    RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0),
    ARRAY(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10), 1, null) as rast
   ),
   RESAMPLED_RASTER AS (
    SELECT RS_Resample(rast, 1.2, -1.4, true, null) as resample_rast from INPUT_RASTER
   )
SELECT RS_AsMatrix(resample_rast) as rast_matrix, RS_Metadata(resample_rast) as rast_metadata from RESAMPLED_RASTER
```

Output:

```sql
|       NaN         NaN         NaN         NaN         NaN         NaN         NaN|
|       NaN    3.050000    3.650000    4.250000    5.160000    6.690000    7.200000|
|       NaN    5.150000    5.750000    6.350000    7.250000    8.750000    9.250000|
|       NaN    7.250000    7.850000    8.450000    9.070000    9.730000    9.950000|
|       NaN    7.400000    8.000000    8.600000    9.200000    9.800000   10.000000|

(0.0, 0.0, 7.0, 5.0, 1.2, -1.4, 0.0, 0.0, 0.0, 1.0)
```

SQL Example

```sql
WITH INPUT_RASTER AS (
    SELECT RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0), ARRAY(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10), 1, null) as rast
),
REF_RASTER AS (
    SELECT RS_MakeEmptyRaster(2, 'd', 6, 5, 1, -1, 1.2, -1.4, 0, 0, 0) as ref_rast
),
RESAMPLED_RASTER AS (
    SELECT RS_Resample(rast, ref_rast, true, null) as resample_rast from INPUT_RASTER, REF_RASTER
)
SELECT RS_AsMatrix(resample_rast) as rast_matrix, RS_Metadata(resample_rast) as rast_metadata from RESAMPLED_RASTER
```

Output:

```sql
| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|
| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|
| 4.0   4.0   5.0   6.0   6.0   9.0   9.0|
| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|
| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|

(-0.20000000298023224, 0.4000000059604645, 7.0, 5.0, 1.2, -1.4, 0.0, 0.0, 0.0, 1.0)
```
