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

# RS_MakeRaster

Introduction: Creates a raster from the given array of pixel values. The width, height, geo-reference information, and
the CRS will be taken from the given reference raster. The data type of the resulting raster will be DOUBLE and the
number of bands of the resulting raster will be `data.length / (refRaster.width * refRaster.height)`.

Return type: `Raster`

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
