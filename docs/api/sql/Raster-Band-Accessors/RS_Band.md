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

# RS_Band

Introduction: Returns a new raster consisting 1 or more bands of an existing raster. It can build new rasters from
existing ones, export only selected bands from a multiband raster, or rearrange the order of bands in a raster dataset.

Format:

`RS_Band(raster: Raster, bands: ARRAY[Integer])`

Return type: `Raster`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_NumBands(
        RS_Band(
            RS_AddBandFromArray(
                RS_MakeEmptyRaster(2, 5, 5, 3, -215, 2, -2, 2, 2, 0),
                Array(16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215, 229, 241, 248, 249),
                1, 0d
            ), Array(1,1,1)
        )
    )
```

Output:

```
3
```
