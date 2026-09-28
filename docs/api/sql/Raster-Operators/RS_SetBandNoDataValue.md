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

# RS_SetBandNoDataValue

Introduction: This sets the no data value for a specified band in the raster. If the band index is not provided, band 1 is assumed by default. Passing a `null` value for `noDataValue` will remove the no data value and that will ensure all pixels are included in functions rather than excluded as no data.

Only the band's metadata changes; pixel values are left as they are. To move the no-data value while carrying the existing no-data pixels over to the new value, use [RS_ReplaceBandNoDataValue](RS_ReplaceBandNoDataValue.md).

!!!Note
    Since `v2.0.0`, the four-argument form `RS_SetBandNoDataValue(raster, bandIndex, noDataValue, replace)` is removed. Replace `RS_SetBandNoDataValue(raster, bandIndex, noDataValue, true)` with `RS_ReplaceBandNoDataValue(raster, bandIndex, noDataValue)`.

Format:

```
RS_SetBandNoDataValue(raster: Raster, bandIndex: Integer = 1, noDataValue: Double)
```

Return type: `Raster`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_BandNoDataValue(
        RS_SetBandNoDataValue(
            RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0),
            -999
            )
        )
```

Output:

```
-999
```

SQL Example

`NaN` can be used as the no data value of a floating point band. Every `NaN` pixel then counts as no data. Setting `NaN` on an integer band throws an `IllegalArgumentException`.

```sql
SELECT RS_BandNoDataValue(
        RS_SetBandNoDataValue(
            RS_MakeEmptyRaster(1, 'F', 20, 20, 2, 22, 1),
            double('NaN')
            )
        )
```

Output:

```
NaN
```
