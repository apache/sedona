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

# RS_ReplaceBandNoDataValue

Introduction: Moves the no-data value of a band to `noDataValue`, carrying the no-data pixels with it. Every pixel of the band that holds the current no-data value is rewritten to `noDataValue`, which then becomes the band's no-data value, so the same pixels read as no-data before and after the call. [RS_SetBandNoDataValue](RS_SetBandNoDataValue.md), by contrast, changes only the declared value.

A pixel holds the current no-data value when it compares equal to it numerically: `-0.0` matches a `0.0` no-data value, and any `NaN` matches a `NaN` no-data value. Other bands are unchanged.

The band must already have a no-data value; otherwise there is nothing to replace and an `IllegalArgumentException` is thrown. A `null` raster, band index or `noDataValue` returns `null`.

Format:

```
RS_ReplaceBandNoDataValue(raster: Raster, bandIndex: Integer, noDataValue: Double)
```

Return type: `Raster`

Since: `v2.0.0`

SQL Example

A band whose no-data value is `0` keeps the same pixels as no-data after its no-data value moves to `-9999`, so the count of valid pixels is unchanged:

```sql
SELECT RS_Count(
        RS_ReplaceBandNoDataValue(
            RS_AddBandFromArray(
                RS_MakeEmptyRaster(1, 'd', 2, 2, 0, 2, 1),
                array(0d, 1d, 0d, 2d), 1, 0d),
            1, -9999),
        1, true)
```

Output:

```
2
```
