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

# RS_SummaryStatsAll

Introduction: Returns summary stats struct consisting of count, sum, mean, stddev, min, max for a given band in raster. If band is not specified then it defaults to `1`.

!!!Note
    If excludeNoDataValue is set `true` then it will only count pixels with value not equal to the nodata value of the raster.
    Set excludeNoDataValue to `false` to get count of all pixels in raster.

!!!Note
    If the mentioned band index doesn't exist, this will throw an `IllegalArgumentException`.

Formats:

`RS_SummaryStatsAll(raster: Raster, band: Integer = 1, excludeNoDataValue: Boolean = true)`

`RS_SummaryStatsAll(raster: Raster, band: Integer = 1)`

`RS_SummaryStatsAll(raster: Raster)`

Return type: `Struct<count: Long, sum: Double, mean: Double, stddev: Double, min: Double, max: Double>`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_SummaryStatsAll(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), 1, false)
```

Output:

```
{25.0, 204.0, 8.16, 9.4678403028357, 0.0, 25.0}
```

SQL Example

```sql
SELECT RS_SummaryStatsAll(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), 1)
```

Output:

```
{14.0, 204.0, 14.571428571428571, 11.509091348732502, 1.0, 25.0}
```
