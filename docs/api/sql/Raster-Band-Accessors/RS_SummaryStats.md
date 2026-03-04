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

# RS_SummaryStats

Introduction: Returns summary statistic for a particular band based on the `statType` parameter. The function defaults to band index of `1` when `band` is not specified and excludes noDataValue if `excludeNoDataValue` is not specified.

`statType` parameter takes the following strings:

- `count`: Total count of all pixels in the specified band
- `sum`: Sum of all pixel values in the specified band
- `mean`: Mean value of all pixel values in the specified band
- `stddev`: Standard deviation of all pixels in the specified band
- `min`: Minimum pixel value in the specified band
- `max`: Maximum pixel value in the specified band

!!!Note
    If excludeNoDataValue is set `true` then it will only count pixels with value not equal to the nodata value of the raster.
    Set excludeNoDataValue to `false` to get count of all pixels in raster.

Formats:

`RS_SummaryStats(raster: Raster, statType: String, band: Integer = 1, excludeNoDataValue: Boolean = true)`

`RS_SummaryStats(raster: Raster, statType: String, band: Integer = 1)`

`RS_SummaryStats(raster: Raster, statType: String)`

Return type: `Double`

Since: `v1.6.0`

SQL Example

```sql
SELECT RS_SummaryStats(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), "stddev", 1, false)
```

Output:

```
9.4678403028357
```
