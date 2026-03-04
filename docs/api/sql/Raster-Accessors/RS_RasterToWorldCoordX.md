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

# RS_RasterToWorldCoordX

Introduction: Returns the upper left X coordinate of the given row and column of the given raster geometric units of the geo-referenced raster. If any out of bounds values are given, the X coordinate of the assumed point considering existing raster pixel size and skew values will be returned.

Format: `RS_RasterToWorldCoordX(raster: Raster, colX: Integer, rowY: Integer)`

Return type: `Double`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_RasterToWorldCoordX(ST_MakeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326), 1, 1) from rasters
```

Output:

```
-123
```
