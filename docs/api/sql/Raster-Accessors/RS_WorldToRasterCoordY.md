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

# RS_WorldToRasterCoordY

Introduction: Returns the Y coordinate of the grid coordinate of the given world coordinates as an integer.

Format:

`RS_WorldToRasterCoordY(raster: Raster, point: Geometry)`

`RS_WorldToRasterCoordY(raster: Raster, x: Double, y: Double)`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_WorldToRasterCoordY(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), ST_GeomFromText('POINT (-50 50)'));
```

Output:

```
2
```

SQL Example

```sql
SELECT RS_WorldToRasterCoordY(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0), -50, 49);
```

Output:

```
3
```

!!!Tip
    For non-skewed rasters, you can provide any value for longitude and the intended value of world latitude, to get the desired answer
