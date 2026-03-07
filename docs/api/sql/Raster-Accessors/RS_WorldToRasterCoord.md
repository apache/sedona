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

# RS_WorldToRasterCoord

Introduction: Returns the grid coordinate of the given world coordinates as a Point.

![RS_WorldToRasterCoord](../../../image/RS_WorldToRasterCoord/RS_WorldToRasterCoord.svg "RS_WorldToRasterCoord")

Format:

`RS_WorldToRasterCoord(raster: Raster, point: Geometry)`

`RS_WorldToRasterCoord(raster: Raster, x: Double, y: Point)`

Return type: `Geometry`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_WorldToRasterCoord(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326), -53, 51) from rasters;
```

Output:

```
POINT (1 1)
```

SQL Example

```sql
SELECT RS_WorldToRasterCoord(ST_MakeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326), ST_GeomFromText('POINT (-52 51)')) from rasters;
```

Output:

```
POINT (2 1)
```

!!!Note
    If the given geometry point is not in the same CRS as the given raster, the given geometry will be transformed to the given raster's CRS. You can use [ST_Transform](../Spatial-Reference-System/ST_Transform.md) to transform the geometry beforehand.
