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

# RS_Contains

Introduction: Returns true if the geometry or raster on the left side contains the geometry or raster on the right side.
The convex hull of the raster is considered in the test.

The rules for testing spatial relationship is the same as `RS_Intersects`.

Format:

`RS_Contains(raster: Raster, geom: Geometry)`

`RS_Contains(geom: Geometry, raster: Raster)`

`RS_Contains(raster0: Raster, raster1: Raster)`

Return type: `Boolean`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((5 5, 5 10, 10 10, 10 5, 5 5))')) rast_geom,
    RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), RS_MakeEmptyRaster(1, 10, 10, 2, 22, 1)) rast_rast
```

Output:

```
+---------+---------+
|rast_geom|rast_rast|
+---------+---------+
|     true|     true|
+---------+---------+
```
