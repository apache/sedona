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

# ST_3DMakeBox

Introduction: Build a `Box3D` from two corner POINT Z geometries. Mirrors PostGIS's `ST_3DMakeBox`. The corners are taken verbatim — no swapping or validation of ordering — so inverted bounds are preserved as supplied. POINT inputs without a Z dimension contribute `z = 0`, matching PostGIS.

![ST_3DMakeBox builds a cuboid from two corner POINT Z geometries](../../../../image/box3d/st_3dmakebox.svg "ST_3DMakeBox: cuboid from two corner points")

Format: `ST_3DMakeBox(lowerLeft: Point, upperRight: Point)`

Return type: `Box3D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_3DMakeBox(ST_PointZ(1.0, 2.0, 3.0), ST_PointZ(4.0, 5.0, 6.0)))
```

Output:

```
BOX3D(1.0 2.0 3.0, 4.0 5.0 6.0)
```

Throws `IllegalArgumentException` if either argument is not a POINT. Returns `NULL` if either point is `NULL` or empty.
