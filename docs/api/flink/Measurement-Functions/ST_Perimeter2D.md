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

# ST_Perimeter2D

Introduction: This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries). For other types, it returns 0. To measure lines, use [ST_Length](ST_Length.md).

To get the perimeter in meters, set `use_spheroid` to `true`. This calculates the geodesic perimeter using the WGS84 spheroid. When using `use_spheroid`, the `lenient` parameter defaults to true, assuming the geometry uses EPSG:4326. To throw an exception instead, set `lenient` to `false`.

!!!Info
    This function is an alias for [ST_Perimeter](ST_Perimeter.md).

Format:

`ST_Perimeter2D(geom: Geometry)`

`ST_Perimeter2D(geom: Geometry, use_spheroid: Boolean)`

`ST_Perimeter2D(geom: Geometry, use_spheroid: Boolean, lenient: Boolean = True)`

Return type: `Double`

Since: `v1.7.1`

SQL Example:

```sql
SELECT ST_Perimeter2D(
        ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')
)
```

Output:

```
20.0
```

SQL Example:

```sql
SELECT ST_Perimeter2D(
        ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))', 4326),
        true, false
)
```

Output:

```
2216860.5497177234
```
