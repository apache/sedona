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

# ST_LengthSpheroid

Introduction: Return the geodesic perimeter of A using WGS84 spheroid. Unit is meter. Works better for large geometries (country level) compared to `ST_Length` + `ST_Transform`. It is equivalent to PostGIS `ST_Length(geography, use_spheroid=true)` and `ST_LengthSpheroid` function and produces nearly identical results.

Geometry must be in EPSG:4326 (WGS84) projection and must be in ==lon/lat== order. You can use ==ST_FlipCoordinates== to swap lat and lon.

!!!note
    By default, this function uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

!!!Warning
    Since `v1.7.0`, this function only supports LineString, MultiLineString, and GeometryCollections containing linear geometries. Use [ST_Perimeter](ST_Perimeter.md) for polygons.

Format: `ST_LengthSpheroid (A: Geometry)`

Return type: `Double`

Since: `v1.4.1`

SQL Example

```sql
SELECT ST_LengthSpheroid(ST_GeomFromWKT('LINESTRING (0 0, 2 0)'))
```

Output:

```
222638.98158654713
```
