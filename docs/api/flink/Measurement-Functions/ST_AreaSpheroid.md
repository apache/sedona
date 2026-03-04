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

# ST_AreaSpheroid

Introduction: Return the geodesic area of A using WGS84 spheroid. Unit is meter. Works better for large geometries (country level) compared to `ST_Area` + `ST_Transform`. It is equivalent to PostGIS `ST_Area(geography, use_spheroid=true)` function and produces nearly identical results.

Geometry must be in EPSG:4326 (WGS84) projection and must be in ==lon/lat== order. You can use ==ST_FlipCoordinates== to swap lat and lon.

!!!note
    By default, this function uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

Format: `ST_AreaSpheroid (A: Geometry)`

Return type: `Double`

Since: `v1.4.1`

Example:

```sql
SELECT ST_AreaSpheroid(ST_GeomFromWKT('Polygon ((34 35, 28 30, 25 34, 34 35))'))
```

Output:

```
201824850811.76245
```
