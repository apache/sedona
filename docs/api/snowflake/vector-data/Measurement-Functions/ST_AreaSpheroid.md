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

Introduction: Return the geodesic area of A using WGS84 spheroid. Unit is square meter. Works better for large geometries (country level) compared to `ST_Area` + `ST_Transform`. It is equivalent to PostGIS `ST_Area(geography, use_spheroid=true)` function and produces nearly identical results.

Geometry must be in EPSG:4326 (WGS84) projection and must be in ==lat/lon== order. You can use ==ST_FlipCoordinates== to swap lat and lon.

Format: `ST_AreaSpheroid (A:geometry)`

Return type: `Double`

SQL example:

```sql
SELECT ST_AreaSpheroid(ST_GeomFromWKT('Polygon ((35 34, 30 28, 34 25, 35 34))'))
```

Output: `201824850811.76245`
