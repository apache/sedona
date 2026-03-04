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

# ST_DistanceSpheroid

Introduction: Return the geodesic distance of A using WGS84 spheroid. Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that cover large regions such as continents or the entire planet. It is equivalent to PostGIS `ST_Distance(geography, use_spheroid=true)` and `ST_DistanceSpheroid` function and produces nearly identical results. It provides slower but more accurate result compared to `ST_DistanceSphere`.

Geometry must be in EPSG:4326 (WGS84) projection and must be in ==lat/lon== order. You can use ==ST_FlipCoordinates== to swap lat and lon. For non-point data, we first take the centroids of both geometries and then compute the distance.

Format: `ST_DistanceSpheroid (A:geometry)`

Return type: `Double`

SQL example:

```sql
SELECT ST_DistanceSpheroid(ST_GeomFromWKT('POINT (51.3168 -0.56)'), ST_GeomFromWKT('POINT (55.9533 -3.1883)'))
```

Output: `544430.9411996207`
