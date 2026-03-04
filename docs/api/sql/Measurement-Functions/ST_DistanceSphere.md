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

# ST_DistanceSphere

Introduction: Return the haversine / great-circle distance of A using a given earth radius (default radius: 6371008.0). Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that cover large regions such as continents or the entire planet. It is equivalent to PostGIS `ST_Distance(geography, use_spheroid=false)` and `ST_DistanceSphere` function and produces nearly identical results. It provides faster but less accurate result compared to `ST_DistanceSpheroid`.

Geometry must be in EPSG:4326 (WGS84) projection and must be in ==lon/lat== order. You can use ==ST_FlipCoordinates== to swap lat and lon. For non-point data, we first take the centroids of both geometries and then compute the distance.

!!!note
    By default, this function uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

Format: `ST_DistanceSphere (A: Geometry)`

Return type: `Double`

Since: `v1.4.1`

SQL Example

```sql
SELECT ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'))
```

Output:

```
543796.9506134904
```

SQL Example

```sql
SELECT ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'), 6378137.0)
```

Output:

```
544405.4459192449
```
