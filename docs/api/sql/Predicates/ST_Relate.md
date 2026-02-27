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

# ST_Relate

Introduction: The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relationship between the two input geometry objects.

The second variant of the function evaluates whether the two input geometries satisfy a specific spatial relationship defined by the provided `intersectionMatrix` pattern.

!!!Note
    It is important to note that this function is not optimized for use in spatial join operations. Certain DE-9IM relationships can hold true for geometries that do not intersect or are disjoint. As a result, it is recommended to utilize other dedicated spatial functions specifically optimized for spatial join processing.

Format:

`ST_Relate(geom1: Geometry, geom2: Geometry)`

`ST_Relate(geom1: Geometry, geom2: Geometry, intersectionMatrix: String)`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_Relate(
        ST_GeomFromWKT('LINESTRING (1 1, 5 5)'),
        ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))')
)
```

Output:

```
1010F0212
```

SQL Example

```sql
SELECT ST_Relate(
        ST_GeomFromWKT('LINESTRING (1 1, 5 5)'),
        ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'),
       "1010F0212"
)
```

Output:

```
true
```
