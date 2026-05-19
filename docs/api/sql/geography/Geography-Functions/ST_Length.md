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

# ST_Length

Introduction: Returns the spherical length of a geography in meters, calculated on the sphere. The Earth is modeled as a sphere of radius `R = 6 371 008 m` (the authalic Earth radius). Each edge between successive vertices is interpreted as a great-circle arc; the per-edge arc-angles are summed and scaled by `R`.

Multi-linestrings sum the children's lengths; geography collections recurse and add up the lengths of their linear members. Returns `0.0` for non-linear geographies (points, polygons) and for `NULL`.

![ST_Length on a Geography on the sphere](../../../../image/ST_Length_geography/ST_Length_geography.svg "ST_Length on a Geography (sphere-native)")

If you specifically want the WGS84 ellipsoidal length (which differs from the spherical value by up to ~0.5 % depending on latitude), convert via `ST_GeogToGeometry` first and call the Geometry-typed `ST_Length(..., useSpheroid => true)` overload instead.

Format:

`ST_Length (A: Geography)`

Return type: `Double`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_Length(ST_GeogFromWKT('LINESTRING (0 0, 1 0)'));
```

Output (in meters):

```
111195.10117748393
```

The result is approximately 111.2 km — one degree of arc on a sphere of radius `R = 6 371 008 m`.
