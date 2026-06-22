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

# ST_Area

Introduction: Return the spherical area of a geography in square meters, calculated on the sphere. The Earth is modeled as a sphere of radius `R = 6 371 008 m` (the mean Earth radius), not the WGS84 ellipsoid; the result is the area of the polygon's interior on that sphere. Multi-polygons sum the children's areas and geography collections recurse. Returns `0.0` for non-areal geographies (points, linestrings) and for `NULL`.

![ST_Area on a Geography (sphere-native)](../../../image/ST_Area_geography/ST_Area_geography.svg "ST_Area on a Geography (sphere-native)")

If the input ring is wound in the orientation that would describe the rest of the planet, Sedona returns the smaller of the two regions, so the answer is always bounded by half the surface of the Earth. If you specifically want the WGS84 ellipsoidal value, convert via `ST_GeogToGeometry` first and use the geometry overload.

Format:

`ST_Area (geog: Geography)`

Return type: `Double`

Since: `v1.9.1`

SQL Example:

```sql
SELECT ST_Area(ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 4326))
```

Output:

```
4.945234327988249E10
```
