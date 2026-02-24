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

# ST_GeogToGeometry

Introduction:

This function constructs a planar Geometry object from a Geography. While Sedona makes every effort to preserve the original spatial object, the conversion is not always exact because Geography and Geometry have different underlying models:

* Geography represents shapes on the Earth’s surface (spherical).
* Geometry represents shapes on a flat, Euclidean plane.

This difference can cause certain ambiguities during conversion. For example:

* A polygon in Geography always refers to the region on the Earth’s surface that the ring encloses. When converted to Geometry, however, it becomes unclear whether the polygon is intended to represent the “inside” or its complement (the “outside”) on the sphere.
* Long edges that cross the antimeridian or cover poles may also be represented differently once projected into planar space.

In practice, Sedona preserves the coordinates and ring orientation as closely as possible, but you should be aware that some topological properties (e.g., area, distance) may not match exactly after conversion.

Sedona does not validate or enforce the SRID of the input Geography. Whatever SRID is attached to the Geography will be carried over to the resulting Geometry, even if it is not appropriate for planar interpretation. It is the user’s responsibility to ensure that the Geography’s SRID is meaningful in the target Geometry context.

Format:

`ST_GeogToGeometry (geog: Geography)`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeogToGeometry(ST_GeogFromWKT('MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))', 4326))
```

Output:

```
MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))
```
