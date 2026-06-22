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

* Geography represents shapes on the Earth's surface (spherical).
* Geometry represents shapes on a flat, Euclidean plane.

Because geography vertices are stored as points on the sphere, converting back to planar coordinates can introduce small floating-point differences. Wrap the result with [ST_ReducePrecision](../Geometry-Processing/ST_ReducePrecision.md) if you need a fixed number of decimals.

Sedona does not validate or enforce the SRID of the input Geography. Whatever SRID is attached to the Geography will be carried over to the resulting Geometry.

Format:

`ST_GeogToGeometry (geog: Geography)`

Return type: `Geometry`

Since: `v1.9.1`

Example:

```sql
SELECT ST_ReducePrecision(ST_GeogToGeometry(ST_GeogFromWKT('MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))', 4326)), 6)
```

Output:

```
MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))
```
