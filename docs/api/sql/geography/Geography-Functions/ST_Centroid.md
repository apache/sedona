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

# ST_Centroid

Introduction: Returns the spherical centroid of a geography as a Geography point, computed on the sphere using S2:

- **Polygon / MultiPolygon** — area-weighted centroid via `S2Polygon.getCentroid()`.
- **LineString / MultiLineString** — length-weighted centroid via `S2Polyline.getCentroid()`.
- **Point / MultiPoint** — mean of the unit vectors.
- **GeographyCollection** — recursive weighted sum across the children.

The result is the unit-length centroid on the sphere. Unlike a planar (lon/lat) centroid, it is correct for antimeridian-crossing and high-latitude geographies. As with JTS for non-convex shapes, the centroid may lie outside the input geometry. Returns `NULL` when the centroid is undefined (empty geometry, or antipodal points whose unit vectors cancel).

Format:

`ST_Centroid (A: Geography)`

Return type: `Geography`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsEWKT(ST_Centroid(ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')));
```

Output (small `O(d²/R²)` spherical correction vs the planar `(1 1)`):

```
POINT (1 1)
```

For an antimeridian-crossing polygon, the spherical centroid stays on the antimeridian instead of jumping to the opposite side of the planet, which a planar centroid would do:

```sql
SELECT ST_AsEWKT(ST_Centroid(ST_GeogFromWKT('POLYGON ((170 -1, -170 -1, -170 1, 170 1, 170 -1))')));
-- result: POINT near (180, 0) (or (-180, 0)), NOT (0, 0)
```
