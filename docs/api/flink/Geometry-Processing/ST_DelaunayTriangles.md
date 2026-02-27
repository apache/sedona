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

# ST_DelaunayTriangles

Introduction: This function computes the [Delaunay triangulation](https://en.wikipedia.org/wiki/Delaunay_triangulation) for the set of vertices in the input geometry. An optional `tolerance` parameter allows snapping nearby input vertices together prior to triangulation and can improve robustness in certain scenarios by handling near-coincident vertices. The default for  `tolerance` is 0. The Delaunay triangulation geometry is bounded by the convex hull of the input vertex set.

The output geometry representation depends on the provided `flag`:

- `0` - a GeometryCollection of triangular Polygons (default option)
- `1` - a MultiLinestring of the edges of the triangulation

Format:

`ST_DelaunayTriangles(geometry: Geometry)`

`ST_DelaunayTriangles(geometry: Geometry, tolerance: Double)`

`ST_DelaunayTriangles(geometry: Geometry, tolerance: Double, flag: Integer)`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_DelaunayTriangles(
        ST_GeomFromWKT('POLYGON ((10 10, 15 30, 20 25, 25 35, 30 20, 40 30, 50 10, 45 5, 35 15, 30 5, 25 15, 20 10, 15 20, 10 10))')
)
```

Output:

```
GEOMETRYCOLLECTION (POLYGON ((15 30, 10 10, 15 20, 15 30)), POLYGON ((15 30, 15 20, 20 25, 15 30)), POLYGON ((15 30, 20 25, 25 35, 15 30)), POLYGON ((25 35, 20 25, 30 20, 25 35)), POLYGON ((25 35, 30 20, 40 30, 25 35)), POLYGON ((40 30, 30 20, 35 15, 40 30)), POLYGON ((40 30, 35 15, 50 10, 40 30)), POLYGON ((50 10, 35 15, 45 5, 50 10)), POLYGON ((30 5, 45 5, 35 15, 30 5)), POLYGON ((30 5, 35 15, 25 15, 30 5)), POLYGON ((30 5, 25 15, 20 10, 30 5)), POLYGON ((30 5, 20 10, 10 10, 30 5)), POLYGON ((10 10, 20 10, 15 20, 10 10)), POLYGON ((15 20, 20 10, 25 15, 15 20)), POLYGON ((15 20, 25 15, 20 25, 15 20)), POLYGON ((20 25, 25 15, 30 20, 20 25)), POLYGON ((30 20, 25 15, 35 15, 30 20)))
```
