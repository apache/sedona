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

# ST_Contains

Introduction: Return true if A fully contains B. Polymorphic over input type:

- `(Geometry, Geometry)` — topological containment via JTS.
- `(Geography, Geography)` — topological containment via S2.
- `(Box2D, Box2D)` — closed-interval bbox containment on both axes. Matches PostGIS `~` on `box2d`. Throws `IllegalArgumentException` on inverted bounds (`xmin > xmax` or `ymin > ymax`).
- `(Box3D, Box3D)` — closed-interval bbox containment on all three axes. Equal boxes contain each other. Throws on inverted bounds on any axis.

![ST_Contains returning true](../../../image/ST_Contains/ST_Contains_true.svg "ST_Contains returning true")
![ST_Contains returning false](../../../image/ST_Contains/ST_Contains_false.svg "ST_Contains returning false")

Format:

- `ST_Contains(A: Geometry, B: Geometry)`
- `ST_Contains(A: Geography, B: Geography)`
- `ST_Contains(A: Box2D, B: Box2D)` (Since `v1.9.1`)
- `ST_Contains(A: Box3D, B: Box3D)` (Since `v1.9.1`)

Return type: `Boolean`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Contains(ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'), ST_GeomFromWKT('POINT(174 149)'))
```

Output:

```
false
```

Box2D example:

```sql
SELECT ST_Contains(
    ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 10.0)),
    ST_MakeBox2D(ST_Point(2.0, 2.0), ST_Point(5.0, 5.0)))
```

Output:

```
true
```

## Box2D optimization

`ST_Contains(box_col, lit_box)` over a `Box2D` column and a literal `Box2D` (and the reversed form) is recognised by Sedona's spatial optimizer:

- **Filter pushdown.** When the column is a `Box2D` stored in GeoParquet, the predicate translates to Parquet row-group inequalities on the `xmin` / `ymin` / `xmax` / `ymax` leaves. See [Box2D filter pushdown](../Optimizer.md#box2d-filter-pushdown).
- **Spatial join.** `ST_Contains(a, b)` between two `Box2D` columns is planned as a range or broadcast-index join with `SpatialPredicate.COVERS` semantics (closed-interval containment — JTS `contains`, strict-interior, would reject edge-sharing pairs). See [Box2D spatial join](../Optimizer.md#box2d-spatial-join).
