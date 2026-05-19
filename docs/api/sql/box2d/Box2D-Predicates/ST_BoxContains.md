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

# ST_BoxContains

Introduction: Closed-interval bbox containment over two `Box2D` arguments. Returns `true` if argument `a` fully contains argument `b` on **both** axes. Matches PostGIS `~` on `box2d`. Equal boxes contain each other; edge- and corner-shared boxes count as contained.

The predicate is asymmetric: `ST_BoxContains(a, b)` is not generally equal to `ST_BoxContains(b, a)`.

Format: `ST_BoxContains(a: Box2D, b: Box2D)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_BoxContains(
    ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 10.0)),
    ST_MakeBox2D(ST_Point(2.0, 2.0), ST_Point(8.0, 8.0)))
```

Output:

```
true
```

## Optimization

`ST_BoxContains(box_col, lit_box)` (and the reversed form) is recognised by Sedona's spatial optimizer:

- **Filter pushdown.** Translates to Parquet row-group inequalities on the `Box2D` column's `xmin` / `ymin` / `xmax` / `ymax` leaves. See [Box2D filter pushdown](../../Optimizer.md#box2d-filter-pushdown).
- **Spatial join.** Routes through the range / broadcast-index join executors with `SpatialPredicate.COVERS` semantics — JTS `covers` matches `ST_BoxContains`'s closed-interval contract (JTS `contains`, strict-interior, would reject edge-sharing pairs). See [Box2D spatial join](../../Optimizer.md#box2d-spatial-join).

## Errors

Throws `IllegalArgumentException` if either argument has inverted bounds. Inverted-bound values are reserved for a future antimeridian-wraparound semantics.

Returns `NULL` if either argument is `NULL`.
