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

# ST_BoxIntersects

Introduction: Closed-interval bbox intersection over two `Box2D` arguments. Returns `true` if the boxes overlap on **both** the X and Y axes. Matches PostGIS `&&` on `box2d`. Edge- and corner-touching boxes count as intersecting.

The predicate is symmetric — `ST_BoxIntersects(a, b)` and `ST_BoxIntersects(b, a)` return the same value.

Format: `ST_BoxIntersects(a: Box2D, b: Box2D)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_BoxIntersects(
    ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 10.0)),
    ST_MakeBox2D(ST_Point(5.0, 5.0), ST_Point(15.0, 15.0)))
```

Output:

```
true
```

## Optimization

`ST_BoxIntersects(box_col, lit_box)` is recognised by Sedona's spatial optimizer:

- **Filter pushdown.** When the column is a `Box2D` stored in GeoParquet, the predicate translates to Parquet row-group inequalities on the `xmin` / `ymin` / `xmax` / `ymax` leaves. See [Box2D filter pushdown](../../Optimizer.md#box2d-filter-pushdown).
- **Spatial join.** `ST_BoxIntersects(a, b)` between two `Box2D` columns is planned as a range or broadcast-index join. See [Box2D spatial join](../../Optimizer.md#box2d-spatial-join).

## Errors

Throws `IllegalArgumentException` if either argument has inverted bounds (`xmin > xmax` or `ymin > ymax`). Inverted-bound values are reserved for a future antimeridian-wraparound semantics; planar predicates have no defined meaning for them today.

Returns `NULL` if either argument is `NULL`.
