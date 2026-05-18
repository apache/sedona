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

# ST_DWithin

Introduction: Closed-interval planar distance test between two `Box2D` rectangles. Returns `true` if the minimum Euclidean distance between `a` and `b` is less than or equal to `distance`.

Overlapping or edge/corner-touching boxes have distance `0` and therefore match for any non-negative radius. This is the `Box2D` overload of `ST_DWithin`; the [Geometry input form](../../Predicates/ST_DWithin.md) and the [Geography input form](../../geography/Geography-Functions/ST_DWithin.md) handle the other types.

Format: `ST_DWithin(a: Box2D, b: Box2D, distance: Double)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_DWithin(
    ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 10.0)),
    ST_MakeBox2D(ST_Point(11.0, 0.0), ST_Point(12.0, 1.0)),
    1.0)
```

Output:

```
true
```

## Optimization

`ST_DWithin(box_a, box_b, distance)` between two `Box2D` columns routes through Sedona's distance-join planner: the broadcast-index path (with a hinted side) or the partition-based `DistanceJoinExec`. The Box2D values materialise as rectangular polygons at the join boundary, after which the existing distance-expansion + index-probe + refine pipeline runs unchanged. See [Box2D spatial join](../../Optimizer.md#box2d-spatial-join).

## Errors

Throws `IllegalArgumentException` if either box has inverted bounds (`xmin > xmax` or `ymin > ymax`). Inverted-bound values are reserved for a future antimeridian-wraparound semantics.

Returns `NULL` if any argument is `NULL`.
