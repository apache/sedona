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

# ST_Expand

Introduction: Expand a `Box2D` outward by a per-axis or uniform delta. The expansion is applied symmetrically on each side (e.g., `dx = 1` widens the box by `1` on the left and `1` on the right). Negative deltas shrink the box; if they would invert it (`xmin > xmax` or `ymin > ymax`) the inverted box is returned as-is — callers can detect the degenerate result via the accessor functions.

This is the `Box2D` overload of [ST_Expand](../../Bounding-Box-Functions/ST_Expand.md), which also accepts a `Geometry`.

Format:

`ST_Expand(box: Box2D, uniformDelta: Double)`

`ST_Expand(box: Box2D, deltaX: Double, deltaY: Double)`

Return type: `Box2D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_Expand(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 20.0)), 1.0))
```

Output:

```
BOX(-1.0 -1.0, 11.0 21.0)
```

Per-axis form:

```sql
SELECT ST_AsText(ST_Expand(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 20.0)), 1.0, 2.0))
```

Output:

```
BOX(-1.0 -2.0, 11.0 22.0)
```

Returns `NULL` on `NULL` input.
