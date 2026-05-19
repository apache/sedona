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

# ST_GeomFromBox2D

Introduction: Convert a `Box2D` to a closed rectangular polygon `Geometry`. Equivalent to PostGIS `box2d::geometry`. Degenerate boxes return the appropriate lower-dimensional geometry:

| Box2D shape                                                | Returned geometry |
| :---                                                       | :--- |
| Both axes have non-zero extent                             | `POLYGON` (closed rectangle) |
| One axis collapsed (e.g. `xmin == xmax`)                   | `LINESTRING` along the non-collapsed axis |
| Both axes collapsed (`xmin == xmax && ymin == ymax`)       | `POINT` |

Format: `ST_GeomFromBox2D(box: Box2D)`

Return type: `Geometry`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_GeomFromBox2D(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(2.0, 4.0))))
```

Output:

```
POLYGON ((0 0, 0 4, 2 4, 2 0, 0 0))
```

`ST_GeomFromBox2D` is also produced by the SQL cast `CAST(box AS geometry)` — see [Type conversion](../Box2D-Functions.md#type-conversion).

Returns `NULL` on `NULL` input.
