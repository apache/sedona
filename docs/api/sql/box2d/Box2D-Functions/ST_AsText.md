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

# ST_AsText

Introduction: Return the `BOX(xmin ymin, xmax ymax)` text representation of a `Box2D` value. The format matches PostGIS's text representation of the `box2d` type and is intended for display and round-tripping with PostGIS-compatible tooling — not as a parseable round-trip into Sedona (use [ST_MakeBox2D](../Box2D-Constructors/ST_MakeBox2D.md) for that).

This is the `Box2D` overload of `ST_AsText`; the [Geometry variant](../../Geometry-Output/ST_AsText.md) emits standard WKT.

Format: `ST_AsText(box: Box2D)`

Return type: `String`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 20.0)))
```

Output:

```
BOX(0.0 0.0, 10.0 20.0)
```

Returns `NULL` on `NULL` input.
