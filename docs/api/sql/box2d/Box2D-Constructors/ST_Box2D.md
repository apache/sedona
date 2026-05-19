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

# ST_Box2D

Introduction: Return the planar bounding box of a Geometry as a typed `Box2D` value (four doubles: `xmin`, `ymin`, `xmax`, `ymax`).

`ST_Box2D` is the typed counterpart to [ST_Envelope](../../Bounding-Box-Functions/ST_Envelope.md). `ST_Envelope` returns the envelope as a `Geometry` — typically a polygon, but JTS may return a `Point` or `LineString` for degenerate inputs. `ST_Box2D` always returns a `Box2D` value that serialises to a struct of four non-nullable doubles and round-trips through Parquet without WKB overhead.

Format: `ST_Box2D(geom: Geometry)`

Return type: `Box2D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_Box2D(ST_GeomFromWKT('LINESTRING (0 0, 10 20)')))
```

Output:

```
BOX(0.0 0.0, 10.0 20.0)
```

`ST_Box2D` is also produced by the SQL cast `CAST(geom AS box2d)` — see [Type conversion](../Box2D-Functions.md#type-conversion).

Returns `NULL` for `NULL` or empty geometry input.
