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

Introduction: Return the PostGIS-style text representation of a `Box3D`: `BOX3D(xmin ymin zmin, xmax ymax zmax)`. This is the `Box3D` overload of `ST_AsText`; the [Geometry variant](../../Geometry-Output/ST_AsText.md) emits standard WKT.

The bounds are emitted exactly as stored — `ST_AsText` does not normalize or reorder the corners. The text is not WKT (WKT has no `BOX3D` type), so it lives outside the `asWKT` family.

Format: `ST_AsText(box: Box3D)`

Return type: `String`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_3DMakeBox(ST_PointZ(0.0, 0.0, -3.0), ST_PointZ(5.0, 10.0, 7.0)))
```

Output:

```
BOX3D(0.0 0.0 -3.0, 5.0 10.0 7.0)
```

Returns `NULL` on `NULL` input.
