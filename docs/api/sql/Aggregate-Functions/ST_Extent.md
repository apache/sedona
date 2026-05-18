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

# ST_Extent

Introduction: Return the planar bounding box of all geometries in `A` as a typed [Box2D](../box2d/Box2D-Functions.md). Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. Mirrors PostGIS `ST_Extent`.

`ST_Extent` is the typed counterpart to [ST_Envelope_Agg](ST_Envelope_Agg.md). `ST_Envelope_Agg` returns the envelope as a `Geometry`; `ST_Extent` returns it as a `Box2D` value that serialises to a struct of four doubles.

Format: `ST_Extent(A: geometryColumn)`

Return type: `Box2D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_Extent(geom))
FROM VALUES
    (ST_GeomFromText('POINT (1 1)')),
    (ST_GeomFromText('POINT (5 7)')),
    (ST_GeomFromText('LINESTRING (3 2, 6 4)')) AS t(geom)
```

Output:

```
BOX(1.0 1.0, 6.0 7.0)
```
