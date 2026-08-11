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

# ST_SharedPaths

Introduction: Returns the paths shared by two lineal geometries, grouped by traversal direction.

![ST_SharedPaths](../../../image/ST_SharedPaths/ST_SharedPaths.svg "ST_SharedPaths")

Format: `ST_SharedPaths(A: Geometry, B: Geometry)`

Return type: `Geometry`

Since: `v2.0.0`

Both inputs must be a `LineString` or `MultiLineString` and must have the same SRID. The result is
a `GeometryCollection` containing exactly two `MultiLineString` elements. The first contains paths
traversed in the same direction by both inputs; the second contains paths traversed in opposite
directions. Coordinates in both elements follow the direction of `A`.

The matching input SRID is retained. As in PostGIS, a non-empty result retains a Z dimension when
either input has Z, while M values are dropped. An empty result is two-dimensional.

When the inputs do not share a path, including when they intersect only at points, both elements
are empty `MultiLineString` geometries.

SQL Example

```sql
SELECT ST_SharedPaths(
    ST_GeomFromWKT('LINESTRING (0 0, 10 0)'),
    ST_GeomFromWKT('LINESTRING (15 0, 5 0)')
)
```

Output:

```
GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((5 0, 10 0)))
```
