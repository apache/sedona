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

# ST_Intersection

Introduction: Returns the closed-set spherical intersection of two Geography values. Edges are interpreted as great-circle arcs, and the result can be passed directly to Geography functions such as `ST_Area` for measurements in square meters.

Format:

`ST_Intersection(A: Geography, B: Geography)`

Return type: `Geography`

Since: `v1.9.1`

Both inputs must be Geography values. Boundary-only intersections are retained: crossing lines or touching polygon vertices can produce a Point, while shared polygon edges can produce a LineString. Results containing several dimensions are returned as a GeometryCollection.

Polygon ring roles follow the simple-features structure: the first ring is a shell and subsequent rings are holes, regardless of input winding. Each shell is normalized to an area of at most one hemisphere. A shell intended to represent more than half the sphere is therefore interpreted as its complement; reversing the ring's coordinate sequence does not select the larger region.

The result is two-dimensional and copies the first input's SRID without transforming either input or validating that their SRIDs match. An explicitly empty operand produces `GEOMETRYCOLLECTION EMPTY`. A disjoint result from two non-empty inputs preserves the lower input dimension, such as `LINESTRING EMPTY` for two disjoint lines.

SQL Example

```sql
WITH overlap AS (
  SELECT ST_Intersection(
    ST_GeogFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 4326),
    ST_GeogFromWKT('POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))', 4326)
  ) AS geog
)
SELECT ST_GeometryType(geog), ST_Area(geog)
FROM overlap;
```

Output:

```
ST_Polygon | 3.071055126726233E11
```
