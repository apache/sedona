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

# ST_Contains

Introduction: Tests whether geography A fully contains geography B using S2 spherical boolean operations. Returns true if every point of B is inside or on the boundary of A.

Polygon ring roles follow the simple-features structure: the first ring is a shell and subsequent rings are holes, regardless of input winding. Sedona preserves the submitted coordinate order for structural output such as `ST_AsText`, while normalizing only the S2-facing traversal used by spherical operations.

Each Geography polygon shell is normalized to an area of at most one hemisphere. A shell intended to represent more than half the sphere is therefore interpreted as its complement; reversing the ring's coordinate sequence does not select the larger region.

![ST_Contains returning true](../../../../image/ST_Contains_geography/ST_Contains_geography_true.svg "ST_Contains returning true")
![ST_Contains returning false](../../../../image/ST_Contains_geography/ST_Contains_geography_false.svg "ST_Contains returning false")

Format:

`ST_Contains (A: Geography, B: Geography)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_Contains(
  ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'),
  ST_GeogFromWKT('POINT (0.5 0.5)')
);
```

Output:

```
true
```
