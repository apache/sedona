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

# ST_BoundingDiagonal

Introduction: Returns a linestring spanning minimum and maximum values of each dimension of the given geometry's coordinates as its start and end point respectively.
If an empty geometry is provided, the returned LineString is also empty.
If a single vertex (POINT) is provided, the returned LineString has both the start and end points same as the points coordinates

Format: `ST_BoundingDiagonal(geom: Geometry)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_BoundingDiagonal(ST_GeomFromWKT(geom))
```

Input: `POLYGON ((1 1 1, 3 3 3, 0 1 4, 4 4 0, 1 1 1))`

Output: `LINESTRING Z(0 1 1, 4 4 4)`

Input: `POINT (10 10)`

Output: `LINESTRING (10 10, 10 10)`

Input: `GEOMETRYCOLLECTION(POLYGON ((5 5 5, -1 2 3, -1 -1 0, 5 5 5)), POINT (10 3 3))`

Output: `LINESTRING Z(-1 -1 0, 10 5 5)`
