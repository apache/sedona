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

# ST_ConcaveHull

Introduction: Return the Concave Hull of polygon A, with alpha set to pctConvex[0, 1] in the Delaunay Triangulation method, the concave hull will not contain a hole unless allowHoles is set to true

Format:

`ST_ConcaveHull (A: Geometry, pctConvex: Double)`

`ST_ConcaveHull (A: Geometry, pctConvex: Double, allowHoles: Boolean)`

Return type: `Geometry`

Since: `v1.4.0`

SQL Example

```sql
SELECT ST_ConcaveHull(ST_GeomFromWKT('POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))'), 1)
```

Output:

```
POLYGON ((125 100, 20 40, 50 60, 175 150, 125 100))
```
