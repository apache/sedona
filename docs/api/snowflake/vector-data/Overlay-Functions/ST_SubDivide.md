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

# ST_SubDivide

Introduction: Returns a multi-geometry divided based of given maximum number of vertices.

A minimum of 5 vertices is required for maxVertices parameter to form a closed box.

![ST_SubDivide](../../../../image/ST_SubDivide/ST_SubDivide.svg "ST_SubDivide")

Format: `ST_SubDivide(geom: geometry, maxVertices: int)`

Return type: `Array<Geometry>`

SQL example:

```sql
SELECT Sedona.ST_AsText(Sedona.ST_SubDivide(Sedona.ST_GeomFromText('LINESTRING(0 0, 85 85, 100 100, 120 120, 21 21, 10 10, 5 5)'), 5));

```

Output:

```
MULTILINESTRING ((0 0, 5 5), (5 5, 10 10), (10 10, 21 21), (21 21, 60 60), (60 60, 85 85), (85 85, 100 100), (100 100, 120 120))
```
