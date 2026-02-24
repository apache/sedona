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

Introduction: Returns list of geometries divided based of given maximum number of vertices.

A minimum of 5 vertices is required for maxVertices parameter to form a closed box.

Format: `ST_SubDivide(geom: Geometry, maxVertices: Integer)`

Since: `v1.1.0`

SQL Example

```sql
SELECT ST_SubDivide(ST_GeomFromText("POLYGON((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"), 5)

```

Output:

```
[
    POLYGON((37.857142857142854 20, 35 10, 10 20, 37.857142857142854 20)),
    POLYGON((15 20, 10 20, 15 40, 15 20)),
    POLYGON((20 20, 15 20, 15 30, 20 30, 20 20)),
    POLYGON((26.428571428571427 20, 20 20, 20 30, 26.4285714 23.5714285, 26.4285714 20)),
    POLYGON((15 30, 15 40, 20 40, 20 30, 15 30)),
    POLYGON((20 40, 26.4285714 40, 26.4285714 32.1428571, 20 30, 20 40)),
    POLYGON((37.8571428 20, 30 20, 34.0476190 32.1428571, 37.8571428 32.1428571, 37.8571428 20)),
    POLYGON((34.0476190 34.6825396, 26.4285714 32.1428571, 26.4285714 40, 34.0476190 40, 34.0476190 34.6825396)),
    POLYGON((34.0476190 32.1428571, 35 35, 37.8571428 35, 37.8571428 32.1428571, 34.0476190 32.1428571)),
    POLYGON((35 35, 34.0476190 34.6825396, 34.0476190 35, 35 35)),
    POLYGON((34.0476190 35, 34.0476190 40, 37.8571428 40, 37.8571428 35, 34.0476190 35)),
    POLYGON((30 20, 26.4285714 20, 26.4285714 23.5714285, 30 20)),
    POLYGON((15 40, 37.8571428 43.8095238, 37.8571428 40, 15 40)),
    POLYGON((45 45, 37.8571428 20, 37.8571428 43.8095238, 45 45))
]
```

SQL Example

```sql
SELECT ST_SubDivide(ST_GeomFromText("LINESTRING(0 0, 85 85, 100 100, 120 120, 21 21, 10 10, 5 5)"), 5)
```

Output:

```
[
    LINESTRING(0 0, 5 5)
    LINESTRING(5 5, 10 10)
    LINESTRING(10 10, 21 21)
    LINESTRING(21 21, 60 60)
    LINESTRING(60 60, 85 85)
    LINESTRING(85 85, 100 100)
    LINESTRING(100 100, 120 120)
]
```
