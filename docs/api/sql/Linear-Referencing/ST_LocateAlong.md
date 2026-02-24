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

# ST_LocateAlong

Introduction: This function computes Point or MultiPoint geometries representing locations along a measured input geometry (LineString or MultiLineString) corresponding to the provided measure value(s). Polygonal geometry inputs are not supported. The output points lie directly on the input line at the specified measure positions.

Additionally, an optional `offset` parameter can shift the resulting points left or right from the input line. A positive offset displaces the points to the left side, while a negative value offsets them to the right side by the given distance.

This allows identifying precise locations along a measured linear geometry based on supplied measure values, with the ability to offset the output points if needed.

Format:

`ST_LocateAlong(linear: Geometry, measure: Double, offset: Double)`

`ST_LocateAlong(linear: Geometry, measure: Double)`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_LocateAlong(
        ST_GeomFromText('LINESTRING M (10 30 1, 50 50 1, 30 110 2, 70 90 2, 180 140 3, 130 190 3)')
)
```

Output:

```
MULTIPOINT M((30 110 2), (50 100 2), (70 90 2))
```
