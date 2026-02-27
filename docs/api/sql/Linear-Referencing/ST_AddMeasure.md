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

# ST_AddMeasure

Introduction: Computes a new geometry with measure (M) values linearly interpolated between start and end points. For geometries lacking M dimensions, M values are added. Existing M values are overwritten by the new values. Applies only to LineString and MultiLineString inputs.

Format: `ST_AddMeasure(geom: Geometry, measureStart: Double, measureEnd: Double)`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_AsText(ST_AddMeasure(
        ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)')
))
```

Output:

```
LINESTRING M(0 0 10, 1 0 16, 2 0 22, 3 0 28, 4 0 34, 5 0 40)
```
