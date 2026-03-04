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

# ST_InterpolatePoint

Introduction: Returns the interpolated measure value of a linear measured LineString at the point closest to the specified point.

!!!Note
    Make sure that both geometries have the same SRID, otherwise the function will throw an IllegalArgumentException.

Format: `ST_InterpolatePoint(linestringM: Geometry, point: Geometry)`

Return type: `Double`

Since: `v1.7.0`

SQL Example

```sql
SELECT ST_InterpolatePoint(
    ST_GeomFromWKT("LINESTRING M (0 0 0, 2 0 2, 4 0 4)"),
    ST_GeomFromWKT("POINT (1 1)")
    )
```

Output:

```
1.0
```
