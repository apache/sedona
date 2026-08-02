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

# ST_EqualsExact

Introduction: Return true if A and B have the same structure and their corresponding coordinates are equal within a tolerance.

Unlike `ST_Equals`, this predicate requires geometry types, component order, ring order, and vertex order to match. The tolerance is the maximum distance allowed between each pair of corresponding coordinates. The comparison uses x and y coordinates and ignores z and m coordinates.

Format: `ST_EqualsExact (A: Geometry, B: Geometry, tolerance: Double)`

Return type: `Boolean`

Since: `v1.9.1`

Example:

```sql
SELECT ST_EqualsExact(
    ST_GeomFromWKT('POINT (0 0)'),
    ST_GeomFromWKT('POINT (0.03 0.04)'),
    0.05
)
```

Output:

```
true
```

The order of coordinates must match:

```sql
SELECT ST_EqualsExact(
    ST_GeomFromWKT('LINESTRING (0 0, 1 1)'),
    ST_GeomFromWKT('LINESTRING (1 1, 0 0)'),
    0.0
)
```

Output:

```
false
```
