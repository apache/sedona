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

# ST_Simplify

Introduction: This function simplifies the input geometry by applying the Douglas-Peucker algorithm.

!!!Note
    The simplification may not preserve topology, potentially producing invalid geometries. Use [ST_SimplifyPreserveTopology](ST_SimplifyPreserveTopology.md) to retain valid topology after simplification.

Format: `ST_Simplify(geom: Geometry, tolerance: Double)`

SQL Example:

```sql
SELECT ST_Simplify(ST_Buffer(ST_GeomFromWKT('POINT (0 2)'), 10), 1)
```

Output:

```
POLYGON ((10 2, 7.0710678118654755 -5.071067811865475, 0.0000000000000006 -8, -7.071067811865475 -5.0710678118654755, -10 1.9999999999999987, -7.071067811865477 9.071067811865476, -0.0000000000000018 12, 7.071067811865474 9.071067811865477, 10 2))
```
