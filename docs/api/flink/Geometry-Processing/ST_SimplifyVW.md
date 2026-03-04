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

# ST_SimplifyVW

Introduction: This function simplifies the input geometry by applying the Visvalingam-Whyatt algorithm.

![ST_SimplifyVW](../../../image/ST_SimplifyVW/ST_SimplifyVW.svg "ST_SimplifyVW")

!!!Note
    The simplification may not preserve topology, potentially producing invalid geometries. Use [ST_SimplifyPreserveTopology](ST_SimplifyPreserveTopology.md) to retain valid topology after simplification.

Format: `ST_SimplifyVW(geom: Geometry, tolerance: Double)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_SimplifyVW(ST_GeomFromWKT('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 80)
```

Output:

```
POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 43 19, 24 39, 8 25))
```
