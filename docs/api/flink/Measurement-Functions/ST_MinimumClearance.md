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

# ST_MinimumClearance

Introduction: The minimum clearance is a metric that quantifies a geometry's tolerance to changes in coordinate precision or vertex positions. It represents the maximum distance by which vertices can be adjusted without introducing invalidity to the geometry's structure. A larger minimum clearance value indicates greater robustness against such perturbations.

For a geometry with a minimum clearance of `x`, the following conditions hold:

- No two distinct vertices are separated by a distance less than `x`.
- No vertex lies within a distance `x` from any line segment it is not an endpoint of.

For geometries with no definable minimum clearance, such as single Point geometries or MultiPoint geometries where all points occupy the same location, the function returns `Double.MAX_VALUE`.

Format: `ST_MinimumClearance(geometry: Geometry)`

Return type: `Double`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_MinimumClearance(
        ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))')
)
```

Output:

```
0.5
```
