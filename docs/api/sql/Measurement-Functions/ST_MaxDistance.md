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

# ST_MaxDistance

Introduction: Calculates and returns the length value representing the maximum distance between any two points across the input geometries. This function is an alias for `ST_LongestDistance`.

Format: `ST_MaxDistance(geom1: Geometry, geom2: Geometry)`

Return type: `Double`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_MaxDistance(
        ST_GeomFromText("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"),
        ST_GeomFromText("POLYGON ((10 20, 30 30, 40 20, 30 10, 10 20))")
)
```

Output:

```
36.05551275463989
```
