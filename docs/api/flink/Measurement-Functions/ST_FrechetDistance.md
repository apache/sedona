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

# ST_FrechetDistance

Introduction: Computes and returns discrete [Frechet Distance](https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance) between the given two geometries,
based on [Computing Discrete Frechet Distance](http://www.kr.tuwien.ac.at/staff/eiter/et-archive/files/cdtr9464.pdf)

If any of the geometries is empty, returns 0.0

Format: `ST_FrechetDistance(g1: Geometry, g2: Geometry)`

Return type: `Double`

Since: `v1.5.0`

Example:

```sql
SELECT ST_FrechetDistance(ST_GeomFromWKT('POINT (0 1)'), ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)'))
```

Output:

```
5.0990195135927845
```
