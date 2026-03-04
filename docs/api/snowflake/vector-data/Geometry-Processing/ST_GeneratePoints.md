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

# ST_GeneratePoints

Introduction: Generates a specified quantity of pseudo-random points within the boundaries of the provided polygonal geometry. When `seed` is either zero or not defined then output will be random.

Format:

`ST_GeneratePoints(geom: Geometry, numPoints: Integer, seed: Long = 0)`

`ST_GeneratePoints(geom: Geometry, numPoints: Integer)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_GeneratePoints(
        ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), 4
)
```

Output:

!!!Note
    Due to the pseudo-random nature of point generation, the output of this function will vary between executions and may not match any provided examples.

```
MULTIPOINT ((0.2393028905520183 0.9721563442837837), (0.3805848547053376 0.7546556656982678), (0.0950295778200995 0.2494334895495989), (0.4133520939987385 0.3447046312451945))
```
