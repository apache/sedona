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

Introduction: Returns the maximum geodesic distance in meters between any two points on the two geography objects. Uses S2 S2FurthestEdgeQuery for spherical computation.

Format:

`ST_MaxDistance (A: Geography, B: Geography)`

Return type: `Double`

Since: `v1.9.0`

SQL Example

```sql
SELECT ST_MaxDistance(
  ST_GeogFromWKT('LINESTRING (0 0, 1 0)'),
  ST_GeogFromWKT('POINT (0.5 1)')
);
```

Output:

```
175592.32
```

The maximum distance is from one endpoint of the linestring to the point.
