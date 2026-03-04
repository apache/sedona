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

# ST_PointN

Introduction: Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL if there is no linestring in the geometry.

Format: `ST_PointN(A: Geometry, B: Integer)`

Return type: `Geometry`

Since: `v1.2.1`

Examples:

```sql
SELECT ST_PointN(df.geometry, 2)
FROM df
```

Input: `LINESTRING(0 0, 1 2, 2 4, 3 6), 2`

Output: `POINT (1 2)`

Input: `LINESTRING(0 0, 1 2, 2 4, 3 6), -2`

Output: `POINT (2 4)`

Input: `CIRCULARSTRING(1 1, 1 2, 2 4, 3 6, 1 2, 1 1), -1`

Output: `POINT (1 1)`
