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

# ST_StraightSkeleton

Introduction: Computes the straight skeleton of a polygonal geometry. The straight skeleton is a method of representing a polygon by a topological skeleton, formed by a continuous shrinking process where each edge moves inward in parallel at a uniform speed.

This function uses the weighted straight skeleton algorithm based on Felkel's approach.

This function may have significant performance limitations when processing polygons with a very large number of vertices. For very large polygons (e.g., 10,000+ vertices), applying vertex reduction or simplification is essential to achieve practical computation times.

Format: `ST_StraightSkeleton(geom: geometry)`

SQL example:

```sql
SELECT Sedona.ST_StraightSkeleton(
  ST_GeometryFromWKT('POLYGON ((45 0, 55 0, 55 40, 70 40, 70 50, 30 50, 30 40, 45 40, 45 0))')
)
```

Output: `MULTILINESTRING ((50 5, 50 45), (50 45, 35 45), (50 45, 65 45), (35 45, 30 45), (35 45, 40 40), (65 45, 70 45), (65 45, 60 40), (50 5, 45 5), (50 5, 55 5))`
