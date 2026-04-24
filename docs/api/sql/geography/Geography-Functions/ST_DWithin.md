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

# ST_DWithin

Introduction: Tests whether two geographies are within a given geodesic distance (in meters) of each other on the sphere. The minimum great-circle distance between any two points on the two geographies is compared against the threshold; the test is inclusive (returns true when the minimum distance equals the threshold).

Format:

`ST_DWithin (A: Geography, B: Geography, distance: Double)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_DWithin(
  ST_GeogFromWKT('POINT (0 0)', 4326),
  ST_GeogFromWKT('POINT (0 1)', 4326),
  200000.0
);
```

Output:

```
true
```

The same pair of points with a tighter threshold:

```sql
SELECT ST_DWithin(
  ST_GeogFromWKT('POINT (0 0)', 4326),
  ST_GeogFromWKT('POINT (0 1)', 4326),
  100000.0
);
```

Output:

```
false
```
