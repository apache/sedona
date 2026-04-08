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

# ST_Area

Introduction: Returns the geodesic area of a geography object in square meters, calculated on the WGS84 spheroid using GeographicLib. Returns 0 for non-polygon geometries (points, linestrings).

Format:

`ST_Area (A: Geography)`

Return type: `Double`

Since: `v1.9.0`

SQL Example

```sql
SELECT ST_Area(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'));
```

Output:

```
12308778361.469452
```

The result is approximately 12,309 km^2 — the area of a 1-degree by 1-degree box near the equator.
