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

# ST_Length

Introduction: Returns the geodesic length of a geography object in meters, calculated on the WGS84 spheroid using GeographicLib. Linestring and multilinestring geographies have non-zero length, and geometrycollection geographies return the sum of the lengths of their linear components. Returns 0 for points and polygons.

Format:

`ST_Length (A: Geography)`

Return type: `Double`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_Length(ST_GeogFromWKT('LINESTRING (0 0, 1 1)'));
```

Output:

```
156899.56829134026
```

The result is approximately 157 km — the geodesic length of a line from (0,0) to (1,1).
