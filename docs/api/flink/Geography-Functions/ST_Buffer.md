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

# ST_Buffer

Introduction: Return the geodesic buffer of a geography. The distance is always interpreted as meters on the spheroid. The result is a polygon approximating the buffer.

![ST_Buffer of a Geography point: a geodesic buffer on the sphere](../../../image/ST_Buffer_geography/ST_Buffer_geography_point.svg "ST_Buffer of a Geography point: a geodesic buffer on the sphere")

Format:

`ST_Buffer (geog: Geography, distanceMeters: Double)`

`ST_Buffer (geog: Geography, distanceMeters: Double, parameters: String)`

Return type: `Geography`

Since: `v1.9.1`

Geography is always spheroidal, so the boolean `useSpheroid` argument accepted by the geometry `ST_Buffer` is not supported for geography inputs: `ST_Buffer(geog, distance, useSpheroid)` throws a clear error. Use `ST_Buffer(geog, distance)` or `ST_Buffer(geog, distance, parameters)` instead.

SQL Example:

```sql
SELECT ST_Buffer(ST_GeogFromWKT('POINT (10 10)', 4326), 100000)
```

Output:

```
POLYGON ((10.9 10.2, 10.8 10.3, 10.8 10.5, 10.6 10.6, ...))
```
