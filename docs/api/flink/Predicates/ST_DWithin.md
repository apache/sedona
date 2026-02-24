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

Introduction: Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'.

If `useSpheroid` is passed true, ST_DWithin uses Sedona's ST_DistanceSpheroid to check the spheroid distance between the centroids of two geometries. The unit of the distance in this case is meter.

If `useSpheroid` is passed false, ST_DWithin uses Euclidean distance and the unit of the distance is the same as the CRS of the geometries. To obtain the correct result, please consider using ST_Transform to put data in an appropriate CRS.

If useSpheroid is not given, it defaults to false

Format: `ST_DWithin (leftGeometry: Geometry, rightGeometry: Geometry, distance: Double, useSpheroid: Optional(Boolean) = false)`

Since: `v1.5.1`

Example:

```sql
SELECT ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 0)'), 2.5)
```

Output:

```
true
```

```text
Check for distance between New York and Seattle (< 4000 km)
```

```sql
SELECT ST_DWithin(ST_GeomFromWKT(-122.335167 47.608013), ST_GeomFromWKT(-73.935242 40.730610), 4000000, true)
```

Output:

```
true
```
