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

# ST_OffsetCurve

![ST_OffsetCurve](../../../image/ST_OffsetCurve/ST_OffsetCurve.svg "ST_OffsetCurve")

Introduction: Returns a line at a given offset distance from a linear geometry. If the distance is positive, the offset is on the left side of the input line; if it is negative, it is on the right side. Returns null for empty geometries.

The optional third parameter `quadrantSegments` controls the number of line segments used to approximate a quarter circle at round joins. The default value is 8.

Format: `ST_OffsetCurve(geometry: Geometry, distance: Double, quadrantSegments: Integer)`

Format: `ST_OffsetCurve(geometry: Geometry, distance: Double)`

Return type: `Geometry`

Since: `v1.9.0`

SQL Example:

![ST_OffsetCurve Positive](../../../image/ST_OffsetCurve/ST_OffsetCurve_positive.svg "ST_OffsetCurve Positive Offset")

```sql
SELECT ST_AsText(ST_OffsetCurve(ST_GeomFromWKT('LINESTRING(0 0, 10 0, 10 10)'), 5.0))
```

Output: `LINESTRING (0 5, 5 5, 5 10)`

SQL Example:

![ST_OffsetCurve Negative](../../../image/ST_OffsetCurve/ST_OffsetCurve_negative.svg "ST_OffsetCurve Negative Offset")

```sql
SELECT ST_NPoints(ST_OffsetCurve(ST_GeomFromWKT('LINESTRING(0 0, 10 0, 10 10)'), -3.0))
```

Output: `11`

SQL Example:

![ST_OffsetCurve QuadrantSegments](../../../image/ST_OffsetCurve/ST_OffsetCurve_quadrant.svg "ST_OffsetCurve with quadrantSegments")

```sql
SELECT ST_NPoints(ST_OffsetCurve(ST_GeomFromWKT('LINESTRING(0 0, 10 0, 10 10)'), -3.0, 16))
```

Output: `19`
