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

# ST_IsPolygonCCW

Introduction: Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. Polygonal components nested inside a `GEOMETRYCOLLECTION`, at any depth, are recursively inspected. Non-polygonal components (points, line strings, etc.) are ignored.

An input with no polygonal components at all - points, line strings, and empty geometry collections included - vacuously returns `true`, since there is no ring to violate the requested orientation. This also covers `POLYGON EMPTY` and `MULTIPOLYGON EMPTY`. A SQL `NULL` input returns `NULL`.

!!!note
	Starting with Sedona 2.0.0, `ST_IsPolygonCCW` matches PostGIS behavior for non-polygonal and `GEOMETRYCOLLECTION` inputs, and its Flink wrapper's return type changed from a primitive `boolean` to a nullable `Boolean` so `NULL` inputs propagate correctly. Earlier versions returned `false` for non-polygonal/collection inputs and for `NULL`. See the [release notes](../../../setup/release-notes.md#sedona-200) for details.

![ST_IsPolygonCCW](../../../image/ST_IsPolygonCCW/ST_IsPolygonCCW.svg "ST_IsPolygonCCW")

Format: `ST_IsPolygonCCW(geom: Geometry)`

Return type: `Boolean`

Since: `v1.6.0`

SQL Example:

```sql
SELECT ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))'))
```

Output:

```
true
```
