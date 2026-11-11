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

# ST_H3ToPoint

Introduction: Returns the center of an H3 cell as a point geometry. This uses H3's cell center, which can differ from the centroid of the cell polygon because of the grid projection.

Coordinates are longitude/latitude (X/Y) in degrees. The result has SRID 0, consistent with `ST_H3ToGeom`. To assign WGS 84, use `ST_SetSRID(ST_H3ToPoint(cell), 4326)`.

Null input returns null. Invalid cell indexes raise an error.

Format: `ST_H3ToPoint(cell: Long)`

Return type: `Geometry` (Point)

Since: `v2.0.0`

SQL Example

```sql
SELECT ST_AsText(ST_H3ToPoint(617700169958293503))
```

Output (coordinates rounded):

```text
POINT (-122.41845932318309 37.776702349435695)
```
