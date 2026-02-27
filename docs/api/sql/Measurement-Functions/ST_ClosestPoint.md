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

# ST_ClosestPoint

Introduction: Returns the 2-dimensional point on geom1 that is closest to geom2. This is the first point of the shortest line between the geometries. If using 3D geometries, the Z coordinates will be ignored. If you have a 3D Geometry, you may prefer to use ST_3DClosestPoint.
It will throw an exception indicates illegal argument if one of the params is an empty geometry.

Format: `ST_ClosestPoint(g1: Geometry, g2: Geometry)`

Since: `v1.5.0`

SQL Example

```sql
SELECT ST_AsText( ST_ClosestPoint(g1, g2)) As ptwkt;
```

Input: `g1: POINT (160 40), g2: LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190)`

Output: `POINT(160 40)`

Input: `g1: LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190), g2: POINT (160 40)`

Output: `POINT(125.75342465753425 115.34246575342466)`

Input: `g1: 'POLYGON ((190 150, 20 10, 160 70, 190 150))', g2: ST_Buffer('POINT(80 160)', 30)`

Output: `POINT(131.59149149528952 101.89887534906197)`
