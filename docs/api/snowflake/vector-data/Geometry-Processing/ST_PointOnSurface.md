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

# ST_PointOnSurface

Introduction: Returns a POINT guaranteed to lie on the surface.

![ST_PointOnSurface](../../../../image/ST_PointOnSurface/ST_PointOnSurface.svg "ST_PointOnSurface")

Format: `ST_PointOnSurface(A:geometry)`

Return type: `Geometry`

Examples:

```
SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('POINT(0 5)')));
 st_astext
------------
 POINT(0 5)

SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('LINESTRING(0 5, 0 10)')));
 st_astext
------------
 POINT(0 5)

SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')));
   st_astext
----------------
 POINT(2.5 2.5)

SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('LINESTRING(0 5 1, 0 0 1, 0 10 2)')));
   st_astext
----------------
 POINT Z(0 0 1)

```
