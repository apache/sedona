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

# ST_Rotate

Introduction: Rotates a geometry by a specified angle in radians counter-clockwise around a given origin point. The origin for rotation can be specified as either a POINT geometry or x and y coordinates. If the origin is not specified, the geometry is rotated around POINT(0 0).

Formats;

`ST_Rotate (geometry: Geometry, angle: Double)`

`ST_Rotate (geometry: Geometry, angle: Double, originX: Double, originY: Double)`

`ST_Rotate (geometry: Geometry, angle: Double, pointOrigin: Geometry)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_Rotate(ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 1 0, 1 1, 0 0))'), 10, 0, 0)
```

Output:

```
SRID=4326;POLYGON ((0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0))
```
