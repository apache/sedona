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

# ST_Angle

Introduction: Computes and returns the angle between two vectors represented by the provided points or linestrings.

There are three variants possible for ST_Angle:

`ST_Angle(point1: Geometry, point2: Geometry, point3: Geometry, point4: Geometry)`
Computes the angle formed by vectors represented by point1 - point2 and point3 - point4

`ST_Angle(point1: Geometry, point2: Geometry, point3: Geometry)`
Computes the angle formed by vectors represented by point2 - point1 and point2 - point3

`ST_Angle(line1: Geometry, line2: Geometry)`
Computes the angle formed by vectors S1 - E1 and S2 - E2, where S and E denote start and end points respectively

!!!Note
    If any other geometry type is provided, ST_Angle throws an IllegalArgumentException.
    Additionally, if any of the provided geometry is empty, ST_Angle throws an IllegalArgumentException.

!!!Note
    If a 3D geometry is provided, ST_Angle computes the angle ignoring the z ordinate, equivalent to calling ST_Angle for corresponding 2D geometries.

!!!Tip
    ST_Angle returns the angle in radian between 0 and 2\Pi. To convert the angle to degrees, use [ST_Degrees](ST_Degrees.md).

Format: `ST_Angle(p1, p2, p3, p4) | ST_Angle(p1, p2, p3) | ST_Angle(line1, line2)`

Since: `v1.5.0`

SQL Example

```sql
SELECT ST_Angle(ST_GeomFromWKT('POINT(0 0)'), ST_GeomFromWKT('POINT (1 1)'), ST_GeomFromWKT('POINT(1 0)'), ST_GeomFromWKT('POINT(6 2)'))
```

Output:

```
0.4048917862850834
```

SQL Example

```sql
SELECT ST_Angle(ST_GeomFromWKT('POINT (1 1)'), ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT(3 2)'))
```

Output:

```
0.19739555984988044
```

SQL Example

```sql
SELECT ST_Angle(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('LINESTRING (0 0, 3 2)'))
```

Output:

```
0.19739555984988044
```
