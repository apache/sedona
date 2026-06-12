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

# ST_Box3D

Introduction: Return the planar 3D bounding box of a Geometry as a typed `Box3D` value (six doubles: `xmin`, `ymin`, `zmin`, `xmax`, `ymax`, `zmax`).

Geometries without a Z dimension fold into `zmin = zmax = 0`, matching PostGIS. `ST_Box3D` is the 3D counterpart to [ST_Box2D](../../box2d/Box2D-Constructors/ST_Box2D.md); it always returns a `Box3D` value that serialises to a struct of six non-nullable doubles and round-trips through Parquet without WKB overhead.

![ST_Box3D returns the tightest cuboid enclosing a geometry](../../../../image/box3d/st_box3d.svg "ST_Box3D: bounding cuboid of a geometry")

Format: `ST_Box3D(geom: Geometry)`

Return type: `Box3D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_Box3D(ST_GeomFromWKT('LINESTRING Z(0 0 -3, 5 10 7)')))
```

Output:

```
BOX3D(0.0 0.0 -3.0, 5.0 10.0 7.0)
```

A 2D geometry folds its Z extent to 0:

```sql
SELECT ST_AsText(ST_Box3D(ST_GeomFromWKT('LINESTRING (0 0, 5 10)')))
```

Output:

```
BOX3D(0.0 0.0 0.0, 5.0 10.0 0.0)
```

`ST_Box3D` is also produced by the SQL cast `CAST(geom AS box3d)` — see [Type conversion](../Box3D-Functions.md#type-conversion).

Returns `NULL` for `NULL` or empty geometry input.
