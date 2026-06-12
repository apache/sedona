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

# ST_3DExtent

Introduction: Return the 3D bounding box of all geometries in `A` as a typed [Box3D](../box3d/Box3D-Functions.md). Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. Geometries without a Z dimension fold into `z = 0`. Mirrors PostGIS `ST_3DExtent`.

`ST_3DExtent` is the 3D counterpart to [ST_Extent](ST_Extent.md), which returns a `Box2D`.

![ST_3DExtent aggregates a column of geometries into the enclosing cuboid](../../../image/box3d/st_3dextent.svg "ST_3DExtent: enclosing cuboid over a column")

Format: `ST_3DExtent(A: geometryColumn)`

Return type: `Box3D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_3DExtent(geom))
FROM VALUES
    (ST_GeomFromText('POINT Z (1 2 3)')),
    (ST_GeomFromText('POINT Z (4 5 -1)')),
    (ST_GeomFromText('LINESTRING (-3 0, 0 0)')) AS t(geom)
```

Output:

```
BOX3D(-3.0 0.0 -1.0, 4.0 5.0 3.0)
```

The XY-only linestring contributes `z = 0`, which sits between the `-1` and `3` of the two POINT Z rows, so it does not move either Z bound.
