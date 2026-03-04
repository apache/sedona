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

# ST_Union

Introduction:

Variant 1: Return the union of geometry A and B.

Variant 2: This function accepts an array of Geometry objects and returns the geometric union of all geometries in the input array. If the polygons within the input array do not share common boundaries, the ST_Union result will be a MultiPolygon geometry.

![ST_Union](../../../image/ST_Union/ST_Union.svg "ST_Union")

Format:

`ST_Union (A: Geometry, B: Geometry)`

`ST_Union (geoms: Array(Geometry))`

Return type: `Geometry`

Since: `v1.6.0`

SQL Example

```sql
SELECT ST_Union(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), ST_GeomFromWKT('POLYGON ((1 -2, 5 0, 1 2, 1 -2))'))
```

Output:

```
POLYGON ((3 -1, 3 -3, -3 -3, -3 3, 3 3, 3 1, 5 0, 3 -1))
```

SQL Example

```sql
SELECT ST_Union(
    Array(
        ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'),
        ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')
    )
)
```

Output:

```
POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))
```
