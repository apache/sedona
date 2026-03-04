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

# ST_TriangulatePolygon

Introduction: Generates the constrained Delaunay triangulation for the input Polygon. The constrained Delaunay triangulation is a set of triangles created from the Polygon's vertices that covers the Polygon area precisely, while maximizing the combined interior angles across all triangles compared to other possible triangulations. This produces the highest quality triangulation representation of the Polygon geometry. The function returns a GeometryCollection of Polygon geometries comprising this optimized constrained Delaunay triangulation. Polygons with holes and MultiPolygon types are supported. For any other geometry type provided, such as Point, LineString, etc., an empty GeometryCollection will be returned.

Format: `ST_TriangulatePolygon(geom: Geometry)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_TriangulatePolygon(
        ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))')
    )
```

Output:

```
GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))
```
