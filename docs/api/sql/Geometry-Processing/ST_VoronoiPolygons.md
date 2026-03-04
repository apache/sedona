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

# ST_VoronoiPolygons

Introduction: Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry. The result is a GeometryCollection of Polygons that covers an envelope larger than the extent of the input vertices. Returns null if input geometry is null. Returns an empty geometry collection if the input geometry contains only one vertex. Returns an empty geometry collection if the extend_to envelope has zero area.

Format: `ST_VoronoiPolygons(g1: Geometry, tolerance: Double, extend_to: Geometry)`

Optional parameters:

`tolerance` : The distance within which vertices will be considered equivalent. Robustness of the algorithm can be improved by supplying a nonzero tolerance distance. (default = 0.0)

`extend_to` : If a geometry is supplied as the "extend_to" parameter, the diagram will be extended to cover the envelope of the "extend_to" geometry, unless that envelope is smaller than the default envelope (default = NULL. By default, we extend the bounding box of the diagram by the max between bounding box's height and bounding box's width).

Return type: `Geometry`

Since: `v1.5.0`

SQL Example

```sql
SELECT st_astext(ST_VoronoiPolygons(ST_GeomFromText('MULTIPOINT ((0 0), (1 1))')));
```

Output:

```
GEOMETRYCOLLECTION(POLYGON((-1 2,2 -1,-1 -1,-1 2)),POLYGON((-1 2,2 2,2 -1,-1 2)))
```
