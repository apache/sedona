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

# ST_LabelPoint

Introduction: `ST_LabelPoint` computes and returns a label point for a given polygon or geometry collection. The label point is chosen to be sufficiently far from boundaries of the geometry. For a regular Polygon this will be the
centroid.

The algorithm is derived from Tippecanoe’s `polygon_to_anchor`, an approximate solution for label point generation, designed to be faster than optimal algorithms like `polylabel`. It searches for a “good enough” label point within a limited number of iterations. For geometry collections, only the largest Polygon by area is considered. While `ST_Centroid` is a fast algorithm to calculate the center of mass of a (Multi)Polygon, it may place the point outside of the Polygon or near a boundary for concave shapes, polygons with holes, or MultiPolygons.

`ST_LabelPoint` takes up to 3 arguments,

- `geometry`: input geometry (e.g., a Polygon or GeometryCollection) for which the anchor point is to be calculated.
- `gridResolution` (Optional, default is 16): Controls the resolution of the search grid for refining the label point. A higher resolution increases the grid density, providing a higher chance of finding a good enough result at the cost of runtime. For example, a gridResolution of 16 divides the bounding box of the polygon into a 16x16 grid.
- `goodnessThreshold` (Optional, default is 0.2): Determines the minimum acceptable “goodness” value for the anchor point. Higher thresholds prioritize points farther from boundaries but may require more computation.

!!!note
    - `ST_LabelPoint` throws an `IllegalArgumentException` if the input geometry has an area of zero or less.
    - Holes within polygons are respected. Points within a hole are given a goodness of 0.
    - For GeometryCollections, only the largest polygon by area is considered.

!!!tip
    - Use `ST_LabelPoint` for tasks such as label placement, identifying representative points for polygons, or other spatial analyses where an internal reference point is preferred but not required. If intersection of the point and the original geometry is required, use of an algorithm like `polylabel` should be considered.
    - `ST_LabelPoint` offers a faster, approximate solution for label point generation, making it ideal for large datasets or real-time applications.

Format:

```sql
ST_LabelPoint(geometry: Geometry)
```

```sql
ST_LabelPoint(geometry: Geometry, gridResolution: Integer)
```

```sql
ST_LabelPoint(geometry: Geometry, gridResolution: Integer, goodnessThreshold: Double)
```

Return type: `Geometry`

Since: `v1.7.1`

SQL Example:

```
SELECT ST_LabelPoint(ST_GeomFromWKT('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))
```

Output:

```
POINT (2 2)
```

SQL Example:

```
SELECT ST_LabelPoint(ST_GeomFromWKT('GEOMETRYCOLLECTION(POLYGON ((-112.840785 33.435962, -112.840785 33.708284, -112.409597 33.708284, -112.409597 33.435962, -112.840785 33.435962)), POLYGON ((-112.309264 33.398167, -112.309264 33.746007, -111.787444 33.746007, -111.787444 33.398167, -112.309264 33.398167)))'))
```

Output:

```
POINT (-112.04835399999999 33.57208699999999)
```

SQL Example:

```
SELECT ST_LabelPoint(ST_GeomFromWKT('POLYGON ((-112.654072 33.114485, -112.313516 33.653431, -111.63515 33.314399, -111.497829 33.874913, -111.692825 33.431378, -112.376684 33.788215, -112.654072 33.114485))', 4326))
```

Output:

```
SRID=4326;POINT (-112.0722602222832 33.53914975012836)
```
