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

# Geometry Processing

These functions compute geometric constructions, or alter geometry size or shape.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_ApproximateMedialAxis](ST_ApproximateMedialAxis.md) | Computes an approximate medial axis of a polygonal geometry. The medial axis is a representation of the "centerline" or "skeleton" of the polygon. This function first computes the straight skeleton... | v1.8.0 |
| [ST_Buffer](ST_Buffer.md) | Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance. The function supports both Planar/Euclidean and Spheroidal/Geo... | v1.6.0 |
| [ST_BuildArea](ST_BuildArea.md) | Returns the areal geometry formed by the constituent linework of the input geometry. | v1.2.1 |
| [ST_Centroid](ST_Centroid.md) | Return the centroid point of A | v1.0.0 |
| [ST_ConcaveHull](ST_ConcaveHull.md) | Return the Concave Hull of polygon A, with alpha set to pctConvex[0, 1] in the Delaunay Triangulation method, the concave hull will not contain a hole unless allowHoles is set to true | v1.4.0 |
| [ST_ConvexHull](ST_ConvexHull.md) | Return the Convex Hull of polygon A | v1.0.0 |
| [ST_DelaunayTriangles](ST_DelaunayTriangles.md) | This function computes the [Delaunay triangulation](https://en.wikipedia.org/wiki/Delaunay_triangulation) for the set of vertices in the input geometry. An optional `tolerance` parameter allows sna... | v1.6.1 |
| [ST_GeneratePoints](ST_GeneratePoints.md) | Generates a specified quantity of pseudo-random points within the boundaries of the provided polygonal geometry. When `seed` is either zero or not defined then output will be random. | v1.6.1 |
| [ST_GeometricMedian](ST_GeometricMedian.md) | Computes the approximate geometric median of a MultiPoint geometry using the Weiszfeld algorithm. The geometric median provides a centrality measure that is less sensitive to outlier points than th... | v1.4.1 |
| [ST_LabelPoint](ST_LabelPoint.md) | `ST_LabelPoint` computes and returns a label point for a given polygon or geometry collection. The label point is chosen to be sufficiently far from boundaries of the geometry. For a regular Polygo... | v1.7.1 |
| [ST_MaximumInscribedCircle](ST_MaximumInscribedCircle.md) | Finds the largest circle that is contained within a (multi)polygon, or which does not overlap any lines and points. Returns a row with fields: | v1.6.1 |
| [ST_MinimumBoundingCircle](ST_MinimumBoundingCircle.md) | Returns the smallest circle polygon that contains a geometry. The optional quadrantSegments parameter determines how many segments to use per quadrant and the default number of segments has been ch... | v1.0.1 |
| [ST_MinimumBoundingRadius](ST_MinimumBoundingRadius.md) | Returns a struct containing the center point and radius of the smallest circle that contains a geometry. | v1.0.1 |
| [ST_OrientedEnvelope](ST_OrientedEnvelope.md) | Returns the minimum-area rotated rectangle enclosing a geometry. The rectangle may be rotated relative to the coordinate axes. Degenerate inputs may result in a Point or LineString being returned. | v1.8.1 |
| [ST_PointOnSurface](ST_PointOnSurface.md) | Returns a POINT guaranteed to lie on the surface. | v1.2.1 |
| [ST_Polygonize](ST_Polygonize.md) | Generates a GeometryCollection composed of polygons that are formed from the linework of an input GeometryCollection. When the input does not contain any linework that forms a polygon, the function... | v1.6.0 |
| [ST_ReducePrecision](ST_ReducePrecision.md) | Reduce the decimals places in the coordinates of the geometry to the given number of decimal places. The last decimal place will be rounded. This function was called ST_PrecisionReduce in versions ... | v1.0.0 |
| [ST_Simplify](ST_Simplify.md) | This function simplifies the input geometry by applying the Douglas-Peucker algorithm. | v1.7.0 |
| [ST_SimplifyPolygonHull](ST_SimplifyPolygonHull.md) | This function computes a topology-preserving simplified hull, either outer or inner, for a polygonal geometry input. An outer hull fully encloses the original geometry, while an inner hull lies ent... | v1.6.1 |
| [ST_SimplifyPreserveTopology](ST_SimplifyPreserveTopology.md) | Simplifies a geometry and ensures that the result is a valid geometry having the same dimension and number of components as the input, and with the components having the same topological relationship. | v1.0.0 |
| [ST_SimplifyVW](ST_SimplifyVW.md) | This function simplifies the input geometry by applying the Visvalingam-Whyatt algorithm. | v1.6.1 |
| [ST_Snap](ST_Snap.md) | Snaps the vertices and segments of the `input` geometry to `reference` geometry within the specified `tolerance` distance. The `tolerance` parameter controls the maximum snap distance. | v1.6.0 |
| [ST_StraightSkeleton](ST_StraightSkeleton.md) | Computes the straight skeleton of a polygonal geometry. The straight skeleton is a method of representing a polygon by a topological skeleton, formed by a continuous shrinking process where each ed... | v1.8.0 |
| [ST_TriangulatePolygon](ST_TriangulatePolygon.md) | Generates the constrained Delaunay triangulation for the input Polygon. The constrained Delaunay triangulation is a set of triangles created from the Polygon's vertices that covers the Polygon area... | v1.6.1 |
| [ST_VoronoiPolygons](ST_VoronoiPolygons.md) | Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry. The result is a GeometryCollection of Polygons that covers an envelope larger than the extent of the input vert... | v1.5.0 |
