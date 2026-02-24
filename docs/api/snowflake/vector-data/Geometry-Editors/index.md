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

# Geometry Editors

These functions create modified geometries by changing type, structure, or vertices.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AddPoint](ST_AddPoint.md) | RETURN Linestring with additional point at the given index, if position is not available the point will be added at the end of line. |  |
| [ST_Collect](ST_Collect.md) | Build an appropriate `Geometry`, `MultiGeometry`, or `GeometryCollection` to contain the `Geometry`s in it. For example: |  |
| [ST_CollectionExtract](ST_CollectionExtract.md) | Returns a homogeneous multi-geometry from a given geometry collection. |  |
| [ST_FlipCoordinates](ST_FlipCoordinates.md) | Returns a version of the given geometry with X and Y axis flipped. |  |
| [ST_Force2D](ST_Force2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force_2D](ST_Force_2D.md). |  |
| [ST_Force3D](ST_Force3D.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... |  |
| [ST_Force3DZ](ST_Force3DZ.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... |  |
| [ST_Force_2D](ST_Force_2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force2D](ST_Force2D.md). |  |
| [ST_ForceCollection](ST_ForceCollection.md) | This function converts the input geometry into a GeometryCollection, regardless of the original geometry type. If the input is a multipart geometry, such as a MultiPolygon or MultiLineString, it wi... |  |
| [ST_ForcePolygonCCW](ST_ForcePolygonCCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to counter-clockwise and interior rings to clockwise orientation. Non-polygonal geometries are returned unchanged. |  |
| [ST_ForcePolygonCW](ST_ForcePolygonCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to clockwise and interior rings to counter-clockwise orientation. Non-polygonal geometries are returned unchanged. |  |
| [ST_ForceRHR](ST_ForceRHR.md) | Sets the orientation of polygon vertex orderings to follow the Right-Hand-Rule convention. The exterior ring will have a clockwise winding order, while any interior rings are oriented counter-clock... |  |
| [ST_LineFromMultiPoint](ST_LineFromMultiPoint.md) | Creates a LineString from a MultiPoint geometry. |  |
| [ST_LineMerge](ST_LineMerge.md) | Returns a LineString or MultiLineString formed by sewing together the constituent line work of a MULTILINESTRING. |  |
| [ST_MakeLine](ST_MakeLine.md) | Creates a LineString containing the points of Point, MultiPoint, or LineString geometries. Other geometry types cause an error. |  |
| [ST_MakePolygon](ST_MakePolygon.md) | Function to convert closed linestring to polygon including holes. The holes must be a MultiLinestring. If holes are provided, they should be fully contained within the shell. Holes outside the shel... |  |
| [ST_Multi](ST_Multi.md) | Returns a MultiGeometry object based on the geometry input. ST_Multi is basically an alias for ST_Collect with one geometry. |  |
| [ST_Normalize](ST_Normalize.md) | Returns the input geometry in its normalized form. |  |
| [ST_Polygon](ST_Polygon.md) | Function to create a polygon built from the given LineString and sets the spatial reference system from the srid |  |
| [ST_Project](ST_Project.md) | Calculates a new point location given a starting point, distance, and azimuth. The azimuth indicates the direction, expressed in radians, and is measured in a clockwise manner starting from true no... |  |
| [ST_RemovePoint](ST_RemovePoint.md) | RETURN Line with removed point at given index, position can be omitted and then last one will be removed. |  |
| [ST_RemoveRepeatedPoints](ST_RemoveRepeatedPoints.md) | This function eliminates consecutive duplicate points within a geometry, preserving endpoints of LineStrings. It operates on (Multi)LineStrings, (Multi)Polygons, and MultiPoints, processing Geometr... |  |
| [ST_Reverse](ST_Reverse.md) | Return the geometry with vertex order reversed |  |
| [ST_Segmentize](ST_Segmentize.md) | Returns a modified geometry having no segment longer than the given max_segment_length. | v1.8.0 |
| [ST_SetPoint](ST_SetPoint.md) | Replace Nth point of linestring with given point. Index is 0-based. Negative index are counted backwards, e.g., -1 is last point. |  |
| [ST_ShiftLongitude](ST_ShiftLongitude.md) | Modifies longitude coordinates in geometries, shifting values between -180..0 degrees to 180..360 degrees and vice versa. This is useful for normalizing data across the International Date Line and ... | v1.6.0 |
