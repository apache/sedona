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

# Geometry Accessors

These functions extract information and properties from geometry objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [GeometryType](GeometryType.md) | Returns the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc. This function also indicates if the geometry is measured, by returning a string of the form 'POINTM'. | v1.5.0 |
| [ST_Boundary](ST_Boundary.md) | Returns the closure of the combinatorial boundary of this Geometry. | v1.0.0 |
| [ST_CoordDim](ST_CoordDim.md) | Returns the coordinate dimensions of the geometry. It is an alias of `ST_NDims`. | v1.5.0 |
| [ST_CrossesDateLine](ST_CrossesDateLine.md) | This function determines if a given geometry crosses the International Date Line. It operates by checking if the difference in longitude between any pair of consecutive points in the geometry excee... | v1.6.0 |
| [ST_Dimension](ST_Dimension.md) | Return the topological dimension of this Geometry object, which must be less than or equal to the coordinate dimension. OGC SPEC s2.1.1.1 - returns 0 for POINT, 1 for LINESTRING, 2 for POLYGON, and... | v1.5.0 |
| [ST_Dump](ST_Dump.md) | It expands the geometries. If the geometry is simple (Point, Polygon Linestring etc.) it returns the geometry itself, if the geometry is collection or multi it returns record for each of collection... | v1.0.0 |
| [ST_DumpPoints](ST_DumpPoints.md) | Returns list of Points which geometry consists of. | v1.0.0 |
| [ST_EndPoint](ST_EndPoint.md) | Returns last point of given linestring. | v1.0.0 |
| [ST_ExteriorRing](ST_ExteriorRing.md) | Returns a line string representing the exterior ring of the POLYGON geometry. Return NULL if the geometry is not a polygon. | v1.0.0 |
| [ST_GeometryN](ST_GeometryN.md) | Return the 0-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return null | v1.0.0 |
| [ST_GeometryType](ST_GeometryType.md) | Returns the type of the geometry as a string. EG: 'ST_Linestring', 'ST_Polygon' etc. | v1.0.0 |
| [ST_HasM](ST_HasM.md) | Checks for the presence of M coordinate values representing measures or linear references. Returns true if the input geometry includes an M coordinate, false otherwise. | v1.6.1 |
| [ST_HasZ](ST_HasZ.md) | Checks for the presence of Z coordinate values representing measures or linear references. Returns true if the input geometry includes an Z coordinate, false otherwise. | v1.6.1 |
| [ST_InteriorRingN](ST_InteriorRingN.md) | Returns the Nth interior linestring ring of the polygon geometry. Returns NULL if the geometry is not a polygon or the given N is out of range | v1.0.0 |
| [ST_IsClosed](ST_IsClosed.md) | RETURNS true if the LINESTRING start and end point are the same. | v1.0.0 |
| [ST_IsCollection](ST_IsCollection.md) | Returns `TRUE` if the geometry type of the input is a geometry collection type. Collection types are the following: | v1.5.0 |
| [ST_IsEmpty](ST_IsEmpty.md) | Test if a geometry is empty geometry | v1.2.1 |
| [ST_IsPolygonCCW](ST_IsPolygonCCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. | v1.6.0 |
| [ST_IsPolygonCW](ST_IsPolygonCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. | v1.6.0 |
| [ST_IsRing](ST_IsRing.md) | RETURN true if LINESTRING is ST_IsClosed and ST_IsSimple. | v1.0.0 |
| [ST_IsSimple](ST_IsSimple.md) | Test if geometry's only self-intersections are at boundary points. | v1.0.0 |
| [ST_M](ST_M.md) | Returns M Coordinate of given Point, null otherwise. | v1.6.1 |
| [ST_NDims](ST_NDims.md) | Returns the coordinate dimension of the geometry. | v1.3.1 |
| [ST_NPoints](ST_NPoints.md) | Return points of the geometry | v1.0.0 |
| [ST_NRings](ST_NRings.md) | Returns the number of rings in a Polygon or MultiPolygon. Contrary to ST_NumInteriorRings, this function also takes into account the number of exterior rings. | v1.4.1 |
| [ST_NumGeometries](ST_NumGeometries.md) | Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the number of geometries, for single geometries will return 1. | v1.0.0 |
| [ST_NumInteriorRing](ST_NumInteriorRing.md) | Returns number of interior rings of polygon geometries. It is an alias of [ST_NumInteriorRings](ST_NumInteriorRings.md). | v1.6.1 |
| [ST_NumInteriorRings](ST_NumInteriorRings.md) | RETURNS number of interior rings of polygon geometries. | v1.0.0 |
| [ST_NumPoints](ST_NumPoints.md) | Returns number of points in a LineString | v1.4.1 |
| [ST_PointN](ST_PointN.md) | Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL ... | v1.2.1 |
| [ST_Points](ST_Points.md) | Returns a MultiPoint geometry consisting of all the coordinates of the input geometry. It preserves duplicate points as well as M and Z coordinates. | v1.6.1 |
| [ST_StartPoint](ST_StartPoint.md) | Returns first point of given linestring. | v1.0.0 |
| [ST_X](ST_X.md) | Returns X Coordinate of given Point null otherwise. | v1.0.0 |
| [ST_Y](ST_Y.md) | Returns Y Coordinate of given Point, null otherwise. | v1.0.0 |
| [ST_Z](ST_Z.md) | Returns Z Coordinate of given Point, null otherwise. | v1.2.0 |
| [ST_Zmflag](ST_Zmflag.md) | Returns a code indicating the Z and M coordinate dimensions present in the input geometry. | v1.6.1 |
