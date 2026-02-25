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

# Geometry Functions

The `Geometry` type in Sedona represents planar (2D Euclidean) spatial objects such as points, lines, and polygons. All coordinates are treated as Cartesian (x, y) values, and spatial operations — distance, area, intersection, etc. — use flat-plane math.

## Geometry Constructors

These functions create geometry objects from various textual or binary formats, or from coordinate values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_GeomCollFromText](Geometry-Constructors/ST_GeomCollFromText.md) | Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`. | v1.6.1 |
| [ST_GeometryFromText](Geometry-Constructors/ST_GeometryFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) | v1.6.1 |
| [ST_GeomFromEWKB](Geometry-Constructors/ST_GeomFromEWKB.md) | Construct a Geometry from EWKB string or Binary. This function is an alias of [ST_GeomFromWKB](Geometry-Constructors/ST_GeomFromWKB.md). | v1.6.1 |
| [ST_GeomFromEWKT](Geometry-Constructors/ST_GeomFromEWKT.md) | Construct a Geometry from OGC Extended WKT | v1.5.0 |
| [ST_GeomFromGeoHash](Geometry-Constructors/ST_GeomFromGeoHash.md) | Create Geometry from geohash string and optional precision | v1.1.1 |
| [ST_GeomFromGeoJSON](Geometry-Constructors/ST_GeomFromGeoJSON.md) | Construct a Geometry from GeoJson | v1.0.0 |
| [ST_GeomFromGML](Geometry-Constructors/ST_GeomFromGML.md) | Construct a Geometry from GML. | v1.3.0 |
| [ST_GeomFromKML](Geometry-Constructors/ST_GeomFromKML.md) | Construct a Geometry from KML. | v1.3.0 |
| [ST_GeomFromMySQL](Geometry-Constructors/ST_GeomFromMySQL.md) | Construct a Geometry from MySQL Geometry binary. | v1.8.0 |
| [ST_GeomFromText](Geometry-Constructors/ST_GeomFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) | v1.0.0 |
| [ST_GeomFromWKB](Geometry-Constructors/ST_GeomFromWKB.md) | Construct a Geometry from WKB string or Binary. This function also supports EWKB format. | v1.0.0 |
| [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.0.0 |
| [ST_LineFromText](Geometry-Constructors/ST_LineFromText.md) | Construct a Line from Wkt text | v1.2.1 |
| [ST_LineFromWKB](Geometry-Constructors/ST_LineFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. | v1.6.1 |
| [ST_LineStringFromText](Geometry-Constructors/ST_LineStringFromText.md) | Construct a LineString from Text, delimited by Delimiter | v1.0.0 |
| [ST_LinestringFromWKB](Geometry-Constructors/ST_LinestringFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format and it is an alias of [ST_LineFromWKB](Geometry-Constructors/ST_LineFromWKB.md). | v1.6.1 |
| [ST_MakeEnvelope](Geometry-Constructors/ST_MakeEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY, and an optional SRID. | v1.7.0 |
| [ST_MakePoint](Geometry-Constructors/ST_MakePoint.md) | Creates a 2D, 3D Z or 4D ZM Point geometry. Use [ST_MakePointM](Geometry-Constructors/ST_MakePointM.md) to make points with XYM coordinates. Z and M values are optional. | v1.5.0 |
| [ST_MakePointM](Geometry-Constructors/ST_MakePointM.md) | Creates a point with X, Y, and M coordinate. Use [ST_MakePoint](Geometry-Constructors/ST_MakePoint.md) to make points with XY, XYZ, or XYZM coordinates. | v1.6.1 |
| [ST_MLineFromText](Geometry-Constructors/ST_MLineFromText.md) | Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown). | v1.3.1 |
| [ST_MPointFromText](Geometry-Constructors/ST_MPointFromText.md) | Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`. | v1.6.1 |
| [ST_MPolyFromText](Geometry-Constructors/ST_MPolyFromText.md) | Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown). | v1.3.1 |
| [ST_Point](Geometry-Constructors/ST_Point.md) | Construct a Point from X and Y | v1.0.0 |
| [ST_PointFromGeoHash](Geometry-Constructors/ST_PointFromGeoHash.md) | Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` valu... | v1.6.1 |
| [ST_PointFromText](Geometry-Constructors/ST_PointFromText.md) | Construct a Point from Text, delimited by Delimiter | v1.0.0 |
| [ST_PointFromWKB](Geometry-Constructors/ST_PointFromWKB.md) | Construct a Point geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. | v1.6.1 |
| [ST_PointM](Geometry-Constructors/ST_PointM.md) | Construct a Point from X, Y and M and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z and M coordinates. | v1.6.1 |
| [ST_PointZ](Geometry-Constructors/ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. | v1.4.0 |
| [ST_PointZM](Geometry-Constructors/ST_PointZM.md) | Construct a Point from X, Y, Z, M and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z and M coordinates. | v1.6.1 |
| [ST_PolygonFromEnvelope](Geometry-Constructors/ST_PolygonFromEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY. | v1.0.0 |
| [ST_PolygonFromText](Geometry-Constructors/ST_PolygonFromText.md) | Construct a Polygon from Text, delimited by Delimiter. Path must be closed | v1.0.0 |

## Geometry Accessors

These functions extract information and properties from geometry objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [GeometryType](Geometry-Accessors/GeometryType.md) | Returns the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc. This function also indicates if the geometry is measured, by returning a string of the form 'POINTM'. | v1.5.0 |
| [ST_Boundary](Geometry-Accessors/ST_Boundary.md) | Returns the closure of the combinatorial boundary of this Geometry. | v1.0.0 |
| [ST_CoordDim](Geometry-Accessors/ST_CoordDim.md) | Returns the coordinate dimensions of the geometry. It is an alias of `ST_NDims`. | v1.5.0 |
| [ST_CrossesDateLine](Geometry-Accessors/ST_CrossesDateLine.md) | This function determines if a given geometry crosses the International Date Line. It operates by checking if the difference in longitude between any pair of consecutive points in the geometry excee... | v1.6.0 |
| [ST_Dimension](Geometry-Accessors/ST_Dimension.md) | Return the topological dimension of this Geometry object, which must be less than or equal to the coordinate dimension. OGC SPEC s2.1.1.1 - returns 0 for POINT, 1 for LINESTRING, 2 for POLYGON, and... | v1.5.0 |
| [ST_Dump](Geometry-Accessors/ST_Dump.md) | It expands the geometries. If the geometry is simple (Point, Polygon Linestring etc.) it returns the geometry itself, if the geometry is collection or multi it returns record for each of collection... | v1.0.0 |
| [ST_DumpPoints](Geometry-Accessors/ST_DumpPoints.md) | Returns list of Points which geometry consists of. | v1.0.0 |
| [ST_EndPoint](Geometry-Accessors/ST_EndPoint.md) | Returns last point of given linestring. | v1.0.0 |
| [ST_ExteriorRing](Geometry-Accessors/ST_ExteriorRing.md) | Returns a line string representing the exterior ring of the POLYGON geometry. Return NULL if the geometry is not a polygon. | v1.0.0 |
| [ST_GeometryN](Geometry-Accessors/ST_GeometryN.md) | Return the 0-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return null | v1.0.0 |
| [ST_GeometryType](Geometry-Accessors/ST_GeometryType.md) | Returns the type of the geometry as a string. EG: 'ST_Linestring', 'ST_Polygon' etc. | v1.0.0 |
| [ST_HasM](Geometry-Accessors/ST_HasM.md) | Checks for the presence of M coordinate values representing measures or linear references. Returns true if the input geometry includes an M coordinate, false otherwise. | v1.6.1 |
| [ST_HasZ](Geometry-Accessors/ST_HasZ.md) | Checks for the presence of Z coordinate values representing measures or linear references. Returns true if the input geometry includes an Z coordinate, false otherwise. | v1.6.1 |
| [ST_InteriorRingN](Geometry-Accessors/ST_InteriorRingN.md) | Returns the Nth interior linestring ring of the polygon geometry. Returns NULL if the geometry is not a polygon or the given N is out of range | v1.0.0 |
| [ST_IsClosed](Geometry-Accessors/ST_IsClosed.md) | RETURNS true if the LINESTRING start and end point are the same. | v1.0.0 |
| [ST_IsCollection](Geometry-Accessors/ST_IsCollection.md) | Returns `TRUE` if the geometry type of the input is a geometry collection type. Collection types are the following: | v1.5.0 |
| [ST_IsEmpty](Geometry-Accessors/ST_IsEmpty.md) | Test if a geometry is empty geometry | v1.2.1 |
| [ST_IsPolygonCCW](Geometry-Accessors/ST_IsPolygonCCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. | v1.6.0 |
| [ST_IsPolygonCW](Geometry-Accessors/ST_IsPolygonCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. | v1.6.0 |
| [ST_IsRing](Geometry-Accessors/ST_IsRing.md) | RETURN true if LINESTRING is ST_IsClosed and ST_IsSimple. | v1.0.0 |
| [ST_IsSimple](Geometry-Accessors/ST_IsSimple.md) | Test if geometry's only self-intersections are at boundary points. | v1.0.0 |
| [ST_M](Geometry-Accessors/ST_M.md) | Returns M Coordinate of given Point, null otherwise. | v1.6.1 |
| [ST_NDims](Geometry-Accessors/ST_NDims.md) | Returns the coordinate dimension of the geometry. | v1.3.1 |
| [ST_NPoints](Geometry-Accessors/ST_NPoints.md) | Return points of the geometry | v1.0.0 |
| [ST_NRings](Geometry-Accessors/ST_NRings.md) | Returns the number of rings in a Polygon or MultiPolygon. Contrary to ST_NumInteriorRings, this function also takes into account the number of exterior rings. | v1.4.1 |
| [ST_NumGeometries](Geometry-Accessors/ST_NumGeometries.md) | Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the number of geometries, for single geometries will return 1. | v1.0.0 |
| [ST_NumInteriorRing](Geometry-Accessors/ST_NumInteriorRing.md) | Returns number of interior rings of polygon geometries. It is an alias of [ST_NumInteriorRings](Geometry-Accessors/ST_NumInteriorRings.md). | v1.6.1 |
| [ST_NumInteriorRings](Geometry-Accessors/ST_NumInteriorRings.md) | RETURNS number of interior rings of polygon geometries. | v1.0.0 |
| [ST_NumPoints](Geometry-Accessors/ST_NumPoints.md) | Returns number of points in a LineString | v1.4.1 |
| [ST_PointN](Geometry-Accessors/ST_PointN.md) | Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL ... | v1.2.1 |
| [ST_Points](Geometry-Accessors/ST_Points.md) | Returns a MultiPoint geometry consisting of all the coordinates of the input geometry. It preserves duplicate points as well as M and Z coordinates. | v1.6.1 |
| [ST_StartPoint](Geometry-Accessors/ST_StartPoint.md) | Returns first point of given linestring. | v1.0.0 |
| [ST_X](Geometry-Accessors/ST_X.md) | Returns X Coordinate of given Point null otherwise. | v1.0.0 |
| [ST_Y](Geometry-Accessors/ST_Y.md) | Returns Y Coordinate of given Point, null otherwise. | v1.0.0 |
| [ST_Z](Geometry-Accessors/ST_Z.md) | Returns Z Coordinate of given Point, null otherwise. | v1.2.0 |
| [ST_Zmflag](Geometry-Accessors/ST_Zmflag.md) | Returns a code indicating the Z and M coordinate dimensions present in the input geometry. | v1.6.1 |

## Geometry Editors

These functions create modified geometries by changing type, structure, or vertices.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AddPoint](Geometry-Editors/ST_AddPoint.md) | RETURN Linestring with additional point at the given index, if position is not available the point will be added at the end of line. | v1.0.0 |
| [ST_Collect](Geometry-Editors/ST_Collect.md) | Returns MultiGeometry object based on geometry column/s or array with geometries | v1.2.0 |
| [ST_CollectionExtract](Geometry-Editors/ST_CollectionExtract.md) | Returns a homogeneous multi-geometry from a given geometry collection. | v1.2.1 |
| [ST_FlipCoordinates](Geometry-Editors/ST_FlipCoordinates.md) | Returns a version of the given geometry with X and Y axis flipped. | v1.0.0 |
| [ST_Force2D](Geometry-Editors/ST_Force2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force_2D](Geometry-Editors/ST_Force_2D.md). | v1.8.0 |
| [ST_Force3D](Geometry-Editors/ST_Force3D.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... | v1.4.1 |
| [ST_Force3DM](Geometry-Editors/ST_Force3DM.md) | Forces the geometry into XYM mode. Retains any existing M coordinate, but removes the Z coordinate if present. Assigns a default M value of 0.0 if `mValue` is not specified. | v1.6.1 |
| [ST_Force3DZ](Geometry-Editors/ST_Force3DZ.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... | v1.6.1 |
| [ST_Force4D](Geometry-Editors/ST_Force4D.md) | Converts the input geometry to 4D XYZM representation. Retains original Z and M values if present. Assigning 0.0 defaults if `mValue` and `zValue` aren't specified. The output contains X, Y, Z, and... | v1.6.1 |
| [ST_Force_2D](Geometry-Editors/ST_Force_2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force2D](Geometry-Editors/ST_Force2D.md). | v1.2.1 |
| [ST_ForceCollection](Geometry-Editors/ST_ForceCollection.md) | This function converts the input geometry into a GeometryCollection, regardless of the original geometry type. If the input is a multipart geometry, such as a MultiPolygon or MultiLineString, it wi... | v1.6.1 |
| [ST_ForcePolygonCCW](Geometry-Editors/ST_ForcePolygonCCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to counter-clockwise and interior rings to clockwise orientation. Non-polygonal geometries are returned unchanged. | v1.6.0 |
| [ST_ForcePolygonCW](Geometry-Editors/ST_ForcePolygonCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to clockwise and interior rings to counter-clockwise orientation. Non-polygonal geometries are returned unchanged. | v1.6.0 |
| [ST_ForceRHR](Geometry-Editors/ST_ForceRHR.md) | Sets the orientation of polygon vertex orderings to follow the Right-Hand-Rule convention. The exterior ring will have a clockwise winding order, while any interior rings are oriented counter-clock... | v1.6.1 |
| [ST_LineFromMultiPoint](Geometry-Editors/ST_LineFromMultiPoint.md) | Creates a LineString from a MultiPoint geometry. | v1.3.0 |
| [ST_LineMerge](Geometry-Editors/ST_LineMerge.md) | Returns a LineString or MultiLineString formed by sewing together the constituent line work of a MULTILINESTRING. | v1.0.0 |
| [ST_LineSegments](Geometry-Editors/ST_LineSegments.md) | This function transforms a LineString containing multiple coordinates into an array of LineStrings, each with precisely two coordinates. The `lenient` argument, true by default, prevents an excepti... | v1.7.1 |
| [ST_MakeLine](Geometry-Editors/ST_MakeLine.md) | Creates a LineString containing the points of Point, MultiPoint, or LineString geometries. Other geometry types cause an error. | v1.5.0 |
| [ST_MakePolygon](Geometry-Editors/ST_MakePolygon.md) | Function to convert closed linestring to polygon including holes. If holes are provided, they should be fully contained within the shell. Holes outside the shell will produce an invalid polygon (ma... | v1.1.0 |
| [ST_Multi](Geometry-Editors/ST_Multi.md) | Returns a MultiGeometry object based on the geometry input. ST_Multi is basically an alias for ST_Collect with one geometry. | v1.2.0 |
| [ST_Normalize](Geometry-Editors/ST_Normalize.md) | Returns the input geometry in its normalized form. | v1.3.0 |
| [ST_Polygon](Geometry-Editors/ST_Polygon.md) | Function to create a polygon built from the given LineString and sets the spatial reference system from the srid | v1.5.0 |
| [ST_Project](Geometry-Editors/ST_Project.md) | Calculates a new point location given a starting point, distance, and azimuth. The azimuth indicates the direction, expressed in radians, and is measured in a clockwise manner starting from true no... | v1.7.0 |
| [ST_RemovePoint](Geometry-Editors/ST_RemovePoint.md) | RETURN Line with removed point at given index, position can be omitted and then last one will be removed. | v1.0.0 |
| [ST_RemoveRepeatedPoints](Geometry-Editors/ST_RemoveRepeatedPoints.md) | This function eliminates consecutive duplicate points within a geometry, preserving endpoints of LineStrings. It operates on (Multi)LineStrings, (Multi)Polygons, and MultiPoints, processing Geometr... | v1.7.0 |
| [ST_Reverse](Geometry-Editors/ST_Reverse.md) | Return the geometry with vertex order reversed | v1.2.1 |
| [ST_Segmentize](Geometry-Editors/ST_Segmentize.md) | Returns a modified geometry having no segment longer than the given max_segment_length. | v1.8.0 |
| [ST_SetPoint](Geometry-Editors/ST_SetPoint.md) | Replace Nth point of linestring with given point. Index is 0-based. Negative index are counted backwards, e.g., -1 is last point. | v1.3.0 |
| [ST_ShiftLongitude](Geometry-Editors/ST_ShiftLongitude.md) | Modifies longitude coordinates in geometries, shifting values between -180..0 degrees to 180..360 degrees and vice versa. This is useful for normalizing data across the International Date Line and ... | v1.6.0 |

## Geometry Output

These functions convert geometry objects into various textual or binary formats.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AsBinary](Geometry-Output/ST_AsBinary.md) | Return the Well-Known Binary representation of a geometry | v1.1.1 |
| [ST_AsEWKB](Geometry-Output/ST_AsEWKB.md) | Return the Extended Well-Known Binary representation of a geometry. EWKB is an extended version of WKB which includes the SRID of the geometry. The format originated in PostGIS but is supported by ... | v1.1.1 |
| [ST_AsEWKT](Geometry-Output/ST_AsEWKT.md) | Return the Extended Well-Known Text representation of a geometry. EWKT is an extended version of WKT which includes the SRID of the geometry. The format originated in PostGIS but is supported by ma... | v1.2.1 |
| [ST_AsGeoJSON](Geometry-Output/ST_AsGeoJSON.md) | Return the [GeoJSON](https://geojson.org/) string representation of a geometry | v1.6.1 |
| [ST_AsGML](Geometry-Output/ST_AsGML.md) | Return the [GML](https://www.ogc.org/standards/gml) string representation of a geometry | v1.3.0 |
| [ST_AsHEXEWKB](Geometry-Output/ST_AsHEXEWKB.md) | This function returns the input geometry encoded to a text representation in HEXEWKB format. The HEXEWKB encoding can use either little-endian (NDR) or big-endian (XDR) byte ordering. If no encodin... | v1.6.1 |
| [ST_AsKML](Geometry-Output/ST_AsKML.md) | Return the [KML](https://www.ogc.org/standards/kml) string representation of a geometry | v1.3.0 |
| [ST_AsText](Geometry-Output/ST_AsText.md) | Return the Well-Known Text string representation of a geometry. It will support M coordinate if present since v1.5.0. | v1.0.0 |
| [ST_GeoHash](Geometry-Output/ST_GeoHash.md) | Returns GeoHash of the geometry with given precision | v1.1.1 |

## Predicates

These functions test spatial relationships between geometries, returning boolean values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Contains](Predicates/ST_Contains.md) | Return true if A fully contains B | v1.0.0 |
| [ST_CoveredBy](Predicates/ST_CoveredBy.md) | Return true if A is covered by B | v1.3.0 |
| [ST_Covers](Predicates/ST_Covers.md) | Return true if A covers B | v1.3.0 |
| [ST_Crosses](Predicates/ST_Crosses.md) | Return true if A crosses B | v1.0.0 |
| [ST_Disjoint](Predicates/ST_Disjoint.md) | Return true if A and B are disjoint | v1.2.1 |
| [ST_DWithin](Predicates/ST_DWithin.md) | Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'. | v1.5.1 |
| [ST_Equals](Predicates/ST_Equals.md) | Return true if A equals to B | v1.0.0 |
| [ST_Intersects](Predicates/ST_Intersects.md) | Return true if A intersects B | v1.0.0 |
| [ST_OrderingEquals](Predicates/ST_OrderingEquals.md) | Returns true if the geometries are equal and the coordinates are in the same order | v1.2.1 |
| [ST_Overlaps](Predicates/ST_Overlaps.md) | Return true if A overlaps B | v1.0.0 |
| [ST_Relate](Predicates/ST_Relate.md) | The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relati... | v1.6.1 |
| [ST_RelateMatch](Predicates/ST_RelateMatch.md) | This function tests the relationship between two [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrices representing geometry intersections. It evalua... | v1.6.1 |
| [ST_Touches](Predicates/ST_Touches.md) | Return true if A touches B | v1.0.0 |
| [ST_Within](Predicates/ST_Within.md) | Return true if A is fully contained by B | v1.0.0 |

## Measurement Functions

These functions compute measurements of distance, area, length, and angles.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_3DDistance](Measurement-Functions/ST_3DDistance.md) | Return the 3-dimensional minimum cartesian distance between A and B | v1.2.0 |
| [ST_Angle](Measurement-Functions/ST_Angle.md) | Computes and returns the angle between two vectors represented by the provided points or linestrings. | v1.5.0 |
| [ST_Area](Measurement-Functions/ST_Area.md) | Return the area of A | v1.0.0 |
| [ST_AreaSpheroid](Measurement-Functions/ST_AreaSpheroid.md) | Return the geodesic area of A using WGS84 spheroid. Unit is square meter. Works better for large geometries (country level) compared to `ST_Area` + `ST_Transform`. It is equivalent to PostGIS `ST_A... | v1.4.1 |
| [ST_Azimuth](Measurement-Functions/ST_Azimuth.md) | Returns Azimuth for two given points in radians. Returns null if the two points are identical. | v1.0.0 |
| [ST_ClosestPoint](Measurement-Functions/ST_ClosestPoint.md) | Returns the 2-dimensional point on geom1 that is closest to geom2. This is the first point of the shortest line between the geometries. If using 3D geometries, the Z coordinates will be ignored. If... | v1.5.0 |
| [ST_Degrees](Measurement-Functions/ST_Degrees.md) | Convert an angle in radian to degrees. | v1.5.0 |
| [ST_Distance](Measurement-Functions/ST_Distance.md) | Return the Euclidean distance between A and B | v1.0.0 |
| [ST_DistanceSphere](Measurement-Functions/ST_DistanceSphere.md) | Return the haversine / great-circle distance of A using a given earth radius (default radius: 6371008.0). Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets tha... | v1.4.1 |
| [ST_DistanceSpheroid](Measurement-Functions/ST_DistanceSpheroid.md) | Return the geodesic distance of A using WGS84 spheroid. Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that cover large regions such as continents or the en... | v1.4.1 |
| [ST_FrechetDistance](Measurement-Functions/ST_FrechetDistance.md) | Computes and returns discrete [Frechet Distance](https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance) between the given two geometries, based on [Computing Discrete Frechet Distance](http://www.kr.... | v1.5.0 |
| [ST_HausdorffDistance](Measurement-Functions/ST_HausdorffDistance.md) | Returns a discretized (and hence approximate) [Hausdorff distance](https://en.wikipedia.org/wiki/Hausdorff_distance) between the given 2 geometries. Optionally, a densityFraction parameter can be s... | v1.5.0 |
| [ST_Length](Measurement-Functions/ST_Length.md) | Returns the perimeter of A. | v1.0.0 |
| [ST_Length2D](Measurement-Functions/ST_Length2D.md) | Returns the perimeter of A. This function is an alias of [ST_Length](Measurement-Functions/ST_Length.md). | v1.6.1 |
| [ST_LengthSpheroid](Measurement-Functions/ST_LengthSpheroid.md) | Return the geodesic perimeter of A using WGS84 spheroid. Unit is meter. Works better for large geometries (country level) compared to `ST_Length` + `ST_Transform`. It is equivalent to PostGIS `ST_L... | v1.4.1 |
| [ST_LongestLine](Measurement-Functions/ST_LongestLine.md) | Returns the LineString geometry representing the maximum distance between any two points from the input geometries. | v1.6.1 |
| [ST_MaxDistance](Measurement-Functions/ST_MaxDistance.md) | Calculates and returns the length value representing the maximum distance between any two points across the input geometries. This function is an alias for `ST_LongestDistance`. | v1.6.1 |
| [ST_MinimumClearance](Measurement-Functions/ST_MinimumClearance.md) | The minimum clearance is a metric that quantifies a geometry's tolerance to changes in coordinate precision or vertex positions. It represents the maximum distance by which vertices can be adjusted... | v1.6.1 |
| [ST_MinimumClearanceLine](Measurement-Functions/ST_MinimumClearanceLine.md) | This function returns a two-point LineString geometry representing the minimum clearance distance of the input geometry. If the input geometry does not have a defined minimum clearance, such as for... | v1.6.1 |
| [ST_Perimeter](Measurement-Functions/ST_Perimeter.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... | v1.7.0 |
| [ST_Perimeter2D](Measurement-Functions/ST_Perimeter2D.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... | v1.7.1 |

## Geometry Processing

These functions compute geometric constructions, or alter geometry size or shape.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_ApproximateMedialAxis](Geometry-Processing/ST_ApproximateMedialAxis.md) | Computes an approximate medial axis of a polygonal geometry. The medial axis is a representation of the "centerline" or "skeleton" of the polygon. This function first computes the straight skeleton... | v1.8.0 |
| [ST_Buffer](Geometry-Processing/ST_Buffer.md) | Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance. The function supports both Planar/Euclidean and Spheroidal/Geo... | v1.6.0 |
| [ST_BuildArea](Geometry-Processing/ST_BuildArea.md) | Returns the areal geometry formed by the constituent linework of the input geometry. | v1.2.1 |
| [ST_Centroid](Geometry-Processing/ST_Centroid.md) | Return the centroid point of A | v1.0.0 |
| [ST_ConcaveHull](Geometry-Processing/ST_ConcaveHull.md) | Return the Concave Hull of polygon A, with alpha set to pctConvex[0, 1] in the Delaunay Triangulation method, the concave hull will not contain a hole unless allowHoles is set to true | v1.4.0 |
| [ST_ConvexHull](Geometry-Processing/ST_ConvexHull.md) | Return the Convex Hull of polygon A | v1.0.0 |
| [ST_DelaunayTriangles](Geometry-Processing/ST_DelaunayTriangles.md) | This function computes the [Delaunay triangulation](https://en.wikipedia.org/wiki/Delaunay_triangulation) for the set of vertices in the input geometry. An optional `tolerance` parameter allows sna... | v1.6.1 |
| [ST_GeneratePoints](Geometry-Processing/ST_GeneratePoints.md) | Generates a specified quantity of pseudo-random points within the boundaries of the provided polygonal geometry. When `seed` is either zero or not defined then output will be random. | v1.6.1 |
| [ST_GeometricMedian](Geometry-Processing/ST_GeometricMedian.md) | Computes the approximate geometric median of a MultiPoint geometry using the Weiszfeld algorithm. The geometric median provides a centrality measure that is less sensitive to outlier points than th... | v1.4.1 |
| [ST_LabelPoint](Geometry-Processing/ST_LabelPoint.md) | `ST_LabelPoint` computes and returns a label point for a given polygon or geometry collection. The label point is chosen to be sufficiently far from boundaries of the geometry. For a regular Polygo... | v1.7.1 |
| [ST_MaximumInscribedCircle](Geometry-Processing/ST_MaximumInscribedCircle.md) | Finds the largest circle that is contained within a (multi)polygon, or which does not overlap any lines and points. Returns a row with fields: | v1.6.1 |
| [ST_MinimumBoundingCircle](Geometry-Processing/ST_MinimumBoundingCircle.md) | Returns the smallest circle polygon that contains a geometry. The optional quadrantSegments parameter determines how many segments to use per quadrant and the default number of segments has been ch... | v1.0.1 |
| [ST_MinimumBoundingRadius](Geometry-Processing/ST_MinimumBoundingRadius.md) | Returns a struct containing the center point and radius of the smallest circle that contains a geometry. | v1.0.1 |
| [ST_OrientedEnvelope](Geometry-Processing/ST_OrientedEnvelope.md) | Returns the minimum-area rotated rectangle enclosing a geometry. The rectangle may be rotated relative to the coordinate axes. Degenerate inputs may result in a Point or LineString being returned. | v1.8.1 |
| [ST_PointOnSurface](Geometry-Processing/ST_PointOnSurface.md) | Returns a POINT guaranteed to lie on the surface. | v1.2.1 |
| [ST_Polygonize](Geometry-Processing/ST_Polygonize.md) | Generates a GeometryCollection composed of polygons that are formed from the linework of an input GeometryCollection. When the input does not contain any linework that forms a polygon, the function... | v1.6.0 |
| [ST_ReducePrecision](Geometry-Processing/ST_ReducePrecision.md) | Reduce the decimals places in the coordinates of the geometry to the given number of decimal places. The last decimal place will be rounded. This function was called ST_PrecisionReduce in versions ... | v1.0.0 |
| [ST_Simplify](Geometry-Processing/ST_Simplify.md) | This function simplifies the input geometry by applying the Douglas-Peucker algorithm. | v1.7.0 |
| [ST_SimplifyPolygonHull](Geometry-Processing/ST_SimplifyPolygonHull.md) | This function computes a topology-preserving simplified hull, either outer or inner, for a polygonal geometry input. An outer hull fully encloses the original geometry, while an inner hull lies ent... | v1.6.1 |
| [ST_SimplifyPreserveTopology](Geometry-Processing/ST_SimplifyPreserveTopology.md) | Simplifies a geometry and ensures that the result is a valid geometry having the same dimension and number of components as the input, and with the components having the same topological relationship. | v1.0.0 |
| [ST_SimplifyVW](Geometry-Processing/ST_SimplifyVW.md) | This function simplifies the input geometry by applying the Visvalingam-Whyatt algorithm. | v1.6.1 |
| [ST_Snap](Geometry-Processing/ST_Snap.md) | Snaps the vertices and segments of the `input` geometry to `reference` geometry within the specified `tolerance` distance. The `tolerance` parameter controls the maximum snap distance. | v1.6.0 |
| [ST_StraightSkeleton](Geometry-Processing/ST_StraightSkeleton.md) | Computes the straight skeleton of a polygonal geometry. The straight skeleton is a method of representing a polygon by a topological skeleton, formed by a continuous shrinking process where each ed... | v1.8.0 |
| [ST_TriangulatePolygon](Geometry-Processing/ST_TriangulatePolygon.md) | Generates the constrained Delaunay triangulation for the input Polygon. The constrained Delaunay triangulation is a set of triangles created from the Polygon's vertices that covers the Polygon area... | v1.6.1 |
| [ST_VoronoiPolygons](Geometry-Processing/ST_VoronoiPolygons.md) | Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry. The result is a GeometryCollection of Polygons that covers an envelope larger than the extent of the input vert... | v1.5.0 |

## Overlay Functions

These functions compute results arising from the overlay of two geometries. These are also known as point-set theoretic boolean operations.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Difference](Overlay-Functions/ST_Difference.md) | Return the difference between geometry A and B (return part of geometry A that does not intersect geometry B) | v1.2.0 |
| [ST_Intersection](Overlay-Functions/ST_Intersection.md) | Return the intersection geometry of A and B | v1.0.0 |
| [ST_Split](Overlay-Functions/ST_Split.md) | Split an input geometry by another geometry (called the blade). Linear (LineString or MultiLineString) geometry can be split by a Point, MultiPoint, LineString, MultiLineString, Polygon, or MultiPo... | v1.4.0 |
| [ST_SubDivide](Overlay-Functions/ST_SubDivide.md) | Returns list of geometries divided based of given maximum number of vertices. | v1.1.0 |
| [ST_SubDivideExplode](Overlay-Functions/ST_SubDivideExplode.md) | It works the same as ST_SubDivide but returns new rows with geometries instead of list. | v1.1.0 |
| [ST_SymDifference](Overlay-Functions/ST_SymDifference.md) | Return the symmetrical difference between geometry A and B (return parts of geometries which are in either of the sets, but not in their intersection) | v1.2.0 |
| [ST_UnaryUnion](Overlay-Functions/ST_UnaryUnion.md) | This variant of [ST_Union](Overlay-Functions/ST_Union.md) operates on a single geometry input. The input geometry can be a simple Geometry type, a MultiGeometry, or a GeometryCollection. The function calculates the ge... | v1.6.1 |
| [ST_Union](Overlay-Functions/ST_Union.md) | Variant 1: Return the union of geometry A and B. | v1.2.0 |

## Affine Transformations

These functions change the position and shape of geometries using affine transformations.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Affine](Affine-Transformations/ST_Affine.md) | Apply an affine transformation to the given geometry. |  |
| [ST_Rotate](Affine-Transformations/ST_Rotate.md) | Rotates a geometry by a specified angle in radians counter-clockwise around a given origin point. The origin for rotation can be specified as either a POINT geometry or x and y coordinates. If the ... | v1.6.1 |
| [ST_RotateX](Affine-Transformations/ST_RotateX.md) | Performs a counter-clockwise rotation of the specified geometry around the X-axis by the given angle measured in radians. | v1.6.1 |
| [ST_RotateY](Affine-Transformations/ST_RotateY.md) | Performs a counter-clockwise rotation of the specified geometry around the Y-axis by the given angle measured in radians. | v1.7.0 |
| [ST_Scale](Affine-Transformations/ST_Scale.md) | This function scales the geometry to a new size by multiplying the ordinates with the corresponding scaling factors provided as parameters `scaleX` and `scaleY`. | v1.7.0 |
| [ST_ScaleGeom](Affine-Transformations/ST_ScaleGeom.md) | This function scales the input geometry (`geometry`) to a new size. It does this by multiplying the coordinates of the input geometry with corresponding values from another geometry (`factor`) repr... | v1.7.0 |
| [ST_Translate](Affine-Transformations/ST_Translate.md) | Returns the input geometry with its X, Y and Z coordinates (if present in the geometry) translated by deltaX, deltaY and deltaZ (if specified) | v1.4.1 |

## Aggregate Functions

These functions perform aggregate operations on groups of geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Collect_Agg](Aggregate-Functions/ST_Collect_Agg.md) | Collects all geometries in a geometry column into a single multi-geometry (MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection). Unlike `ST_Union_Agg`, this function does not dissolve ... | v1.8.1 |
| [ST_Envelope_Agg](Aggregate-Functions/ST_Envelope_Agg.md) | Return the entire envelope boundary of all geometries in A. Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. This behavior is consistent with PostG... | v1.0.0 |
| [ST_Intersection_Agg](Aggregate-Functions/ST_Intersection_Agg.md) | Return the polygon intersection of all polygons in A | v1.0.0 |
| [ST_Union_Agg](Aggregate-Functions/ST_Union_Agg.md) | Return the polygon union of all polygons in A | v1.0.0 |

## Linear Referencing

These functions work with linear referencing, measures along lines, and trajectory data.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AddMeasure](Linear-Referencing/ST_AddMeasure.md) | Computes a new geometry with measure (M) values linearly interpolated between start and end points. For geometries lacking M dimensions, M values are added. Existing M values are overwritten by the... | v1.6.1 |
| [ST_InterpolatePoint](Linear-Referencing/ST_InterpolatePoint.md) | Returns the interpolated measure value of a linear measured LineString at the point closest to the specified point. | v1.7.0 |
| [ST_IsValidTrajectory](Linear-Referencing/ST_IsValidTrajectory.md) | This function checks if a geometry is a valid trajectory representation. For a trajectory to be considered valid, it must be a LineString that includes measure (M) values. The key requirement is th... | v1.6.1 |
| [ST_LineInterpolatePoint](Linear-Referencing/ST_LineInterpolatePoint.md) | Returns a point interpolated along a line. First argument must be a LINESTRING. Second argument is a Double between 0 and 1 representing fraction of total linestring length the point has to be loca... | v1.0.1 |
| [ST_LineLocatePoint](Linear-Referencing/ST_LineLocatePoint.md) | Returns a double between 0 and 1, representing the location of the closest point on the LineString as a fraction of its total length. The first argument must be a LINESTRING, and the second argumen... | v1.5.1 |
| [ST_LineSubstring](Linear-Referencing/ST_LineSubstring.md) | Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length. Second and third arguments are Double values between 0 and 1. This only works w... | v1.0.1 |
| [ST_LocateAlong](Linear-Referencing/ST_LocateAlong.md) | This function computes Point or MultiPoint geometries representing locations along a measured input geometry (LineString or MultiLineString) corresponding to the provided measure value(s). Polygona... | v1.6.1 |

## Spatial Reference System

These functions work with the Spatial Reference System of geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BestSRID](Spatial-Reference-System/ST_BestSRID.md) | Returns the estimated most appropriate Spatial Reference Identifier (SRID) for a given geometry, based on its spatial extent and location. It evaluates the geometry's bounding envelope and selects ... | v1.6.0 |
| [ST_SetSRID](Spatial-Reference-System/ST_SetSRID.md) | Sets the spatial reference system identifier (SRID) of the geometry. | v1.1.1 |
| [ST_SRID](Spatial-Reference-System/ST_SRID.md) | Return the spatial reference system identifier (SRID) of the geometry. | v1.1.1 |
| [ST_Transform](Spatial-Reference-System/ST_Transform.md) | Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS. If the `SourceCRS` is not specified, CRS will be fetched from the geometry using [ST_SRID](#s... | v1.2.0 |

## Geometry Validation

These functions test whether geometries are valid and can repair invalid geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_IsValid](Geometry-Validation/ST_IsValid.md) | Test if a geometry is well-formed. The function can be invoked with just the geometry or with an additional flag (from `v1.5.1`). The flag alters the validity checking behavior. The flags parameter... | v1.0.0 |
| [ST_IsValidDetail](Geometry-Validation/ST_IsValidDetail.md) | Returns a row, containing a boolean `valid` stating if a geometry is valid, a string `reason` stating why it is invalid and a geometry `location` pointing out where it is invalid. | v1.6.1 |
| [ST_IsValidReason](Geometry-Validation/ST_IsValidReason.md) | Returns text stating if the geometry is valid. If not, it provides a reason why it is invalid. The function can be invoked with just the geometry or with an additional flag. The flag alters the val... | v1.5.1 |
| [ST_MakeValid](Geometry-Validation/ST_MakeValid.md) | Given an invalid geometry, create a valid representation of the geometry. | v1.0.0 |

## Bounding Box Functions

These functions produce or operate on bounding boxes and compute extent values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BoundingDiagonal](Bounding-Box-Functions/ST_BoundingDiagonal.md) | Returns a linestring spanning minimum and maximum values of each dimension of the given geometry's coordinates as its start and end point respectively. If an empty geometry is provided, the returne... | v1.5.0 |
| [ST_Envelope](Bounding-Box-Functions/ST_Envelope.md) | Return the envelope boundary of A | v1.0.0 |
| [ST_Expand](Bounding-Box-Functions/ST_Expand.md) | Returns a geometry expanded from the bounding box of the input. The expansion can be specified in two ways: | v1.6.1 |
| [ST_MMax](Bounding-Box-Functions/ST_MMax.md) | Returns M maxima of the given geometry or null if there is no M coordinate. | v1.6.1 |
| [ST_MMin](Bounding-Box-Functions/ST_MMin.md) | Returns M minima of the given geometry or null if there is no M coordinate. | v1.6.1 |
| [ST_XMax](Bounding-Box-Functions/ST_XMax.md) | Returns the maximum X coordinate of a geometry | v1.2.1 |
| [ST_XMin](Bounding-Box-Functions/ST_XMin.md) | Returns the minimum X coordinate of a geometry | v1.2.1 |
| [ST_YMax](Bounding-Box-Functions/ST_YMax.md) | Return the minimum Y coordinate of A | v1.2.1 |
| [ST_YMin](Bounding-Box-Functions/ST_YMin.md) | Return the minimum Y coordinate of A | v1.2.1 |
| [ST_ZMax](Bounding-Box-Functions/ST_ZMax.md) | Returns Z maxima of the given geometry or null if there is no Z coordinate. | v1.3.1 |
| [ST_ZMin](Bounding-Box-Functions/ST_ZMin.md) | Returns Z minima of the given geometry or null if there is no Z coordinate. | v1.3.1 |

## Spatial Indexing

These functions work with spatial indexing systems including Bing Tiles, H3, S2, and GeoHash.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BingTile](Spatial-Indexing/ST_BingTile.md) | Creates a Bing Tile quadkey from tile XY coordinates and a zoom level. | v1.9.0 |
| [ST_BingTileAt](Spatial-Indexing/ST_BingTileAt.md) | Returns the Bing Tile quadkey for a given point (longitude, latitude) at a specified zoom level. | v1.9.0 |
| [ST_BingTileCellIDs](Spatial-Indexing/ST_BingTileCellIDs.md) | Returns an array of Bing Tile quadkey strings that cover the given geometry at the specified zoom level. | v1.9.0 |
| [ST_BingTilePolygon](Spatial-Indexing/ST_BingTilePolygon.md) | Returns the bounding polygon (Geometry) of the Bing Tile identified by the given quadkey. | v1.9.0 |
| [ST_BingTilesAround](Spatial-Indexing/ST_BingTilesAround.md) | Returns an array of Bing Tile quadkey strings representing the neighborhood tiles around the tile that contains the given point (longitude, latitude) at the specified zoom level. Returns the 3×3 ne... | v1.9.0 |
| [ST_BingTileToGeom](Spatial-Indexing/ST_BingTileToGeom.md) | Returns an array of Polygons for the corresponding Bing Tile quadkeys. | v1.9.0 |
| [ST_BingTileX](Spatial-Indexing/ST_BingTileX.md) | Returns the tile X coordinate of the Bing Tile identified by the given quadkey. | v1.9.0 |
| [ST_BingTileY](Spatial-Indexing/ST_BingTileY.md) | Returns the tile Y coordinate of the Bing Tile identified by the given quadkey. | v1.9.0 |
| [ST_BingTileZoomLevel](Spatial-Indexing/ST_BingTileZoomLevel.md) | Returns the zoom level of the Bing Tile identified by the given quadkey. | v1.9.0 |
| [ST_GeoHashNeighbor](Spatial-Indexing/ST_GeoHashNeighbor.md) | Returns the neighbor geohash cell in the given direction. Valid directions are: `n`, `ne`, `e`, `se`, `s`, `sw`, `w`, `nw` (case-insensitive). | v1.9.0 |
| [ST_GeoHashNeighbors](Spatial-Indexing/ST_GeoHashNeighbors.md) | Returns the 8 neighboring geohash cells of a given geohash string. The result is an array of 8 geohash strings in the order: N, NE, E, SE, S, SW, W, NW. | v1.9.0 |
| [ST_H3CellDistance](Spatial-Indexing/ST_H3CellDistance.md) | return result of h3 function [gridDistance(cel1, cell2)](https://h3geo.org/docs/api/traversal#griddistance). As described by H3 documentation > Finding the distance can fail because the two indexes... | v1.5.0 |
| [ST_H3CellIDs](Spatial-Indexing/ST_H3CellIDs.md) | Cover the geometry by H3 cell IDs with the given resolution(level). To understand the cell statistics please refer to [H3 Doc](https://h3geo.org/docs/core-library/restable) H3 native fill functions... | v1.5.0 |
| [ST_H3KRing](Spatial-Indexing/ST_H3KRing.md) | return the result of H3 function [gridDisk(cell, k)](https://h3geo.org/docs/api/traversal#griddisk). | v1.5.0 |
| [ST_H3ToGeom](Spatial-Indexing/ST_H3ToGeom.md) | Return the result of H3 function [cellsToMultiPolygon(cells)](https://h3geo.org/docs/api/regions#cellstolinkedmultipolygon--cellstomultipolygon). | v1.6.0 |
| [ST_S2CellIDs](Spatial-Indexing/ST_S2CellIDs.md) | Cover the geometry with Google S2 Cells, return the corresponding cell IDs with the given level. The level indicates the [size of cells](https://s2geometry.io/resources/s2cell_statistics.html). Wit... | v1.4.0 |
| [ST_S2ToGeom](Spatial-Indexing/ST_S2ToGeom.md) | Returns an array of Polygons for the corresponding S2 cell IDs. | v1.6.0 |

## Clustering Functions

These functions implement spatial clustering algorithms.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_DBSCAN](Clustering-Functions/ST_DBSCAN.md) | Performs a DBSCAN clustering across the entire dataframe. | v1.7.1 |
| [ST_LocalOutlierFactor](Clustering-Functions/ST_LocalOutlierFactor.md) | Computes the Local Outlier Factor (LOF) for each point in the input dataset. | v1.7.1 |

## Spatial Statistics

These functions compute spatial statistics and spatial weights.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BinaryDistanceBandColumn](Spatial-Statistics/ST_BinaryDistanceBandColumn.md) | Introduction: Returns a `weights` column containing every record in a dataframe within a specified `threshold` distance. | v1.7.1 |
| [ST_GLocal](Spatial-Statistics/ST_GLocal.md) | Runs Getis and Ord's G Local (Gi or Gi*) statistic on the geometry given the `weights` and `level`. | v1.7.1 |
| [ST_WeightedDistanceBandColumn](Spatial-Statistics/ST_WeightedDistanceBandColumn.md) | Introduction: Returns a `weights` column containing every record in a dataframe within a specified `threshold` distance. | v1.7.1 |

## Address Functions

These functions parse and expand street addresses using the libpostal library.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ExpandAddress](Address-Functions/ExpandAddress.md) | Returns an array of expanded forms of the input address string. This is backed by the [libpostal](https://github.com/openvenues/libpostal) library's address expanding functionality. | v1.8.0 |
| [ParseAddress](Address-Functions/ParseAddress.md) | Returns an array of the components (e.g. street, postal code) of the input address string. This is backed by the [libpostal](https://github.com/openvenues/libpostal) library's address parsing funct... | v1.8.0 |
