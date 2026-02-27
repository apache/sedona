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

| Function | Description |
| :--- | :--- |
| [ST_GeomCollFromText](Geometry-Constructors/ST_GeomCollFromText.md) | Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`. |
| [ST_GeometryFromText](Geometry-Constructors/ST_GeometryFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) |
| [ST_GeomFromEWKB](Geometry-Constructors/ST_GeomFromEWKB.md) | Construct a Geometry from EWKB string or Binary. This function is an alias of [ST_GeomFromWKB](Geometry-Constructors/ST_GeomFromWKB.md). |
| [ST_GeomFromEWKT](Geometry-Constructors/ST_GeomFromEWKT.md) | Construct a Geometry from OGC Extended WKT |
| [ST_GeomFromGeoHash](Geometry-Constructors/ST_GeomFromGeoHash.md) | Create Geometry from geohash string and optional precision |
| [ST_GeomFromGeoJSON](Geometry-Constructors/ST_GeomFromGeoJSON.md) | Construct a Geometry from GeoJson |
| [ST_GeomFromGML](Geometry-Constructors/ST_GeomFromGML.md) | Construct a Geometry from GML. |
| [ST_GeomFromKML](Geometry-Constructors/ST_GeomFromKML.md) | Construct a Geometry from KML. |
| [ST_GeomFromText](Geometry-Constructors/ST_GeomFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) |
| [ST_GeomFromWKB](Geometry-Constructors/ST_GeomFromWKB.md) | Construct a Geometry from WKB string or Binary. This function also supports EWKB format. |
| [ST_GeomFromWKT](Geometry-Constructors/ST_GeomFromWKT.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). |
| [ST_LineFromText](Geometry-Constructors/ST_LineFromText.md) | Construct a Line from Wkt text |
| [ST_LineFromWKB](Geometry-Constructors/ST_LineFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. |
| [ST_LineStringFromText](Geometry-Constructors/ST_LineStringFromText.md) | Construct a LineString from Text, delimited by Delimiter |
| [ST_LinestringFromWKB](Geometry-Constructors/ST_LinestringFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format and it is an alias of [ST_LineFromWKB](Geometry-Constructors/ST_LineFromWKB.md). |
| [ST_MakeEnvelope](Geometry-Constructors/ST_MakeEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY, and an optional SRID. |
| [ST_MakePoint](Geometry-Constructors/ST_MakePoint.md) | Creates a 2D, 3D Z or 4D ZM Point geometry. Use ST_MakePointM to make points with XYM coordinates. Z and M values are optional. |
| [ST_MLineFromText](Geometry-Constructors/ST_MLineFromText.md) | Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown). |
| [ST_MPointFromText](Geometry-Constructors/ST_MPointFromText.md) | Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`. |
| [ST_MPolyFromText](Geometry-Constructors/ST_MPolyFromText.md) | Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown). |
| [ST_Point](Geometry-Constructors/ST_Point.md) | Construct a Point from X and Y |
| [ST_PointFromGeoHash](Geometry-Constructors/ST_PointFromGeoHash.md) | Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` value... |
| [ST_PointFromText](Geometry-Constructors/ST_PointFromText.md) | Construct a Point from Text, delimited by Delimiter |
| [ST_PointFromWKB](Geometry-Constructors/ST_PointFromWKB.md) | Construct a Point geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. |
| [ST_PointZ](Geometry-Constructors/ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. |
| [ST_PointZ](Geometry-Constructors/ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. |
| [ST_PolygonFromEnvelope](Geometry-Constructors/ST_PolygonFromEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY. |
| [ST_PolygonFromText](Geometry-Constructors/ST_PolygonFromText.md) | Construct a Polygon from Text, delimited by Delimiter. Path must be closed |

## Geometry Accessors

These functions extract information and properties from geometry objects.

| Function | Description |
| :--- | :--- |
| [GeometryType](Geometry-Accessors/GeometryType.md) | Returns the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc. This function also indicates if the geometry is measured, by returning a string of the form 'POINTM'. |
| [ST_Boundary](Geometry-Accessors/ST_Boundary.md) | Returns the closure of the combinatorial boundary of this Geometry. |
| [ST_CoordDim](Geometry-Accessors/ST_CoordDim.md) | Returns the coordinate dimensions of the geometry. It is an alias of `ST_NDims`. |
| [ST_CrossesDateLine](Geometry-Accessors/ST_CrossesDateLine.md) | This function determines if a given geometry crosses the International Date Line. It operates by checking if the difference in longitude between any pair of consecutive points in the geometry excee... |
| [ST_Dimension](Geometry-Accessors/ST_Dimension.md) | Return the topological dimension of this Geometry object, which must be less than or equal to the coordinate dimension. OGC SPEC s2.1.1.1 - returns 0 for POINT, 1 for LINESTRING, 2 for POLYGON, and... |
| [ST_Dump](Geometry-Accessors/ST_Dump.md) | This function takes a GeometryCollection/Multi Geometry object and returns a set of geometries containing the individual geometries that make up the input geometry. The function is useful for break... |
| [ST_DumpPoints](Geometry-Accessors/ST_DumpPoints.md) | Returns a MultiPoint geometry which consists of individual points that compose the input line string. |
| [ST_EndPoint](Geometry-Accessors/ST_EndPoint.md) | Returns last point of given linestring. |
| [ST_ExteriorRing](Geometry-Accessors/ST_ExteriorRing.md) | Returns a line string representing the exterior ring of the POLYGON geometry. Return NULL if the geometry is not a polygon. |
| [ST_GeometryN](Geometry-Accessors/ST_GeometryN.md) | Return the 0-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return null |
| [ST_GeometryType](Geometry-Accessors/ST_GeometryType.md) | Returns the type of the geometry as a string. EG: 'ST_Linestring', 'ST_Polygon' etc. |
| [ST_HasZ](Geometry-Accessors/ST_HasZ.md) | Checks for the presence of Z coordinate values representing measures or linear references. Returns true if the input geometry includes an Z coordinate, false otherwise. |
| [ST_InteriorRingN](Geometry-Accessors/ST_InteriorRingN.md) | Returns the Nth interior linestring ring of the polygon geometry. Returns NULL if the geometry is not a polygon or the given N is out of range |
| [ST_IsClosed](Geometry-Accessors/ST_IsClosed.md) | RETURNS true if the LINESTRING start and end point are the same. |
| [ST_IsCollection](Geometry-Accessors/ST_IsCollection.md) | Returns `TRUE` if the geometry type of the input is a geometry collection type. Collection types are the following: |
| [ST_IsEmpty](Geometry-Accessors/ST_IsEmpty.md) | Test if a geometry is empty geometry |
| [ST_IsPolygonCCW](Geometry-Accessors/ST_IsPolygonCCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. |
| [ST_IsPolygonCW](Geometry-Accessors/ST_IsPolygonCW.md) | Returns true if all polygonal components in the input geometry have their exterior rings oriented counter-clockwise and interior rings oriented clockwise. |
| [ST_IsRing](Geometry-Accessors/ST_IsRing.md) | RETURN true if LINESTRING is ST_IsClosed and ST_IsSimple. |
| [ST_IsSimple](Geometry-Accessors/ST_IsSimple.md) | Test if geometry's only self-intersections are at boundary points. |
| [ST_NDims](Geometry-Accessors/ST_NDims.md) | Returns the coordinate dimension of the geometry. |
| [ST_NPoints](Geometry-Accessors/ST_NPoints.md) | Return points of the geometry |
| [ST_NRings](Geometry-Accessors/ST_NRings.md) | Returns the number of rings in a Polygon or MultiPolygon. Contrary to ST_NumInteriorRings, this function also takes into account the number of exterior rings. |
| [ST_NumGeometries](Geometry-Accessors/ST_NumGeometries.md) | Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the number of geometries, for single geometries will return 1. |
| [ST_NumInteriorRing](Geometry-Accessors/ST_NumInteriorRing.md) | Returns number of interior rings of polygon geometries. It is an alias of [ST_NumInteriorRings](Geometry-Accessors/ST_NumInteriorRings.md). |
| [ST_NumInteriorRings](Geometry-Accessors/ST_NumInteriorRings.md) | RETURNS number of interior rings of polygon geometries. |
| [ST_NumPoints](Geometry-Accessors/ST_NumPoints.md) | Returns number of points in a LineString |
| [ST_PointN](Geometry-Accessors/ST_PointN.md) | Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL ... |
| [ST_Points](Geometry-Accessors/ST_Points.md) | Returns a MultiPoint geometry consisting of all the coordinates of the input geometry. It preserves duplicate points as well as M and Z coordinates. |
| [ST_StartPoint](Geometry-Accessors/ST_StartPoint.md) | Returns first point of given linestring. |
| [ST_X](Geometry-Accessors/ST_X.md) | Returns X Coordinate of given Point null otherwise. |
| [ST_Y](Geometry-Accessors/ST_Y.md) | Returns Y Coordinate of given Point, null otherwise. |
| [ST_Z](Geometry-Accessors/ST_Z.md) | Returns Z Coordinate of given Point, null otherwise. |

## Geometry Editors

These functions create modified geometries by changing type, structure, or vertices.

| Function | Description |
| :--- | :--- |
| [ST_AddPoint](Geometry-Editors/ST_AddPoint.md) | RETURN Linestring with additional point at the given index, if position is not available the point will be added at the end of line. |
| [ST_Collect](Geometry-Editors/ST_Collect.md) | Build an appropriate `Geometry`, `MultiGeometry`, or `GeometryCollection` to contain the `Geometry`s in it. For example: |
| [ST_CollectionExtract](Geometry-Editors/ST_CollectionExtract.md) | Returns a homogeneous multi-geometry from a given geometry collection. |
| [ST_FlipCoordinates](Geometry-Editors/ST_FlipCoordinates.md) | Returns a version of the given geometry with X and Y axis flipped. |
| [ST_Force2D](Geometry-Editors/ST_Force2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force_2D](Geometry-Editors/ST_Force_2D.md). |
| [ST_Force3D](Geometry-Editors/ST_Force3D.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... |
| [ST_Force3DZ](Geometry-Editors/ST_Force3DZ.md) | Forces the geometry into a 3-dimensional model so that all output representations will have X, Y and Z coordinates. An optionally given zValue is tacked onto the geometry if the geometry is 2-dimen... |
| [ST_Force_2D](Geometry-Editors/ST_Force_2D.md) | Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates. This function is an alias of [ST_Force2D](Geometry-Editors/ST_Force2D.md). |
| [ST_ForceCollection](Geometry-Editors/ST_ForceCollection.md) | This function converts the input geometry into a GeometryCollection, regardless of the original geometry type. If the input is a multipart geometry, such as a MultiPolygon or MultiLineString, it wi... |
| [ST_ForcePolygonCCW](Geometry-Editors/ST_ForcePolygonCCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to counter-clockwise and interior rings to clockwise orientation. Non-polygonal geometries are returned unchanged. |
| [ST_ForcePolygonCW](Geometry-Editors/ST_ForcePolygonCW.md) | For (Multi)Polygon geometries, this function sets the exterior ring orientation to clockwise and interior rings to counter-clockwise orientation. Non-polygonal geometries are returned unchanged. |
| [ST_ForceRHR](Geometry-Editors/ST_ForceRHR.md) | Sets the orientation of polygon vertex orderings to follow the Right-Hand-Rule convention. The exterior ring will have a clockwise winding order, while any interior rings are oriented counter-clock... |
| [ST_LineFromMultiPoint](Geometry-Editors/ST_LineFromMultiPoint.md) | Creates a LineString from a MultiPoint geometry. |
| [ST_LineMerge](Geometry-Editors/ST_LineMerge.md) | Returns a LineString or MultiLineString formed by sewing together the constituent line work of a MULTILINESTRING. |
| [ST_MakeLine](Geometry-Editors/ST_MakeLine.md) | Creates a LineString containing the points of Point, MultiPoint, or LineString geometries. Other geometry types cause an error. |
| [ST_MakePolygon](Geometry-Editors/ST_MakePolygon.md) | Function to convert closed linestring to polygon including holes. The holes must be a MultiLinestring. If holes are provided, they should be fully contained within the shell. Holes outside the shel... |
| [ST_Multi](Geometry-Editors/ST_Multi.md) | Returns a MultiGeometry object based on the geometry input. ST_Multi is basically an alias for ST_Collect with one geometry. |
| [ST_Normalize](Geometry-Editors/ST_Normalize.md) | Returns the input geometry in its normalized form. |
| [ST_Polygon](Geometry-Editors/ST_Polygon.md) | Function to create a polygon built from the given LineString and sets the spatial reference system from the srid |
| [ST_Project](Geometry-Editors/ST_Project.md) | Calculates a new point location given a starting point, distance, and azimuth. The azimuth indicates the direction, expressed in radians, and is measured in a clockwise manner starting from true no... |
| [ST_RemovePoint](Geometry-Editors/ST_RemovePoint.md) | RETURN Line with removed point at given index, position can be omitted and then last one will be removed. |
| [ST_RemoveRepeatedPoints](Geometry-Editors/ST_RemoveRepeatedPoints.md) | This function eliminates consecutive duplicate points within a geometry, preserving endpoints of LineStrings. It operates on (Multi)LineStrings, (Multi)Polygons, and MultiPoints, processing Geometr... |
| [ST_Reverse](Geometry-Editors/ST_Reverse.md) | Return the geometry with vertex order reversed |
| [ST_Segmentize](Geometry-Editors/ST_Segmentize.md) | Returns a modified geometry having no segment longer than the given max_segment_length. |
| [ST_SetPoint](Geometry-Editors/ST_SetPoint.md) | Replace Nth point of linestring with given point. Index is 0-based. Negative index are counted backwards, e.g., -1 is last point. |
| [ST_ShiftLongitude](Geometry-Editors/ST_ShiftLongitude.md) | Modifies longitude coordinates in geometries, shifting values between -180..0 degrees to 180..360 degrees and vice versa. This is useful for normalizing data across the International Date Line and ... |

## Geometry Output

These functions convert geometry objects into various textual or binary formats.

| Function | Description |
| :--- | :--- |
| [ST_AsBinary](Geometry-Output/ST_AsBinary.md) | Return the Well-Known Binary representation of a geometry |
| [ST_AsEWKB](Geometry-Output/ST_AsEWKB.md) | Return the Extended Well-Known Binary representation of a geometry. EWKB is an extended version of WKB which includes the SRID of the geometry. The format originated in PostGIS but is supported by ... |
| [ST_AsEWKT](Geometry-Output/ST_AsEWKT.md) | Return the Extended Well-Known Text representation of a geometry. EWKT is an extended version of WKT which includes the SRID of the geometry. The format originated in PostGIS but is supported by ma... |
| [ST_AsGeoJSON](Geometry-Output/ST_AsGeoJSON.md) | Return the [GeoJSON](https://geojson.org/) string representation of a geometry |
| [ST_AsGML](Geometry-Output/ST_AsGML.md) | Return the [GML](https://www.ogc.org/standards/gml) string representation of a geometry |
| [ST_AsHEXEWKB](Geometry-Output/ST_AsHEXEWKB.md) | This function returns the input geometry encoded to a text representation in HEXEWKB format. The HEXEWKB encoding can use either little-endian (NDR) or big-endian (XDR) byte ordering. If no encoding... |
| [ST_AsKML](Geometry-Output/ST_AsKML.md) | Return the [KML](https://www.ogc.org/standards/kml) string representation of a geometry |
| [ST_AsText](Geometry-Output/ST_AsText.md) | Return the Well-Known Text string representation of a geometry |
| [ST_GeoHash](Geometry-Output/ST_GeoHash.md) | Returns GeoHash of the geometry with given precision |

## Predicates

These functions test spatial relationships between geometries, returning boolean values.

| Function | Description |
| :--- | :--- |
| [ST_Contains](Predicates/ST_Contains.md) | Return true if A fully contains B |
| [ST_CoveredBy](Predicates/ST_CoveredBy.md) | Return true if A is covered by B |
| [ST_Covers](Predicates/ST_Covers.md) | Return true if A covers B |
| [ST_Crosses](Predicates/ST_Crosses.md) | Return true if A crosses B |
| [ST_Disjoint](Predicates/ST_Disjoint.md) | Return true if A and B are disjoint |
| [ST_DWithin](Predicates/ST_DWithin.md) | Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'. This function essentially checks if the shortest distance between the envelope of the two geometries is <= the ... |
| [ST_Equals](Predicates/ST_Equals.md) | Return true if A equals to B |
| [ST_Intersects](Predicates/ST_Intersects.md) | Return true if A intersects B |
| [ST_OrderingEquals](Predicates/ST_OrderingEquals.md) | Returns true if the geometries are equal and the coordinates are in the same order |
| [ST_Overlaps](Predicates/ST_Overlaps.md) | Return true if A overlaps B |
| [ST_Relate](Predicates/ST_Relate.md) | The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relati... |
| [ST_RelateMatch](Predicates/ST_RelateMatch.md) | This function tests the relationship between two [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrices representing geometry intersections. It evalua... |
| [ST_Touches](Predicates/ST_Touches.md) | Return true if A touches B |
| [ST_Within](Predicates/ST_Within.md) | Return true if A is fully contained by B |

## Measurement Functions

These functions compute measurements of distance, area, length, and angles.

| Function | Description |
| :--- | :--- |
| [ST_3DDistance](Measurement-Functions/ST_3DDistance.md) | Return the 3-dimensional minimum cartesian distance between A and B |
| [ST_Angle](Measurement-Functions/ST_Angle.md) | Computes and returns the angle between two vectors represented by the provided points or linestrings. |
| [ST_Area](Measurement-Functions/ST_Area.md) | Return the area of A |
| [ST_AreaSpheroid](Measurement-Functions/ST_AreaSpheroid.md) | Return the geodesic area of A using WGS84 spheroid. Unit is square meter. Works better for large geometries (country level) compared to `ST_Area` + `ST_Transform`. It is equivalent to PostGIS `ST_A... |
| [ST_Azimuth](Measurement-Functions/ST_Azimuth.md) | Returns Azimuth for two given points in radians. Returns null if the two points are identical. |
| [ST_ClosestPoint](Measurement-Functions/ST_ClosestPoint.md) | Returns the 2-dimensional point on geom1 that is closest to geom2. This is the first point of the shortest line between the geometries. If using 3D geometries, the Z coordinates will be ignored. If... |
| [ST_Degrees](Measurement-Functions/ST_Degrees.md) | Convert an angle in radian to degrees. |
| [ST_Distance](Measurement-Functions/ST_Distance.md) | Return the Euclidean distance between A and B |
| [ST_DistanceSphere](Measurement-Functions/ST_DistanceSphere.md) | Return the haversine / great-circle distance of A using a given earth radius (default radius: 6371008.0). Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that... |
| [ST_DistanceSpheroid](Measurement-Functions/ST_DistanceSpheroid.md) | Return the geodesic distance of A using WGS84 spheroid. Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that cover large regions such as continents or the en... |
| [ST_FrechetDistance](Measurement-Functions/ST_FrechetDistance.md) | Computes and returns discrete [Frechet Distance](https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance) between the given two geometries, based on [Computing Discrete Frechet Distance](http://www.kr.... |
| [ST_HausdorffDistance](Measurement-Functions/ST_HausdorffDistance.md) | Returns a discretized (and hence approximate) [Hausdorff distance](https://en.wikipedia.org/wiki/Hausdorff_distance) between the given 2 geometries. Optionally, a densityFraction parameter can be s... |
| [ST_Length](Measurement-Functions/ST_Length.md) | Returns the perimeter of A. |
| [ST_Length2D](Measurement-Functions/ST_Length2D.md) | Returns the perimeter of A. This function is an alias of [ST_Length](Measurement-Functions/ST_Length.md). |
| [ST_LengthSpheroid](Measurement-Functions/ST_LengthSpheroid.md) | Return the geodesic perimeter of A using WGS84 spheroid. Unit is meter. Works better for large geometries (country level) compared to `ST_Length` + `ST_Transform`. It is equivalent to PostGIS `ST_L... |
| [ST_LongestLine](Measurement-Functions/ST_LongestLine.md) | Returns the LineString geometry representing the maximum distance between any two points from the input geometries. |
| [ST_MaxDistance](Measurement-Functions/ST_MaxDistance.md) | Calculates and returns the length value representing the maximum distance between any two points across the input geometries. This function is an alias for `ST_LongestDistance`. |
| [ST_MinimumClearance](Measurement-Functions/ST_MinimumClearance.md) | The minimum clearance is a metric that quantifies a geometry's tolerance to changes in coordinate precision or vertex positions. It represents the maximum distance by which vertices can be adjusted... |
| [ST_MinimumClearanceLine](Measurement-Functions/ST_MinimumClearanceLine.md) | This function returns a two-point LineString geometry representing the minimum clearance distance of the input geometry. If the input geometry does not have a defined minimum clearance, such as for... |
| [ST_Perimeter](Measurement-Functions/ST_Perimeter.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... |
| [ST_Perimeter2D](Measurement-Functions/ST_Perimeter2D.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... |

## Geometry Processing

These functions compute geometric constructions, or alter geometry size or shape.

| Function | Description |
| :--- | :--- |
| [ST_ApproximateMedialAxis](Geometry-Processing/ST_ApproximateMedialAxis.md) | Computes an approximate medial axis of a polygonal geometry. The medial axis is a representation of the "centerline" or "skeleton" of the polygon. This function first computes the straight skeleton... |
| [ST_Buffer](Geometry-Processing/ST_Buffer.md) | Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance. The function supports both Planar/Euclidean and Spheroidal/Geo... |
| [ST_BuildArea](Geometry-Processing/ST_BuildArea.md) | Returns the areal geometry formed by the constituent linework of the input geometry. |
| [ST_Centroid](Geometry-Processing/ST_Centroid.md) | Return the centroid point of A |
| [ST_ConcaveHull](Geometry-Processing/ST_ConcaveHull.md) | Return the Concave Hull of polygon A, with alpha set to pctConvex[0, 1] in the Delaunay Triangulation method, the concave hull will not contain a hole unless allowHoles is set to true |
| [ST_ConvexHull](Geometry-Processing/ST_ConvexHull.md) | Return the Convex Hull of polygon A |
| [ST_DelaunayTriangles](Geometry-Processing/ST_DelaunayTriangles.md) | This function computes the [Delaunay triangulation](https://en.wikipedia.org/wiki/Delaunay_triangulation) for the set of vertices in the input geometry. An optional `tolerance` parameter allows sna... |
| [ST_GeneratePoints](Geometry-Processing/ST_GeneratePoints.md) | Generates a specified quantity of pseudo-random points within the boundaries of the provided polygonal geometry. When `seed` is either zero or not defined then output will be random. |
| [ST_GeometricMedian](Geometry-Processing/ST_GeometricMedian.md) | Computes the approximate geometric median of a MultiPoint geometry using the Weiszfeld algorithm. The geometric median provides a centrality measure that is less sensitive to outlier points than th... |
| [ST_LabelPoint](Geometry-Processing/ST_LabelPoint.md) | `ST_LabelPoint` computes and returns a label point for a given polygon or geometry collection. The label point is chosen to be sufficiently far from boundaries of the geometry. For a regular Polygo... |
| [ST_MaximumInscribedCircle](Geometry-Processing/ST_MaximumInscribedCircle.md) | Finds the largest circle that is contained within a (multi)polygon, or which does not overlap any lines and points. Returns a row with fields: |
| [ST_MinimumBoundingCircle](Geometry-Processing/ST_MinimumBoundingCircle.md) | Returns the smallest circle polygon that contains a geometry. |
| [ST_MinimumBoundingRadius](Geometry-Processing/ST_MinimumBoundingRadius.md) | Returns two columns containing the center point and radius of the smallest circle that contains a geometry. |
| [ST_OrientedEnvelope](Geometry-Processing/ST_OrientedEnvelope.md) | Returns the minimum-area rotated rectangle enclosing a geometry. The rectangle may be rotated relative to the coordinate axes. Degenerate inputs may result in a Point or LineString being returned. |
| [ST_PointOnSurface](Geometry-Processing/ST_PointOnSurface.md) | Returns a POINT guaranteed to lie on the surface. |
| [ST_Polygonize](Geometry-Processing/ST_Polygonize.md) | Generates a GeometryCollection composed of polygons that are formed from the linework of an input GeometryCollection. When the input does not contain any linework that forms a polygon, the function... |
| [ST_ReducePrecision](Geometry-Processing/ST_ReducePrecision.md) | Reduce the decimals places in the coordinates of the geometry to the given number of decimal places. The last decimal place will be rounded. This function was called ST_PrecisionReduce in versions ... |
| [ST_Simplify](Geometry-Processing/ST_Simplify.md) | This function simplifies the input geometry by applying the Douglas-Peucker algorithm. |
| [ST_SimplifyPolygonHull](Geometry-Processing/ST_SimplifyPolygonHull.md) | This function computes a topology-preserving simplified hull, either outer or inner, for a polygonal geometry input. An outer hull fully encloses the original geometry, while an inner hull lies ent... |
| [ST_SimplifyPreserveTopology](Geometry-Processing/ST_SimplifyPreserveTopology.md) | Simplifies a geometry and ensures that the result is a valid geometry having the same dimension and number of components as the input, and with the components having the same topological relationship. |
| [ST_SimplifyVW](Geometry-Processing/ST_SimplifyVW.md) | This function simplifies the input geometry by applying the Visvalingam-Whyatt algorithm. |
| [ST_Snap](Geometry-Processing/ST_Snap.md) | Snaps the vertices and segments of the `input` geometry to `reference` geometry within the specified `tolerance` distance. The `tolerance` parameter controls the maximum snap distance. |
| [ST_StraightSkeleton](Geometry-Processing/ST_StraightSkeleton.md) | Computes the straight skeleton of a polygonal geometry. The straight skeleton is a method of representing a polygon by a topological skeleton, formed by a continuous shrinking process where each ed... |
| [ST_TriangulatePolygon](Geometry-Processing/ST_TriangulatePolygon.md) | Generates the constrained Delaunay triangulation for the input Polygon. The constrained Delaunay triangulation is a set of triangles created from the Polygon's vertices that covers the Polygon area... |
| [ST_VoronoiPolygons](Geometry-Processing/ST_VoronoiPolygons.md) | Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry. The result is a GeometryCollection of Polygons that covers an envelope larger than the extent of the input vert... |

## Overlay Functions

These functions compute results arising from the overlay of two geometries. These are also known as point-set theoretic boolean operations.

| Function | Description |
| :--- | :--- |
| [ST_Difference](Overlay-Functions/ST_Difference.md) | Return the difference between geometry A and B (return part of geometry A that does not intersect geometry B) |
| [ST_Intersection](Overlay-Functions/ST_Intersection.md) | Return the intersection geometry of A and B |
| [ST_Split](Overlay-Functions/ST_Split.md) | Split an input geometry by another geometry (called the blade). Linear (LineString or MultiLineString) geometry can be split by a Point, MultiPoint, LineString, MultiLineString, Polygon, or MultiPo... |
| [ST_SubDivide](Overlay-Functions/ST_SubDivide.md) | Returns a multi-geometry divided based of given maximum number of vertices. |
| [ST_SubDivideExplode](Overlay-Functions/ST_SubDivideExplode.md) | It works the same as ST_SubDivide but returns new rows with geometries instead of a multi-geometry. |
| [ST_SymDifference](Overlay-Functions/ST_SymDifference.md) | Return the symmetrical difference between geometry A and B (return parts of geometries which are in either of the sets, but not in their intersection) |
| [ST_UnaryUnion](Overlay-Functions/ST_UnaryUnion.md) | This variant of [ST_Union](Overlay-Functions/ST_Union.md) operates on a single geometry input. The input geometry can be a simple Geometry type, a MultiGeometry, or a GeometryCollection. The function calculates the ge... |
| [ST_Union](Overlay-Functions/ST_Union.md) | Return the union of geometry A and B |

## Affine Transformations

These functions change the position and shape of geometries using affine transformations.

| Function | Description |
| :--- | :--- |
| [ST_Affine](Affine-Transformations/ST_Affine.md) | Apply an affine transformation to the given geometry. |
| [ST_Rotate](Affine-Transformations/ST_Rotate.md) | Rotates a geometry by a specified angle in radians counter-clockwise around a given origin point. The origin for rotation can be specified as either a POINT geometry or x and y coordinates. If the ... |
| [ST_RotateX](Affine-Transformations/ST_RotateX.md) | Performs a counter-clockwise rotation of the specified geometry around the X-axis by the given angle measured in radians. |
| [ST_RotateY](Affine-Transformations/ST_RotateY.md) | Performs a counter-clockwise rotation of the specified geometry around the Y-axis by the given angle measured in radians. |
| [ST_Scale](Affine-Transformations/ST_Scale.md) | This function scales the geometry to a new size by multiplying the ordinates with the corresponding scaling factors provided as parameters `scaleX` and `scaleY`. |
| [ST_ScaleGeom](Affine-Transformations/ST_ScaleGeom.md) | This function scales the input geometry (`geometry`) to a new size. It does this by multiplying the coordinates of the input geometry with corresponding values from another geometry (`factor`) repr... |
| [ST_Translate](Affine-Transformations/ST_Translate.md) | Returns the input geometry with its X, Y and Z coordinates (if present in the geometry) translated by deltaX, deltaY and deltaZ (if specified) |

## Aggregate Functions

These functions perform aggregate operations on groups of geometries.

| Function | Description |
| :--- | :--- |
| [ST_Envelope_Agg](Aggregate-Functions/ST_Envelope_Agg.md) | Return the entire envelope boundary of all geometries in A. Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. This behavior is consistent with PostG... |
| [ST_Intersection_Agg](Aggregate-Functions/ST_Intersection_Agg.md) | Return the polygon intersection of all polygons in A |
| [ST_Union_Agg](Aggregate-Functions/ST_Union_Agg.md) | Return the polygon union of all polygons in A |

## Linear Referencing

These functions work with linear referencing, measures along lines, and trajectory data.

| Function | Description |
| :--- | :--- |
| [ST_LineInterpolatePoint](Linear-Referencing/ST_LineInterpolatePoint.md) | Returns a point interpolated along a line. First argument must be a LINESTRING. Second argument is a Double between 0 and 1 representing fraction of total linestring length the point has to be loca... |
| [ST_LineLocatePoint](Linear-Referencing/ST_LineLocatePoint.md) | Returns a double between 0 and 1, representing the location of the closest point on the LineString as a fraction of its total length. The first argument must be a LINESTRING, and the second argument... |
| [ST_LineSubstring](Linear-Referencing/ST_LineSubstring.md) | Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length. Second and third arguments are Double values between 0 and 1. This only works w... |

## Spatial Reference System

These functions work with the Spatial Reference System of geometries.

| Function | Description |
| :--- | :--- |
| [ST_BestSRID](Spatial-Reference-System/ST_BestSRID.md) | Returns the estimated most appropriate Spatial Reference Identifier (SRID) for a given geometry, based on its spatial extent and location. It evaluates the geometry's bounding envelope and selects ... |
| [ST_SetSRID](Spatial-Reference-System/ST_SetSRID.md) | Sets the spatial reference system identifier (SRID) of the geometry. |
| [ST_SRID](Spatial-Reference-System/ST_SRID.md) | Return the spatial reference system identifier (SRID) of the geometry. |
| [ST_Transform](Spatial-Reference-System/ST_Transform.md) | Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS. |

## Geometry Validation

These functions test whether geometries are valid and can repair invalid geometries.

| Function | Description |
| :--- | :--- |
| [ST_IsValid](Geometry-Validation/ST_IsValid.md) | Test if a geometry is well-formed. The function can be invoked with just the geometry or with an additional flag (from `v1.5.1`). The flag alters the validity checking behavior. The flags parameter... |
| [ST_IsValidDetail](Geometry-Validation/ST_IsValidDetail.md) | Returns a row, containing a boolean `valid` stating if a geometry is valid, a string `reason` stating why it is invalid and a geometry `location` pointing out where it is invalid. |
| [ST_IsValidReason](Geometry-Validation/ST_IsValidReason.md) | Returns text stating if the geometry is valid. If not, it provides a reason why it is invalid. The function can be invoked with just the geometry or with an additional flag. The flag alters the val... |
| [ST_MakeValid](Geometry-Validation/ST_MakeValid.md) | Given an invalid geometry, create a valid representation of the geometry. |

## Bounding Box Functions

These functions produce or operate on bounding boxes and compute extent values.

| Function | Description |
| :--- | :--- |
| [ST_BoundingDiagonal](Bounding-Box-Functions/ST_BoundingDiagonal.md) | Returns a linestring spanning minimum and maximum values of each dimension of the given geometry's coordinates as its start and end point respectively. If an empty geometry is provided, the returned... |
| [ST_Envelope](Bounding-Box-Functions/ST_Envelope.md) | Return the envelope boundary of A |
| [ST_Expand](Bounding-Box-Functions/ST_Expand.md) | Returns a geometry expanded from the bounding box of the input. The expansion can be specified in two ways: |
| [ST_XMax](Bounding-Box-Functions/ST_XMax.md) | Returns the maximum X coordinate of a geometry |
| [ST_XMin](Bounding-Box-Functions/ST_XMin.md) | Returns the minimum X coordinate of a geometry |
| [ST_YMax](Bounding-Box-Functions/ST_YMax.md) | Return the minimum Y coordinate of A |
| [ST_YMin](Bounding-Box-Functions/ST_YMin.md) | Return the minimum Y coordinate of A |
| [ST_ZMax](Bounding-Box-Functions/ST_ZMax.md) | Returns Z maxima of the given geometry or null if there is no Z coordinate. |
| [ST_ZMin](Bounding-Box-Functions/ST_ZMin.md) | Returns Z minima of the given geometry or null if there is no Z coordinate. |

## Spatial Indexing

These functions work with spatial indexing systems including Bing Tiles, H3, S2, and GeoHash.

| Function | Description |
| :--- | :--- |
| [ST_BingTile](Spatial-Indexing/ST_BingTile.md) | Creates a Bing Tile quadkey from tile XY coordinates and a zoom level. |
| [ST_BingTileAt](Spatial-Indexing/ST_BingTileAt.md) | Returns the Bing Tile quadkey for a given point (longitude, latitude) at a specified zoom level. |
| [ST_BingTileCellIDs](Spatial-Indexing/ST_BingTileCellIDs.md) | Returns an array of Bing Tile quadkey strings that cover the given geometry at the specified zoom level. |
| [ST_BingTilePolygon](Spatial-Indexing/ST_BingTilePolygon.md) | Returns the bounding polygon (Geometry) of the Bing Tile identified by the given quadkey. |
| [ST_BingTilesAround](Spatial-Indexing/ST_BingTilesAround.md) | Returns an array of Bing Tile quadkey strings representing the neighborhood tiles around the tile that contains the given point (longitude, latitude) at the specified zoom level. Returns the 3×3 ne... |
| [ST_BingTileToGeom](Spatial-Indexing/ST_BingTileToGeom.md) | Returns a GeometryCollection of Polygons for the corresponding Bing Tile quadkeys. |
| [ST_BingTileX](Spatial-Indexing/ST_BingTileX.md) | Returns the tile X coordinate of the Bing Tile identified by the given quadkey. |
| [ST_BingTileY](Spatial-Indexing/ST_BingTileY.md) | Returns the tile Y coordinate of the Bing Tile identified by the given quadkey. |
| [ST_BingTileZoomLevel](Spatial-Indexing/ST_BingTileZoomLevel.md) | Returns the zoom level of the Bing Tile identified by the given quadkey. |
| [ST_GeoHashNeighbor](Spatial-Indexing/ST_GeoHashNeighbor.md) | Returns the neighbor geohash cell in the given direction. Valid directions are: `n`, `ne`, `e`, `se`, `s`, `sw`, `w`, `nw` (case-insensitive). |
| [ST_GeoHashNeighbors](Spatial-Indexing/ST_GeoHashNeighbors.md) | Returns the 8 neighboring geohash cells of a given geohash string. The result is an array of 8 geohash strings in the order: N, NE, E, SE, S, SW, W, NW. |
| [ST_S2CellIDs](Spatial-Indexing/ST_S2CellIDs.md) | Cover the geometry with Google S2 Cells, return the corresponding cell IDs with the given level. The level indicates the [size of cells](https://s2geometry.io/resources/s2cell_statistics.html). With... |
