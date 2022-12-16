#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from functools import partial
from typing import Optional, Union

from pyspark.sql import Column

from sedona.sql.dataframe_api import call_sedona_function, ColumnOrName, ColumnOrNameOrNumber, validate_argument_types


__all__ = [
    "ST_3DDistance",
    "ST_AddPoint",
    "ST_Area",
    "ST_AsBinary",
    "ST_AsEWKB",
    "ST_AsEWKT",
    "ST_AsGeoJSON",
    "ST_AsGML",
    "ST_AsKML",
    "ST_AsText",
    "ST_Azimuth",
    "ST_Boundary",
    "ST_Buffer",
    "ST_BuildArea",
    "ST_Centroid",
    "ST_Collect",
    "ST_CollectionExtract",
    "ST_ConvexHull",
    "ST_Difference",
    "ST_Distance",
    "ST_Dump",
    "ST_DumpPoints",
    "ST_EndPoint",
    "ST_Envelope",
    "ST_ExteriorRing",
    "ST_FlipCoordinates",
    "ST_Force_2D",
    "ST_GeoHash",
    "ST_GeometryN",
    "ST_GeometryType",
    "ST_InteriorRingN",
    "ST_Intersection",
    "ST_IsClosed",
    "ST_IsEmpty",
    "ST_IsRing",
    "ST_IsSimple",
    "ST_IsValid",
    "ST_Length",
    "ST_LineFromMultiPoint",
    "ST_LineInterpolatePoint",
    "ST_LineMerge",
    "ST_LineSubstring",
    "ST_MakePolygon",
    "ST_MakeValid",
    "ST_MinimumBoundingCircle",
    "ST_MinimumBoundingRadius",
    "ST_Multi",
    "ST_Normalize",
    "ST_NPoints",
    "ST_NDims",
    "ST_NumGeometries",
    "ST_NumInteriorRings",
    "ST_PointN",
    "ST_PointOnSurface",
    "ST_PrecisionReduce",
    "ST_RemovePoint",
    "ST_Reverse",
    "ST_SetPoint",
    "ST_SetSRID",
    "ST_SRID",
    "ST_StartPoint",
    "ST_SubDivide",
    "ST_SubDivideExplode",
    "ST_SimplifyPreserveTopology",
    "ST_SymDifference",
    "ST_Transform",
    "ST_Union",
    "ST_X",
    "ST_XMax",
    "ST_XMin",
    "ST_Y",
    "ST_YMax",
    "ST_YMin",
    "ST_Z",
    "ST_ZMax",
    "ST_ZMin",
]


_call_st_function = partial(call_sedona_function, "st_functions")
    

@validate_argument_types
def ST_3DDistance(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the 3-dimensional minimum Cartesian distance between two geometry columns.

    :param a: One geometry column to use in the calculation.
    :type a: ColumnOrName
    :param b: Other geometry column to use in the calculation.
    :type b: ColumnOrName
    :return: Minimum cartesian distance between a and b as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_3DDistance", (a, b))


@validate_argument_types
def ST_AddPoint(line_string: ColumnOrName, point: ColumnOrName, index: Optional[Union[ColumnOrName, int]] = None) -> Column:
    """Add a point to either the end of a linestring or a specified index.
    If index is not provided then point will be added to the end of line_string.

    :param line_string: Linestring geometry column to add point to.
    :type line_string: ColumnOrName
    :param point: Point geometry column to add to line_string.
    :type point: ColumnOrName
    :param index: 0-based index to insert point at in line_string, if None then point is appended to the end of line_string, defaults to None
    :type index: Optional[Union[ColumnOrName, int]], optional
    :return: Linestring geometry column with point added.
    :rtype: Column
    """
    args = (line_string, point) if index is None else (line_string, point, index)
    return _call_st_function("ST_AddPoint", args)


@validate_argument_types
def ST_Area(geometry: ColumnOrName) -> Column:
    """Calculate the area of a geometry.

    :param geometry: Geometry column to calculate the area of.
    :type geometry: ColumnOrName
    :return: Area of geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Area", geometry)


@validate_argument_types
def ST_AsBinary(geometry: ColumnOrName) -> Column:
    """Generate the Well-Known Binary (WKB) representation of a geometry.

    :param geometry: Geometry column to generate WKB for.
    :type geometry: ColumnOrName
    :return: Well-Known Binary representation of geometry as a binary column.
    :rtype: Column
    """
    return _call_st_function("ST_AsBinary", geometry)


@validate_argument_types
def ST_AsEWKB(geometry: ColumnOrName) -> Column:
    """Generate the Extended Well-Known Binary representation of a geometry.
    As opposed to WKB, EWKB will include the SRID of the geometry.

    :param geometry: Geometry to generate EWKB for.
    :type geometry: ColumnOrName
    :return: Extended Well-Known Binary representation of geometry as a binary column.
    :rtype: Column
    """
    return _call_st_function("ST_AsEWKB", geometry)


@validate_argument_types
def ST_AsEWKT(geometry: ColumnOrName) -> Column:
    """Generate the Extended Well-Known Text representation of a geometry column.
    As opposed to WKT, EWKT will include the SRID of the geometry.

    :param geometry: Geometry column to generate EWKT for.
    :type geometry: ColumnOrName
    :return: Extended Well-Known Text represenation of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_AsEWKT", geometry)


@validate_argument_types
def ST_AsGeoJSON(geometry: ColumnOrName) -> Column:
    """Generate the GeoJSON style represenation of a geometry column.

    :param geometry: Geometry column to generate GeoJSON for.
    :type geometry: ColumnOrName
    :return: GeoJSON representation of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_AsGeoJSON", geometry)


@validate_argument_types
def ST_AsGML(geometry: ColumnOrName) -> Column:
    """Generate the Geography Markup Language (GML) representation of a
    geometry column.

    :param geometry: Geometry column to generate GML for.
    :type geometry: ColumnOrName
    :return: GML representation of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_AsGML", geometry)


@validate_argument_types
def ST_AsKML(geometry: ColumnOrName) -> Column:
    """Generate the KML representation of a geometry column.

    :param geometry: Geometry column to generate KML for.
    :type geometry: ColumnOrName
    :return: KML representation of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_AsKML", geometry)


@validate_argument_types
def ST_AsText(geometry: ColumnOrName) -> Column:
    """Generate the Well-Known Text (WKT) representation of a geometry column.

    :param geometry: Geometry column to generate WKT for.
    :type geometry: ColumnOrName
    :return: WKT representation of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_AsText", geometry)


@validate_argument_types
def ST_Azimuth(point_a: ColumnOrName, point_b: ColumnOrName) -> Column:
    """Calculate the azimuth for two point columns in radians.

    :param point_a: One point geometry column to use for the calculation.
    :type point_a: ColumnOrName
    :param point_b: Other point geometry column to use for the calculation.
    :type point_b: ColumnOrName
    :return: Azimuth for point_a and point_b in radians as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Azimuth", (point_a, point_b))


@validate_argument_types
def ST_Boundary(geometry: ColumnOrName) -> Column:
    """Calculate the closure of the combinatorial boundary of a geometry column.

    :param geometry: Geometry column to calculate the boundary for.
    :type geometry: ColumnOrName
    :return: Boundary of the input geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Boundary", geometry)


@validate_argument_types
def ST_Buffer(geometry: ColumnOrName, buffer: ColumnOrNameOrNumber) -> Column:
    """Calculate a geometry that represents all points whose distance from the 
    input geometry column is equal to or less than a given amount.

    :param geometry: Input geometry column to buffer.
    :type geometry: ColumnOrName
    :param buffer: Either a column or value for the amount to buffer the input geometry by.
    :type buffer: ColumnOrNameOrNumber
    :return: Buffered geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Buffer", (geometry, buffer))


@validate_argument_types
def ST_BuildArea(geometry: ColumnOrName) -> Column:
    """Generate a geometry described by the consituent linework of the input
    geometry column.

    :param geometry: Linestring or multilinestring geometry column to use as input.
    :type geometry: ColumnOrName
    :return: Area formed by geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_BuildArea", geometry)


@validate_argument_types
def ST_Centroid(geometry: ColumnOrName) -> Column:
    """Calculate the centroid of the given geometry column.

    :param geometry: Geometry column to calculate a centroid for.
    :type geometry: ColumnOrName
    :return: Centroid of geometry as a point geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Centroid", geometry)


@validate_argument_types
def ST_Collect(*geometries: ColumnOrName) -> Column:
    """Collect multiple geometry columns or an array of geometries into a single
    multi-geometry or geometry collection.

    :param geometries: Either a single geometry column that holds an array of geometries or multiple geometry columns.
    :return: If the types of geometries are homogenous then a multi-geometry is returned, otherwise a geometry collection is returned.
    :rtype: Column
    """
    if len(geometries) == 1:
        return _call_st_function("ST_Collect", geometries)
    else:
        return _call_st_function("ST_Collect", [geometries])


@validate_argument_types
def ST_CollectionExtract(collection: ColumnOrName, geom_type: Optional[Union[ColumnOrName, int]] = None) -> Column:
    """Extract a specific type of geometry from a geometry collection column
    as a multi-geometry column.

    :param collection: Column for the geometry collection.
    :type collection: ColumnOrName
    :param geom_type: Type of geometry to extract where 1 is point, 2 is linestring, and 3 is polygon, if None then the highest dimension geometry is extracted, defaults to None
    :type geom_type: Optional[Union[ColumnOrName, int]], optional
    :return: Multi-geometry column containing all geometry from collection of the selected type.
    :rtype: Column
    """
    args = (collection,) if geom_type is None else (collection, geom_type)
    return _call_st_function("ST_CollectionExtract", args)


@validate_argument_types
def ST_ConvexHull(geometry: ColumnOrName) -> Column:
    """Generate the convex hull of a geometry column.

    :param geometry: Geometry column to generate a convex hull for.
    :type geometry: ColumnOrName
    :return: Convex hull of geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_ConvexHull", geometry)


@validate_argument_types
def ST_Difference(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the difference of two geometry columns. This difference
    is not symmetric. It only returns the part of geometry a that is not
    in b.

    :param a: Geometry column to use in the calculation.
    :type a: ColumnOrName
    :param b: Geometry column to subtract from geometry column a.
    :type b: ColumnOrName
    :return: Part of geometry a that is not in b as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Difference", (a, b))


@validate_argument_types
def ST_Distance(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the minimum cartesian distance between two geometry columns.

    :param a: Geometry column to use in the calculation.
    :type a: ColumnOrName
    :param b: Other geometry column to use in the calculation.
    :type b: ColumnOrName
    :return: Two-dimensional cartesian distance between a and b as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Distance", (a, b))


@validate_argument_types
def ST_Dump(geometry: ColumnOrName) -> Column:
    """Returns an array of geometries that are members of a multi-geometry
    or geometry collection column. If the input geometry is a regular geometry
    then the geometry is returned inside of a single element array. 

    :param geometry: Geometry column to dump.
    :type geometry: ColumnOrName
    :return: Array of geometries column comprised of the members of geometry. 
    :rtype: Column
    """
    return _call_st_function("ST_Dump", geometry)


@validate_argument_types
def ST_DumpPoints(geometry: ColumnOrName) -> Column:
    """Return the list of points of a geometry column. Specifically, return
    the vertices of the input geometry as an array.

    :param geometry: Geometry column to dump the points of.
    :type geometry: ColumnOrName
    :return: Array of point geometry column comprised of the vertices of geometry.
    :rtype: Column
    """
    return _call_st_function("ST_DumpPoints", geometry)


@validate_argument_types
def ST_EndPoint(line_string: ColumnOrName) -> Column:
    """Return the last point of a linestring geometry column.

    :param line_string: Linestring geometry column to get the end point of.
    :type line_string: ColumnOrName
    :return: The last point of the linestring geometry column as a point geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_EndPoint", line_string)


@validate_argument_types
def ST_Envelope(geometry: ColumnOrName) -> Column:
    """Calculate the envelope boundary of a geometry column.

    :param geometry: Geometry column to calculate the envelope of.
    :type geometry: ColumnOrName
    :return: Envelope of geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Envelope", geometry)


@validate_argument_types
def ST_ExteriorRing(polygon: ColumnOrName) -> Column:
    """Get a linestring representing the exterior ring of a polygon geometry
    column.

    :param polygon: Polygon geometry column to get the exterior ring of.
    :type polygon: ColumnOrName
    :return: Exterior ring of polygon as a linestring geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_ExteriorRing", polygon)


@validate_argument_types
def ST_FlipCoordinates(geometry: ColumnOrName) -> Column:
    """Flip the X and Y coordinates of a geometry column.

    :param geometry: Geometry column to flip coordinates for.
    :type geometry: ColumnOrName
    :return: Geometry column identical to geometry except with flipped coordinates.
    :rtype: Column
    """
    return _call_st_function("ST_FlipCoordinates", geometry)


@validate_argument_types
def ST_Force_2D(geometry: ColumnOrName) -> Column:
    """Force the geometry column to only output two dimensional representations.

    :param geometry: Geometry column to force to be 2D.
    :type geometry: ColumnOrName
    :return: Geometry column identical to geometry except with only X and Y coordinates.
    :rtype: Column
    """
    return _call_st_function("ST_Force_2D", geometry)


@validate_argument_types
def ST_GeoHash(geometry: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
    """Return the geohash of a geometry column at a given precision level.

    :param geometry: Geometry column to hash.
    :type geometry: ColumnOrName
    :param precision: Precision level to hash geometry at, given as an integer or an integer column.
    :type precision: Union[ColumnOrName, int]
    :return: Geohash of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_GeoHash", (geometry, precision))


@validate_argument_types
def ST_GeometryN(multi_geometry: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    """Return the geometry at index n (0-th based) of a multi-geometry column.

    :param multi_geometry: Multi-geometry column to get from.
    :type multi_geometry: ColumnOrName
    :param n: Index to select, given as an integer or integer column, 0-th based index, returns null if index is greater than maximum index.
    :type n: Union[ColumnOrName, int]
    :return: Geometry located at index n in multi_geometry as a geometry column.
    :rtype: Column
    :raises ValueError: If 
    """
    if isinstance(n, int) and n < 0:
        raise ValueError(f"Index n for ST_GeometryN must by >= 0: {n} < 0")
    return _call_st_function("ST_GeometryN", (multi_geometry, n))


@validate_argument_types
def ST_GeometryType(geometry: ColumnOrName) -> Column:
    """Return the type of geometry in a given geometry column.

    :param geometry: Geometry column to find the type for.
    :type geometry: ColumnOrName
    :return: Type of geometry as a string column.
    :rtype: Column
    """
    return _call_st_function("ST_GeometryType", geometry)


@validate_argument_types
def ST_InteriorRingN(polygon: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    """Return the index n (0-th based) interior ring of a polygon geometry column.

    :param polygon: Polygon geometry column to get an interior ring from.
    :type polygon: ColumnOrName
    :param n: Index of interior ring to return as either an integer or integer column, 0-th based.
    :type n: Union[ColumnOrName, int]
    :raises ValueError: If n is an integer and less than 0.
    :return: Interior ring at index n as a linestring geometry column or null if n is greater than maximum index 
    :rtype: Column
    """
    if isinstance(n, int) and n < 0:
        raise ValueError(f"Index n for ST_InteriorRingN must by >= 0: {n} < 0")
    return _call_st_function("ST_InteriorRingN", (polygon, n))


@validate_argument_types
def ST_Intersection(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the intersection of two geometry columns.

    :param a: One geometry column to use in the calculation.
    :type a: ColumnOrName
    :param b: Other geometry column to use in the calculation.
    :type b: ColumnOrName
    :return: Intersection of a and b as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Intersection", (a, b))


@validate_argument_types
def ST_IsClosed(geometry: ColumnOrName) -> Column:
    """Check if the linestring in a geometry column is closed (its end point is equal to its start point).

    :param geometry: Linestring geometry column to check.
    :type geometry: ColumnOrName
    :return: True if geometry is closed and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_st_function("ST_IsClosed", geometry)


@validate_argument_types
def ST_IsEmpty(geometry: ColumnOrName) -> Column:
    """Check if the geometry in a geometry column is an empty geometry.

    :param geometry: Geometry column to check.
    :type geometry: ColumnOrName
    :return: True if the geometry is empty and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_st_function("ST_IsEmpty", geometry)


@validate_argument_types
def ST_IsRing(line_string: ColumnOrName) -> Column:
    """Check if a linestring geometry is both closed and simple.

    :param line_string: Linestring geometry column to check.
    :type line_string: ColumnOrName
    :return: True if the linestring is both closed and simple and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_st_function("ST_IsRing", line_string)


@validate_argument_types
def ST_IsSimple(geometry: ColumnOrName) -> Column:
    """Check if a geometry's only intersections are at boundary points.

    :param geometry: Geometry column to check in.
    :type geometry: ColumnOrName
    :return: True if geometry is simple and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_st_function("ST_IsSimple", geometry)


@validate_argument_types
def ST_IsValid(geometry: ColumnOrName) -> Column:
    """Check if a geometry is well formed.

    :param geometry: Geometry column to check in.
    :type geometry: ColumnOrName
    :return: True if geometry is well formed and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_st_function("ST_IsValid", geometry)


@validate_argument_types
def ST_Length(geometry: ColumnOrName) -> Column:
    """Calculate the length of a linestring geometry.

    :param geometry: Linestring geometry column to calculate length for.
    :type geometry: ColumnOrName
    :return: Length of geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Length", geometry)


@validate_argument_types
def ST_LineFromMultiPoint(geometry: ColumnOrName) -> Column:
    """Creates a LineString from a MultiPoint geometry.

    :param geometry: MultiPoint geometry column to create LineString from.
    :type geometry: ColumnOrName
    :return: LineString geometry of a MultiPoint geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_LineFromMultiPoint", geometry)


@validate_argument_types
def ST_LineInterpolatePoint(geometry: ColumnOrName, fraction: ColumnOrNameOrNumber) -> Column:
    """Calculate a point that is interpolated along a linestring.

    :param geometry: Linestring geometry column to interpolate from.
    :type geometry: ColumnOrName
    :param fraction: Fraction of total length along geometry to generate a point for.
    :type fraction: ColumnOrNameOrNumber
    :return: Interpolated point as a point geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_LineInterpolatePoint", (geometry, fraction))


@validate_argument_types
def ST_LineMerge(multi_line_string: ColumnOrName) -> Column:
    """Sew together the constituent line work of a multilinestring into a
    single linestring.

    :param multi_line_string: Multilinestring geometry column to merge.
    :type multi_line_string: ColumnOrName
    :return: Linestring geometry column resulting from the merger of multi_line_string.
    :rtype: Column
    """
    return _call_st_function("ST_LineMerge", multi_line_string)


@validate_argument_types
def ST_LineSubstring(line_string: ColumnOrName, start_fraction: ColumnOrNameOrNumber, end_fraction: ColumnOrNameOrNumber) -> Column:
    """Generate a substring of a linestring geometry column.

    :param line_string: Linestring geometry column to generate from.
    :type line_string: ColumnOrName
    :param start_fraction: Fraction of linestring to start from for the resulting substring as either a number or numeric column.
    :type start_fraction: ColumnOrNameOrNumber
    :param end_fraction: Fraction of linestring to end at for the resulting substring as either a number or numeric column.
    :type end_fraction: ColumnOrNameOrNumber
    :return: Smaller linestring that runs from start_fraction to end_fraction of line_string as a linestring geometry column, will be null if either start_fraction or end_fraction are outside the interval [0, 1].
    :rtype: Column
    """
    return _call_st_function("ST_LineSubstring", (line_string, start_fraction, end_fraction))


@validate_argument_types
def ST_MakePolygon(line_string: ColumnOrName, holes: Optional[ColumnOrName] = None) -> Column:
    """Create a polygon geometry from a linestring describing the exterior ring as well as an array of linestrings describing holes.

    :param line_string: Closed linestring geometry column that describes the exterior ring of the polygon.
    :type line_string: ColumnOrName
    :param holes: Optional column for an array of closed geometry columns that describe holes in the polygon, defaults to None.
    :type holes: Optional[ColumnOrName], optional
    :return: Polygon geometry column created from the input linestrings.
    :rtype: Column
    """
    args = (line_string,) if holes is None else (line_string, holes)
    return _call_st_function("ST_MakePolygon", args)


@validate_argument_types
def ST_MakeValid(geometry: ColumnOrName, keep_collapsed: Optional[Union[ColumnOrName, bool]] = None) -> Column:
    """Convert an invalid geometry in a geometry column into a valid geometry.

    :param geometry: Geometry column that contains the invalid geometry.
    :type geometry: ColumnOrName
    :param keep_collapsed: If True then collapsed geometries are converted to empty geometries, otherwise they will be converted to valid geometries of a lower dimension, if None then the default value of False is used, defaults to None
    :type keep_collapsed: Optional[Union[ColumnOrName, bool]], optional
    :return: Geometry column that contains valid versions of the original geometry.
    :rtype: Column
    """
    args = (geometry,) if keep_collapsed is None else (geometry, keep_collapsed)
    return _call_st_function("ST_MakeValid", args)


@validate_argument_types
def ST_MinimumBoundingCircle(geometry: ColumnOrName, quadrant_segments: Optional[Union[ColumnOrName, int]] = None) -> Column:
    """Generate the minimum bounding circle that contains a geometry.

    :param geometry: Geometry column to generate minimum bounding circles for.
    :type geometry: ColumnOrName
    :param quadrant_segments: Number of quadrant segments to use, if None then use a default value, defaults to None
    :type quadrant_segments: Optional[Union[ColumnOrName, int]], optional
    :return: Geometry column that contains the minimum bounding circles.
    :rtype: Column
    """
    args = (geometry,) if quadrant_segments is None else (geometry, quadrant_segments)
    return _call_st_function("ST_MinimumBoundingCircle", args)


@validate_argument_types
def ST_MinimumBoundingRadius(geometry: ColumnOrName) -> Column:
    """Calculate the minimum bounding radius from the centroid of a geometry that will contain it.

    :param geometry: Geometry column to generate minimum bounding radii for. 
    :type geometry: ColumnOrName
    :return: Struct column with a center field containing point geometry for the center of geometry and a radius field with a double value for the bounding radius.
    :rtype: Column
    """
    return _call_st_function("ST_MinimumBoundingRadius", geometry)


@validate_argument_types
def ST_Multi(geometry: ColumnOrName) -> Column:
    """Convert the geometry column into a multi-geometry column.

    :param geometry: Geometry column to convert.
    :type geometry: ColumnOrName
    :return: Multi-geometry form of geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Multi", geometry)


@validate_argument_types
def ST_Normalize(geometry: ColumnOrName) -> Column:
    """Convert geometry in a geometry column to a canonical form.

    :param geometry: Geometry to convert.
    :type geometry: ColumnOrName
    :return: Geometry with points ordered in a canonical way as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Normalize", geometry)


@validate_argument_types
def ST_NPoints(geometry: ColumnOrName) -> Column:
    """Return the number of points contained in a geometry.

    :param geometry: Geometry column to return for.
    :type geometry: ColumnOrName
    :return: Number of points in a geometry column as an integer column.
    :rtype: Column
    """
    return _call_st_function("ST_NPoints", geometry)


@validate_argument_types
def ST_NDims(geometry: ColumnOrName) -> Column:
    """Return the number of dimensions contained in a geometry.

    :param geometry: Geometry column to return for.
    :type geometry: ColumnOrName
    :return: Number of dimensions in a geometry column as an integer column.
    :rtype: Column
    """
    return _call_st_function("ST_NDims", geometry)


@validate_argument_types
def ST_NumGeometries(geometry: ColumnOrName) -> Column:
    """Return the number of geometries contained in a multi-geometry.

    :param geometry: Multi-geometry column to return for.
    :type geometry: ColumnOrName
    :return: Number of geometries contained in a multi-geometry column as an integer column.
    :rtype: Column
    """
    return _call_st_function("ST_NumGeometries", geometry)


@validate_argument_types
def ST_NumInteriorRings(geometry: ColumnOrName) -> Column:
    """Return the number of interior rings contained in a polygon geometry.

    :param geometry: Polygon geometry column to return for.
    :type geometry: ColumnOrName
    :return: Number of interior rings polygons contain as an integer column.
    :rtype: Column
    """
    return _call_st_function("ST_NumInteriorRings", geometry)


@validate_argument_types
def ST_PointN(geometry: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    """Get the n-th point (starts at 1) for a geometry.

    :param geometry: Geometry column to get the point from.
    :type geometry: ColumnOrName
    :param n: Index for the point to return, 1-based, negative values start from the end so -1 is the last point, values that are out of bounds return null.
    :type n: Union[ColumnOrName, int]
    :return: n-th point from the geometry as a point geometry column, or null if index is out of bounds.
    :rtype: Column
    """
    return _call_st_function("ST_PointN", (geometry, n))


@validate_argument_types
def ST_PointOnSurface(geometry: ColumnOrName) -> Column:
    """Get a point that is guaranteed to lie on the surface.

    :param geometry: Geometry column containing the Surface to get a point from.
    :type geometry: ColumnOrName
    :return: Point that lies on geometry as a point geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_PointOnSurface", geometry)


@validate_argument_types
def ST_PrecisionReduce(geometry: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
    """Reduce the precision of the coordinates in geometry to a specified number of decimal places.

    :param geometry: Geometry to reduce the precision of.
    :type geometry: ColumnOrName
    :param precision: Number of decimal places to reduce the precision to as either an integer or integer column, 0 reduces precision to whole numbers.
    :type precision: Union[ColumnOrName, int]
    :return: Geometry with precision reduced to the indicated number of decimal places as a geometry column, empty geometry if an invalid precision is passed.
    :rtype: Column
    """
    return _call_st_function("ST_PrecisionReduce", (geometry, precision))


@validate_argument_types
def ST_RemovePoint(line_string: ColumnOrName, index: Union[ColumnOrName, int]) -> Column:
    """Remove the specified point (0-th based) for a linestring geometry column.

    :param line_string: Linestring geometry column to remove the point from.
    :type line_string: ColumnOrName
    :param index: Index for the point to remove as either an integer or an integer column, 0-th based, negative numbers are ignored.
    :type index: Union[ColumnOrName, int]
    :return: Linestring geometry column with the specified point removed, or null if the index is out of bounds.
    :rtype: Column
    """
    return _call_st_function("ST_RemovePoint", (line_string, index))


@validate_argument_types
def ST_Reverse(geometry: ColumnOrName) -> Column:
    """Reverse the points for the geometry.

    :param geometry: Geometry column to reverse points for.
    :type geometry: ColumnOrName
    :return: Geometry with points in reverse order compared to the original.
    :rtype: Column
    """
    return _call_st_function("ST_Reverse", geometry)


@validate_argument_types
def ST_SetPoint(line_string: ColumnOrName, index: Union[ColumnOrName, int], point: ColumnOrName) -> Column:
    """Replace a point in a linestring.

    :param line_string: Linestring geometry column which contains the point to be replaced.
    :type line_string: ColumnOrName
    :param index: Index for the point to be replaced, 0-based, negative values start from the end so -1 is the last point.
    :type index: Union[ColumnOrName, int]
    :param point: Point geometry column to be newly set.
    :type point: ColumnOrName
    :return: Linestring geometry column with the replaced point, or null if the index is out of bounds.
    :rtype: Column
    """
    return _call_st_function("ST_SetPoint", (line_string, index, point))


@validate_argument_types
def ST_SetSRID(geometry: ColumnOrName, srid: Union[ColumnOrName, int]) -> Column:
    """Set the SRID for geometry.

    :param geometry: Geometry column to set SRID for.
    :type geometry: ColumnOrName
    :param srid: SRID to set as either an integer or an integer column.
    :type srid: Union[ColumnOrName, int]
    :return: Geometry column with SRID set to srid.
    :rtype: Column
    """
    return _call_st_function("ST_SetSRID", (geometry, srid))


@validate_argument_types
def ST_SRID(geometry: ColumnOrName) -> Column:
    """Get the SRID of geometry.

    :param geometry: Geometry column to get SRID from.
    :type geometry: ColumnOrName
    :return: SRID of geometry in the geometry column as an integer column.
    :rtype: Column
    """
    return _call_st_function("ST_SRID", geometry)


@validate_argument_types
def ST_StartPoint(line_string: ColumnOrName) -> Column:
    """Get the first point from a linestring.

    :param line_string: Linestring geometry column to get the first points for.
    :type line_string: ColumnOrName
    :return: First of the linestring geometry as a point geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_StartPoint", line_string)


@validate_argument_types
def ST_SubDivide(geometry: ColumnOrName, max_vertices: Union[ColumnOrName, int]) -> Column:
    """Subdivide a geometry into an array of geometries with at maximum number of vertices in each.

    :param geometry: Geometry column to subdivide.
    :type geometry: ColumnOrName
    :param max_vertices: Maximum number of vertices to have in each subdivision.
    :type max_vertices: Union[ColumnOrName, int]
    :return: Array of geometries that represent the subdivision of the original geometry.
    :rtype: Column
    """
    return _call_st_function("ST_SubDivide", (geometry, max_vertices))


@validate_argument_types
def ST_SubDivideExplode(geometry: ColumnOrName, max_vertices: Union[ColumnOrName, int]) -> Column:
    """Same as ST_SubDivide except also explode the generated array into multiple rows.

    :param geometry: Geometry column to subdivide.
    :type geometry: ColumnOrName
    :param max_vertices: Maximum number of vertices to have in each subdivision.
    :type max_vertices: Union[ColumnOrName, int]
    :return: Individual geometries exploded from the returned array of ST_SubDivide.
    :rtype: Column
    """
    return _call_st_function("ST_SubDivideExplode", (geometry, max_vertices))


@validate_argument_types
def ST_SimplifyPreserveTopology(geometry: ColumnOrName, distance_tolerance: ColumnOrNameOrNumber) -> Column:
    """Simplify a geometry within a specified tolerance while preserving topological relationships.

    :param geometry: Geometry column to simplify.
    :type geometry: ColumnOrName
    :param distance_tolerance: Tolerance for merging points together to simplify the geometry as either a number or numeric column.
    :type distance_tolerance: ColumnOrNameOrNumber
    :return: Simplified geometry as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_SimplifyPreserveTopology", (geometry, distance_tolerance))


@validate_argument_types
def ST_SymDifference(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the symmetric difference of two geometries (the regions that are only in one of them).

    :param a: One geometry column to use.
    :type a: ColumnOrName
    :param b: Other geometry column to use.
    :type b: ColumnOrName
    :return: Geometry representing the symmetric difference of a and b as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_SymDifference", (a, b))


@validate_argument_types
def ST_Transform(geometry: ColumnOrName, source_crs: ColumnOrName, target_crs: ColumnOrName, disable_error: Optional[Union[ColumnOrName, bool]] = None) -> Column:
    """Convert a geometry from one coordinate system to another coordinate system.

    :param geometry: Geometry column to convert.
    :type geometry: ColumnOrName
    :param source_crs: Original coordinate system for geometry as a string, a string constant must be wrapped as a string literal (using pyspark.sql.functions.lit).
    :type source_crs: ColumnOrName
    :param target_crs: Coordinate system geometry will be converted to as a string, a string constant must be wrapped as a string literal (using pyspark.sql.functions.lit).
    :type target_crs: ColumnOrName
    :param disable_error: Whether to disable the error "Bursa wolf parameters required", defaults to None
    :type disable_error: Optional[Union[ColumnOrName, bool]], optional
    :return: Geometry converted to the target coordinate system as an 
    :rtype: Column
    """
    args = (geometry, source_crs, target_crs) if disable_error is None else (geometry, source_crs, target_crs, disable_error)
    return _call_st_function("ST_Transform", args)


@validate_argument_types
def ST_Union(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Calculate the union of two geometries.

    :param a: One geometry column to use.
    :type a: ColumnOrName
    :param b: Other geometry column to use.
    :type b: ColumnOrName
    :return: Geometry representing the union of a and b as a geometry column.
    :rtype: Column
    """
    return _call_st_function("ST_Union", (a, b))


@validate_argument_types
def ST_X(point: ColumnOrName) -> Column:
    """Return the X coordinate of a point geometry.

    :param point: Point geometry column to get the coordinate for.
    :type point: ColumnOrName
    :return: X coordinate of the point geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_X", point)


@validate_argument_types
def ST_XMax(geometry: ColumnOrName) -> Column:
    """Calculate the maximum X coordinate for a geometry.

    :param geometry: Geometry column to get maximum X coordinate for.
    :type geometry: ColumnOrName
    :return: Maximum X coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_XMax", geometry)


@validate_argument_types
def ST_XMin(geometry: ColumnOrName) -> Column:
    """Calculate the minimum X coordinate for a geometry.

    :param geometry: Geometry column to get minimum X coordinate for.
    :type geometry: ColumnOrName
    :return: Minimum X coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_XMin", geometry)


@validate_argument_types
def ST_Y(point: ColumnOrName) -> Column:
    """Return the Y coordinate of a point geometry.

    :param point: Point geometry column to return the Y coordinate for.
    :type point: ColumnOrName
    :return: Y coordinate of the point geometry column as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Y", point)


@validate_argument_types
def ST_YMax(geometry: ColumnOrName) -> Column:
    """Calculate the maximum Y coordinate for a geometry.

    :param geometry: Geometry column to get the maximum Y coordinate for.
    :type geometry: ColumnOrName
    :return: Maximum Y coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_YMax", geometry)


@validate_argument_types
def ST_YMin(geometry: ColumnOrName) -> Column:
    """Calculate the minimum Y coordinate for a geometry.

    :param geometry: Geometry column to get the minimum Y coordinate for.
    :type geometry: ColumnOrName
    :return: Minimum Y coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_YMin", geometry)


@validate_argument_types
def ST_Z(point: ColumnOrName) -> Column:
    """Return the Z coordinate of a point geometry.

    :param point: Point geometry column to get the Z coordinate from.
    :type point: ColumnOrName
    :return: Z coordinate for the point geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_Z", point)

@validate_argument_types
def ST_ZMax(geometry: ColumnOrName) -> Column:
    """Return the maximum Z coordinate of a geometry.

    :param geometry: Geometry column to get the maximum Z coordinate from.
    :type geometry: ColumnOrName
    :return: Maximum Z coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_ZMax", geometry)

@validate_argument_types
def ST_ZMin(geometry: ColumnOrName) -> Column:
    """Return the minimum Z coordinate of a geometry.

    :param geometry: Geometry column to get the minimum Z coordinate from.
    :type geometry: ColumnOrName
    :return: Minimum Z coordinate for the geometry as a double column.
    :rtype: Column
    """
    return _call_st_function("ST_ZMin", geometry)