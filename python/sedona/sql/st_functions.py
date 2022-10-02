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
    "ST_NumGeometries",
    "ST_NumInteriorRings",
    "ST_PointN",
    "ST_PointOnSurface",
    "ST_PrecisionReduce",
    "ST_RemovePoint",
    "ST_Reverse",
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
]


_call_st_function = partial(call_sedona_function, "st_functions")
    

@validate_argument_types
def ST_3DDistance(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_3DDistance", (a, b))


@validate_argument_types
def ST_AddPoint(line_string: ColumnOrName, point: ColumnOrName, index: Optional[Union[ColumnOrName, int]] = None) -> Column:
    args = (line_string, point) if index is None else (line_string, point, index)
    return _call_st_function("ST_AddPoint", args)


@validate_argument_types
def ST_Area(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Area", geometry)


@validate_argument_types
def ST_AsBinary(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsBinary", geometry)


@validate_argument_types
def ST_AsEWKB(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsEWKB", geometry)


@validate_argument_types
def ST_AsEWKT(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsEWKT", geometry)


@validate_argument_types
def ST_AsGeoJSON(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsGeoJSON", geometry)


@validate_argument_types
def ST_AsGML(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsGML", geometry)


@validate_argument_types
def ST_AsKML(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsKML", geometry)


@validate_argument_types
def ST_AsText(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_AsText", geometry)


@validate_argument_types
def ST_Azimuth(point_a: ColumnOrName, point_b: ColumnOrName) -> Column:
    return _call_st_function("ST_Azimuth", (point_a, point_b))


@validate_argument_types
def ST_Boundary(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Boundary", geometry)


@validate_argument_types
def ST_Buffer(geometry: ColumnOrName, buffer: ColumnOrNameOrNumber) -> Column:
    return _call_st_function("ST_Buffer", (geometry, buffer))


@validate_argument_types
def ST_BuildArea(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_BuildArea", geometry)


@validate_argument_types
def ST_Centroid(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Centroid", geometry)


@validate_argument_types
def ST_Collect(*geometries: ColumnOrName) -> Column:
    if len(geometries) == 1:
        return _call_st_function("ST_Collect", geometries)
    else:
        return _call_st_function("ST_Collect", [geometries])


@validate_argument_types
def ST_CollectionExtract(collection: ColumnOrName, geom_type: Optional[Union[ColumnOrName, int]] = None) -> Column:
    args = (collection,) if geom_type is None else (collection, geom_type)
    return _call_st_function("ST_CollectionExtract", args)


@validate_argument_types
def ST_ConvexHull(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_ConvexHull", geometry)


@validate_argument_types
def ST_Difference(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_Difference", (a, b))


@validate_argument_types
def ST_Distance(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_Distance", (a, b))


@validate_argument_types
def ST_Dump(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Dump", geometry)


@validate_argument_types
def ST_DumpPoints(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_DumpPoints", geometry)


@validate_argument_types
def ST_EndPoint(line_string: ColumnOrName) -> Column:
    return _call_st_function("ST_EndPoint", line_string)


@validate_argument_types
def ST_Envelope(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Envelope", geometry)


@validate_argument_types
def ST_ExteriorRing(polygon: ColumnOrName) -> Column:
    return _call_st_function("ST_ExteriorRing", polygon)


@validate_argument_types
def ST_FlipCoordinates(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_FlipCoordinates", geometry)


@validate_argument_types
def ST_Force_2D(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Force_2D", geometry)


@validate_argument_types
def ST_GeoHash(geometry: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_GeoHash", (geometry, precision))


@validate_argument_types
def ST_GeometryN(multi_geometry: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_GeometryN", (multi_geometry, n))


@validate_argument_types
def ST_GeometryType(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_GeometryType", geometry)


@validate_argument_types
def ST_InteriorRingN(polygon: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_InteriorRingN", (polygon, n))


@validate_argument_types
def ST_Intersection(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_Intersection", (a, b))


@validate_argument_types
def ST_IsClosed(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_IsClosed", geometry)


@validate_argument_types
def ST_IsEmpty(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_IsEmpty", geometry)


@validate_argument_types
def ST_IsRing(line_string: ColumnOrName) -> Column:
    return _call_st_function("ST_IsRing", line_string)


@validate_argument_types
def ST_IsSimple(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_IsSimple", geometry)


@validate_argument_types
def ST_IsValid(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_IsValid", geometry)


@validate_argument_types
def ST_Length(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Length", geometry)


@validate_argument_types
def ST_LineInterpolatePoint(geometry: ColumnOrName, fraction: ColumnOrNameOrNumber) -> Column:
    return _call_st_function("ST_LineInterpolatePoint", (geometry, fraction))


@validate_argument_types
def ST_LineMerge(multi_line_string: ColumnOrName) -> Column:
    return _call_st_function("ST_LineMerge", multi_line_string)


@validate_argument_types
def ST_LineSubstring(line_string: ColumnOrName, start_fraction: ColumnOrNameOrNumber, end_fraction: ColumnOrNameOrNumber) -> Column:
    return _call_st_function("ST_LineSubstring", (line_string, start_fraction, end_fraction))


@validate_argument_types
def ST_MakePolygon(line_string: ColumnOrName, holes: ColumnOrName) -> Column:
    return _call_st_function("ST_MakePolygon", (line_string, holes))


@validate_argument_types
def ST_MakeValid(geometry: ColumnOrName, keep_collapsed: Optional[Union[ColumnOrName, bool]] = None) -> Column:
    args = (geometry,) if keep_collapsed is None else (geometry, keep_collapsed)
    return _call_st_function("ST_MakeValid", args)


@validate_argument_types
def ST_MinimumBoundingCircle(geometry: ColumnOrName, quadrant_segments: Optional[Union[ColumnOrName, int]] = None) -> Column:
    args = (geometry,) if quadrant_segments is None else (geometry, quadrant_segments)
    return _call_st_function("ST_MinimumBoundingCircle", args)


@validate_argument_types
def ST_MinimumBoundingRadius(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_MinimumBoundingRadius", geometry)


@validate_argument_types
def ST_Multi(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Multi", geometry)


@validate_argument_types
def ST_Normalize(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Normalize", geometry)


@validate_argument_types
def ST_NPoints(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_NPoints", geometry)


@validate_argument_types
def ST_NumGeometries(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_NumGeometries", geometry)


@validate_argument_types
def ST_NumInteriorRings(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_NumInteriorRings", geometry)


@validate_argument_types
def ST_PointN(geometry: ColumnOrName, n: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_PointN", (geometry, n))


@validate_argument_types
def ST_PointOnSurface(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_PointOnSurface", geometry)


@validate_argument_types
def ST_PrecisionReduce(geometry: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_PrecisionReduce", (geometry, precision))


@validate_argument_types
def ST_RemovePoint(line_string: ColumnOrName, index: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_RemovePoint", (line_string, index))


@validate_argument_types
def ST_Reverse(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_Reverse", geometry)


@validate_argument_types
def ST_SetSRID(geometry: ColumnOrName, srid: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_SetSRID", (geometry, srid))


@validate_argument_types
def ST_SRID(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_SRID", geometry)


@validate_argument_types
def ST_StartPoint(line_string: ColumnOrName) -> Column:
    return _call_st_function("ST_StartPoint", line_string)


@validate_argument_types
def ST_SubDivide(geometry: ColumnOrName, max_vertices: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_SubDivide", (geometry, max_vertices))


@validate_argument_types
def ST_SubDivideExplode(geometry: ColumnOrName, max_vertices: Union[ColumnOrName, int]) -> Column:
    return _call_st_function("ST_SubDivideExplode", (geometry, max_vertices))


@validate_argument_types
def ST_SimplifyPreserveTopology(geometry: ColumnOrName, distance_tolerance: ColumnOrNameOrNumber) -> Column:
    return _call_st_function("ST_SimplifyPreserveTopology", (geometry, distance_tolerance))


@validate_argument_types
def ST_SymDifference(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_SymDifference", (a, b))


@validate_argument_types
def ST_Transform(geometry: ColumnOrName, source_crs: ColumnOrName, target_crs: ColumnOrName, disable_error: Optional[Union[ColumnOrName, bool]] = None) -> Column:
    args = (geometry, source_crs, target_crs) if disable_error is None else (geometry, source_crs, target_crs, disable_error)
    return _call_st_function("ST_Transform", args)


@validate_argument_types
def ST_Union(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_st_function("ST_Union", (a, b))


@validate_argument_types
def ST_X(point: ColumnOrName) -> Column:
    return _call_st_function("ST_X", point)


@validate_argument_types
def ST_XMax(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_XMax", geometry)


@validate_argument_types
def ST_XMin(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_XMin", geometry)


@validate_argument_types
def ST_Y(point: ColumnOrName) -> Column:
    return _call_st_function("ST_Y", point)


@validate_argument_types
def ST_YMax(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_YMax", geometry)


@validate_argument_types
def ST_YMin(geometry: ColumnOrName) -> Column:
    return _call_st_function("ST_YMin", geometry)


@validate_argument_types
def ST_Z(point: ColumnOrName) -> Column:
    return _call_st_function("ST_Z", point)
