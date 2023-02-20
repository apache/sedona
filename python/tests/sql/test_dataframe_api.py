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

from typing import Callable, Tuple

from pyspark.sql import functions as f, Row
import pytest
from shapely.geometry.base import BaseGeometry

from sedona.sql import (
    st_aggregates as sta,
    st_constructors as stc,
    st_functions as stf,
    st_predicates as stp,
)

from tests.test_base import TestBase


test_configurations = [
    # constructors
    (stc.ST_GeomFromGeoHash, ("geohash", 4), "constructor", "ST_PrecisionReduce(geom, 2)", "POLYGON ((0.7 1.05, 1.05 1.05, 1.05 0.88, 0.7 0.88, 0.7 1.05))"),
    (stc.ST_GeomFromGeoJSON, ("geojson",), "constructor", "", "POINT (0 1)"),
    (stc.ST_GeomFromGML, ("gml",), "constructor", "", "LINESTRING (-71.16 42.25, -71.17 42.25, -71.18 42.25)"),
    (stc.ST_GeomFromKML, ("kml",), "constructor", "", "LINESTRING (-71.16 42.26, -71.17 42.26)"),
    (stc.ST_GeomFromText, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeomFromWKB, ("wkb",), "constructor", "ST_PrecisionReduce(geom, 2)", "LINESTRING (-2.1 -0.35, -1.5 -0.67)"),
    (stc.ST_GeomFromWKT, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_LineFromText, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_LineStringFromText, ("multiple_point", lambda: f.lit(',')), "constructor", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stc.ST_Point, ("x", "y"), "constructor", "", "POINT (0 1)"),
    (stc.ST_PointFromText, ("single_point", lambda: f.lit(',')), "constructor", "", "POINT (0 1)"),
    (stc.ST_PolygonFromEnvelope, ("minx", "miny", "maxx", "maxy"), "min_max_x_y", "", "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"),
    (stc.ST_PolygonFromEnvelope, (0.0, 1.0, 2.0, 3.0), "null", "", "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"),
    (stc.ST_PolygonFromText, ("multiple_point", lambda: f.lit(',')), "constructor", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),

    # functions
    (stf.ST_3DDistance, ("a", "b"), "two_points", "", 5.0),
    (stf.ST_AddPoint, ("line", lambda: f.expr("ST_Point(1.0, 1.0)")), "linestring_geom", "", "LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 1 1)"),
    (stf.ST_AddPoint, ("line", lambda: f.expr("ST_Point(1.0, 1.0)"), 1), "linestring_geom", "", "LINESTRING (0 0, 1 1, 1 0, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_Area, ("geom",), "triangle_geom", "", 0.5),
    (stf.ST_AsBinary, ("point",), "point_geom", "", "01010000000000000000000000000000000000f03f"),
    (stf.ST_AsEWKB, (lambda: f.expr("ST_SetSRID(point, 3021)"),), "point_geom", "", "0101000020cd0b00000000000000000000000000000000f03f"),
    (stf.ST_AsEWKT, (lambda: f.expr("ST_SetSRID(point, 4326)"),), "point_geom", "", "SRID=4326;POINT (0 1)"),
    (stf.ST_AsGeoJSON, ("point",), "point_geom", "", "{\"type\":\"Point\",\"coordinates\":[0.0,1.0]}"),
    (stf.ST_AsGML, ("point",), "point_geom", "", "<gml:Point>\n  <gml:coordinates>\n    0.0,1.0 \n  </gml:coordinates>\n</gml:Point>\n"),
    (stf.ST_AsKML, ("point",), "point_geom", "", "<Point>\n  <coordinates>0.0,1.0</coordinates>\n</Point>\n"),
    (stf.ST_AsText, ("point",), "point_geom", "", "POINT (0 1)"),
    (stf.ST_Azimuth, ("a", "b"), "two_points", "geom * 180.0 / pi()", 90.0),
    (stf.ST_Boundary, ("geom",), "triangle_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_Buffer, ("point", 1.0), "point_geom", "ST_PrecisionReduce(geom, 2)", "POLYGON ((0.98 0.8, 0.92 0.62, 0.83 0.44, 0.71 0.29, 0.56 0.17, 0.38 0.08, 0.2 0.02, 0 0, -0.2 0.02, -0.38 0.08, -0.56 0.17, -0.71 0.29, -0.83 0.44, -0.92 0.62, -0.98 0.8, -1 1, -0.98 1.2, -0.92 1.38, -0.83 1.56, -0.71 1.71, -0.56 1.83, -0.38 1.92, -0.2 1.98, 0 2, 0.2 1.98, 0.38 1.92, 0.56 1.83, 0.71 1.71, 0.83 1.56, 0.92 1.38, 0.98 1.2, 1 1, 0.98 0.8))"),
    (stf.ST_BuildArea, ("geom",), "multiline_geom", "ST_Normalize(geom)", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_Centroid, ("geom",), "triangle_geom", "ST_PrecisionReduce(geom, 2)", "POINT (0.67 0.33)"),
    (stf.ST_Collect, (lambda: f.expr("array(a, b)"),), "two_points", "", "MULTIPOINT Z (0 0 0, 3 0 4)"),
    (stf.ST_Collect, ("a", "b"), "two_points", "", "MULTIPOINT Z (0 0 0, 3 0 4)"),
    (stf.ST_CollectionExtract, ("geom",), "geom_collection", "", "MULTILINESTRING ((0 0, 1 0))"),
    (stf.ST_CollectionExtract, ("geom", 1), "geom_collection", "", "MULTIPOINT (0 0)"),
    (stf.ST_ConcaveHull, ("geom", 1.0), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_ConcaveHull, ("geom", 1.0, True), "triangle_geom", "", "POLYGON ((1 1, 1 0, 0 0, 1 1))"),
    (stf.ST_ConvexHull, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_Difference, ("a", "b"), "overlapping_polys", "", "POLYGON ((1 0, 0 0, 0 1, 1 1, 1 0))"),
    (stf.ST_Distance, ("a", "b"), "two_points", "", 3.0),
    (stf.ST_Dump, ("geom",), "multipoint", "", ["POINT (0 0)", "POINT (1 1)"]),
    (stf.ST_DumpPoints, ("line",), "linestring_geom", "", ["POINT (0 0)", "POINT (1 0)", "POINT (2 0)", "POINT (3 0)", "POINT (4 0)", "POINT (5 0)"]),
    (stf.ST_EndPoint, ("line",), "linestring_geom", "", "POINT (5 0)"),
    (stf.ST_Envelope, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
    (stf.ST_ExteriorRing, ("geom",), "triangle_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_FlipCoordinates, ("point",), "point_geom", "", "POINT (1 0)"),
    (stf.ST_Force_2D, ("point",), "point_geom", "", "POINT (0 1)"),
    (stf.ST_GeometryN, ("geom", 0), "multipoint", "", "POINT (0 0)"),
    (stf.ST_GeometryType, ("point",), "point_geom", "", "ST_Point"),
    (stf.ST_InteriorRingN, ("geom", 0), "geom_with_hole", "", "LINESTRING (1 1, 2 2, 2 1, 1 1)"),
    (stf.ST_Intersection, ("a", "b"), "overlapping_polys", "", "POLYGON ((2 0, 1 0, 1 1, 2 1, 2 0))"),
    (stf.ST_IsClosed, ("geom",), "closed_linestring_geom", "", True),
    (stf.ST_IsEmpty, ("geom",), "empty_geom", "", True),
    (stf.ST_IsRing, ("line",), "linestring_geom", "", False),
    (stf.ST_IsSimple, ("geom",), "triangle_geom", "", True),
    (stf.ST_IsValid, (lambda: f.col("geom"),), "triangle_geom", "", True),
    (stf.ST_Length, ("line",), "linestring_geom", "", 5.0),
    (stf.ST_LineFromMultiPoint, ("multipoint",), "multipoint_geom", "", "LINESTRING (10 40, 40 30, 20 20, 30 10)"),
    (stf.ST_LineInterpolatePoint, ("line", 0.5), "linestring_geom", "", "POINT (2.5 0)"),
    (stf.ST_LineMerge, ("geom",), "multiline_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_LineSubstring, ("line", 0.5, 1.0), "linestring_geom", "", "LINESTRING (2.5 0, 3 0, 4 0, 5 0)"),
    (stf.ST_MakeValid, ("geom",), "invalid_geom", "", "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))"),
    (stf.ST_MakePolygon, ("geom",), "closed_linestring_geom", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_MinimumBoundingCircle, ("line",), "linestring_geom", "ST_PrecisionReduce(geom, 2)", "POLYGON ((4.95 -0.49, 4.81 -0.96, 4.58 -1.39, 4.27 -1.77, 3.89 -2.08, 3.46 -2.31, 2.99 -2.45, 2.5 -2.5, 2.01 -2.45, 1.54 -2.31, 1.11 -2.08, 0.73 -1.77, 0.42 -1.39, 0.19 -0.96, 0.05 -0.49, 0 0, 0.05 0.49, 0.19 0.96, 0.42 1.39, 0.73 1.77, 1.11 2.08, 1.54 2.31, 2.01 2.45, 2.5 2.5, 2.99 2.45, 3.46 2.31, 3.89 2.08, 4.27 1.77, 4.58 1.39, 4.81 0.96, 4.95 0.49, 5 0, 4.95 -0.49))"),
    (stf.ST_MinimumBoundingCircle, ("line", 2), "linestring_geom", "ST_PrecisionReduce(geom, 2)", "POLYGON ((4.27 -1.77, 2.5 -2.5, 0.73 -1.77, 0 0, 0.73 1.77, 2.5 2.5, 4.27 1.77, 5 0, 4.27 -1.77))"),
    (stf.ST_MinimumBoundingRadius, ("line",), "linestring_geom", "", {"center": "POINT (2.5 0)", "radius": 2.5}),
    (stf.ST_Multi, ("point",), "point_geom", "", "MULTIPOINT (0 1)"),
    (stf.ST_Normalize, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_NPoints, ("line",), "linestring_geom", "", 6),
    (stf.ST_NumGeometries, ("geom",), "multipoint", "", 2),
    (stf.ST_NumInteriorRings, ("geom",), "geom_with_hole", "", 1),
    (stf.ST_PointN, ("line", 2), "linestring_geom", "", "POINT (1 0)"),
    (stf.ST_PointOnSurface, ("line",), "linestring_geom", "", "POINT (2 0)"),
    (stf.ST_PrecisionReduce, ("geom", 1), "precision_reduce_point", "", "POINT (0.1 0.2)"),
    (stf.ST_RemovePoint, ("line", 1), "linestring_geom", "", "LINESTRING (0 0, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_Reverse, ("line",), "linestring_geom", "", "LINESTRING (5 0, 4 0, 3 0, 2 0, 1 0, 0 0)"),
    (stf.ST_S2CellIDs, ("point", 30), "point_geom", "", [1153451514845492609]),
    (stf.ST_SetPoint, ("line", 1, lambda: f.expr("ST_Point(1.0, 1.0)")), "linestring_geom", "", "LINESTRING (0 0, 1 1, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_SetSRID, ("point", 3021), "point_geom", "ST_SRID(geom)", 3021),
    (stf.ST_SimplifyPreserveTopology, ("geom", 0.2), "0.9_poly", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_Split, ("a", "b"), "overlapping_polys", "", "MULTIPOLYGON (((1 0, 0 0, 0 1, 1 1, 1 0)), ((2 0, 2 1, 3 1, 3 0, 2 0)))"),
    (stf.ST_SRID, ("point",), "point_geom", "", 0),
    (stf.ST_StartPoint, ("line",), "linestring_geom", "", "POINT (0 0)"),
    (stf.ST_SubDivide, ("line", 5), "linestring_geom", "", ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]),
    (stf.ST_SubDivideExplode, ("line", 5), "linestring_geom", "collect_list(geom)", ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]),
    (stf.ST_SymDifference, ("a", "b"), "overlapping_polys", "", "MULTIPOLYGON (((1 0, 0 0, 0 1, 1 1, 1 0)), ((2 0, 2 1, 3 1, 3 0, 2 0)))"),
    (stf.ST_Transform, ("point", lambda: f.lit("EPSG:4326"), lambda: f.lit("EPSG:32649")), "point_geom", "ST_PrecisionReduce(geom, 2)", "POINT (-33788209.77 0)"),
    (stf.ST_Union, ("a", "b"), "overlapping_polys", "", "POLYGON ((1 0, 0 0, 0 1, 1 1, 2 1, 3 1, 3 0, 2 0, 1 0))"),
    (stf.ST_X, ("b",), "two_points", "", 3.0),
    (stf.ST_XMax, ("line",), "linestring_geom", "", 5.0),
    (stf.ST_XMin, ("line",), "linestring_geom", "", 0.0),
    (stf.ST_Y, ("b",), "two_points", "", 0.0),
    (stf.ST_YMax, ("geom",), "triangle_geom", "", 1.0),
    (stf.ST_YMin, ("geom",), "triangle_geom", "", 0.0),
    (stf.ST_Z, ("b",), "two_points", "", 4.0),

    # predicates
    (stp.ST_Contains, ("geom", lambda: f.expr("ST_Point(0.5, 0.25)")), "triangle_geom", "", True),
    (stp.ST_Crosses, ("line", "poly"), "line_crossing_poly", "", True),
    (stp.ST_Disjoint, ("a", "b"), "two_points", "", True),
    (stp.ST_Equals, ("line", lambda: f.expr("ST_Reverse(line)")), "linestring_geom", "", True),
    (stp.ST_Intersects, ("a", "b"), "overlapping_polys", "", True),
    (stp.ST_OrderingEquals, ("line", lambda: f.expr("ST_Reverse(line)")), "linestring_geom", "", False),
    (stp.ST_Overlaps, ("a", "b"), "overlapping_polys", "", True),
    (stp.ST_Touches, ("a", "b"), "touching_polys", "", True),
    (stp.ST_Within, (lambda: f.expr("ST_Point(0.5, 0.25)"), "geom"), "triangle_geom", "", True),
    (stp.ST_Covers, ("geom", lambda: f.expr("ST_Point(0.5, 0.25)")), "triangle_geom", "", True),
    (stp.ST_CoveredBy, (lambda: f.expr("ST_Point(0.5, 0.25)"), "geom"), "triangle_geom", "", True),
    (stp.ST_Contains, ("geom", lambda: f.expr("ST_Point(0.0, 0.0)")), "triangle_geom", "", False),
    (stp.ST_Within, (lambda: f.expr("ST_Point(0.0, 0.0)"), "geom"), "triangle_geom", "", False),
    (stp.ST_Covers, ("geom", lambda: f.expr("ST_Point(0.0, 0.0)")), "triangle_geom", "", True),
    (stp.ST_CoveredBy, (lambda: f.expr("ST_Point(0.0, 0.0)"), "geom"), "triangle_geom", "", True),

    # aggregates
    (sta.ST_Envelope_Aggr, ("geom",), "exploded_points", "", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
    (sta.ST_Intersection_Aggr, ("geom",), "exploded_polys", "", "LINESTRING (1 0, 1 1)"),
    (sta.ST_Union_Aggr, ("geom",), "exploded_polys", "", "POLYGON ((1 0, 0 0, 0 1, 1 1, 2 1, 2 0, 1 0))"),
]

wrong_type_configurations = [
    # constructors
    (stc.ST_GeomFromGeoHash, (None, 4)),
    (stc.ST_GeomFromGeoHash, ("", None)),
    (stc.ST_GeomFromGeoHash, ("", 4.0)),
    (stc.ST_GeomFromGeoJSON, (None,)),
    (stc.ST_GeomFromGML, (None,)),
    (stc.ST_GeomFromKML, (None,)),
    (stc.ST_GeomFromText, (None,)),
    (stc.ST_GeomFromWKB, (None,)),
    (stc.ST_GeomFromWKT, (None,)),
    (stc.ST_LineFromText, (None,)),
    (stc.ST_LineStringFromText, (None, "")),
    (stc.ST_LineStringFromText, ("", None)),
    (stc.ST_Point, (None, "")),
    (stc.ST_Point, ("", None)),
    (stc.ST_PointFromText, (None, "")),
    (stc.ST_PointFromText, ("", None)),
    (stc.ST_PolygonFromEnvelope, (None, "", "", "")),
    (stc.ST_PolygonFromEnvelope, ("", None, "", "")),
    (stc.ST_PolygonFromEnvelope, ("", "", None, "")),
    (stc.ST_PolygonFromEnvelope, ("", "", "", None)),
    (stc.ST_PolygonFromText, (None, "")),
    (stc.ST_PolygonFromText, ("", None)),

    # functions
    (stf.ST_3DDistance, (None, "")),
    (stf.ST_3DDistance, ("", None)),
    (stf.ST_AddPoint, (None, "")),
    (stf.ST_AddPoint, ("", None)),
    (stf.ST_Area, (None,)),
    (stf.ST_AsBinary, (None,)),
    (stf.ST_AsEWKB, (None,)),
    (stf.ST_AsEWKT, (None,)),
    (stf.ST_AsGeoJSON, (None,)),
    (stf.ST_AsGML, (None,)),
    (stf.ST_AsKML, (None,)),
    (stf.ST_AsText, (None,)),
    (stf.ST_Azimuth, (None, "")),
    (stf.ST_Azimuth, ("", None)),
    (stf.ST_Boundary, (None,)),
    (stf.ST_Buffer, (None, 1.0)),
    (stf.ST_Buffer, ("", None)),
    (stf.ST_BuildArea, (None,)),
    (stf.ST_Centroid, (None,)),
    (stf.ST_Collect, (None,)),
    (stf.ST_CollectionExtract, (None,)),
    (stf.ST_ConcaveHull, (None, 1.0)),
    (stf.ST_ConvexHull, (None,)),
    (stf.ST_Difference, (None, "b")),
    (stf.ST_Difference, ("", None)),
    (stf.ST_Distance, (None, "")),
    (stf.ST_Distance, ("", None)),
    (stf.ST_Dump, (None,)),
    (stf.ST_DumpPoints, (None,)),
    (stf.ST_EndPoint, (None,)),
    (stf.ST_Envelope, (None,)),
    (stf.ST_ExteriorRing, (None,)),
    (stf.ST_FlipCoordinates, (None,)),
    (stf.ST_Force_2D, (None,)),
    (stf.ST_GeometryN, (None, 0)),
    (stf.ST_GeometryN, ("", None)),
    (stf.ST_GeometryN, ("", 0.0)),
    (stf.ST_GeometryType, (None,)),
    (stf.ST_InteriorRingN, (None, 0)),
    (stf.ST_InteriorRingN, ("", None)),
    (stf.ST_InteriorRingN, ("", 0.0)),
    (stf.ST_Intersection, (None, "")),
    (stf.ST_Intersection, ("", None)),
    (stf.ST_IsClosed, (None,)),
    (stf.ST_IsEmpty, (None,)),
    (stf.ST_IsRing, (None,)),
    (stf.ST_IsSimple, (None,)),
    (stf.ST_IsValid, (None,)),
    (stf.ST_Length, (None,)),
    (stf.ST_LineFromMultiPoint, (None,)),
    (stf.ST_LineInterpolatePoint, (None, 0.5)),
    (stf.ST_LineInterpolatePoint, ("", None)),
    (stf.ST_LineMerge, (None,)),
    (stf.ST_LineSubstring, (None, 0.5, 1.0)),
    (stf.ST_LineSubstring, ("", None, 1.0)),
    (stf.ST_LineSubstring, ("", 0.5, None)),
    (stf.ST_MakeValid, (None,)),
    (stf.ST_MakePolygon, (None,)),
    (stf.ST_MinimumBoundingCircle, (None,)),
    (stf.ST_MinimumBoundingRadius, (None,)),
    (stf.ST_Multi, (None,)),
    (stf.ST_Normalize, (None,)),
    (stf.ST_NPoints, (None,)),
    (stf.ST_NumGeometries, (None,)),
    (stf.ST_NumInteriorRings, (None,)),
    (stf.ST_PointN, (None, 2)),
    (stf.ST_PointN, ("", None)),
    (stf.ST_PointN, ("", 2.0)),
    (stf.ST_PointOnSurface, (None,)),
    (stf.ST_PrecisionReduce, (None, 1)),
    (stf.ST_PrecisionReduce, ("", None)),
    (stf.ST_PrecisionReduce, ("", 1.0)),
    (stf.ST_RemovePoint, (None, 1)),
    (stf.ST_RemovePoint, ("", None)),
    (stf.ST_RemovePoint, ("", 1.0)),
    (stf.ST_Reverse, (None,)),
    (stf.ST_S2CellIDs, (None, 2)),
    (stf.ST_SetPoint, (None, 1, "")),
    (stf.ST_SetPoint, ("", None, "")),
    (stf.ST_SetPoint, ("", 1, None)),
    (stf.ST_SetSRID, (None, 3021)),
    (stf.ST_SetSRID, ("", None)),
    (stf.ST_SetSRID, ("", 3021.0)),
    (stf.ST_SimplifyPreserveTopology, (None, 0.2)),
    (stf.ST_SimplifyPreserveTopology, ("", None)),
    (stf.ST_SRID, (None,)),
    (stf.ST_StartPoint, (None,)),
    (stf.ST_SubDivide, (None, 5)),
    (stf.ST_SubDivide, ("", None)),
    (stf.ST_SubDivide, ("", 5.0)),
    (stf.ST_SubDivideExplode, (None, 5)),
    (stf.ST_SubDivideExplode, ("", None)),
    (stf.ST_SubDivideExplode, ("", 5.0)),
    (stf.ST_SymDifference, (None, "")),
    (stf.ST_SymDifference, ("", None)),
    (stf.ST_Transform, (None, "", "")),
    (stf.ST_Transform, ("", None, "")),
    (stf.ST_Transform, ("", "", None)),
    (stf.ST_Union, (None, "")),
    (stf.ST_Union, ("", None)),
    (stf.ST_X, (None,)),
    (stf.ST_XMax, (None,)),
    (stf.ST_XMin, (None,)),
    (stf.ST_Y, (None,)),
    (stf.ST_YMax, (None,)),
    (stf.ST_YMin, (None,)),
    (stf.ST_Z, (None,)),

    # predicates
    (stp.ST_Contains, (None, "")),
    (stp.ST_Contains, ("", None)),
    (stp.ST_Crosses, (None, "")),
    (stp.ST_Crosses, ("", None)),
    (stp.ST_Disjoint, (None, "")),
    (stp.ST_Disjoint, ("", None)),
    (stp.ST_Equals, (None, "")),
    (stp.ST_Equals, ("", None)),
    (stp.ST_Intersects, (None, "")),
    (stp.ST_Intersects, ("", None)),
    (stp.ST_OrderingEquals, (None, "")),
    (stp.ST_OrderingEquals, ("", None)),
    (stp.ST_Overlaps, (None, "")),
    (stp.ST_Overlaps, ("", None)),
    (stp.ST_Touches, (None, "")),
    (stp.ST_Touches, ("", None)),
    (stp.ST_Within, (None, "")),
    (stp.ST_Within, ("", None)),

    # aggregates
    (sta.ST_Envelope_Aggr, (None,)),
    (sta.ST_Intersection_Aggr, (None,)),
    (sta.ST_Union_Aggr, (None,)),
]

class TestDataFrameAPI(TestBase):

    @pytest.fixture
    def base_df(self, request):
        wkb = '0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf'
        geojson = "{ \"type\": \"Feature\", \"properties\": { \"prop\": \"01\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 0.0, 1.0 ] }},"
        gml_string = "<gml:LineString srsName=\"EPSG:4269\"><gml:coordinates>-71.16,42.25 -71.17,42.25 -71.18,42.25</gml:coordinates></gml:LineString>"
        kml_string = "<LineString><coordinates>-71.16,42.26 -71.17,42.26</coordinates></LineString>"

        if request.param == "constructor":
            return TestDataFrameAPI.spark.sql("SELECT null").selectExpr(
                "0.0 AS x",
                "1.0 AS y",
                "'0.0,1.0' AS single_point",
                "'0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0' AS multiple_point",
                f"X'{wkb}' AS wkb",
                f"'{geojson}' AS geojson",
                "'s00twy01mt' AS geohash",
                f"'{gml_string}' AS gml",
                f"'{kml_string}' AS kml"
            )
        elif request.param == "point_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_Point(0.0, 1.0) AS point")
        elif request.param == "linestring_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS line")
        elif request.param == "linestring_wkt":
            return TestDataFrameAPI.spark.sql("SELECT 'LINESTRING (1 2, 3 4)' AS wkt")
        elif request.param == "min_max_x_y":
            return TestDataFrameAPI.spark.sql("SELECT 0.0 AS minx, 1.0 AS miny, 2.0 AS maxx, 3.0 AS maxy")
        elif request.param == "multipoint_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))') AS multipoint")
        elif request.param == "null":
            return TestDataFrameAPI.spark.sql("SELECT null")
        elif request.param == "triangle_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
        elif request.param == "two_points":
            return TestDataFrameAPI.spark.sql("SELECT ST_PointZ(0.0, 0.0, 0.0) AS a, ST_PointZ(3.0, 0.0, 4.0) AS b")
        elif request.param == "invalid_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS geom")
        elif request.param == "overlapping_polys":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON((1 0, 3 0, 3 1, 1 1, 1 0))') AS b")
        elif request.param == "multipoint":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
        elif request.param == "geom_with_hole":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
        elif request.param == "0.9_poly":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom")
        elif request.param == "precision_reduce_point":
            return TestDataFrameAPI.spark.sql("SELECT ST_Point(0.12, 0.23) AS geom")
        elif request.param == "closed_linestring_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
        elif request.param == "empty_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_Difference(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0)) AS geom")
        elif request.param == "multiline_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 0))') AS geom")
        elif request.param == "geom_collection":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
        elif request.param == "exploded_points":
            return TestDataFrameAPI.spark.sql("SELECT explode(array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0))) AS geom")
        elif request.param == "exploded_polys":
            return TestDataFrameAPI.spark.sql("SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))) AS geom")
        elif request.param == "touching_polys":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))') AS b")
        elif request.param == "line_crossing_poly":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 1)') AS line, ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 2, 1 2, 1 0))') AS poly")
        raise ValueError(f"Invalid base_df name passed: {request.param}")

    def _id_test_configuration(val):
        if isinstance(val, Callable):
            return val.__name__
        elif isinstance(val, Tuple):
            return f"{val}"
        elif isinstance(val, dict):
            return f"{val}"
        return val

    @pytest.mark.parametrize("func,args,base_df,post_process,expected_result", test_configurations, ids=_id_test_configuration, indirect=["base_df"])
    def test_dataframe_function(self, func, args, base_df, post_process, expected_result):
        args = [arg() if isinstance(arg, Callable) else arg for arg in args]

        if len(args) == 1:
            df = base_df.select(func(args[0]).alias("geom"))
        else:
            df = base_df.select(func(*args).alias("geom"))

        if post_process:
            df = df.selectExpr(f"{post_process} AS geom")

        actual_result = df.collect()[0][0]

        if isinstance(actual_result, BaseGeometry):
            actual_result = actual_result.wkt
        elif isinstance(actual_result, bytearray):
            actual_result = actual_result.hex()
        elif isinstance(actual_result, Row):
            actual_result = {k: v.wkt if isinstance(v, BaseGeometry) else v for k, v in actual_result.asDict().items()}
        elif isinstance(actual_result, list):
            actual_result = sorted([x.wkt if isinstance(x, BaseGeometry) else x for x in actual_result])

        assert(actual_result == expected_result)

    @pytest.mark.parametrize("func,args", wrong_type_configurations, ids=_id_test_configuration)
    def test_call_function_with_wrong_type(self, func, args):
        with pytest.raises(ValueError, match=f"Incorrect argument type: [A-Za-z_0-9]+ for {func.__name__} should be [A-Za-z0-9\\[\\]_, ]+ but received [A-Za-z0-9_]+."):
            func(*args)
