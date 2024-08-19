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

from sedona.sql.st_aggregates import *
from sedona.sql.st_constructors import *
from sedona.sql.st_functions import *
from sedona.sql.st_predicates import *

from sedona.sql import (
    st_aggregates as sta,
    st_constructors as stc,
    st_functions as stf,
    st_predicates as stp,
)

from tests.test_base import TestBase


test_configurations = [
    # constructors
    (stc.ST_GeomFromGeoHash, ("geohash", 4), "constructor", "ST_ReducePrecision(geom, 2)", "POLYGON ((0.7 1.05, 1.05 1.05, 1.05 0.88, 0.7 0.88, 0.7 1.05))"),
    (stc.ST_GeomFromGeoJSON, ("geojson",), "constructor", "", "POINT (0 1)"),
    (stc.ST_GeomFromGML, ("gml",), "constructor", "", "LINESTRING (-71.16 42.25, -71.17 42.25, -71.18 42.25)"),
    (stc.ST_GeomFromKML, ("kml",), "constructor", "", "LINESTRING (-71.16 42.26, -71.17 42.26)"),
    (stc.ST_GeomFromText, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeomFromText, ("wkt",4326), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeometryFromText, ("wkt", 4326), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeomFromWKB, ("wkbLine",), "constructor", "ST_ReducePrecision(geom, 2)", "LINESTRING (-2.1 -0.35, -1.5 -0.67)"),
    (stc.ST_GeomFromEWKB, ("wkbLine",), "constructor", "ST_ReducePrecision(geom, 2)", "LINESTRING (-2.1 -0.35, -1.5 -0.67)"),
    (stc.ST_GeomFromWKT, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeomFromWKT, ("wkt",4326), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_GeomFromEWKT, ("ewkt",), "linestring_ewkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_LineFromText, ("wkt",), "linestring_wkt", "", "LINESTRING (1 2, 3 4)"),
    (stc.ST_LineFromWKB, ("wkbLine",), "constructor", "ST_ReducePrecision(geom, 2)", "LINESTRING (-2.1 -0.35, -1.5 -0.67)"),
    (stc.ST_LinestringFromWKB, ("wkbLine",), "constructor", "ST_ReducePrecision(geom, 2)", "LINESTRING (-2.1 -0.35, -1.5 -0.67)"),
    (stc.ST_LineStringFromText, ("multiple_point", lambda: f.lit(',')), "constructor", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stc.ST_Point, ("x", "y"), "constructor", "", "POINT (0 1)"),
    (stc.ST_PointZ, ("x", "y", "z", 4326), "constructor", "", "POINT Z (0 1 2)"),
    (stc.ST_PointZ, ("x", "y", "z"), "constructor", "", "POINT Z (0 1 2)"),
    (stc.ST_MPolyFromText, ("mpoly",), "constructor", "" , "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))"),
    (stc.ST_MPolyFromText, ("mpoly", 4326), "constructor", "" , "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))"),
    (stc.ST_MLineFromText, ("mline", ), "constructor", "" , "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))"),
    (stc.ST_MLineFromText, ("mline", 4326), "constructor", "" , "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))"),
    (stc.ST_MPointFromText, ("mpoint", ), "constructor", "" , "MULTIPOINT (10 10, 20 20, 30 30)"),
    (stc.ST_MPointFromText, ("mpoint", 4326), "constructor", "" , "MULTIPOINT (10 10, 20 20, 30 30)"),
    (stc.ST_PointFromText, ("single_point", lambda: f.lit(',')), "constructor", "", "POINT (0 1)"),
    (stc.ST_PointFromWKB, ("wkbPoint",), "constructor", "", "POINT (10 15)"),
    (stc.ST_MakePoint, ("x", "y", "z"), "constructor", "", "POINT Z (0 1 2)"),
    (stc.ST_MakePointM, ("x", "y", "z"), "constructor", "ST_AsText(geom)", "POINT M(0 1 2)"),
    (stc.ST_PolygonFromEnvelope, ("minx", "miny", "maxx", "maxy"), "min_max_x_y", "", "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"),
    (stc.ST_PolygonFromEnvelope, (0.0, 1.0, 2.0, 3.0), "null", "", "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"),
    (stc.ST_PolygonFromText, ("multiple_point", lambda: f.lit(',')), "constructor", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stc.ST_GeomCollFromText, ("collection",), "constructor", "", "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 1 1))"),
    (stc.ST_GeomCollFromText, ("collection", 4326), "constructor", "ST_SRID(geom)", 4326),

    # functions
    (stf.GeometryType, ("line",), "linestring_geom", "", "LINESTRING"),
    (stf.ST_3DDistance, ("a", "b"), "two_points", "", 5.0),
    (stf.ST_Affine, ("geom", 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), "square_geom", "", "POLYGON ((2 3, 4 5, 5 6, 3 4, 2 3))"),
    (stf.ST_Affine, ("geom", 1.0, 2.0, 1.0, 2.0, 1.0, 2.0,), "square_geom", "", "POLYGON ((2 3, 4 5, 5 6, 3 4, 2 3))"),
    (stf.ST_AddMeasure, ("line", 10.0, 40.0), "linestring_geom", "ST_AsText(geom)", "LINESTRING M(0 0 10, 1 0 16, 2 0 22, 3 0 28, 4 0 34, 5 0 40)"),
    (stf.ST_AddPoint, ("line", lambda: f.expr("ST_Point(1.0, 1.0)")), "linestring_geom", "", "LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 1 1)"),
    (stf.ST_AddPoint, ("line", lambda: f.expr("ST_Point(1.0, 1.0)"), 1), "linestring_geom", "", "LINESTRING (0 0, 1 1, 1 0, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_Angle, ("p1", "p2", "p3", "p4", ), "four_points", "", 0.4048917862850834),
    (stf.ST_Angle, ("p1", "p2", "p3",), "three_points", "", 0.19739555984988078),
    (stf.ST_Angle, ("line1", "line2"), "two_lines", "", 0.19739555984988078),
    (stf.ST_Degrees, ("angleRad",), "two_lines_angle_rad", "", 11.309932474020213),
    (stf.ST_Area, ("geom",), "triangle_geom", "", 0.5),
    (stf.ST_AreaSpheroid, ("point",), "point_geom", "", 0.0),
    (stf.ST_AsBinary, ("point",), "point_geom", "", "01010000000000000000000000000000000000f03f"),
    (stf.ST_AsEWKB, (lambda: f.expr("ST_SetSRID(point, 3021)"),), "point_geom", "", "0101000020cd0b00000000000000000000000000000000f03f"),
    (stf.ST_AsHEXEWKB, ("point",), "point_geom", "", "01010000000000000000000000000000000000F03F"),
    (stf.ST_AsEWKT, (lambda: f.expr("ST_SetSRID(point, 4326)"),), "point_geom", "", "SRID=4326;POINT (0 1)"),
    (stf.ST_AsGeoJSON, ("point",), "point_geom", "", "{\"type\":\"Point\",\"coordinates\":[0.0,1.0]}"),
    (stf.ST_AsGeoJSON, ("point", lambda: f.lit("feature")), "point_geom", "", "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,1.0]},\"properties\":{}}"),
    (stf.ST_AsGeoJSON, ("point", lambda: f.lit("featurecollection")), "point_geom", "", "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,1.0]},\"properties\":{}}]}"),
    (stf.ST_AsGML, ("point",), "point_geom", "", "<gml:Point>\n  <gml:coordinates>\n    0.0,1.0 \n  </gml:coordinates>\n</gml:Point>\n"),
    (stf.ST_AsKML, ("point",), "point_geom", "", "<Point>\n  <coordinates>0.0,1.0</coordinates>\n</Point>\n"),
    (stf.ST_AsText, ("point",), "point_geom", "", "POINT (0 1)"),
    (stf.ST_Azimuth, ("a", "b"), "two_points", "geom * 180.0 / pi()", 90.0),
    (stf.ST_BestSRID, ("geom",), "triangle_geom", "", 3395),
    (stf.ST_Boundary, ("geom",), "triangle_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_Buffer, ("point", 1.0), "point_geom", "ST_ReducePrecision(geom, 2)", "POLYGON ((0.98 0.8, 0.92 0.62, 0.83 0.44, 0.71 0.29, 0.56 0.17, 0.38 0.08, 0.2 0.02, 0 0, -0.2 0.02, -0.38 0.08, -0.56 0.17, -0.71 0.29, -0.83 0.44, -0.92 0.62, -0.98 0.8, -1 1, -0.98 1.2, -0.92 1.38, -0.83 1.56, -0.71 1.71, -0.56 1.83, -0.38 1.92, -0.2 1.98, 0 2, 0.2 1.98, 0.38 1.92, 0.56 1.83, 0.71 1.71, 0.83 1.56, 0.92 1.38, 0.98 1.2, 1 1, 0.98 0.8))"),
    (stf.ST_Buffer, ("point", 1.0, True), "point_geom", "", "POLYGON ((0.0000089758113634 1.0000000082631704, 0.0000088049473096 0.9999982455016537, 0.0000082957180969 0.9999965501645043, 0.0000074676931201 0.9999949874025787, 0.0000063526929175 0.9999936172719434, 0.0000049935663218 0.9999924924259491, 0.000003442543808 0.9999916560917964, 0.0000017592303026 0.9999911404093366, 0.0000000083146005 0.999990965195956, -0.0000017429165916 0.9999911371850047, -0.0000034271644398 0.9999916497670388, -0.0000049797042467 0.9999924832438165, -0.0000063408727792 0.9999936055852924, -0.0000074583610959 0.9999949736605123, -0.0000082892247492 0.9999965348951139, -0.0000088015341156 0.999998229291728, -0.0000089756014341 0.9999999917356441, -0.0000088047373942 1.0000017544971334, -0.0000082955082053 1.000003449834261, -0.0000074674832591 1.000005012596174, -0.000006352483086 1.0000063827268086, -0.0000049933565169 1.000007507572813, -0.0000034423340222 1.0000083439069856, -0.0000017590205255 1.0000088595894723, -0.0000000081048183 1.0000090348028825, 0.0000017431263877 1.0000088628138615, 0.0000034273742602 1.0000083502318489, 0.000004979914097 1.0000075167550835, 0.0000063410826597 1.000006394413609, 0.000007458571003 1.0000050263383786, 0.000008289434675 1.000003465103757, 0.0000088017440489 1.0000017707071163, 0.0000089758113634 1.0000000082631704))"),
    (stf.ST_BuildArea, ("geom",), "multiline_geom", "ST_Normalize(geom)", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_BoundingDiagonal, ("geom",), "square_geom", "ST_BoundingDiagonal(geom)", "LINESTRING (1 0, 2 1)"),
    (stf.ST_Centroid, ("geom",), "triangle_geom", "ST_ReducePrecision(geom, 2)", "POINT (0.67 0.33)"),
    (stf.ST_Collect, (lambda: f.expr("array(a, b)"),), "two_points", "", "MULTIPOINT Z (0 0 0, 3 0 4)"),
    (stf.ST_Collect, ("a", "b"), "two_points", "", "MULTIPOINT Z (0 0 0, 3 0 4)"),
    (stf.ST_ClosestPoint, ("point", "line",), "point_and_line", "", "POINT (0 1)"),
    (stf.ST_CollectionExtract, ("geom",), "geom_collection", "", "MULTILINESTRING ((0 0, 1 0))"),
    (stf.ST_CollectionExtract, ("geom", 1), "geom_collection", "", "MULTIPOINT (0 0)"),
    (stf.ST_ConcaveHull, ("geom", 1.0), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_ConcaveHull, ("geom", 1.0, True), "triangle_geom", "", "POLYGON ((1 1, 1 0, 0 0, 1 1))"),
    (stf.ST_ConvexHull, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_CoordDim, ("point",), "point_geom", "", 2),
    (stf.ST_CrossesDateLine, ("line",), "line_crossing_dateline", "", True),
    (stf.ST_Difference, ("a", "b"), "overlapping_polys", "", "POLYGON ((1 0, 0 0, 0 1, 1 1, 1 0))"),
    (stf.ST_Dimension, ("geom",), "geometry_geom_collection", "", 1),
    (stf.ST_Distance, ("a", "b"), "two_points", "", 3.0),
    (stf.ST_DistanceSpheroid, ("point", "point"), "point_geom", "", 0.0),
    (stf.ST_DistanceSphere, ("point", "point"), "point_geom", "", 0.0),
    (stf.ST_DistanceSphere, ("point", "point", 6378137.0), "point_geom", "", 0.0),
    (stf.ST_DelaunayTriangles, ("multipoint", ), "multipoint_geom", "", "GEOMETRYCOLLECTION (POLYGON ((10 40, 20 20, 40 30, 10 40)), POLYGON ((40 30, 20 20, 30 10, 40 30)))"),
    (stf.ST_Dump, ("geom",), "multipoint", "", ["POINT (0 0)", "POINT (1 1)"]),
    (stf.ST_DumpPoints, ("line",), "linestring_geom", "", ["POINT (0 0)", "POINT (1 0)", "POINT (2 0)", "POINT (3 0)", "POINT (4 0)", "POINT (5 0)"]),
    (stf.ST_EndPoint, ("line",), "linestring_geom", "", "POINT (5 0)"),
    (stf.ST_Envelope, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
    (stf.ST_Expand, ("geom", 2.0), "triangle_geom", "", "POLYGON ((-2 -2, -2 3, 3 3, 3 -2, -2 -2))"),
    (stf.ST_Expand, ("geom", 2.0, 2.0), "triangle_geom", "", "POLYGON ((-2 -2, -2 3, 3 3, 3 -2, -2 -2))"),
    (stf.ST_ExteriorRing, ("geom",), "triangle_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_FlipCoordinates, ("point",), "point_geom", "", "POINT (1 0)"),
    (stf.ST_Force_2D, ("point",), "point_geom", "", "POINT (0 1)"),
    (stf.ST_Force3D, ("point", 1.0), "point_geom", "", "POINT Z (0 1 1)"),
    (stf.ST_Force3DM, ("point", 1.0), "point_geom", "ST_AsText(geom)", "POINT M(0 1 1)"),
    (stf.ST_Force3DZ, ("point", 1.0), "point_geom", "", "POINT Z (0 1 1)"),
    (stf.ST_Force4D, ("point", 1.0, 1.0), "point_geom", "ST_AsText(geom)", "POINT ZM(0 1 1 1)"),
    (stf.ST_ForceCollection, ("multipoint",), "multipoint_geom", "ST_NumGeometries(geom)", 4),
    (stf.ST_ForcePolygonCW, ("geom",), "geom_with_hole", "", "POLYGON ((0 0, 3 3, 3 0, 0 0), (1 1, 2 1, 2 2, 1 1))"),
    (stf.ST_ForcePolygonCCW, ("geom",), "geom_with_hole", "", "POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))"),
    (stf.ST_ForceRHR, ("geom",), "geom_with_hole", "", "POLYGON ((0 0, 3 3, 3 0, 0 0), (1 1, 2 1, 2 2, 1 1))"),
    (stf.ST_FrechetDistance, ("point", "line",), "point_and_line", "", 5.0990195135927845),
    (stf.ST_GeometricMedian, ("multipoint",), "multipoint_geom", "", "POINT (22.500002656424286 21.250001168173426)"),
    (stf.ST_GeneratePoints, ("geom", 15), "square_geom", "ST_NumGeometries(geom)", 15),
    (stf.ST_GeneratePoints, ("geom", 15, 100), "square_geom", "ST_NumGeometries(geom)", 15),
    (stf.ST_GeometryN, ("geom", 0), "multipoint", "", "POINT (0 0)"),
    (stf.ST_GeometryType, ("point",), "point_geom", "", "ST_Point"),
    (stf.ST_HausdorffDistance, ("point", "line",), "point_and_line", "", 5.0990195135927845),
    (stf.ST_InteriorRingN, ("geom", 0), "geom_with_hole", "", "LINESTRING (1 1, 2 2, 2 1, 1 1)"),
    (stf.ST_Intersection, ("a", "b"), "overlapping_polys", "", "POLYGON ((2 0, 1 0, 1 1, 2 1, 2 0))"),
    (stf.ST_IsCollection, ("geom",), "geom_collection", "", True),
    (stf.ST_IsClosed, ("geom",), "closed_linestring_geom", "", True),
    (stf.ST_IsEmpty, ("geom",), "empty_geom", "", True),
    (stf.ST_IsPolygonCW, ("geom",), "geom_with_hole", "", False),
    (stf.ST_IsPolygonCCW, ("geom",), "geom_with_hole", "", True),
    (stf.ST_IsRing, ("line",), "linestring_geom", "", False),
    (stf.ST_IsSimple, ("geom",), "triangle_geom", "", True),
    (stf.ST_IsValidTrajectory, ("line",), "4D_line", "", False),
    (stf.ST_IsValid, ("geom",), "triangle_geom", "", True),
    (stf.ST_IsValid, ("geom", 1), "triangle_geom", "", True),
    (stf.ST_IsValid, ("geom", 0), "triangle_geom", "", True),
    (stf.ST_IsValidDetail, ("geom",), "triangle_geom", "", Row(valid=True, reason=None, location=None).asDict()),
    (stf.ST_IsValidDetail, ("geom", 1), "triangle_geom", "", Row(valid=True, reason=None, location=None).asDict()),
    (stf.ST_Length, ("line",), "linestring_geom", "", 5.0),
    (stf.ST_Length2D, ("line",), "linestring_geom", "", 5.0),
    (stf.ST_LengthSpheroid, ("point",), "point_geom", "", 0.0),
    (stf.ST_LineFromMultiPoint, ("multipoint",), "multipoint_geom", "", "LINESTRING (10 40, 40 30, 20 20, 30 10)"),
    (stf.ST_LineInterpolatePoint, ("line", 0.5), "linestring_geom", "", "POINT (2.5 0)"),
    (stf.ST_LineLocatePoint, ("line", "point"), "line_and_point", "", 0.5),
    (stf.ST_LineMerge, ("geom",), "multiline_geom", "", "LINESTRING (0 0, 1 0, 1 1, 0 0)"),
    (stf.ST_LineSubstring, ("line", 0.5, 1.0), "linestring_geom", "", "LINESTRING (2.5 0, 3 0, 4 0, 5 0)"),
    (stf.ST_LongestLine, ("geom", "geom"), "geom_collection", "", "LINESTRING (0 0, 1 0)"),
    (stf.ST_LocateAlong, ("line", 1.0), "4D_line", "ST_AsText(geom)", "MULTIPOINT ZM((1 1 1 1))"),
    (stf.ST_LocateAlong, ("line", 1.0, 2.0), "4D_line", "ST_AsText(geom)", "MULTIPOINT ZM((-0.4142135623730949 2.414213562373095 1 1), (2.414213562373095 -0.4142135623730949 1 1))"),
    (stf.ST_HasZ, ("a",), "two_points", "", True),
    (stf.ST_HasM, ("point",), "4D_point", "", True),
    (stf.ST_M, ("point",), "4D_point", "", 4.0),
    (stf.ST_MMin, ("line",), "4D_line", "", -1.0),
    (stf.ST_MMax, ("line",), "4D_line", "", 3.0),
    (stf.ST_MakeValid, ("geom",), "invalid_geom", "", "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))"),
    (stf.ST_MakeLine, ("line1", "line2"), "two_lines", "", "LINESTRING (0 0, 1 1, 0 0, 3 2)"),
    (stf.ST_MaximumInscribedCircle, ("geom",), "triangle_geom", "ST_AsText(geom.center)", "POINT (0.70703125 0.29296875)"),
    (stf.ST_MaximumInscribedCircle, ("geom",), "triangle_geom", "ST_AsText(geom.nearest)", "POINT (0.5 0.5)"),
    (stf.ST_MaximumInscribedCircle, ("geom",), "triangle_geom", "geom.radius", 0.2927864015850548),
    (stf.ST_MaxDistance, ("a", "b"), "overlapping_polys", "", 3.1622776601683795),
    (stf.ST_Points, ("line",), "linestring_geom", "ST_Normalize(geom)", "MULTIPOINT (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_Polygon, ("geom", 4236), "closed_linestring_geom", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_Polygonize, ("geom",), "noded_linework", "ST_Normalize(geom)", "GEOMETRYCOLLECTION (POLYGON ((0 2, 1 3, 2 4, 2 3, 2 2, 1 2, 0 2)), POLYGON ((2 2, 2 3, 2 4, 3 3, 4 2, 3 2, 2 2)))"),
    (stf.ST_MakePolygon, ("geom",), "closed_linestring_geom", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_MinimumClearance, ("geom",), "invalid_geom", "", 2.0),
    (stf.ST_MinimumClearanceLine, ("geom",), "invalid_geom", "", "LINESTRING (5 3, 3 3)"),
    (stf.ST_MinimumBoundingCircle, ("line", 8), "linestring_geom", "ST_ReducePrecision(geom, 2)", "POLYGON ((4.95 -0.49, 4.81 -0.96, 4.58 -1.39, 4.27 -1.77, 3.89 -2.08, 3.46 -2.31, 2.99 -2.45, 2.5 -2.5, 2.01 -2.45, 1.54 -2.31, 1.11 -2.08, 0.73 -1.77, 0.42 -1.39, 0.19 -0.96, 0.05 -0.49, 0 0, 0.05 0.49, 0.19 0.96, 0.42 1.39, 0.73 1.77, 1.11 2.08, 1.54 2.31, 2.01 2.45, 2.5 2.5, 2.99 2.45, 3.46 2.31, 3.89 2.08, 4.27 1.77, 4.58 1.39, 4.81 0.96, 4.95 0.49, 5 0, 4.95 -0.49))"),
    (stf.ST_MinimumBoundingCircle, ("line", 2), "linestring_geom", "ST_ReducePrecision(geom, 2)", "POLYGON ((4.27 -1.77, 2.5 -2.5, 0.73 -1.77, 0 0, 0.73 1.77, 2.5 2.5, 4.27 1.77, 5 0, 4.27 -1.77))"),
    (stf.ST_MinimumBoundingRadius, ("line",), "linestring_geom", "", {"center": "POINT (2.5 0)", "radius": 2.5}),
    (stf.ST_Multi, ("point",), "point_geom", "", "MULTIPOINT (0 1)"),
    (stf.ST_Normalize, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 1 1, 1 0, 0 0))"),
    (stf.ST_NPoints, ("line",), "linestring_geom", "", 6),
    (stf.ST_NRings, ("geom",), "square_geom", "", 1),
    (stf.ST_NumGeometries, ("geom",), "multipoint", "", 2),
    (stf.ST_NumInteriorRings, ("geom",), "geom_with_hole", "", 1),
    (stf.ST_NumInteriorRing, ("geom",), "geom_with_hole", "", 1),
    (stf.ST_NumPoints, ("line",), "linestring_geom", "", 6),
    (stf.ST_PointN, ("line", 2), "linestring_geom", "", "POINT (1 0)"),
    (stf.ST_PointOnSurface, ("line",), "linestring_geom", "", "POINT (2 0)"),
    (stf.ST_ReducePrecision, ("geom", 1), "precision_reduce_point", "", "POINT (0.1 0.2)"),
    (stf.ST_RemovePoint, ("line", 1), "linestring_geom", "", "LINESTRING (0 0, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_Reverse, ("line",), "linestring_geom", "", "LINESTRING (5 0, 4 0, 3 0, 2 0, 1 0, 0 0)"),
    (stf.ST_RotateX, ("line", 10.0), "4D_line", "ST_ReducePrecision(geom, 2)", "LINESTRING Z (1 -0.3 -1.383092639965822, 2 -0.59 -2.766185279931644, 3 -0.89 -4.149277919897466, -1 0.3 1.383092639965822)"),
    (stf.ST_Rotate, ("line", 10.0), "linestring_geom", "ST_ReducePrecision(geom, 2)", "LINESTRING (0 0, -0.84 -0.54, -1.68 -1.09, -2.52 -1.63, -3.36 -2.18, -4.2 -2.72)"),
    (stf.ST_Rotate, ("line", 10.0, 0.0, 0.0), "linestring_geom", "ST_ReducePrecision(geom, 2)", "LINESTRING (0 0, -0.84 -0.54, -1.68 -1.09, -2.52 -1.63, -3.36 -2.18, -4.2 -2.72)"),
    (stf.ST_S2CellIDs, ("point", 30), "point_geom", "", [1153451514845492609]),
    (stf.ST_S2ToGeom, (lambda: f.expr("array(1154047404513689600)"),), "null", "ST_ReducePrecision(geom[0], 5)", "POLYGON ((0 2.46041, 2.46041 2.46041, 2.46041 0, 0 0, 0 2.46041))"),
    (stf.ST_SetPoint, ("line", 1, lambda: f.expr("ST_Point(1.0, 1.0)")), "linestring_geom", "", "LINESTRING (0 0, 1 1, 2 0, 3 0, 4 0, 5 0)"),
    (stf.ST_SetSRID, ("point", 3021), "point_geom", "ST_SRID(geom)", 3021),
    (stf.ST_ShiftLongitude, ("geom",), "triangle_geom", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_SimplifyPreserveTopology, ("geom", 0.2), "0.9_poly", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_SimplifyVW, ("geom", 0.1), "0.9_poly", "", "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
    (stf.ST_SimplifyPolygonHull, ("geom", 0.3, False), "polygon_unsimplified", "", "POLYGON ((30 10, 40 40, 10 20, 30 10))"),
    (stf.ST_SimplifyPolygonHull, ("geom", 0.3), "polygon_unsimplified", "", "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))"),
    (stf.ST_Snap, ("poly", "line", 2.525), "poly_and_line", "" ,"POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))"),
    (stf.ST_Split, ("line", "points"), "multipoint_splitting_line", "", "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))"),
    (stf.ST_SRID, ("point",), "point_geom", "", 0),
    (stf.ST_StartPoint, ("line",), "linestring_geom", "", "POINT (0 0)"),
    (stf.ST_SubDivide, ("line", 5), "linestring_geom", "", ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]),
    (stf.ST_SubDivideExplode, ("line", 5), "linestring_geom", "collect_list(geom)", ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]),
    (stf.ST_SymDifference, ("a", "b"), "overlapping_polys", "", "MULTIPOLYGON (((1 0, 0 0, 0 1, 1 1, 1 0)), ((2 0, 2 1, 3 1, 3 0, 2 0)))"),
    (stf.ST_Transform, ("point", lambda: f.lit("EPSG:4326"), lambda: f.lit("EPSG:32649")), "point_geom", "ST_ReducePrecision(geom, 2)", "POINT (-34870890.91 1919456.06)"),
    (stf.ST_Translate, ("geom", 1.0, 1.0,), "square_geom", "", "POLYGON ((2 1, 2 2, 3 2, 3 1, 2 1))"),
    (stf.ST_TriangulatePolygon, ("geom",), "square_geom", "", "GEOMETRYCOLLECTION (POLYGON ((1 0, 1 1, 2 1, 1 0)), POLYGON ((2 1, 2 0, 1 0, 2 1)))"),
    (stf.ST_Union, ("a", "b"), "overlapping_polys", "", "POLYGON ((1 0, 0 0, 0 1, 1 1, 2 1, 3 1, 3 0, 2 0, 1 0))"),
    (stf.ST_Union, ("polys",), "array_polygons", "", "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"),
    (stf.ST_UnaryUnion, ("geom",), "overlapping_mPolys", "", "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))"),
    (stf.ST_VoronoiPolygons, ("geom",), "multipoint", "", "GEOMETRYCOLLECTION (POLYGON ((-1 -1, -1 2, 2 -1, -1 -1)), POLYGON ((-1 2, 2 2, 2 -1, -1 2)))"),
    (stf.ST_X, ("b",), "two_points", "", 3.0),
    (stf.ST_XMax, ("line",), "linestring_geom", "", 5.0),
    (stf.ST_XMin, ("line",), "linestring_geom", "", 0.0),
    (stf.ST_Y, ("b",), "two_points", "", 0.0),
    (stf.ST_YMax, ("geom",), "triangle_geom", "", 1.0),
    (stf.ST_YMin, ("geom",), "triangle_geom", "", 0.0),
    (stf.ST_Z, ("b",), "two_points", "", 4.0),
    (stf.ST_Zmflag, ("b",), "two_points", "", 2),
    (stf.ST_IsValidReason, ("geom",), "triangle_geom", "", "Valid Geometry"),
    (stf.ST_IsValidReason, ("geom", 1), "triangle_geom", "", "Valid Geometry"),

    # predicates
    (stp.ST_Contains, ("geom", lambda: f.expr("ST_Point(0.5, 0.25)")), "triangle_geom", "", True),
    (stp.ST_Crosses, ("line", "poly"), "line_crossing_poly", "", True),
    (stp.ST_Disjoint, ("a", "b"), "two_points", "", True),
    (stp.ST_Equals, ("line", lambda: f.expr("ST_Reverse(line)")), "linestring_geom", "", True),
    (stp.ST_Intersects, ("a", "b"), "overlapping_polys", "", True),
    (stp.ST_OrderingEquals, ("line", lambda: f.expr("ST_Reverse(line)")), "linestring_geom", "", False),
    (stp.ST_Overlaps, ("a", "b"), "overlapping_polys", "", True),
    (stp.ST_Touches, ("a", "b"), "touching_polys", "", True),
    (stp.ST_Relate, ("a", "b"), "touching_polys", "", "FF2F11212"),
    (stp.ST_Relate, ("a", "b", lambda: f.lit("FF2F11212")), "touching_polys", "", True),
    (stp.ST_RelateMatch, (lambda: f.lit("101202FFF"), lambda: f.lit("TTTTTTFFF")), "touching_polys", "", True),
    (stp.ST_Within, (lambda: f.expr("ST_Point(0.5, 0.25)"), "geom"), "triangle_geom", "", True),
    (stp.ST_Covers, ("geom", lambda: f.expr("ST_Point(0.5, 0.25)")), "triangle_geom", "", True),
    (stp.ST_CoveredBy, (lambda: f.expr("ST_Point(0.5, 0.25)"), "geom"), "triangle_geom", "", True),
    (stp.ST_Contains, ("geom", lambda: f.expr("ST_Point(0.0, 0.0)")), "triangle_geom", "", False),
    (stp.ST_Within, (lambda: f.expr("ST_Point(0.0, 0.0)"), "geom"), "triangle_geom", "", False),
    (stp.ST_Covers, ("geom", lambda: f.expr("ST_Point(0.0, 0.0)")), "triangle_geom", "", True),
    (stp.ST_CoveredBy, (lambda: f.expr("ST_Point(0.0, 0.0)"), "geom"), "triangle_geom", "", True),
    (stp.ST_DWithin, ("origin", "point", 5.0,), "origin_and_point", "", True),
    (stp.ST_DWithin, ("ny", "seattle", 4000000.0, True), "ny_seattle", "", True),

    # aggregates
    (sta.ST_Envelope_Aggr, ("geom",), "exploded_points", "", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
    (sta.ST_Intersection_Aggr, ("geom",), "exploded_polys", "", "LINESTRING (1 0, 1 1)"),
    (sta.ST_Union_Aggr, ("geom",), "exploded_polys", "", "POLYGON ((0 0, 0 1, 1 1, 2 1, 2 0, 1 0, 0 0))"),
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
    (stc.ST_LinestringFromWKB, (None,)),
    (stc.ST_GeomFromEWKB, (None,)),
    (stc.ST_GeomFromWKT, (None,)),
    (stc.ST_GeometryFromText, (None,)),
    (stc.ST_LineFromText, (None,)),
    (stc.ST_LineStringFromText, (None, "")),
    (stc.ST_LineStringFromText, ("", None)),
    (stc.ST_Point, (None, "")),
    (stc.ST_Point, ("", None)),
    (stc.ST_PointFromGeoHash, (None, 4)),
    (stc.ST_PointFromGeoHash, (None,)),
    (stc.ST_PointFromText, (None, "")),
    (stc.ST_PointFromText, ("", None)),
    (stc.ST_PointFromWKB, (None,)),
    (stc.ST_MPointFromText, (None,)),
    (stc.ST_PolygonFromEnvelope, (None, "", "", "")),
    (stc.ST_PolygonFromEnvelope, ("", None, "", "")),
    (stc.ST_PolygonFromEnvelope, ("", "", None, "")),
    (stc.ST_PolygonFromEnvelope, ("", "", "", None)),
    (stc.ST_PolygonFromText, (None, "")),
    (stc.ST_PolygonFromText, ("", None)),
    (stc.ST_PolygonFromText, (None, None)),
    (stc.ST_PolygonFromText, ("", None)),
    (stc.ST_MakePointM, (None, None, None)),
    (stc.ST_MakePointM, (None, "", "")),

    # functions
    (stf.ST_3DDistance, (None, "")),
    (stf.ST_3DDistance, ("", None)),
    (stf.ST_AddMeasure, (None, None, None)),
    (stf.ST_AddMeasure, ("", None, "")),
    (stf.ST_AddPoint, (None, "")),
    (stf.ST_AddPoint, ("", None)),
    (stf.ST_Area, (None,)),
    (stf.ST_AsBinary, (None,)),
    (stf.ST_AsEWKB, (None,)),
    (stf.ST_AsHEXEWKB, (None,)),
    (stf.ST_AsEWKT, (None,)),
    (stf.ST_AsGeoJSON, (None,)),
    (stf.ST_AsGML, (None,)),
    (stf.ST_AsKML, (None,)),
    (stf.ST_AsText, (None,)),
    (stf.ST_Azimuth, (None, "")),
    (stf.ST_Azimuth, ("", None)),
    (stf.ST_BestSRID, (None,)),
    (stf.ST_Boundary, (None,)),
    (stf.ST_Buffer, (None, 1.0)),
    (stf.ST_Buffer, ("", None)),
    (stf.ST_BuildArea, (None,)),
    (stf.ST_Centroid, (None,)),
    (stf.ST_Collect, (None,)),
    (stf.ST_CollectionExtract, (None,)),
    (stf.ST_ConcaveHull, (None, 1.0)),
    (stf.ST_ConvexHull, (None,)),
    (stf.ST_CrossesDateLine, (None,)),
    (stf.ST_Difference, (None, "b")),
    (stf.ST_Difference, ("", None)),
    (stf.ST_Distance, (None, "")),
    (stf.ST_Distance, ("", None)),
    (stf.ST_Dump, (None,)),
    (stf.ST_DumpPoints, (None,)),
    (stf.ST_DelaunayTriangles, (None,)),
    (stf.ST_EndPoint, (None,)),
    (stf.ST_Envelope, (None,)),
    (stf.ST_Expand, (None,"")),
    (stf.ST_Expand, (None,None)),
    (stf.ST_Expand, ("",None)),
    (stf.ST_ExteriorRing, (None,)),
    (stf.ST_FlipCoordinates, (None,)),
    (stf.ST_Force_2D, (None,)),
    (stf.ST_Force3DM, (None,)),
    (stf.ST_Force3DZ, (None,)),
    (stf.ST_Force4D, (None,)),
    (stf.ST_ForceCollection, (None,)),
    (stf.ST_ForcePolygonCW, (None,)),
    (stf.ST_ForcePolygonCCW, (None,)),
    (stf.ST_ForceRHR, (None,)),
    (stf.ST_GeometryN, (None, 0)),
    (stf.ST_GeometryN, ("", None)),
    (stf.ST_GeometryN, ("", 0.0)),
    (stf.ST_GeometryType, (None,)),
    (stf.ST_GeneratePoints, (None, 0.0)),
    (stf.ST_GeneratePoints, ("", None)),
    (stf.ST_InteriorRingN, (None, 0)),
    (stf.ST_InteriorRingN, ("", None)),
    (stf.ST_InteriorRingN, ("", 0.0)),
    (stf.ST_Intersection, (None, "")),
    (stf.ST_Intersection, ("", None)),
    (stf.ST_IsClosed, (None,)),
    (stf.ST_IsEmpty, (None,)),
    (stf.ST_IsPolygonCW, (None,)),
    (stf.ST_IsPolygonCCW, (None,)),
    (stf.ST_IsRing, (None,)),
    (stf.ST_IsSimple, (None,)),
    (stf.ST_IsValidTrajectory, (None,)),
    (stf.ST_IsValidDetail, (None,)),
    (stf.ST_IsValid, (None,)),
    (stf.ST_IsValidReason, (None,)),
    (stf.ST_Length, (None,)),
    (stf.ST_Length2D, (None,)),
    (stf.ST_LineFromMultiPoint, (None,)),
    (stf.ST_LineInterpolatePoint, (None, 0.5)),
    (stf.ST_LineInterpolatePoint, ("", None)),
    (stf.ST_LineLocatePoint, (None, "")),
    (stf.ST_LineLocatePoint, ("", None)),
    (stf.ST_LineMerge, (None,)),
    (stf.ST_LineSubstring, (None, 0.5, 1.0)),
    (stf.ST_LineSubstring, ("", None, 1.0)),
    (stf.ST_LineSubstring, ("", 0.5, None)),
    (stf.ST_LongestLine, (None, "")),
    (stf.ST_LongestLine, (None, None)),
    (stf.ST_LongestLine, ("", None)),
    (stf.ST_LocateAlong, (None, "")),
    (stf.ST_LocateAlong, (None, None)),
    (stf.ST_LocateAlong, ("", None)),
    (stf.ST_HasZ, (None,)),
    (stf.ST_HasM, (None,)),
    (stf.ST_M, (None,)),
    (stf.ST_MMin, (None,)),
    (stf.ST_MMax, (None,)),
    (stf.ST_MakeValid, (None,)),
    (stf.ST_MakePolygon, (None,)),
    (stf.ST_MaximumInscribedCircle, (None,)),
    (stf.ST_MaxDistance, (None, None)),
    (stf.ST_MaxDistance, (None, "")),
    (stf.ST_MaxDistance, ("", None)),
    (stf.ST_MinimumClearance, (None,)),
    (stf.ST_MinimumClearanceLine, (None,)),
    (stf.ST_MinimumBoundingCircle, (None,)),
    (stf.ST_MinimumBoundingRadius, (None,)),
    (stf.ST_Multi, (None,)),
    (stf.ST_Normalize, (None,)),
    (stf.ST_NPoints, (None,)),
    (stf.ST_NumGeometries, (None,)),
    (stf.ST_NumInteriorRings, (None,)),
    (stf.ST_NumInteriorRing, (None,)),
    (stf.ST_PointN, (None, 2)),
    (stf.ST_PointN, ("", None)),
    (stf.ST_PointN, ("", 2.0)),
    (stf.ST_PointOnSurface, (None,)),
    (stf.ST_ReducePrecision, (None, 1)),
    (stf.ST_ReducePrecision, ("", None)),
    (stf.ST_ReducePrecision, ("", 1.0)),
    (stf.ST_RemovePoint, (None, 1)),
    (stf.ST_RemovePoint, ("", None)),
    (stf.ST_RemovePoint, ("", 1.0)),
    (stf.ST_Reverse, (None,)),
    (stf.ST_Rotate, (None,None,)),
    (stf.ST_Rotate, (None,None)),
    (stf.ST_S2CellIDs, (None, 2)),
    (stf.ST_S2ToGeom, (None,)),
    (stf.ST_SetPoint, (None, 1, "")),
    (stf.ST_SetPoint, ("", None, "")),
    (stf.ST_SetPoint, ("", 1, None)),
    (stf.ST_SetSRID, (None, 3021)),
    (stf.ST_SetSRID, ("", None)),
    (stf.ST_SetSRID, ("", 3021.0)),
    (stf.ST_ShiftLongitude, (None,)),
    (stf.ST_SimplifyPreserveTopology, (None, 0.2)),
    (stf.ST_SimplifyPreserveTopology, ("", None)),
    (stf.ST_SimplifyVW, (None, 2)),
    (stf.ST_SimplifyVW, ("", None)),
    (stf.ST_SimplifyPolygonHull, ("", None)),
    (stf.ST_SimplifyPolygonHull, (None, None)),
    (stf.ST_Snap, (None, None, 12)),
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
    (stf.ST_TriangulatePolygon, (None,)),
    (stf.ST_Union, (None, "")),
    (stf.ST_Union, (None,)),
    (stf.ST_UnaryUnion, (None,)),
    (stf.ST_X, (None,)),
    (stf.ST_XMax, (None,)),
    (stf.ST_XMin, (None,)),
    (stf.ST_Y, (None,)),
    (stf.ST_YMax, (None,)),
    (stf.ST_YMin, (None,)),
    (stf.ST_Z, (None,)),
    (stf.ST_Zmflag, (None,)),

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
    (stp.ST_Relate, (None, "")),
    (stp.ST_Relate, ("", None)),
    (stp.ST_RelateMatch, (None, "")),
    (stp.ST_RelateMatch, ("", None)),
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
        wkbLine = '0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf'
        wkbPoint = '010100000000000000000024400000000000002e40'
        wkb = '0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf'
        mpoly = 'MULTIPOLYGON(((0 0 ,20 0 ,20 20 ,0 20 ,0 0 ),(5 5 ,5 7 ,7 7 ,7 5 ,5 5)))'
        mline = 'MULTILINESTRING((1 2, 3 4), (4 5, 6 7))'
        mpoint = 'MULTIPOINT ((10 10), (20 20), (30 30))'
        geojson = "{ \"type\": \"Feature\", \"properties\": { \"prop\": \"01\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 0.0, 1.0 ] }},"
        gml_string = "<gml:LineString srsName=\"EPSG:4269\"><gml:coordinates>-71.16,42.25 -71.17,42.25 -71.18,42.25</gml:coordinates></gml:LineString>"
        kml_string = "<LineString><coordinates>-71.16,42.26 -71.17,42.26</coordinates></LineString>"
        wktCollection = 'GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1)))'

        if request.param == "constructor":
            return TestDataFrameAPI.spark.sql("SELECT null").selectExpr(
                "0.0 AS x",
                "1.0 AS y",
                "2.0 AS z",
                "'0.0,1.0' AS single_point",
                "'0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0' AS multiple_point",
                f"X'{wkbLine}' AS wkbLine",
                f"X'{wkbPoint}' AS wkbPoint",
                f"X'{wkb}' AS wkb",
                f"'{mpoly}' AS mpoly",
                f"'{mline}' AS mline",
                f"'{mpoint}' AS mpoint",
                f"'{geojson}' AS geojson",
                "'s00twy01mt' AS geohash",
                f"'{gml_string}' AS gml",
                f"'{kml_string}' AS kml",
                f"'{wktCollection}' AS collection"
            )
        elif request.param == "point_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_Point(0.0, 1.0) AS point")
        elif request.param == "linestring_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS line")
        elif request.param == "linestring_wkt":
            return TestDataFrameAPI.spark.sql("SELECT 'LINESTRING (1 2, 3 4)' AS wkt")
        elif request.param == "linestring_ewkt":
            return TestDataFrameAPI.spark.sql("SELECT 'SRID=4269;LINESTRING (1 2, 3 4)' AS ewkt")
        elif request.param == "min_max_x_y":
            return TestDataFrameAPI.spark.sql("SELECT 0.0 AS minx, 1.0 AS miny, 2.0 AS maxx, 3.0 AS maxy")
        elif request.param == "x_y_z_m_srid":
            return TestDataFrameAPI.spark.sql("SELECT 1.0 AS x, 2.0 AS y, 3.0 AS z, 100.9 AS m, 4326 AS srid")
        elif request.param == "multipoint_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))') AS multipoint")
        elif request.param == "null":
            return TestDataFrameAPI.spark.sql("SELECT null")
        elif request.param == "triangle_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
        elif request.param == "two_points":
            return TestDataFrameAPI.spark.sql("SELECT ST_PointZ(0.0, 0.0, 0.0) AS a, ST_PointZ(3.0, 0.0, 4.0) AS b")
        elif request.param == "4D_point":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT ZM(1 2 3 4)') AS point")
        elif request.param == "4D_line":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') AS line")
        elif request.param == "invalid_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS geom")
        elif request.param == "overlapping_polys":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON((1 0, 3 0, 3 1, 1 1, 1 0))') AS b")
        elif request.param == "overlapping_mPolys":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))') AS geom")
        elif request.param == "multipoint":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
        elif request.param == "geom_with_hole":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
        elif request.param == "0.9_poly":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom")
        elif request.param == "polygon_unsimplified":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))') AS geom")
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
        elif request.param == "square_geom":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom")
        elif request.param == "four_points":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT (0 0)') AS p1, ST_GeomFromWKT('POINT (1 1)') AS p2, ST_GeomFromWKT('POINT (1 0)') AS p3, ST_GeomFromWKT('POINT (6 2)') AS p4")
        elif request.param == "three_points":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT (1 1)') AS p1, ST_GeomFromWKT('POINT (0 0)') AS p2, ST_GeomFromWKT('POINT (3 2)') AS p3")
        elif request.param == "two_lines":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS line1, ST_GeomFromWKT('LINESTRING (0 0, 3 2)') AS line2")
        elif request.param == "two_lines_angle_rad":
            return TestDataFrameAPI.spark.sql("SELECT ST_Angle(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('LINESTRING (0 0, 3 2)')) AS angleRad")
        elif request.param == "geometry_geom_collection":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1, 2 2))') AS geom")
        elif request.param == "point_and_line":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT (0.0 1.0)') AS point, ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS line")
        elif request.param == "line_and_point":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 2, 1 1, 2 0)') AS line, ST_GeomFromWKT('POINT (0 0)') AS point")
        elif request.param == "multipoint_splitting_line":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)') AS line, ST_GeomFromWKT('MULTIPOINT (0.5 0.5, 1 1)') AS points")
        elif request.param == "origin_and_point":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT (0 0)') AS origin, ST_GeomFromWKT('POINT (1 0)') as point")
        elif request.param == "ny_seattle":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POINT (-122.335167 47.608013)') AS seattle, ST_GeomFromWKT('POINT (-73.935242 40.730610)') as ny")
        elif request.param == "line_crossing_dateline":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (179.95 30, -179.95 30)') AS line")
        elif request.param == "array_polygons":
            return TestDataFrameAPI.spark.sql("SELECT array(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')) as polys")
        elif request.param == "poly_and_line":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))') as poly, ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)') as line")
        elif request.param == "noded_linework":
            return TestDataFrameAPI.spark.sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2), LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2), LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4), LINESTRING (2 4, 3 3, 4 2))') as geom")
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
