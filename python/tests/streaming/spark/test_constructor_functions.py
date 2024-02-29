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

import os
import uuid
from typing import List, Any, Optional

import pytest
from pyspark.sql.types import StructType, StructField, Row
from shapely import wkt, wkb

from sedona.sql.types import GeometryType
from tests import tests_resource
from tests.streaming.spark.cases_builder import SuiteContainer
from tests.test_base import TestBase
import math

SCHEMA = StructType(
    [
        StructField("geom", GeometryType())
    ]
)


SEDONA_LISTED_SQL_FUNCTIONS = [
    (SuiteContainer.empty()
     .with_function_name("ST_AsText")
     .with_arguments(["ST_GeomFromText('POINT (21 52)')"])
     .with_expected_result("POINT (21 52)")),
    (SuiteContainer.empty()
     .with_function_name("ST_Buffer")
     .with_arguments(["ST_GeomFromText('POINT (21 52)')", "1.0"])
     .with_expected_result(3.1214451522580533)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_Buffer")
     .with_arguments(["ST_GeomFromText('POINT (21 52)')", "100000", "true"])
     .with_expected_result(4.088135158017784)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_Distance")
     .with_arguments(["ST_GeomFromText('POINT (21 52)')", "ST_GeomFromText('POINT (21 53)')"])
     .with_expected_result(1.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_ConcaveHull")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')", "1.0"])
     .with_expected_result(1.0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_ConvexHull")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')"])
     .with_expected_result(1.0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_Envelope")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')"])
     .with_expected_result(1.0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_LENGTH")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')"])
     .with_expected_result(4.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_Area")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')"])
     .with_expected_result(1.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_Centroid")
     .with_arguments(["ST_GeomFromText('POINT(21.5 52.5)')"])
     .with_expected_result("POINT (21.5 52.5)")
     .with_transform("ST_ASText")),
    (SuiteContainer.empty()
     .with_function_name("ST_Transform")
     .with_arguments(["ST_GeomFromText('POINT(52.5 21.5)')", "'epsg:4326'", "'epsg:2180'"])
     .with_expected_result(-2501415.806893427)
     #.with_expected_result("POINT (-2501415.806893427 4119952.52325666)")
     .with_transform("ST_Y")),
     #.with_transform("ST_ASText")),
    (SuiteContainer.empty()
     .with_function_name("ST_Intersection")
     .with_arguments(["ST_GeomFromText('POINT(21.5 52.5)')", "ST_GeomFromText('POINT(21.5 52.5)')"])
     .with_expected_result(0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_IsValid")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result(True)),
    (SuiteContainer.empty()
     .with_function_name("ST_MakeValid")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')", "false"])
     .with_expected_result(1.0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_ReducePrecision")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')", "9"])
     .with_expected_result(1.0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_IsSimple")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result(True)),
    (SuiteContainer.empty()
     .with_function_name("ST_Buffer")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')", "0.9"])
     .with_expected_result(7.128370573329018)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_AsText")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result("POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))")),
    (SuiteContainer.empty()
        .with_function_name("ST_AsGeoJSON")
        .with_arguments(["ST_GeomFromText('POLYGON ((21 52, 21 53, 22 53, 22 52, 21 52))')"])
        .with_expected_result(
        """{"type":"Polygon","coordinates":[[[21.0,52.0],[21.0,53.0],[22.0,53.0],[22.0,52.0],[21.0,52.0]]]}""")),
    (SuiteContainer.empty()
     .with_function_name("ST_AsBinary")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')"])
     .with_expected_result(wkb.dumps(wkt.loads("POINT(21 52)")))),
    (SuiteContainer.empty()
     .with_function_name("ST_AsEWKB")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')"])
     .with_expected_result(wkb.dumps(wkt.loads("POINT(21 52)")))),
    (SuiteContainer.empty()
     .with_function_name("ST_SRID")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')"])
     .with_expected_result(0)),
    (SuiteContainer.empty()
     .with_function_name("ST_SetSRID")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')", "4326"])
     .with_expected_result(0)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_NPoints")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result(5)),
    (SuiteContainer.empty()
     .with_function_name("ST_SimplifyPreserveTopology")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')", "1.0"])
     .with_expected_result(1)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_GeometryType")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result("ST_Polygon")),
    (SuiteContainer.empty()
     .with_function_name("ST_LineMerge")
     .with_arguments(["ST_GeomFromText('LINESTRING(-29 -27,-30 -29.7,-36 -31,-45 -33,-46 -32)')"])
     .with_expected_result(0.0)
     .with_transform("ST_LENGTH")),
    (SuiteContainer.empty()
     .with_function_name("ST_Azimuth")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')", "ST_GeomFromText('POINT(21 53)')"])
     .with_expected_result(0.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_X")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')"])
     .with_expected_result(21.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_Y")
     .with_arguments(["ST_GeomFromText('POINT(21 52)')"])
     .with_expected_result(52.0)),
    (SuiteContainer.empty()
     .with_function_name("ST_StartPoint")
     .with_arguments(["ST_GeomFromText('LINESTRING(100 150,50 60, 70 80, 160 170)')"])
     .with_expected_result("POINT (100 150)")
     .with_transform("ST_ASText")),
    (SuiteContainer.empty()
     .with_function_name("ST_Endpoint")
     .with_arguments(["ST_GeomFromText('LINESTRING(100 150,50 60, 70 80, 160 170)')"])
     .with_expected_result("POINT (160 170)")
     .with_transform("ST_ASText")),
    (SuiteContainer.empty()
     .with_function_name("ST_Boundary")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result(4)
     .with_transform("ST_LENGTH")),
    (SuiteContainer.empty()
     .with_function_name("ST_ExteriorRing")
     .with_arguments(["ST_GeomFromText('POLYGON ((21 53, 22 53, 22 52, 21 52, 21 53))')"])
     .with_expected_result(4)
     .with_transform("ST_LENGTH")),
    (SuiteContainer.empty()
     .with_function_name("ST_GeometryN")
     .with_arguments(["ST_GeomFromText('MULTIPOINT((1 2), (3 4), (5 6), (8 9))')", "0"])
     .with_expected_result(1)
     .with_transform("ST_X")),
    (SuiteContainer.empty()
     .with_function_name("ST_InteriorRingN")
     .with_arguments([
        "ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))')",
        "0"])
     .with_expected_result(4.0)
     .with_transform("ST_LENGTH")),
    (SuiteContainer.empty()
     .with_function_name("ST_Dump")
     .with_arguments([
        "ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')"])
     .with_expected_result(4)
     .with_transform("SIZE")),
    (SuiteContainer.empty()
     .with_function_name("ST_DumpPoints")
     .with_arguments([
        "ST_GeomFromTEXT('LINESTRING (0 0, 1 1, 1 0)')"])
     .with_expected_result(3)
     .with_transform("SIZE")),
    (SuiteContainer.empty()
     .with_function_name("ST_IsClosed")
     .with_arguments([
        "ST_GeomFROMTEXT('LINESTRING(0 0, 1 1, 1 0)')"])
     .with_expected_result(False)),
    (SuiteContainer.empty()
     .with_function_name("ST_NumInteriorRings")
     .with_arguments([
        "ST_GeomFROMTEXT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))')"])
     .with_expected_result(1)),
    (SuiteContainer.empty()
     .with_function_name("ST_AddPoint")
     .with_arguments([
        "ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)')",
        "ST_GeomFromText('Point(21 52)')",
        "1"])
     .with_expected_result(111.86168327044916)
     .with_transform("ST_Length")),
    (SuiteContainer.empty()
     .with_function_name("ST_RemovePoint")
     .with_arguments([
        "ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)')",
        "1"
    ])
     .with_expected_result("LINESTRING (0 0, 1 0)")
     .with_transform("ST_AsText")),
    (SuiteContainer.empty()
     .with_function_name("ST_IsRing")
     .with_arguments([
        "ST_GeomFromText('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)')"
    ])
     .with_expected_result(True)),
    (SuiteContainer.empty()
     .with_function_name("ST_NumGeometries")
     .with_arguments([
        "ST_GeomFromText('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)')"
    ])
     .with_expected_result(1)),
    (SuiteContainer.empty()
     .with_function_name("ST_FlipCoordinates")
     .with_arguments([
        "ST_GeomFromText('POINT(21 52)')"
    ])
     .with_expected_result(52.0)
     .with_transform("ST_X")),
    (SuiteContainer.empty()
     .with_function_name("ST_MinimumBoundingRadius")
     .with_arguments([
        "ST_GeomFromText('POLYGON((1 1,0 0, -1 1, 1 1))')"
    ])
     .with_expected_result(Row(center=wkt.loads("POINT(0 1)"), radius=1.0))),
    (SuiteContainer.empty()
     .with_function_name("ST_MinimumBoundingCircle")
     .with_arguments([
        "ST_GeomFromText('POLYGON((1 1,0 0, -1 1, 1 1))')",
        "8"])
     .with_expected_result(3.121445152258052)
     .with_transform("ST_AREA")),
    (SuiteContainer.empty()
     .with_function_name("ST_SubDivide")
     .with_arguments([
        "ST_GeomFromText('POLYGON((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))')",
        "5"])
     .with_expected_result(14)
     .with_transform("SIZE")),
    (SuiteContainer.empty()
     .with_function_name("ST_SubDivideExplode")
     .with_arguments([
        "ST_GeomFromText('LINESTRING(0 0, 85 85, 100 100, 120 120, 21 21, 10 10, 5 5)')",
        "5"])
     .with_expected_result(2)
     .with_transform("ST_NPoints")),
    (SuiteContainer.empty()
     .with_function_name("ST_GeoHash")
     .with_arguments([
        "ST_GeomFromText('POINT(21.427834 52.042576573)')",
        "5"])
     .with_expected_result("u3r0p")),
    (SuiteContainer.empty()
     .with_function_name("ST_Collect")
     .with_arguments([
        "ST_GeomFromText('POINT(21.427834 52.042576573)')",
        "ST_GeomFromText('POINT(45.342524 56.342354355)')"])
     .with_expected_result(0.0)
     .with_transform("ST_LENGTH")),
    (SuiteContainer.empty()
     .with_function_name("ST_BestSRID")
     .with_arguments(["ST_GeomFromText('POINT (-177 60)')"])
     .with_expected_result(32601)),
    (SuiteContainer.empty()
     .with_function_name("ST_ShiftLongitude")
     .with_arguments(["ST_GeomFromText('POINT (-177 60)')"])
     .with_expected_result("POINT (183 60)")
     .with_transform("ST_AsText"))
]


def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


class TestConstructorFunctions(TestBase):
    params = {
        "test_geospatial_function_on_stream": SEDONA_LISTED_SQL_FUNCTIONS
    }

    @pytest.mark.sparkstreaming
    def test_geospatial_function_on_stream(self, function_name: str, arguments: List[str],
                                           expected_result: Any, transform: Optional[str]):
      # given input stream

      input_stream = self.spark.readStream.schema(SCHEMA).parquet(os.path.join(
         tests_resource,
         "streaming/geometry_example")
      ).selectExpr(f"{function_name}({', '.join(arguments)}) AS result")

      # and target table
      random_table_name = f"view_{uuid.uuid4().hex}"

      # when saving stream to memory
      streaming_query = input_stream.writeStream.format("memory") \
         .queryName(random_table_name) \
         .outputMode("append").start()

      streaming_query.processAllAvailable()

      # then result should be as expected
      transform_query = "result" if not transform else f"{transform}(result)"
      queryResult = self.spark.sql(f"select {transform_query} from {random_table_name}").collect()[0][0]
      if (type(queryResult) is float and type(expected_result) is float):
         assert math.isclose(queryResult, expected_result, rel_tol=1e-9)
      else:
         assert queryResult == expected_result
