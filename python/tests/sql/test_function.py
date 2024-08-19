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

import math
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, expr
from pyspark.sql.types import StructType, StructField, IntegerType
from sedona.sql.types import GeometryType
from shapely import wkt
from shapely.wkt import loads
from tests import mixed_wkt_geometry_input_location
from tests.sql.resource.sample_data import create_sample_points, create_simple_polygons_df, \
    create_sample_points_df, create_sample_polygons_df, create_sample_lines_df
from tests.test_base import TestBase
from typing import List


class TestPredicateJoin(TestBase):
    geo_schema = StructType(
        [StructField("geom", GeometryType(), False)]
    )

    geo_schema_with_index = StructType(
        [
            StructField("index", IntegerType(), False),
            StructField("geom", GeometryType(), False)
        ]
    )

    geo_pair_schema = StructType(
        [
            StructField("geomA", GeometryType(), False),
            StructField("geomB", GeometryType(), False)
        ]
    )

    def test_st_concave_hull(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_ConcaveHull(polygondf.countyshape, 1.0) from polygondf")
        function_df.show()

    def test_st_convex_hull(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_buffer(self):
        polygon_from_wkt = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polygontable")
        polygon_from_wkt.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_ReducePrecision(ST_Buffer(polygondf.countyshape, 1), 2) from polygondf")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POLYGON ((-98.02 41.77, -98.02 41.78, -98.02 41.8, -98.02 41.81, -98.02 41.82, -98.02 41.83, -98.02 41.84, -98.02 41.85, -98.02 41.86, -98.02 41.87, -98.02 41.89, -98.02 41.9, -98.02 41.91, -98.02 41.92, -98.02 41.93, -98.02 41.95, -98.02 41.98, -98.02 42, -98.02 42.01, -98.02 42.02, -98.02 42.04, -98.02 42.05, -98.02 42.07, -98.02 42.09, -98.02 42.11, -98.02 42.12, -97.99 42.31, -97.93 42.5, -97.84 42.66, -97.72 42.81, -97.57 42.93, -97.4 43.02, -97.21 43.07, -97.02 43.09, -97 43.09, -96.98 43.09, -96.97 43.09, -96.96 43.09, -96.95 43.09, -96.94 43.09, -96.92 43.09, -96.91 43.09, -96.89 43.09, -96.88 43.09, -96.86 43.09, -96.84 43.09, -96.83 43.09, -96.82 43.09, -96.81 43.09, -96.8 43.09, -96.79 43.09, -96.78 43.09, -96.76 43.09, -96.74 43.09, -96.73 43.09, -96.71 43.09, -96.7 43.09, -96.69 43.09, -96.68 43.09, -96.66 43.09, -96.65 43.09, -96.64 43.09, -96.63 43.09, -96.62 43.09, -96.61 43.09, -96.6 43.09, -96.59 43.09, -96.58 43.09, -96.57 43.09, -96.56 43.09, -96.55 43.09, -96.36 43.07, -96.17 43.01, -96 42.92, -95.86 42.8, -95.73 42.66, -95.64 42.49, -95.58 42.31, -95.56 42.12, -95.56 42.1, -95.56 42.09, -95.56 42.08, -95.56 42.07, -95.56 42.06, -95.56 42.04, -95.56 42, -95.56 41.99, -95.56 41.98, -95.56 41.97, -95.56 41.96, -95.56 41.95, -95.56 41.94, -95.56 41.93, -95.56 41.92, -95.56 41.91, -95.56 41.9, -95.56 41.89, -95.56 41.88, -95.56 41.87, -95.56 41.86, -95.56 41.85, -95.56 41.83, -95.56 41.82, -95.56 41.81, -95.56 41.8, -95.56 41.79, -95.56 41.78, -95.56 41.77, -95.56 41.76, -95.56 41.75, -95.56 41.74, -95.58 41.54, -95.63 41.36, -95.72 41.19, -95.85 41.03, -96 40.91, -96.17 40.82, -96.36 40.76, -96.55 40.74, -96.56 40.74, -96.57 40.74, -96.58 40.74, -96.59 40.74, -96.6 40.74, -96.62 40.74, -96.63 40.74, -96.64 40.74, -96.65 40.74, -96.67 40.74, -96.68 40.74, -96.69 40.74, -96.7 40.74, -96.71 40.74, -96.72 40.74, -96.73 40.74, -96.74 40.74, -96.75 40.74, -96.76 40.74, -96.77 40.74, -96.78 40.74, -96.79 40.74, -96.8 40.74, -96.81 40.74, -96.82 40.74, -96.83 40.74, -96.85 40.74, -96.86 40.74, -96.88 40.74, -96.9 40.74, -96.91 40.74, -96.92 40.74, -96.93 40.74, -96.94 40.74, -96.95 40.74, -96.97 40.74, -96.98 40.74, -96.99 40.74, -97.01 40.74, -97.02 40.74, -97.22 40.76, -97.4 40.82, -97.57 40.91, -97.72 41.03, -97.85 41.18, -97.94 41.35, -98 41.54, -98.02 41.73, -98.02 41.75, -98.02 41.76, -98.02 41.77))"

        function_df = self.spark.sql("select ST_ReducePrecision(ST_Buffer(polygondf.countyshape, 1, true), 2) from polygondf")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POLYGON ((-97.02 42.01, -97.02 42.02, -97.02 42.03, -97.02 42.04, -97.02 42.05, -97.02 42.06, -97.02 42.07, -97.02 42.08, -97.02 42.09, -97.01 42.09, -97 42.09, -96.99 42.09, -96.98 42.09, -96.97 42.09, -96.96 42.09, -96.95 42.09, -96.94 42.09, -96.93 42.09, -96.92 42.09, -96.91 42.09, -96.9 42.09, -96.89 42.09, -96.88 42.09, -96.87 42.09, -96.86 42.09, -96.85 42.09, -96.84 42.09, -96.83 42.09, -96.82 42.09, -96.81 42.09, -96.8 42.09, -96.79 42.09, -96.78 42.09, -96.77 42.09, -96.76 42.09, -96.75 42.09, -96.74 42.09, -96.73 42.09, -96.72 42.09, -96.71 42.09, -96.7 42.09, -96.69 42.09, -96.68 42.09, -96.67 42.09, -96.66 42.09, -96.65 42.09, -96.64 42.09, -96.63 42.09, -96.62 42.09, -96.61 42.09, -96.6 42.09, -96.59 42.09, -96.58 42.09, -96.57 42.09, -96.56 42.09, -96.56 42.08, -96.56 42.07, -96.56 42.06, -96.56 42.05, -96.56 42.04, -96.56 42.03, -96.56 42.02, -96.55 42.02, -96.56 42, -96.56 41.99, -96.56 41.98, -96.56 41.97, -96.56 41.96, -96.56 41.95, -96.56 41.94, -96.56 41.93, -96.56 41.92, -96.56 41.91, -96.56 41.9, -96.56 41.89, -96.56 41.88, -96.56 41.87, -96.56 41.86, -96.56 41.85, -96.56 41.84, -96.56 41.83, -96.56 41.82, -96.56 41.81, -96.56 41.8, -96.56 41.79, -96.56 41.78, -96.56 41.77, -96.56 41.76, -96.56 41.75, -96.56 41.74, -96.57 41.74, -96.58 41.74, -96.59 41.74, -96.6 41.74, -96.61 41.74, -96.62 41.74, -96.63 41.74, -96.64 41.74, -96.65 41.74, -96.66 41.74, -96.67 41.74, -96.68 41.74, -96.69 41.74, -96.7 41.74, -96.71 41.74, -96.72 41.74, -96.73 41.74, -96.74 41.74, -96.75 41.74, -96.76 41.74, -96.77 41.74, -96.78 41.74, -96.79 41.74, -96.8 41.74, -96.81 41.74, -96.82 41.74, -96.83 41.74, -96.84 41.74, -96.85 41.74, -96.86 41.74, -96.87 41.74, -96.88 41.74, -96.89 41.74, -96.9 41.74, -96.91 41.74, -96.92 41.74, -96.93 41.74, -96.94 41.74, -96.95 41.74, -96.96 41.74, -96.97 41.74, -96.98 41.74, -96.99 41.74, -97 41.74, -97.01 41.74, -97.02 41.74, -97.02 41.75, -97.02 41.76, -97.02 41.77, -97.02 41.78, -97.02 41.79, -97.02 41.8, -97.02 41.81, -97.02 41.82, -97.02 41.83, -97.02 41.84, -97.02 41.85, -97.02 41.86, -97.02 41.87, -97.02 41.88, -97.02 41.89, -97.02 41.9, -97.02 41.91, -97.02 41.92, -97.02 41.93, -97.02 41.94, -97.02 41.95, -97.02 41.96, -97.02 41.97, -97.02 41.98, -97.02 41.99, -97.02 42, -97.02 42.01))"

        function_df = self.spark.sql("select ST_ReducePrecision(ST_Buffer(polygondf.countyshape, 10, false, 'endcap=square'), 2) from polygondf")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POLYGON ((-107.02 42.06, -107.02 42.07, -107.02 42.09, -107.02 42.11, -107.02 42.32, -107.02 42.33, -107.01 42.42, -107.01 42.43, -106.77 44.33, -106.16 46.15, -105.22 47.82, -103.98 49.27, -102.48 50.47, -100.78 51.36, -98.94 51.9, -97.04 52.09, -97.03 52.09, -97.01 52.09, -96.95 52.09, -96.9 52.09, -96.81 52.09, -96.7 52.09, -96.68 52.09, -96.65 52.09, -96.55 52.09, -96.54 52.09, -96.49 52.09, -96.48 52.09, -94.58 51.89, -92.74 51.33, -91.04 50.43, -89.55 49.23, -88.32 47.76, -87.39 46.08, -86.79 44.25, -86.56 42.35, -86.56 42.18, -86.56 42.17, -86.56 42.1, -86.55 41.99, -86.56 41.9, -86.56 41.78, -86.56 41.77, -86.56 41.75, -86.56 41.73, -86.56 41.7, -86.75 39.76, -87.33 37.89, -88.25 36.17, -89.49 34.66, -91 33.43, -92.72 32.51, -94.59 31.94, -96.53 31.74, -96.55 31.74, -96.56 31.74, -96.57 31.74, -96.58 31.74, -96.6 31.74, -96.69 31.74, -96.72 31.74, -96.75 31.74, -96.94 31.74, -97.02 31.74, -97.04 31.74, -97.06 31.74, -98.99 31.94, -100.85 32.5, -102.56 33.42, -104.07 34.65, -105.31 36.14, -106.23 37.85, -106.81 39.71, -107.02 41.64, -107.02 41.75, -107.02 41.94, -107.02 41.96, -107.02 41.99, -107.02 42.01, -107.02 42.02, -107.02 42.03, -107.02 42.06))"

        function_df = self.spark.sql("select ST_ReducePrecision(ST_Buffer(polygondf.countyshape, 10, true, 'endcap=square'), 2) from polygondf")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POLYGON ((-97.02 42.01, -97.02 42.02, -97.02 42.03, -97.02 42.04, -97.02 42.05, -97.02 42.06, -97.02 42.07, -97.02 42.08, -97.02 42.09, -97.01 42.09, -97 42.09, -96.99 42.09, -96.98 42.09, -96.97 42.09, -96.96 42.09, -96.95 42.09, -96.94 42.09, -96.93 42.09, -96.92 42.09, -96.91 42.09, -96.9 42.09, -96.89 42.09, -96.88 42.09, -96.87 42.09, -96.86 42.09, -96.85 42.09, -96.84 42.09, -96.83 42.09, -96.82 42.09, -96.81 42.09, -96.8 42.09, -96.79 42.09, -96.78 42.09, -96.77 42.09, -96.76 42.09, -96.75 42.09, -96.74 42.09, -96.73 42.09, -96.72 42.09, -96.71 42.09, -96.7 42.09, -96.69 42.09, -96.68 42.09, -96.67 42.09, -96.66 42.09, -96.65 42.09, -96.64 42.09, -96.63 42.09, -96.62 42.09, -96.61 42.09, -96.6 42.09, -96.59 42.09, -96.58 42.09, -96.57 42.09, -96.56 42.09, -96.56 42.08, -96.56 42.07, -96.56 42.06, -96.56 42.05, -96.56 42.04, -96.56 42.03, -96.55 42.03, -96.55 42.02, -96.56 42, -96.56 41.99, -96.56 41.98, -96.56 41.97, -96.56 41.96, -96.56 41.95, -96.56 41.94, -96.56 41.93, -96.56 41.92, -96.56 41.91, -96.56 41.9, -96.56 41.89, -96.56 41.88, -96.56 41.87, -96.56 41.86, -96.56 41.85, -96.56 41.84, -96.56 41.83, -96.56 41.82, -96.56 41.81, -96.56 41.8, -96.56 41.79, -96.56 41.78, -96.56 41.77, -96.56 41.76, -96.56 41.75, -96.56 41.74, -96.57 41.74, -96.58 41.74, -96.59 41.74, -96.6 41.74, -96.61 41.74, -96.62 41.74, -96.63 41.74, -96.64 41.74, -96.65 41.74, -96.66 41.74, -96.67 41.74, -96.68 41.74, -96.69 41.74, -96.7 41.74, -96.71 41.74, -96.72 41.74, -96.73 41.74, -96.74 41.74, -96.75 41.74, -96.76 41.74, -96.77 41.74, -96.78 41.74, -96.79 41.74, -96.8 41.74, -96.81 41.74, -96.82 41.74, -96.83 41.74, -96.84 41.74, -96.85 41.74, -96.86 41.74, -96.87 41.74, -96.88 41.74, -96.89 41.74, -96.9 41.74, -96.91 41.74, -96.92 41.74, -96.93 41.74, -96.94 41.74, -96.95 41.74, -96.96 41.74, -96.97 41.74, -96.98 41.74, -96.99 41.74, -97 41.74, -97.01 41.74, -97.02 41.74, -97.02 41.75, -97.02 41.76, -97.02 41.77, -97.02 41.78, -97.02 41.79, -97.02 41.8, -97.02 41.81, -97.02 41.82, -97.02 41.83, -97.02 41.84, -97.02 41.85, -97.02 41.86, -97.02 41.87, -97.02 41.88, -97.02 41.89, -97.02 41.9, -97.02 41.91, -97.02 41.92, -97.02 41.93, -97.02 41.94, -97.02 41.95, -97.02 41.96, -97.02 41.97, -97.02 41.98, -97.02 41.99, -97.02 42, -97.02 42.01))"

    def test_st_bestsrid(self):
        polygon_from_wkt = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polgontable")
        polygon_from_wkt.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_BestSRID(polygondf.countyshape) from polygondf")
        function_df.show()
        actual = function_df.take(1)[0][0]
        assert actual == 3395

    def test_st_bestsrid(self):
        polygon_from_wkt = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polgontable")
        polygon_from_wkt.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_BestSRID(polygondf.countyshape) from polygondf")
        function_df.show()
        actual = function_df.take(1)[0][0]
        assert actual == 3395

    def test_st_shiftlongitude(self):
        function_df = self.spark.sql("select ST_ShiftLongitude(ST_GeomFromWKT('POLYGON((179 10, -179 10, -179 20, 179 20, 179 10))'))")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POLYGON ((179 10, 181 10, 181 20, 179 20, 179 10))"

        function_df = self.spark.sql("select ST_ShiftLongitude(ST_GeomFromWKT('POINT(-179 10)'))")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "POINT (181 10)"

        function_df = self.spark.sql("select ST_ShiftLongitude(ST_GeomFromWKT('LINESTRING(179 10, 181 10)'))")
        actual = function_df.take(1)[0][0].wkt
        assert actual == "LINESTRING (179 10, -179 10)"

    def test_st_envelope(self):
        polygon_from_wkt = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polygontable")
        polygon_from_wkt.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_expand(self):
        baseDf = self.spark.sql(
            "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom")
        actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 10))").first()[0]
        expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))"
        assert expected == actual

        actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 5, 6))").first()[0]
        expected = "POLYGON Z((45 44 1, 45 86 1, 85 86 3, 85 44 3, 45 44 1))"
        assert expected == actual

        actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 6, 5, -3))").first()[0]
        expected = "POLYGON Z((44 45 4, 44 85 4, 86 85 0, 86 45 0, 44 45 4))"
        assert expected == actual

    def test_st_centroid(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_crossesdateline(self):
        crosses_test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))') as geom")
        crosses_test_table.createOrReplaceTempView("crossesTesttable")
        crosses = self.spark.sql("select(ST_CrossesDateLine(geom)) from crossesTesttable")

        not_crosses_test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as geom")
        not_crosses_test_table.createOrReplaceTempView("notCrossesTesttable")
        not_crosses = self.spark.sql("select(ST_CrossesDateLine(geom)) from notCrossesTesttable")

        assert crosses.take(1)[0][0]
        assert not not_crosses.take(1)[0][0]

    def test_st_length(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false").load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_Length(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_length2d(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false").load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")

        function_df = self.spark.sql("select ST_Length2D(polygondf.countyshape) from polygondf")
        assert function_df.take(1)[0][0] == 1.6244272911181594

    def test_st_area(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Area(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_distance(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_3ddistance(self):
        function_df = self.spark.sql("select ST_3DDistance(ST_PointZ(0.0, 0.0, 5.0), ST_PointZ(1.0, 1.0, -6.0))")
        assert function_df.count() == 1

    def test_st_transform(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql(
            "select ST_ReducePrecision(ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857', false), 2) from polygondf")
        actual = function_df.take(1)[0][0].wkt
        assert actual[:300] == "POLYGON ((-10800163.45 5161718.41, -10800164.34 5162103.12, -10800164.57 5162440.81, -10800164.57 5162443.95, -10800164.57 5162468.37, -10800164.57 5162501.93, -10800165.57 5163066.47, -10800166.9 5163158.61, -10800166.9 5163161.46, -10800167.01 5163167.9, -10800167.01 5163171.5, -10800170.24 516340"

        function_df = self.spark.sql(
            "select ST_ReducePrecision(ST_Transform(ST_SetSRID(polygondf.countyshape, 4326), 'epsg:3857'), 2) from polygondf"
        )
        actual = function_df.take(1)[0][0].wkt
        assert actual[:300] == "POLYGON ((-10800163.45 5161718.41, -10800164.34 5162103.12, -10800164.57 5162440.81, -10800164.57 5162443.95, -10800164.57 5162468.37, -10800164.57 5162501.93, -10800165.57 5163066.47, -10800166.9 5163158.61, -10800166.9 5163161.46, -10800167.01 5163167.9, -10800167.01 5163171.5, -10800170.24 516340"

    def test_st_intersection_intersects_but_not_contains(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersect = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersect.take(1)[0][0].wkt == "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"

    def test_st_intersection_intersects_but_left_contains_right(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_intersects_but_right_contains_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_not_intersects(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON EMPTY"

    def test_st_maximum_inscribed_circle(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))') AS geom")
        actual = baseDf.selectExpr("ST_MaximumInscribedCircle(geom)").take(1)[0][0]
        center = actual.center.wkt
        assert center == "POINT (96.953125 76.328125)"
        nearest = actual.nearest.wkt
        assert nearest == "POINT (140 90)"
        radius = actual.radius
        assert radius == 45.165845650018

    def test_st_is_valid_detail(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))') AS geom")
        actual = baseDf.selectExpr("ST_IsValidDetail(geom)").first()[0]
        expected = Row(valid=True, reason=None, location=None)
        assert expected == actual

        baseDf = self.spark.sql("SELECT ST_GeomFromText('POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))') AS geom")
        actual = baseDf.selectExpr("ST_IsValidDetail(geom)").first()[0]
        expected = Row(valid=False, reason="Ring Self-intersection at or near point (1.0, 1.0, NaN)", location=
        self.spark.sql("SELECT ST_GeomFromText('POINT (1 1)')").first()[0])
        assert expected == actual

    def test_st_is_valid_trajectory(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromText('LINESTRING M (0 0 1, 0 1 2)') as geom1, ST_GeomFromText('LINESTRING M (0 0 1, 0 1 1)') as geom2")
        actual = baseDf.selectExpr("ST_IsValidTrajectory(geom1)").first()[0]
        assert actual

        actual = baseDf.selectExpr("ST_IsValidTrajectory(geom2)").first()[0]
        assert not actual

    def test_st_is_valid(self):
        test_table = self.spark.sql(
            "SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
            "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b"
        )

        assert not test_table.take(1)[0][0]
        assert test_table.take(1)[0][1]

    def test_fixed_null_pointer_exception_in_st_valid(self):
        test_table = self.spark.sql("SELECT ST_IsValid(null)")
        assert test_table.take(1)[0][0] is None

    def test_st_precision_reduce(self):
        test_table = self.spark.sql(
            """SELECT ST_ReducePrecision(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)""")
        test_table.show(truncate=False)
        assert test_table.take(1)[0][0].x == 0.12345679
        test_table = self.spark.sql(
            """SELECT ST_ReducePrecision(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)""")
        test_table.show(truncate=False)
        assert test_table.take(1)[0][0].x == 0.12345678901

    def test_st_is_simple(self):

        test_table = self.spark.sql(
            "SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')) AS a, " +
            "ST_IsSimple(ST_GeomFromText('POLYGON((1 1,3 1,3 3,2 0,1 1))')) as b"
        )
        assert test_table.take(1)[0][0]
        assert not test_table.take(1)[0][1]

    def test_st_as_text(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        wkt_df = self.spark.sql("select ST_AsText(countyshape) as wkt from polygondf")
        assert polygon_df.take(1)[0]["countyshape"].wkt == loads(wkt_df.take(1)[0]["wkt"]).wkt

    def test_st_astext_3d(self):
        input_df = self.spark.createDataFrame([
            ("Point(21 52 87)",),
            ("Polygon((0 0 1, 0 1 1, 1 1 1, 1 0 1, 0 0 1))",),
            ("Linestring(0 0 1, 1 1 2, 1 0 3)",),
            ("MULTIPOINT ((10 40 66), (40 30 77), (20 20 88), (30 10 99))",),
            (
                "MULTIPOLYGON (((30 20 11, 45 40 11, 10 40 11, 30 20 11)), ((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))",),
            ("MULTILINESTRING ((10 10 11, 20 20 11, 10 40 11), (40 40 11, 30 30 11, 40 20 11, 30 10 11))",),
            (
                "MULTIPOLYGON (((40 40 11, 20 45 11, 45 30 11, 40 40 11)), ((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11), (30 20 11, 20 15 11, 20 25 11, 30 20 11)))",),
            ("POLYGON((0 0 11, 0 5 11, 5 5 11, 5 0 11, 0 0 11), (1 1 11, 2 1 11, 2 2 11, 1 2 11, 1 1 11))",),
        ], ["wkt"])

        input_df.createOrReplaceTempView("input_wkt")
        polygon_df = self.spark.sql("select ST_AsText(ST_GeomFromWkt(wkt)) as wkt from input_wkt")
        assert polygon_df.count() == 8

    def test_st_as_text_3d(self):
        polygon_wkt_df = self.spark.read.format("csv"). \
            option("delimiter", "\t"). \
            option("header", "false"). \
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        wkt_df = self.spark.sql("select ST_AsText(countyshape) as wkt from polygondf")
        assert polygon_df.take(1)[0]["countyshape"].wkt == loads(wkt_df.take(1)[0]["wkt"]).wkt

    def test_st_n_points(self):
        test = self.spark.sql(
            "SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")

    def test_st_geometry_type(self):
        test = self.spark.sql(
            "SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")

    def test_st_difference_right_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"

    def test_st_difference_right_not_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))"

    def test_st_difference_left_contains_right(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"

    def test_st_difference_right_not_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON EMPTY"

    def test_st_delaunay_triangles(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 10, 10 20, 20 20, 20 10, 10 10)),((25 10, 25 20, 35 20, 35 10, 25 10)))') AS geom")
        actual = baseDf.selectExpr("ST_DelaunayTriangles(geom)").take(1)[0][0].wkt
        expected = "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 20 10, 10 20)), POLYGON ((10 20, 20 10, 20 20, 10 20)), POLYGON ((20 20, 20 10, 25 10, 20 20)), POLYGON ((20 20, 25 10, 25 20, 20 20)), POLYGON ((25 20, 25 10, 35 10, 25 20)), POLYGON ((25 20, 35 10, 35 20, 25 20)))"
        assert expected == actual

    def test_st_sym_difference_part_of_right_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][
                   0].wkt == "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"

    def test_st_sym_difference_not_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][
                   0].wkt == "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"

    def test_st_sym_difference_contains(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"

    def test_st_union_part_of_right_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') as b")
        test_table.createOrReplaceTempView("test_union")
        union = self.spark.sql("select ST_Union(a,b) from test_union")
        assert union.take(1)[0][0].wkt == "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"

    def test_st_union_not_overlaps_left(self):
        test_table = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_union")
        union = self.spark.sql("select ST_Union(a,b) from test_union")
        assert union.take(1)[0][
                   0].wkt == "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"

    def test_st_union_array_variant(self):
        test_table = self.spark.sql("select array(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'),ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))'), ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))')) as polys")
        actual = test_table.selectExpr("ST_Union(polys)").take(1)[0][0].wkt
        expected = "MULTIPOLYGON (((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)), ((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((4 4, 4 6, 6 6, 6 4, 4 4)))"
        assert expected == actual

    def test_st_unary_union(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))') AS geom")
        actual = baseDf.selectExpr("ST_UnaryUnion(geom)").take(1)[0][0].wkt
        expected = "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))"
        assert expected == actual

    def test_st_azimuth(self):
        sample_points = create_sample_points(20)
        sample_pair_points = [[el, sample_points[1]] for el in sample_points]
        schema = StructType([
            StructField("geomA", GeometryType(), True),
            StructField("geomB", GeometryType(), True)
        ])
        df = self.spark.createDataFrame(sample_pair_points, schema)

        st_azimuth_result = [el[0] * 180 / math.pi for el in df.selectExpr("ST_Azimuth(geomA, geomB)").collect()]

        expected_result = [
            240.0133139011053, 0.0, 270.0, 286.8042682202057, 315.0, 314.9543472191815, 315.0058223408927,
            245.14762725688198, 314.84984546897755, 314.8868529256147, 314.9510567053395, 314.95443984912936,
            314.89925480835245, 314.60187991438806, 314.6834083423315, 314.80689827870725, 314.90290827689506,
            314.90336326341765, 314.7510398533675, 314.73608518601935
        ]

        assert st_azimuth_result == expected_result

        azimuth = self.spark.sql(
            """SELECT ST_Azimuth(ST_Point(25.0, 45.0), ST_Point(75.0, 100.0)) AS degA_B,
                      ST_Azimuth(ST_Point(75.0, 100.0), ST_Point(25.0, 45.0)) AS degB_A
            """
        ).collect()

        azimuths = [[azimuth1 * 180 / math.pi, azimuth2 * 180 / math.pi] for azimuth1, azimuth2 in azimuth]
        assert azimuths[0] == [42.27368900609373, 222.27368900609372]

    def test_st_x(self):
        point_df = create_sample_points_df(self.spark, 5)
        polygon_df = create_sample_polygons_df(self.spark, 5)
        linestring_df = create_sample_lines_df(self.spark, 5)

        points = point_df \
            .selectExpr("ST_X(geom)").collect()

        polygons = polygon_df.selectExpr("ST_X(geom) as x").filter("x IS NOT NULL")

        linestrings = linestring_df.selectExpr("ST_X(geom) as x").filter("x IS NOT NULL")

        assert ([point[0] for point in points] == [-71.064544, -88.331492, 88.331492, 1.0453, 32.324142])

        assert (not linestrings.count())

        assert (not polygons.count())

    def test_st_y(self):
        point_df = create_sample_points_df(self.spark, 5)
        polygon_df = create_sample_polygons_df(self.spark, 5)
        linestring_df = create_sample_lines_df(self.spark, 5)

        points = point_df \
            .selectExpr("ST_Y(geom)").collect()

        polygons = polygon_df.selectExpr("ST_Y(geom) as y").filter("y IS NOT NULL")

        linestrings = linestring_df.selectExpr("ST_Y(geom) as y").filter("y IS NOT NULL")

        assert ([point[0] for point in points] == [42.28787, 32.324142, 32.324142, 5.3324324, -88.331492])

        assert (not linestrings.count())

        assert (not polygons.count())

    def test_st_z(self):
        point_df = self.spark.sql(
            "select ST_GeomFromWKT('POINT Z (1.1 2.2 3.3)') as geom"
        )
        polygon_df = self.spark.sql(
            "select ST_GeomFromWKT('POLYGON Z ((0 0 2, 0 1 2, 1 1 2, 1 0 2, 0 0 2))') as geom"
        )
        linestring_df = self.spark.sql(
            "select ST_GeomFromWKT('LINESTRING Z (0 0 1, 0 1 2)') as geom"
        )

        points = point_df \
            .selectExpr("ST_Z(geom)").collect()

        polygons = polygon_df.selectExpr("ST_Z(geom) as z").filter("z IS NOT NULL")

        linestrings = linestring_df.selectExpr("ST_Z(geom) as z").filter("z IS NOT NULL")

        assert ([point[0] for point in points] == [3.3])

        assert (not linestrings.count())

        assert (not polygons.count())

    def test_st_zmflag(self):
        actual = self.spark.sql("SELECT ST_Zmflag(ST_GeomFromWKT('POINT (1 2)'))").take(1)[0][0]
        assert actual == 0

        actual = self.spark.sql("SELECT ST_Zmflag(ST_GeomFromWKT('LINESTRING (1 2 3, 4 5 6)'))").take(1)[0][0]
        assert actual == 2

        actual = self.spark.sql("SELECT ST_Zmflag(ST_GeomFromWKT('POLYGON M((1 2 3, 3 4 3, 5 6 3, 3 4 3, 1 2 3))'))").take(1)[0][0]
        assert actual == 1

        actual = self.spark.sql("SELECT ST_Zmflag(ST_GeomFromWKT('MULTIPOLYGON ZM (((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1)), ((15 5 3 1, 20 10 6 2, 10 10 7 3, 15 5 3 1)))'))").take(1)[0][0]
        assert actual == 3

    def test_st_z_max(self):
        linestring_df = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING Z (0 0 1, 0 1 2)') as geom")
        linestring_row = [lnstr_row[0] for lnstr_row in linestring_df.selectExpr("ST_ZMax(geom)").collect()]
        assert (linestring_row == [2.0])

    def test_st_z_min(self):
        linestring_df = self.spark.sql(
            "SELECT ST_GeomFromWKT('POLYGON Z ((0 0 2, 0 1 1, 1 1 2, 1 0 2, 0 0 2))') as geom")
        linestring_row = [lnstr_row[0] for lnstr_row in linestring_df.selectExpr("ST_ZMin(geom)").collect()]
        assert (linestring_row == [1.0])

    def test_st_n_dims(self):
        point_df = self.spark.sql("SELECT ST_GeomFromWKT('POINT(1 1 2)') as geom")
        point_row = [pt_row[0] for pt_row in point_df.selectExpr("ST_NDims(geom)").collect()]
        assert (point_row == [3])

    def test_st_start_point(self):

        point_df = create_sample_points_df(self.spark, 5)
        polygon_df = create_sample_polygons_df(self.spark, 5)
        linestring_df = create_sample_lines_df(self.spark, 5)

        expected_points = [
            "POINT (-112.506968 45.98186)",
            "POINT (-112.519856 45.983586)",
            "POINT (-112.504872 45.919281)",
            "POINT (-112.574945 45.987772)",
            "POINT (-112.520691 42.912313)"
        ]

        points = point_df.selectExpr("ST_StartPoint(geom) as geom").filter("geom IS NOT NULL")

        polygons = polygon_df.selectExpr("ST_StartPoint(geom) as geom").filter("geom IS NOT NULL")

        linestrings = linestring_df.selectExpr("ST_StartPoint(geom) as geom").filter("geom IS NOT NULL")

        assert ([line[0] for line in linestrings.collect()] == [wkt.loads(el) for el in expected_points])

        assert (not points.count())

        assert (not polygons.count())

    def test_st_snap(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 "
                                "))') AS poly, ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)') AS line")
        actual = baseDf.selectExpr("ST_AsText(ST_Snap(poly, line, 2.525))").take(1)[0][0]
        expected = "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))"
        assert (expected == actual)

        actual = baseDf.selectExpr("ST_AsText(ST_Snap(poly, line, 3.125))").take(1)[0][0]
        expected = "POLYGON ((0.5 10.7, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 5.4 8.4, 0.5 10.7))"
        assert (expected == actual)

    def test_st_end_point(self):
        linestring_dataframe = create_sample_lines_df(self.spark, 5)
        other_geometry_dataframe = create_sample_points_df(self.spark, 5). \
            union(create_sample_points_df(self.spark, 5))

        point_data_frame = linestring_dataframe.selectExpr("ST_EndPoint(geom) as geom"). \
            filter("geom IS NOT NULL")

        expected_ending_points = [
            "POINT (-112.504872 45.98186)",
            "POINT (-112.506968 45.983586)",
            "POINT (-112.41643 45.919281)",
            "POINT (-112.519856 45.987772)",
            "POINT (-112.442664 42.912313)"
        ]
        empty_dataframe = other_geometry_dataframe.selectExpr("ST_EndPoint(geom) as geom"). \
            filter("geom IS NOT NULL")

        assert ([wkt_row[0]
                 for wkt_row in point_data_frame.selectExpr("ST_AsText(geom)").collect()] == expected_ending_points)

        assert (empty_dataframe.count() == 0)

    def test_st_minimum_clearance(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
        actual = baseDf.selectExpr("ST_MinimumClearance(geom)").take(1)[0][0]
        assert actual == 0.5

    def test_st_minimum_clearance_line(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
        actual = baseDf.selectExpr("ST_MinimumClearanceLine(geom)").take(1)[0][0].wkt
        assert actual == "LINESTRING (64.5 16, 65 16)"

    def test_st_boundary(self):
        wkt_list = [
            "LINESTRING(1 1,0 0, -1 1)",
            "LINESTRING(100 150,50 60, 70 80, 160 170)",
            "POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))",
            "POLYGON((1 1,0 0, -1 1, 1 1))"
        ]

        geometries = [[wkt.loads(wkt_data)] for wkt_data in wkt_list]

        schema = StructType(
            [StructField("geom", GeometryType(), False)]
        )

        geometry_table = self.spark.createDataFrame(geometries, schema)

        geometry_table.show()

        boundary_table = geometry_table.selectExpr("ST_Boundary(geom) as geom")

        boundary_wkt = [wkt_row[0] for wkt_row in boundary_table.selectExpr("ST_AsText(geom)").collect()]
        assert (boundary_wkt == [
            "MULTIPOINT ((1 1), (-1 1))",
            "MULTIPOINT ((100 150), (160 170))",
            "MULTILINESTRING ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))",
            "LINESTRING (1 1, 0 0, -1 1, 1 1)"
        ])

    def test_st_exterior_ring(self):
        polygon_df = create_simple_polygons_df(self.spark, 5)
        additional_wkt = "POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))"
        additional_wkt_df = self.spark.createDataFrame([[wkt.loads(additional_wkt)]], self.geo_schema)

        polygons_df = polygon_df.union(additional_wkt_df)

        other_geometry_df = create_sample_lines_df(self.spark, 5).union(
            create_sample_points_df(self.spark, 5))

        linestring_df = polygons_df.selectExpr("ST_ExteriorRing(geom) as geom").filter("geom IS NOT NULL")

        empty_df = other_geometry_df.selectExpr("ST_ExteriorRing(geom) as geom").filter("geom IS NOT NULL")

        linestring_wkt = [wkt_row[0] for wkt_row in linestring_df.selectExpr("ST_AsText(geom)").collect()]

        assert (linestring_wkt == ["LINESTRING (0 0, 0 1, 1 1, 1 0, 0 0)", "LINESTRING (0 0, 1 1, 1 2, 1 1, 0 0)"])

        assert (not empty_df.count())

    def test_st_geometry_n(self):
        data_frame = self.__wkt_list_to_data_frame(["MULTIPOINT((1 2), (3 4), (5 6), (8 9))"])
        wkts = [data_frame.selectExpr(f"ST_GeometryN(geom, {i}) as geom").selectExpr("st_asText(geom)").collect()[0][0]
                for i in range(0, 4)]

        assert (wkts == ["POINT (1 2)", "POINT (3 4)", "POINT (5 6)", "POINT (8 9)"])

    def test_st_interior_ring_n(self):
        polygon_df = self.__wkt_list_to_data_frame(
            [
                "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))"]
        )

        other_geometry = create_sample_points_df(self.spark, 5).union(create_sample_lines_df(self.spark, 5))
        wholes = [polygon_df.selectExpr(f"ST_InteriorRingN(geom, {i}) as geom").
                  selectExpr("ST_AsText(geom)").collect()[0][0]
                  for i in range(3)]

        empty_df = other_geometry.selectExpr("ST_InteriorRingN(geom, 1) as geom").filter("geom IS NOT NULL")

        assert (not empty_df.count())
        assert (wholes == ["LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)",
                           "LINESTRING (1 3, 2 3, 2 4, 1 4, 1 3)",
                           "LINESTRING (3 3, 4 3, 4 4, 3 4, 3 3)"])

    def test_st_dumps(self):
        expected_geometries = [
            "POINT (21 52)", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            "LINESTRING (0 0, 1 1, 1 0)",
            "POINT (10 40)", "POINT (40 30)", "POINT (20 20)", "POINT (30 10)",
            "POLYGON ((30 20, 45 40, 10 40, 30 20))",
            "POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))", "LINESTRING (10 10, 20 20, 10 40)",
            "LINESTRING (40 40, 30 30, 40 20, 30 10)",
            "POLYGON ((40 40, 20 45, 45 30, 40 40))",
            "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))",
            "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"
        ]

        geometry_df = self.__wkt_list_to_data_frame(
            [
                "Point(21 52)",
                "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))",
                "Linestring(0 0, 1 1, 1 0)",
                "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
                "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
                "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
                "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
                "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"
            ]
        )

        dumped_geometries = geometry_df.selectExpr("ST_Dump(geom) as geom")

        assert (dumped_geometries.select(explode(col("geom"))).count() == 14)

        collected_geometries = dumped_geometries \
            .select(explode(col("geom")).alias("geom")) \
            .selectExpr("ST_AsText(geom) as geom") \
            .collect()

        assert ([geom_row[0] for geom_row in collected_geometries] == expected_geometries)

    def test_st_dump_points(self):
        expected_points = [
            "POINT (-112.506968 45.98186)",
            "POINT (-112.506968 45.983586)",
            "POINT (-112.504872 45.983586)",
            "POINT (-112.504872 45.98186)",
            "POINT (-71.064544 42.28787)",
            "POINT (0 0)", "POINT (0 1)",
            "POINT (1 1)", "POINT (1 0)",
            "POINT (0 0)"
        ]
        geometry_df = create_sample_lines_df(self.spark, 1) \
            .union(create_sample_points_df(self.spark, 1)) \
            .union(create_simple_polygons_df(self.spark, 1))

        dumped_points = geometry_df.selectExpr("ST_DumpPoints(geom) as geom") \
            .select(explode(col("geom")).alias("geom"))

        assert (dumped_points.count() == 10)

        collected_points = [geom_row[0] for geom_row in dumped_points.selectExpr("ST_AsText(geom)").collect()]
        assert (collected_points == expected_points)

    def test_st_is_closed(self):
        expected_result = [
            [1, True],
            [2, True],
            [3, False],
            [4, True],
            [5, True],
            [6, True],
            [7, True],
            [8, False],
            [9, False],
            [10, False]
        ]
        geometry_list = [
            (1, "Point(21 52)"),
            (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
            (3, "Linestring(0 0, 1 1, 1 0)"),
            (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
            (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
            (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
            (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
            (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
            (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
            (10,
             "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))")
        ]

        geometry_df = self.__wkt_pair_list_with_index_to_data_frame(geometry_list)
        is_closed = geometry_df.selectExpr("index", "ST_IsClosed(geom)").collect()
        is_closed_collected = [[*row] for row in is_closed]
        assert (is_closed_collected == expected_result)

    def test_num_interior_rings(self):
        geometries = [
            (1, "Point(21 52)"),
            (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
            (3, "Linestring(0 0, 1 1, 1 0)"),
            (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
            (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
            (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
            (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
            (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
            (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
            (10,
             "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
            (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")]

        geometry_df = self.__wkt_pair_list_with_index_to_data_frame(geometries)

        number_of_interior_rings = geometry_df.selectExpr("index", "ST_NumInteriorRings(geom) as num")
        collected_interior_rings = [[*row] for row in number_of_interior_rings.filter("num is not null").collect()]
        assert (collected_interior_rings == [[2, 0], [11, 1]])

    def test_num_interior_ring(self):
        geometries = [
            (1, "Point(21 52)"),
            (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
            (3, "Linestring(0 0, 1 1, 1 0)"),
            (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
            (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
            (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
            (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
            (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
            (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
            (10,
             "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
            (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")]

        geometry_df = self.__wkt_pair_list_with_index_to_data_frame(geometries)

        number_of_interior_rings = geometry_df.selectExpr("index", "ST_NumInteriorRing(geom) as num")
        collected_interior_rings = [[*row] for row in number_of_interior_rings.filter("num is not null").collect()]
        assert (collected_interior_rings == [[2, 0], [11, 1]])

    def test_st_add_measure(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (1 1, 2 2, 2 2, 3 3)') as line, ST_GeomFromWKT('MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))') as mline")
        actual = baseDf.selectExpr("ST_AsText(ST_AddMeasure(line, 1, 70))").first()[0]
        expected = "LINESTRING M(1 1 1, 2 2 35.5, 2 2 35.5, 3 3 70)"
        assert expected == actual

        actual = baseDf.selectExpr("ST_AsText(ST_AddMeasure(mline, 10, 70))").first()[0]
        expected = "MULTILINESTRING M((1 0 10, 2 0 20, 4 0 40), (1 0 40, 2 0 50, 4 0 70))"
        assert expected == actual

    def test_st_add_point(self):
        geometry = [
            ("Point(21 52)", "Point(21 52)"),
            ("Point(21 52)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
            ("Linestring(0 0, 1 1, 1 0)", "Point(21 52)"),
            ("Linestring(0 0, 1 1, 1 0, 0 0)", "Linestring(0 0, 1 1, 1 0, 0 0)"),
            ("Point(21 52)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
            ("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", "Point(21 52)"),
            ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", "Point(21 52)"),
            ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
            ("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
            (
                "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))",
                "Point(21 52)"),
            ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)")
        ]
        geometry_df = self.__wkt_pairs_to_data_frame(geometry)
        modified_geometries = geometry_df.selectExpr("ST_AddPoint(geomA, geomB) as geom")
        collected_geometries = [
            row[0] for row in modified_geometries.filter("geom is not null").selectExpr("ST_AsText(geom)").collect()
        ]
        assert (collected_geometries[0] == "LINESTRING (0 0, 1 1, 1 0, 21 52)")

    def test_st_rotate_x(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING (50 160, 50 50, 100 50)') as geom1, ST_GeomFromWKT('LINESTRING(1 2 3, 1 1 1)') AS geom2")

        actual = baseDf.selectExpr("ST_RotateX(geom1, PI())").first()[0].wkt
        expected = "LINESTRING (50 -160, 50 -50, 100 -50)"
        assert expected == actual

        actual = baseDf.selectExpr("ST_RotateX(geom2, PI() / 2)").first()[0].wkt
        expected = "LINESTRING Z (1 -3 2, 1 -0.9999999999999999 1)"
        assert expected == actual

    def test_st_remove_point(self):
        result_and_expected = [
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 0), "LINESTRING (1 1, 1 0, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 1), "LINESTRING (0 0, 1 0, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 2), "LINESTRING (0 0, 1 1, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 3), "LINESTRING (0 0, 1 1, 1 0)"],
            [self.calculate_st_remove("POINT(0 1)", 3), None],
            [self.calculate_st_remove("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", 3), None],
            [self.calculate_st_remove("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))", 0), None],
            [self.calculate_st_remove(
                "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 3), None],
            [self.calculate_st_remove(
                "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", 3), None]
        ]
        for actual, expected in result_and_expected:
            assert (actual == expected)

    def test_isPolygonCW(self):
        actual = self.spark.sql("SELECT ST_IsPolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))").take(1)[0][0]
        assert not actual

        actual = self.spark.sql("SELECT ST_IsPolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))").take(1)[0][0]
        assert actual

    def test_st_simplify_vw(self):
        basedf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)') as geom")
        actual = basedf.selectExpr("ST_SimplifyVW(geom, 30)").take(1)[0][0].wkt
        expected = "LINESTRING (5 2, 7 25, 10 10)"
        assert expected == actual

    def test_st_simplify_polygon_hull(self):
        basedf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))') as geom")
        actual = basedf.selectExpr("ST_SimplifyPolygonHull(geom, 0.3, false)").take(1)[0][0].wkt
        expected = "POLYGON ((30 10, 40 40, 10 20, 30 10))"
        assert expected == actual

        actual = basedf.selectExpr("ST_SimplifyPolygonHull(geom, 0.3)").take(1)[0][0].wkt
        expected = "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))"
        assert expected == actual


    def test_st_is_ring(self):
        result_and_expected = [
            [self.calculate_st_is_ring("LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)"), False],
            [self.calculate_st_is_ring("LINESTRING(2 0, 2 2, 3 3)"), False],
            [self.calculate_st_is_ring("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"), True],
            [self.calculate_st_is_ring("POINT (21 52)"), None],
            [self.calculate_st_is_ring("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"), None],
        ]
        for actual, expected in result_and_expected:
            assert (actual == expected)

    def test_isPolygonCCW(self):
        actual = self.spark.sql("SELECT ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))").take(1)[0][0]
        assert actual

        actual = self.spark.sql("SELECT ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))").take(1)[0][0]
        assert not actual

    def test_forcePolygonCCW(self):
        actualDf = self.spark.sql(
            "SELECT ST_ForcePolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))')) AS polyCW")
        actual = actualDf.selectExpr("ST_AsText(polyCW)").take(1)[0][0]
        expected = "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        assert expected == actual

    def test_st_subdivide(self):
        # Given
        geometry_df = self.__wkt_list_to_data_frame(
            [
                "POINT(21 52)",
                "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
                "LINESTRING (0 0, 1 1, 2 2)"
            ]
        )
        geometry_df.createOrReplaceTempView("geometry")

        # When
        subdivided = geometry_df.select(expr("st_SubDivide(geom, 5)"))

        # Then
        assert subdivided.count() == 3

        assert sum([geometries[0].__len__() for geometries in subdivided.collect()]) == 16

    def test_st_subdivide_explode(self):
        # Given
        geometry_df = self.__wkt_list_to_data_frame(
            [
                "POINT(21 52)",
                "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
                "LINESTRING (0 0, 1 1, 2 2)"
            ]
        )
        geometry_df.createOrReplaceTempView("geometry")

        # When
        subdivided = geometry_df.select(expr("st_SubDivideExplode(geom, 5)"))

        # Then
        assert subdivided.count() == 16

    def test_st_has_z(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON Z ((30 10 5, 40 40 10, 20 40 15, 10 20 20, 30 10 5))') as poly")
        actual = baseDf.selectExpr("ST_HasZ(poly)")
        assert actual

    def test_st_has_m(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))') as poly")
        actual = baseDf.selectExpr("ST_HasM(poly)")
        assert actual

    def test_st_m(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POINT ZM (1 2 3 4)') AS point")
        actual = baseDf.selectExpr("ST_M(point)").take(1)[0][0]
        assert actual == 4.0

    def test_st_m_min(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') AS line")
        actual = baseDf.selectExpr("ST_MMin(line)").take(1)[0][0]
        assert actual == -1.0

        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
        actual = baseDf.selectExpr("ST_MMin(line)").take(1)[0][0]
        assert actual is None

    def test_st_m_max(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') AS line")
        actual = baseDf.selectExpr("ST_MMax(line)").take(1)[0][0]
        assert actual == 3.0

        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
        actual = baseDf.selectExpr("ST_MMax(line)").take(1)[0][0]
        assert actual is None

    def test_st_subdivide_explode_lateral(self):
        # Given
        geometry_df = self.__wkt_list_to_data_frame(
            [
                "POINT(21 52)",
                "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
                "LINESTRING (0 0, 1 1, 2 2)"
            ]
        )

        geometry_df.selectExpr("geom as geometry").createOrReplaceTempView("geometries")

        # When
        lateral_view_result = self.spark. \
            sql("""select geom from geometries LATERAL VIEW ST_SubdivideExplode(geometry, 5) AS geom""")

        # Then
        assert lateral_view_result.count() == 16

    def test_st_make_line(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                ["POINT(0 0)", "POINT(1 1)" , "LINESTRING (0 0, 1 1)"],
                ["MULTIPOINT ((0 0), (1 1))", "MULTIPOINT ((2 2), (2 3))", "LINESTRING (0 0, 1 1, 2 2, 2 3)"],
                ["LINESTRING (0 0, 1 1)", "LINESTRING(2 2, 3 3)", "LINESTRING (0 0, 1 1, 2 2, 3 3)"]
            ]
        ).selectExpr("ST_GeomFromText(_1) AS geom1", "ST_GeomFromText(_2) AS geom2", "_3 AS expected")

        # When calling st_MakeLine
        geom_lines = geometry_df.withColumn("linestring", expr("ST_MakeLine(geom1, geom2)"))

        # Then
        result = geom_lines.selectExpr("ST_AsText(linestring)", "expected"). \
            collect()

        for actual, expected in result:
            assert actual == expected

    def test_st_points(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                # Adding only the input that will result in a non-null polygon
                ["MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))", "MULTIPOINT ((0 0), (1 1), (2 2), (3 3))"]
            ]
        ).selectExpr("ST_GeomFromText(_1) AS geom", "_2 AS expected")

        # When calling st_points
        geom_poly = geometry_df.withColumn("actual", expr("st_normalize(st_points(geom))"))

        result = geom_poly.filter("actual IS NOT NULL").selectExpr("ST_AsText(actual)", "expected"). \
            collect()

        assert result.__len__() == 1

        for actual, expected in result:
            assert actual == expected

    def test_st_polygon(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                ["POINT(21 52)", 4238, None],
                ["POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", 4237, None],
                ["LINESTRING (0 0, 0 1, 1 0, 0 0)", 4236, "POLYGON ((0 0, 0 1, 1 0, 0 0))"]
            ]
        ).selectExpr("ST_GeomFromText(_1) AS geom", "_2 AS srid", "_3 AS expected")

        # When calling st_Polygon
        geom_poly = geometry_df.withColumn("polygon", expr("ST_Polygon(geom, srid)"))

        # Then only based on closed linestring geom is created
        geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected"). \
            show()
        result = geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected"). \
            collect()

        assert result.__len__() == 1

        for actual, expected in result:
            assert actual == expected

    def test_st_polygonize(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                # Adding only the input that will result in a non-null polygon
                ["GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2), LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2), LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4), LINESTRING (2 4, 3 3, 4 2))", "GEOMETRYCOLLECTION (POLYGON ((0 2, 1 3, 2 4, 2 3, 2 2, 1 2, 0 2)), POLYGON ((2 2, 2 3, 2 4, 3 3, 4 2, 3 2, 2 2)))"]
            ]
        ).selectExpr("ST_GeomFromText(_1) AS geom", "_2 AS expected")

        # When calling st_polygonize
        geom_poly = geometry_df.withColumn("actual", expr("st_normalize(st_polygonize(geom))"))

        # Then only based on closed linestring geom is created
        geom_poly.filter("actual IS NOT NULL").selectExpr("ST_AsText(actual)", "expected"). \
            show()
        result = geom_poly.filter("actual IS NOT NULL").selectExpr("ST_AsText(actual)", "expected"). \
            collect()

        assert result.__len__() == 1

        for actual, expected in result:
            assert actual == expected


    def test_st_make_polygon(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                ["POINT(21 52)", None],
                ["POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", None],
                ["LINESTRING (0 0, 0 1, 1 0, 0 0)", "POLYGON ((0 0, 0 1, 1 0, 0 0))"]
            ]
        ).selectExpr("ST_GeomFromText(_1) AS geom", "_2 AS expected")

        # When calling st_MakePolygon
        geom_poly = geometry_df.withColumn("polygon", expr("ST_MakePolygon(geom)"))

        # Then only based on closed linestring geom is created
        geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected"). \
            show()
        result = geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected"). \
            collect()

        assert result.__len__() == 1

        for actual, expected in result:
            assert actual == expected

    def test_st_geohash(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                ["POINT(21 52)", "u3nzvf79zq"],
                ["POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "ssgs3y0zh7"],
                ["LINESTRING (0 0, 1 1, 2 2)", "s00twy01mt"]
            ]
        ).select(expr("St_GeomFromText(_1)").alias("geom"), col("_2").alias("expected_hash"))

        # When
        geohash_df = geometry_df.withColumn("geohash", expr("ST_GeoHash(geom, 10)")). \
            select("geohash", "expected_hash")

        # Then
        geohash = geohash_df.collect()

        for calculated_geohash, expected_geohash in geohash:
            assert calculated_geohash == expected_geohash

    def test_geom_from_geohash(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                [
                    "POLYGON ((20.999990701675415 51.99999690055847, 20.999990701675415 52.0000022649765, 21.000001430511475 52.0000022649765, 21.000001430511475 51.99999690055847, 20.999990701675415 51.99999690055847))",
                    "u3nzvf79zq"
                ],
                [
                    "POLYGON ((26.71875 26.71875, 26.71875 28.125, 28.125 28.125, 28.125 26.71875, 26.71875 26.71875))",
                    "ssg"
                ],
                [
                    "POLYGON ((0.9999918937683105 0.9999972581863403, 0.9999918937683105 1.0000026226043701, 1.0000026226043701 1.0000026226043701, 1.0000026226043701 0.9999972581863403, 0.9999918937683105 0.9999972581863403))",
                    "s00twy01mt"
                ]
            ]
        ).select(expr("ST_GeomFromGeoHash(_2)").alias("geom"), col("_1").alias("expected_polygon"))

        # When
        wkt_df = geometry_df.withColumn("wkt", expr("ST_AsText(geom)")). \
            select("wkt", "expected_polygon").collect()

        for wkt, expected_polygon in wkt_df:
            assert wkt == expected_polygon

    def test_geom_from_geohash_precision(self):
        # Given
        geometry_df = self.spark.createDataFrame(
            [
                ["POLYGON ((11.25 50.625, 11.25 56.25, 22.5 56.25, 22.5 50.625, 11.25 50.625))", "u3nzvf79zq"],
                ["POLYGON ((22.5 22.5, 22.5 28.125, 33.75 28.125, 33.75 22.5, 22.5 22.5))", "ssgs3y0zh7"],
                ["POLYGON ((0 0, 0 5.625, 11.25 5.625, 11.25 0, 0 0))", "s00twy01mt"]
            ]
        ).select(expr("ST_GeomFromGeoHash(_2, 2)").alias("geom"), col("_1").alias("expected_polygon"))

        # When
        wkt_df = geometry_df.withColumn("wkt", expr("ST_ASText(geom)")). \
            select("wkt", "expected_polygon")

        # Then
        geohash = wkt_df.collect()

        for wkt, expected_wkt in geohash:
            assert wkt == expected_wkt

    def test_st_closest_point(self):
        expected = "POINT (0 1)"
        actual_df = self.spark.sql("select ST_AsText(ST_ClosestPoint(ST_GeomFromText('POINT (0 1)'), "
                                   "ST_GeomFromText('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)')))")
        actual = actual_df.take(1)[0][0]
        assert expected == actual

    def test_st_collect_on_array_type(self):
        # given
        geometry_df = self.spark.createDataFrame([
            [1, [loads("POLYGON((1 2,1 4,3 4,3 2,1 2))"), loads("POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5))")]],
            [2, [loads("LINESTRING(1 2, 3 4)"), loads("LINESTRING(3 4, 4 5)")]],
            [3, [loads("POINT(1 2)"), loads("POINT(-2 3)")]]
        ]).toDF("id", "geom")

        # when calculating st collect
        geometry_df_collected = geometry_df.withColumn("collected", expr("ST_Collect(geom)"))

        # then result should be as expected
        assert (set([el[0] for el in geometry_df_collected.selectExpr("ST_AsText(collected)").collect()]) == {
            "MULTILINESTRING ((1 2, 3 4), (3 4, 4 5))",
            "MULTIPOINT ((1 2), (-2 3))",
            "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)), ((0.5 0.5, 5 0, 5 5, 0 5, 0.5 0.5)))"
        })

    def test_st_collect_on_multiple_columns(self):
        # given geometry df with multiple geometry columns
        geometry_df = self.spark.createDataFrame([
            [1, loads("POLYGON((1 2,1 4,3 4,3 2,1 2))"), loads("POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5))")],
            [2, loads("LINESTRING(1 2, 3 4)"), loads("LINESTRING(3 4, 4 5)")],
            [3, loads("POINT(1 2)"), loads("POINT(-2 3)")]
        ]).toDF("id", "geom_left", "geom_right")

        # when calculating st collect on multiple columns
        geometry_df_collected = geometry_df.withColumn("collected", expr("ST_Collect(geom_left, geom_right)"))

        # then result should be calculated
        assert (set([el[0] for el in geometry_df_collected.selectExpr("ST_AsText(collected)").collect()]) == {
            "MULTILINESTRING ((1 2, 3 4), (3 4, 4 5))",
            "MULTIPOINT ((1 2), (-2 3))",
            "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)), ((0.5 0.5, 5 0, 5 5, 0 5, 0.5 0.5)))"
        })

    def test_st_reverse(self):
        test_cases = {
            "'POLYGON((-1 0 0, 1 0 0, 0 0 1, 0 1 0, -1 0 0))'":
                "POLYGON Z((-1 0 0, 0 1 0, 0 0 1, 1 0 0, -1 0 0))",
            "'LINESTRING(0 0, 1 2, 2 4, 3 6)'":
                "LINESTRING (3 6, 2 4, 1 2, 0 0)",
            "'POINT(1 2)'":
                "POINT (1 2)",
            "'MULTIPOINT((10 40 66), (40 30 77), (20 20 88), (30 10 99))'":
                "MULTIPOINT Z((10 40 66), (40 30 77), (20 20 88), (30 10 99))",
            "'MULTIPOLYGON(((30 20 11, 45 40 11, 10 40 11, 30 20 11)), " \
            "((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))'":
                "MULTIPOLYGON Z(((30 20 11, 10 40 11, 45 40 11, 30 20 11)), " \
                "((15 5 11, 5 10 11, 10 20 11, 40 10 11, 15 5 11)))",
            "'MULTILINESTRING((10 10 11, 20 20 11, 10 40 11), " \
            "(40 40 11, 30 30 11, 40 20 11, 30 10 11))'":
                "MULTILINESTRING Z((10 40 11, 20 20 11, 10 10 11), " \
                "(30 10 11, 40 20 11, 30 30 11, 40 40 11))",
            "'MULTIPOLYGON(((40 40 11, 20 45 11, 45 30 11, 40 40 11)), " \
            "((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11)," \
            "(30 20 11, 20 15 11, 20 25 11, 30 20 11)))'":
                "MULTIPOLYGON Z(((40 40 11, 45 30 11, 20 45 11, 40 40 11)), " \
                "((20 35 11, 45 20 11, 30 5 11, 10 10 11, 10 30 11, 20 35 11), " \
                "(30 20 11, 20 25 11, 20 15 11, 30 20 11)))",
            "'POLYGON((0 0 11, 0 5 11, 5 5 11, 5 0 11, 0 0 11), " \
            "(1 1 11, 2 1 11, 2 2 11, 1 2 11, 1 1 11))'":
                "POLYGON Z((0 0 11, 5 0 11, 5 5 11, 0 5 11, 0 0 11), " \
                "(1 1 11, 1 2 11, 2 2 11, 2 1 11, 1 1 11))"
        }
        for input_geom, expected_geom in test_cases.items():
            reversed_geometry = self.spark.sql("select ST_AsText(ST_Reverse(ST_GeomFromText({})))".format(input_geom))
            assert reversed_geometry.take(1)[0][0] == expected_geom

    def calculate_st_is_ring(self, wkt):
        geometry_collected = self.__wkt_list_to_data_frame([wkt]). \
            selectExpr("ST_IsRing(geom) as is_ring") \
            .filter("is_ring is not null").collect()

        return geometry_collected[0][0] if geometry_collected.__len__() != 0 else None

    def calculate_st_remove(self, wkt, index):
        geometry_collected = self.__wkt_list_to_data_frame([wkt]). \
            selectExpr(f"ST_RemovePoint(geom, {index}) as geom"). \
            filter("geom is not null"). \
            selectExpr("ST_AsText(geom)").collect()

        return geometry_collected[0][0] if geometry_collected.__len__() != 0 else None

    def __wkt_pairs_to_data_frame(self, wkt_list: List) -> DataFrame:
        return self.spark.createDataFrame([[wkt.loads(wkt_a), wkt.loads(wkt_b)] for wkt_a, wkt_b in wkt_list],
                                          self.geo_pair_schema)

    def __wkt_list_to_data_frame(self, wkt_list: List) -> DataFrame:
        return self.spark.createDataFrame([[wkt.loads(given_wkt)] for given_wkt in wkt_list], self.geo_schema)

    def __wkt_pair_list_with_index_to_data_frame(self, wkt_list: List) -> DataFrame:
        return self.spark.createDataFrame([[index, wkt.loads(given_wkt)] for index, given_wkt in wkt_list],
                                          self.geo_schema_with_index)

    def test_st_pointonsurface(self):
        tests1 = {
            "'POINT(0 5)'": "POINT (0 5)",
            "'LINESTRING(0 5, 0 10)'": "POINT (0 5)",
            "'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'": "POINT (2.5 2.5)",
            "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'": "POINT Z(0 0 1)"
        }

        for input_geom, expected_geom in tests1.items():
            pointOnSurface = self.spark.sql(
                "select ST_AsText(ST_PointOnSurface(ST_GeomFromText({})))".format(input_geom))
            assert pointOnSurface.take(1)[0][0] == expected_geom

        tests2 = {"'LINESTRING(0 5 1, 0 0 1, 0 10 2)'": "POINT Z(0 0 1)"}

        for input_geom, expected_geom in tests2.items():
            pointOnSurface = self.spark.sql(
                "select ST_AsEWKT(ST_PointOnSurface(ST_GeomFromWKT({})))".format(input_geom))
            assert pointOnSurface.take(1)[0][0] == expected_geom

    def test_st_pointn(self):
        linestring = "'LINESTRING(0 0, 1 2, 2 4, 3 6)'"
        tests = [
            [linestring, 1, "POINT (0 0)"],
            [linestring, 2, "POINT (1 2)"],
            [linestring, -1, "POINT (3 6)"],
            [linestring, -2, "POINT (2 4)"],
            [linestring, 3, "POINT (2 4)"],
            [linestring, 4, "POINT (3 6)"],
            [linestring, 5, None],
            [linestring, -5, None],
            ["'POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))'", 2, None],
            ["'POINT(1 2)'", 1, None]
        ]

        for test in tests:
            point = self.spark.sql(f"select ST_AsText(ST_PointN(ST_GeomFromText({test[0]}), {test[1]}))")
            assert point.take(1)[0][0] == test[2]

    def test_st_force2d(self):
        tests1 = {
            "'POINT(0 5)'": "POINT (0 5)",
            "'POLYGON((0 0 2, 0 5 2, 5 0 2, 0 0 2), (1 1 2, 3 1 2, 1 3 2, 1 1 2))'":
                "POLYGON ((0 0, 0 5, 5 0, 0 0), (1 1, 3 1, 1 3, 1 1))",
            "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'": "LINESTRING (0 5, 0 0, 0 10)"
        }

        for input_geom, expected_geom in tests1.items():
            geom_2d = self.spark.sql(
                "select ST_AsText(ST_Force_2D(ST_GeomFromText({})))".format(input_geom))
            assert geom_2d.take(1)[0][0] == expected_geom

    def test_st_buildarea(self):
        tests = {
            "'MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 20, 10 20, 10 10))'":
                "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10)))",
            "'MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 0, 10 0, 10 10))'":
                "POLYGON ((0 0, 0 10, 10 10, 20 10, 20 0, 10 0, 0 0))",
            "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2))'":
                "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2))",
            "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2), (8 8, 8 12, 12 12, 12 8, 8 8))'":
                "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), ((8 8, 8 12, 12 12, 12 8, 8 8)))",
            "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2), " \
            "(8 8, 8 9, 8 10, 8 11, 8 12, 9 12, 10 12, 11 12, 12 12, 12 11, 12 10, 12 9, 12 8, 11 8, 10 8, 9 8, 8 8))'":
                "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), " \
                "((8 8, 8 9, 8 10, 8 11, 8 12, 9 12, 10 12, 11 12, 12 12, 12 11, 12 10, 12 9, 12 8, 11 8, 10 8, 9 8, 8 8)))",
            "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8),(10 8, 10 12))'":
                "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), ((8 8, 8 12, 12 12, 12 8, 8 8)))",
            "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(10 2, 10 18))'":
                "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2))",
            "'MULTILINESTRING( (0 0, 70 0, 70 70, 0 70, 0 0), (10 10, 10 60, 40 60, 40 10, 10 10), " \
            "(20 20, 20 30, 30 30, 30 20, 20 20), (20 30, 30 30, 30 50, 20 50, 20 30), (50 20, 60 20, 60 40, 50 40, 50 20), " \
            "(50 40, 60 40, 60 60, 50 60, 50 40), (80 0, 110 0, 110 70, 80 70, 80 0), (90 60, 100 60, 100 50, 90 50, 90 60))'":
                "MULTIPOLYGON (((0 0, 0 70, 70 70, 70 0, 0 0), (10 10, 40 10, 40 60, 10 60, 10 10), (50 20, 60 20, 60 40, 60 60, 50 60, 50 40, 50 20)), " \
                "((20 20, 20 30, 20 50, 30 50, 30 30, 30 20, 20 20)), " \
                "((80 0, 80 70, 110 70, 110 0, 80 0), (90 50, 100 50, 100 60, 90 60, 90 50)))"
        }

        for input_geom, expected_geom in tests.items():
            areal_geom = self.spark.sql("select ST_AsText(ST_BuildArea(ST_GeomFromText({})))".format(input_geom))
            assert areal_geom.take(1)[0][0] == expected_geom

    def test_st_line_from_multi_point(self):
        test_cases = {
            "'POLYGON((-1 0 0, 1 0 0, 0 0 1, 0 1 0, -1 0 0))'": None,
            "'LINESTRING(0 0, 1 2, 2 4, 3 6)'": None,
            "'POINT(1 2)'": None,
            "'MULTIPOINT((10 40), (40 30), (20 20), (30 10))'":
                "LINESTRING (10 40, 40 30, 20 20, 30 10)",
            "'MULTIPOINT((10 40 66), (40 30 77), (20 20 88), (30 10 99))'":
                "LINESTRING Z(10 40 66, 40 30 77, 20 20 88, 30 10 99)"
        }
        for input_geom, expected_geom in test_cases.items():
            line_geometry = self.spark.sql(
                "select ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText({})))".format(input_geom))
            assert line_geometry.take(1)[0][0] == expected_geom

    def test_st_locate_along(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('MULTILINESTRING M((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))') as geom")
        actual = baseDf.selectExpr("ST_AsText(ST_LocateAlong(geom, 2))").take(1)[0][0]
        expected = "MULTIPOINT M((3 4 2))"
        assert expected == actual

        actual = baseDf.selectExpr("ST_AsText(ST_LocateAlong(geom, 2, -3))").take(1)[0][0]
        expected = "MULTIPOINT M((5.121320343559642 1.8786796564403572 2), (3 1 2))"
        assert expected == actual

    def test_st_longest_line(self):
        basedf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
        actual = basedf.selectExpr("ST_AsText(ST_LongestLine(geom, geom))").take(1)[0][0]
        expected = "LINESTRING (180 180, 20 50)"
        assert expected == actual

    def test_st_max_distance(self):
        basedf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
        actual = basedf.selectExpr("ST_MaxDistance(geom, geom)").take(1)[0][0]
        expected = 206.15528128088303
        assert expected == actual

    def test_st_s2_cell_ids(self):
        test_cases = [
            "'POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))'",
            "'LINESTRING(0 0, 1 2, 2 4, 3 6)'",
            "'POINT(1 2)'"
        ]
        for input_geom in test_cases:
            cell_ids = self.spark.sql("select ST_S2CellIDs(ST_GeomFromText({}), 6)".format(input_geom)).take(1)[0][0]
            assert isinstance(cell_ids, list)
            assert isinstance(cell_ids[0], int)
        # test null case
        cell_ids = self.spark.sql("select ST_S2CellIDs(null, 6)").take(1)[0][0]
        assert cell_ids is None

    def test_st_s2_to_geom(self):
        df = self.spark.sql("""
        SELECT
            ST_Intersects(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), ST_S2ToGeom(ST_S2CellIDs(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), 10))[0]),
            ST_Intersects(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), ST_S2ToGeom(ST_S2CellIDs(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), 10))[1]),
            ST_Intersects(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), ST_S2ToGeom(ST_S2CellIDs(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), 10))[2])
        """)
        res1, res2, res3 = df.take(1)[0]
        assert res1 and res2 and res3

    def test_st_h3_cell_ids(self):
        test_cases = [
            "'POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))'",
            "'LINESTRING(0 0, 1 2, 2 4, 3 6)'",
            "'POINT(1 2)'"
        ]
        for input_geom in test_cases:
            cell_ids = self.spark.sql("select ST_H3CellIDs(ST_GeomFromText({}), 6, true)".format(input_geom)).take(1)[0][0]
            assert isinstance(cell_ids, list)
            assert isinstance(cell_ids[0], int)
        # test null case
        cell_ids = self.spark.sql("select ST_H3CellIDs(null, 6, true)").take(1)[0][0]
        assert cell_ids is None

    def test_st_h3_cell_distance(self):
        df = self.spark.sql("select ST_H3CellDistance(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[0], ST_H3CellIDs(ST_GeomFromWKT('POINT(1.23 1.59)'), 8, true)[0])")
        assert df.take(1)[0][0] == 78

    def test_st_h3_kring(self):
        df = self.spark.sql("""
        SELECT
            ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[0], 1, true) exactRings,
            ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[0], 1, false) allRings,
            ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true) original_cells
        """)
        exact_rings, all_rings, original_cells = df.take(1)[0]
        assert set(exact_rings + original_cells) == set(all_rings)

    def test_st_h3_togeom(self):
        df = self.spark.sql("""
        SELECT
            ST_Intersects(
                ST_H3ToGeom(ST_H3CellIDs(ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))'), 6, true))[10],
                ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))')
            ),
            ST_Intersects(
                ST_H3ToGeom(ST_H3CellIDs(ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))'), 6, false))[25],
                ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))')
            ),
            ST_Intersects(
                ST_H3ToGeom(ST_H3CellIDs(ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))'), 6, false))[50],
                ST_GeomFromText('POLYGON((-1 0, 1 0, 0 0, 0 1, -1 0))')
            )
        """)
        res1, res2, res3 = df.take(1)[0]
        assert res1 and res2 and res3

    def test_st_numPoints(self):
        actual = self.spark.sql("SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'))").take(1)[0][0]
        expected = 3
        assert expected == actual

    def test_force3D(self):
        expected = 3
        actualDf = self.spark.sql("SELECT ST_Force3D(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'), 1.1) AS geom")
        actual = actualDf.selectExpr("ST_NDims(geom)").take(1)[0][0]
        assert expected == actual

    def test_force3DM(self):
        actualDf = self.spark.sql("SELECT ST_Force3DM(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'), 1.1) AS geom")
        actual = actualDf.selectExpr("ST_HasM(geom)").take(1)[0][0]
        assert actual

    def test_force3DZ(self):
        expected = 3
        actualDf = self.spark.sql("SELECT ST_Force3DZ(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'), 1.1) AS geom")
        actual = actualDf.selectExpr("ST_NDims(geom)").take(1)[0][0]
        assert expected == actual

    def test_force4D(self):
        expected = 4
        actualDf = self.spark.sql("SELECT ST_Force4D(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'), 1.1, 1.1) AS geom")
        actual = actualDf.selectExpr("ST_NDims(geom)").take(1)[0][0]
        assert expected == actual

    def test_st_force_collection(self):
        basedf = self.spark.sql("SELECT ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)') AS mpoint, ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS poly")
        actual = basedf.selectExpr("ST_NumGeometries(ST_ForceCollection(mpoint))").take(1)[0][0]
        assert actual == 6

        actual = basedf.selectExpr("ST_NumGeometries(ST_ForceCollection(poly))").take(1)[0][0]
        assert actual == 1

    def test_forcePolygonCW(self):
        actualDf = self.spark.sql("SELECT ST_ForcePolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')) AS polyCW")
        actual = actualDf.selectExpr("ST_AsText(polyCW)").take(1)[0][0]
        expected = "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
        assert expected == actual

    def test_forceRHR(self):
        actualDf = self.spark.sql("SELECT ST_ForceRHR(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')) AS polyCW")
        actual = actualDf.selectExpr("ST_AsText(polyCW)").take(1)[0][0]
        expected = "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
        assert expected == actual

    def test_generate_points(self):
        actual = self.spark.sql("SELECT ST_NumGeometries(ST_GeneratePoints(ST_Buffer(ST_GeomFromWKT('LINESTRING(50 50,150 150,150 50)'), 10, false, 'endcap=round join=round'), 15))")\
        .first()[0]
        assert actual == 15

        actual = self.spark.sql(
            "SELECT ST_AsText(ST_ReducePrecision(ST_GeneratePoints(ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))'), 5, 10), 5))") \
            .first()[0]
        expected = "MULTIPOINT ((53.82582 2.57803), (13.55212 2.44117), (59.12854 3.70611), (61.37698 7.14985), (10.49657 4.40622))"
        assert expected == actual

        actual = self.spark.sql(
            "SELECT ST_NumGeometries(ST_GeneratePoints(ST_Buffer(ST_GeomFromWKT('LINESTRING(50 50,150 150,150 50)'), 10, false, 'endcap=round join=round'), 15, 100))") \
            .first()[0]
        assert actual == 15

        actual = self.spark.sql("SELECT ST_NumGeometries(ST_GeneratePoints(ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))'), 30))")\
        .first()[0]
        assert actual == 30

    def test_nRings(self):
        expected = 1
        actualDf = self.spark.sql("SELECT ST_GeomFromText('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom")
        actual = actualDf.selectExpr("ST_NRings(geom)").take(1)[0][0]
        assert expected == actual

    def test_trangulatePolygon(self):
        baseDf = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))') as poly")
        actual = baseDf.selectExpr("ST_AsText(ST_TriangulatePolygon(poly))").take(1)[0][0]
        expected = "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))"
        assert actual == expected

    def test_translate(self):
        expected = "POLYGON ((3 5, 3 6, 4 6, 4 5, 3 5))"
        actual_df = self.spark.sql(
            "SELECT ST_Translate(ST_GeomFromText('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 2, 5) AS geom")
        actual = actual_df.selectExpr("ST_AsText(geom)").take(1)[0][0]
        assert expected == actual

    def test_voronoiPolygons(self):
        expected = "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))"
        actual_df = self.spark.sql("SELECT ST_VoronoiPolygons(ST_GeomFromText('MULTIPOINT (0 0, 2 2)')) AS geom")
        actual = actual_df.selectExpr("ST_AsText(geom)").take(1)[0][0]
        assert expected == actual

    def test_frechetDistance(self):
        expected = 5.0990195135927845
        actual_df = self.spark.sql("SELECT ST_FrechetDistance(ST_GeomFromText('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, "
                                   "5 0)'), ST_GeomFromText('POINT (0 1)'))")
        actual = actual_df.take(1)[0][0]
        assert expected == actual

    def test_affine(self):
        expected = "POLYGON Z((2 3 1, 4 5 1, 7 8 2, 2 3 1))"
        actual_df = self.spark.sql("SELECT ST_Affine(ST_GeomFromText('POLYGON ((1 0 1, 1 1 1, 2 2 2, 1 0 1))'), 1, 2, "
                                   "1, 2, 1, 2) AS geom")
        actual = actual_df.selectExpr("ST_AsText(geom)").take(1)[0][0]
        assert expected == actual

    def test_st_ashexewkb(self):
        expected = "0101000000000000000000F03F0000000000000040"
        actual_df = self.spark.sql("SELECT ST_GeomFromText('POINT(1 2)') as point")
        actual = actual_df.selectExpr("ST_AsHEXEWKB(point)").take(1)[0][0]
        assert expected == actual

        expected = "00000000013FF00000000000004000000000000000"
        actual = actual_df.selectExpr("ST_AsHEXEWKB(point, 'XDR')").take(1)[0][0]
        assert expected == actual

    def test_boundingDiagonal(self):
        expected = "LINESTRING (1 0, 2 1)"
        actual_df = self.spark.sql("SELECT ST_BoundingDiagonal(ST_GeomFromText('POLYGON ((1 0, 1 1, 2 1, 2 0, "
                                   "1 0))')) AS geom")
        actual = actual_df.selectExpr("ST_AsText(geom)").take(1)[0][0]
        assert expected == actual

    def test_angle(self):
        expectedDegrees = 11.309932474020195
        expectedRad = 0.19739555984988044
        actual_df = self.spark.sql("SELECT ST_Angle(ST_GeomFromText('LINESTRING (0 0, 1 1)'), ST_GeomFromText('LINESTRING (0 0, 3 2)')) AS angleRad")
        actualRad = actual_df.take(1)[0][0]
        actualDegrees = actual_df.selectExpr("ST_Degrees(angleRad)").take(1)[0][0]
        assert math.isclose(expectedRad, actualRad, rel_tol=1e-9)
        assert math.isclose(expectedDegrees, actualDegrees, rel_tol=1e-9)
    def test_hausdorffDistance(self):
        expected = 5.0
        actual_df = self.spark.sql("SELECT ST_HausdorffDistance(ST_GeomFromText('POLYGON ((1 0 1, 1 1 2, 2 1 5, "
                                   "2 0 1, 1 0 1))'), ST_GeomFromText('POLYGON ((4 0 4, 6 1 4, 6 4 9, 6 1 3, "
                                   "4 0 4))'), 0.5)")
        actual_df_default = self.spark.sql("SELECT ST_HausdorffDistance(ST_GeomFromText('POLYGON ((1 0 1, 1 1 2, "
                                           "2 1 5, "
                                           "2 0 1, 1 0 1))'), ST_GeomFromText('POLYGON ((4 0 4, 6 1 4, 6 4 9, 6 1 3, "
                                           "4 0 4))'))")
        actual = actual_df.take(1)[0][0]
        actual_default = actual_df_default.take(1)[0][0]
        assert expected == actual
        assert expected == actual_default

    def test_st_coord_dim(self):

        point_df = self.spark.sql("SELECT ST_GeomFromWkt('POINT(7 8 6)') AS geom")
        point_row = [pt_row[0] for pt_row in point_df.selectExpr("ST_CoordDim(geom)").collect()]
        assert(point_row == [3])
