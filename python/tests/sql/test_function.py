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
from pyspark.sql import DataFrame
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
        function_df = self.spark.sql("select ST_Buffer(polygondf.countyshape, 1) from polygondf")
        function_df.show()

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
        function_df = self.spark.sql("select ST_Transform(ST_FlipCoordinates(polygondf.countyshape), 'epsg:4326','epsg:3857', false) from polygondf")
        function_df.show()

    def test_st_intersection_intersects_but_not_contains(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersect = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersect.take(1)[0][0].wkt == "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"

    def test_st_intersection_intersects_but_left_contains_right(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_intersects_but_right_contains_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_not_intersects(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON EMPTY"

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
            """SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)""")
        test_table.show(truncate=False)
        assert test_table.take(1)[0][0].x == 0.12345679
        test_table = self.spark.sql(
            """SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)""")
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
            ("MULTIPOLYGON (((30 20 11, 45 40 11, 10 40 11, 30 20 11)), ((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))",),
            ("MULTILINESTRING ((10 10 11, 20 20 11, 10 40 11), (40 40 11, 30 30 11, 40 20 11, 30 10 11))",),
            ("MULTIPOLYGON (((40 40 11, 20 45 11, 45 30 11, 40 40 11)), ((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11), (30 20 11, 20 15 11, 20 25 11, 30 20 11)))",),
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
        test = self.spark.sql("SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")

    def test_st_geometry_type(self):
        test = self.spark.sql("SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")

    def test_st_difference_right_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"

    def test_st_difference_right_not_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))"

    def test_st_difference_left_contains_right(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"

    def test_st_difference_right_not_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as b")
        test_table.createOrReplaceTempView("test_diff")
        diff = self.spark.sql("select ST_Difference(a,b) from test_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON EMPTY"

    def test_st_sym_difference_part_of_right_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][0].wkt == "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"

    def test_st_sym_difference_not_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][0].wkt == "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"

    def test_st_sym_difference_contains(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
        test_table.createOrReplaceTempView("test_sym_diff")
        diff = self.spark.sql("select ST_SymDifference(a,b) from test_sym_diff")
        assert diff.take(1)[0][0].wkt == "POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"

    def test_st_union_part_of_right_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') as b")
        test_table.createOrReplaceTempView("test_union")
        union = self.spark.sql("select ST_Union(a,b) from test_union")
        assert union.take(1)[0][0].wkt == "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"

    def test_st_union_not_overlaps_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
        test_table.createOrReplaceTempView("test_union")
        union = self.spark.sql("select ST_Union(a,b) from test_union")
        assert union.take(1)[0][0].wkt == "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"

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

        assert([point[0] for point in points] == [-71.064544, -88.331492, 88.331492, 1.0453, 32.324142])

        assert(not linestrings.count())

        assert(not polygons.count())

    def test_st_y(self):
        point_df = create_sample_points_df(self.spark, 5)
        polygon_df = create_sample_polygons_df(self.spark, 5)
        linestring_df = create_sample_lines_df(self.spark, 5)

        points = point_df \
            .selectExpr("ST_Y(geom)").collect()

        polygons = polygon_df.selectExpr("ST_Y(geom) as y").filter("y IS NOT NULL")

        linestrings = linestring_df.selectExpr("ST_Y(geom) as y").filter("y IS NOT NULL")

        assert([point[0] for point in points] == [42.28787, 32.324142, 32.324142, 5.3324324, -88.331492])

        assert(not linestrings.count())

        assert(not polygons.count())

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

        assert([point[0] for point in points] == [3.3])

        assert(not linestrings.count())

        assert(not polygons.count())

    def test_st_z_max(self):
        linestring_df = self.spark.sql("SELECT ST_GeomFromWKT('LINESTRING Z (0 0 1, 0 1 2)') as geom")
        linestring_row = [lnstr_row[0] for lnstr_row in linestring_df.selectExpr("ST_ZMax(geom)").collect()]
        assert(linestring_row == [2.0])

    def test_st_z_min(self):
        linestring_df = self.spark.sql("SELECT ST_GeomFromWKT('POLYGON Z ((0 0 2, 0 1 1, 1 1 2, 1 0 2, 0 0 2))') as geom")
        linestring_row = [lnstr_row[0] for lnstr_row in linestring_df.selectExpr("ST_ZMin(geom)").collect()]
        assert(linestring_row == [1.0])

    def test_st_n_dims(self):
        point_df = self.spark.sql("SELECT ST_GeomFromWKT('POINT(1 1 2)') as geom")
        point_row = [pt_row[0] for pt_row in point_df.selectExpr("ST_NDims(geom)").collect()]
        assert(point_row == [3])


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

        assert([line[0] for line in linestrings.collect()] == [wkt.loads(el) for el in expected_points])

        assert(not points.count())

        assert(not polygons.count())

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

        assert([wkt_row[0]
                for wkt_row in point_data_frame.selectExpr("ST_AsText(geom)").collect()] == expected_ending_points)

        assert(empty_dataframe.count() == 0)

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
        assert(boundary_wkt == [
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

        assert(linestring_wkt == ["LINESTRING (0 0, 0 1, 1 1, 1 0, 0 0)", "LINESTRING (0 0, 1 1, 1 2, 1 1, 0 0)"])

        assert(not empty_df.count())

    def test_st_geometry_n(self):
        data_frame = self.__wkt_list_to_data_frame(["MULTIPOINT((1 2), (3 4), (5 6), (8 9))"])
        wkts = [data_frame.selectExpr(f"ST_GeometryN(geom, {i}) as geom").selectExpr("st_asText(geom)").collect()[0][0]
                for i in range(0, 4)]

        assert(wkts == ["POINT (1 2)", "POINT (3 4)", "POINT (5 6)", "POINT (8 9)"])

    def test_st_interior_ring_n(self):
        polygon_df = self.__wkt_list_to_data_frame(
            ["POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))"]
        )

        other_geometry = create_sample_points_df(self.spark, 5).union(create_sample_lines_df(self.spark, 5))
        wholes = [polygon_df.selectExpr(f"ST_InteriorRingN(geom, {i}) as geom").
                      selectExpr("ST_AsText(geom)").collect()[0][0]
                  for i in range(3)]

        empty_df = other_geometry.selectExpr("ST_InteriorRingN(geom, 1) as geom").filter("geom IS NOT NULL")

        assert(not empty_df.count())
        assert(wholes == ["LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)",
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

        assert(dumped_geometries.select(explode(col("geom"))).count() == 14)

        collected_geometries = dumped_geometries \
            .select(explode(col("geom")).alias("geom")) \
            .selectExpr("ST_AsText(geom) as geom") \
            .collect()

        assert([geom_row[0] for geom_row in collected_geometries] == expected_geometries)

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

        assert(dumped_points.count() == 10)

        collected_points = [geom_row[0] for geom_row in dumped_points.selectExpr("ST_AsText(geom)").collect()]
        assert(collected_points == expected_points)

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
            (10, "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))")
        ]

        geometry_df = self.__wkt_pair_list_with_index_to_data_frame(geometry_list)
        is_closed = geometry_df.selectExpr("index", "ST_IsClosed(geom)").collect()
        is_closed_collected = [[*row] for row in is_closed]
        assert(is_closed_collected == expected_result)

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
            (10, "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
            (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")]

        geometry_df = self.__wkt_pair_list_with_index_to_data_frame(geometries)

        number_of_interior_rings = geometry_df.selectExpr("index", "ST_NumInteriorRings(geom) as num")
        collected_interior_rings = [[*row] for row in number_of_interior_rings.filter("num is not null").collect()]
        assert(collected_interior_rings == [[2, 0], [11, 1]])

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
            ("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))", "Point(21 52)"),
            ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)")
        ]
        geometry_df = self.__wkt_pairs_to_data_frame(geometry)
        modified_geometries = geometry_df.selectExpr("ST_AddPoint(geomA, geomB) as geom")
        collected_geometries = [
            row[0] for row in modified_geometries.filter("geom is not null").selectExpr("ST_AsText(geom)").collect()
        ]
        assert(collected_geometries[0] == "LINESTRING (0 0, 1 1, 1 0, 21 52)")

    def test_st_remove_point(self):
        result_and_expected = [
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 0), "LINESTRING (1 1, 1 0, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 1), "LINESTRING (0 0, 1 0, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 2), "LINESTRING (0 0, 1 1, 0 0)"],
            [self.calculate_st_remove("Linestring(0 0, 1 1, 1 0, 0 0)", 3), "LINESTRING (0 0, 1 1, 1 0)"],
            [self.calculate_st_remove("POINT(0 1)", 3), None],
            [self.calculate_st_remove("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", 3), None],
            [self.calculate_st_remove("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))", 0), None],
            [self.calculate_st_remove("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 3), None],
            [self.calculate_st_remove("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", 3), None]
        ]
        for actual, expected in result_and_expected:
            assert(actual == expected)

    def test_st_is_ring(self):
        result_and_expected = [
            [self.calculate_st_is_ring("LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)"), False],
            [self.calculate_st_is_ring("LINESTRING(2 0, 2 2, 3 3)"), False],
            [self.calculate_st_is_ring("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"), True],
            [self.calculate_st_is_ring("POINT (21 52)"), None],
            [self.calculate_st_is_ring("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"), None],
        ]
        for actual, expected in result_and_expected:
            assert(actual == expected)

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
        geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected").\
            show()
        result = geom_poly.filter("polygon IS NOT NULL").selectExpr("ST_AsText(polygon)", "expected").\
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
        geohash_df = geometry_df.withColumn("geohash", expr("ST_GeoHash(geom, 10)")).\
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
        assert(set([el[0] for el in  geometry_df_collected.selectExpr("ST_AsText(collected)").collect()]) == {
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
        assert(set([el[0] for el in  geometry_df_collected.selectExpr("ST_AsText(collected)").collect()]) == {
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
        return self.spark.createDataFrame([[wkt.loads(wkt_a), wkt.loads(wkt_b)] for wkt_a, wkt_b in wkt_list], self.geo_pair_schema)

    def __wkt_list_to_data_frame(self, wkt_list: List) -> DataFrame:
        return self.spark.createDataFrame([[wkt.loads(given_wkt)] for given_wkt in wkt_list], self.geo_schema)

    def __wkt_pair_list_with_index_to_data_frame(self, wkt_list: List) -> DataFrame:
        return self.spark.createDataFrame([[index, wkt.loads(given_wkt)] for index, given_wkt in wkt_list], self.geo_schema_with_index)

    def test_st_pointonsurface(self):
        tests1 = {
        "'POINT(0 5)'":"POINT (0 5)",
        "'LINESTRING(0 5, 0 10)'":"POINT (0 5)",
        "'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'":"POINT (2.5 2.5)",
        "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'":"POINT Z(0 0 1)"
        }

        for input_geom, expected_geom in tests1.items():
            pointOnSurface = self.spark.sql("select ST_AsText(ST_PointOnSurface(ST_GeomFromText({})))".format(input_geom))
            assert pointOnSurface.take(1)[0][0] == expected_geom

        tests2 = { "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'":"POINT Z(0 0 1)" }

        for input_geom, expected_geom in tests2.items():
            pointOnSurface = self.spark.sql("select ST_AsEWKT(ST_PointOnSurface(ST_GeomFromWKT({})))".format(input_geom))
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
            line_geometry = self.spark.sql("select ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText({})))".format(input_geom))
            assert line_geometry.take(1)[0][0] == expected_geom

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
