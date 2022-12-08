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

from tests import csv_point_input_location, area_lm_point_input_location, mixed_wkt_geometry_input_location, \
    mixed_wkb_geometry_input_location, geojson_input_location
from tests.test_base import TestBase


class TestConstructors(TestBase):

    def test_st_point(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        assert point_df.count() == 1000

    def test_st_point_3d(self):
        point_df = self.spark.sql("SELECT ST_Point(1.2345, 2.3456, 3.4567)")
        assert point_df.count() == 1

    def test_st_point_from_text(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(area_lm_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show(truncate=False)

        point_df = self.spark.sql("select ST_PointFromText(concat(_c0,',',_c1),',') as arealandmark from pointtable")
        assert point_df.count() == 121960

    def test_st_geom_from_wkt(self):
        polygon_wkt_df = self.spark.read.format("csv").\
          option("delimiter", "\t").\
          option("header", "false").\
          load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWkt(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_wkt_3d(self):
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
        polygon_df = self.spark.sql("select ST_GeomFromWkt(wkt) as geomn from input_wkt")
        assert polygon_df.count() == 8

    def test_st_geom_from_text(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromText(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_wkb(self):
        polygon_wkb_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkb_geometry_input_location)

        polygon_wkb_df.createOrReplaceTempView("polygontable")
        polygon_wkb_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_geojson(self):
        polygon_json_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(geojson_input_location)

        polygon_json_df.createOrReplaceTempView("polygontable")
        polygon_json_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromGeoJSON(polygontable._c0) as countyshape from polygontable")
        polygon_df.show()
        assert polygon_df.count() == 1000

    def test_line_from_text(self) :
        input_df = self.spark.createDataFrame([("LineString(1 2, 3 4)",)], ["wkt"])
        input_df.createOrReplaceTempView("input_wkt")
        line_df = self.spark.sql("select ST_LineFromText(wkt) as geom from input_wkt")
        assert line_df.count() == 1

    def test_Mline_from_text(self) :
        input_df = self.spark.createDataFrame([("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))",)], ["wkt"])
        input_df.createOrReplaceTempView("input_wkt")
        line_df = self.spark.sql("select ST_MLineFromText(wkt) as geom from input_wkt")
        assert line_df.count() == 1

    def test_MPoly_from_text(self) :
        input_df = self.spark.createDataFrame([("MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))",)], ["wkt"])
        input_df.createOrReplaceTempView("input_wkt")
        line_df = self.spark.sql("select ST_MPolyFromText(wkt) as geom from input_wkt")
        assert line_df.count() == 1