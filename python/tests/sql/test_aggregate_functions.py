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

from shapely.geometry import Polygon

from tests import csv_point_input_location, union_polygon_input_location
from tests.test_base import TestBase


class TestConstructors(TestBase):

    def test_st_envelope_aggr(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        boundary = self.spark.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")

        coordinates = [
            (1.1, 101.1),
            (1.1, 1100.1),
            (1000.1, 1100.1),
            (1000.1, 101.1),
            (1.1, 101.1)
        ]

        polygon = Polygon(coordinates)

        assert boundary.take(1)[0][0] == polygon

    def test_st_union_aggr(self):
        polygon_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(union_polygon_input_location)

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        union = self.spark.sql("select ST_Union_Aggr(polygondf.polygonshape) from polygondf")

        assert union.take(1)[0][0].area == 10100
