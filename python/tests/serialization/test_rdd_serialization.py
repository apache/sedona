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

from sedona.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD, LineStringRDD
from sedona.core.enums import FileDataSplitter, IndexType
from tests.test_base import TestBase
from tests.tools import tests_resource

point_rdd_input_location = os.path.join(tests_resource, "arealm-small.csv")
polygon_rdd_input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
linestring_rdd_input_location = os.path.join(tests_resource, "primaryroads-linestring.csv")
linestring_rdd_splittter = FileDataSplitter.CSV

polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9

point_rdd_offset = 1
point_rdd_splitter = FileDataSplitter.CSV


class TestRDDSerialization(TestBase):

    def test_point_rdd(self):
        point_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )

        collected_points = point_rdd.getRawSpatialRDD().collect()

        points_coordinates = [
            [-88.331492, 32.324142], [-88.175933, 32.360763],
            [-88.388954, 32.357073], [-88.221102, 32.35078]
        ]

        assert [[geo_data.geom.x, geo_data.geom.y] for geo_data in collected_points[:4]] == points_coordinates[:4]

    def test_polygon_rdd(self):
        polygon_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=polygon_rdd_input_location,
            startOffset=polygon_rdd_start_offset,
            endOffset=polygon_rdd_end_offset,
            splitter=polygon_rdd_splitter,
            carryInputData=True
        )

        collected_polygon_rdd = polygon_rdd.getRawSpatialRDD().collect()

        input_wkt_polygons = [
            "POLYGON ((-74.020753 40.836454, -74.020753 40.843768, -74.018162 40.843768, -74.018162 40.836454, -74.020753 40.836454))",
            "POLYGON ((-74.018978 40.837712, -74.018978 40.852181, -74.014938 40.852181, -74.014938 40.837712, -74.018978 40.837712))",
            "POLYGON ((-74.021683 40.833253, -74.021683 40.834288, -74.021368 40.834288, -74.021368 40.833253, -74.021683 40.833253))"
        ]

        assert [geo_data.geom.wkt for geo_data in collected_polygon_rdd][:3] == input_wkt_polygons

    # def test_circle_rdd(self):
    #     object_rdd = PointRDD(
    #         sparkContext=self.sc,
    #         InputLocation=point_rdd_input_location,
    #         Offset=point_rdd_offset,
    #         splitter=point_rdd_splitter,
    #         carryInputData=False
    #     )
    #     circle_rdd = CircleRDD(object_rdd, 0.1)
    #     collected_data = circle_rdd.getRawSpatialRDD().collect()
    #     print([geo_data.geom.wkt for geo_data in collected_data])

    def test_linestring_rdd(self):
        linestring_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=linestring_rdd_input_location,
            startOffset=0,
            endOffset=7,
            splitter=FileDataSplitter.CSV,
            carryInputData=True
        )

        wkt = "LINESTRING (-112.506968 45.98186, -112.506968 45.983586, -112.504872 45.983586, -112.504872 45.98186)"
        collected_linestring_rdd = linestring_rdd.getRawSpatialRDD().collect()

        assert wkt == collected_linestring_rdd[0].geom.wkt

    def test_rectangle_rdd(self):
        pass
