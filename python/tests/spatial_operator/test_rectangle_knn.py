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

from shapely.geometry import LineString, Point, Polygon
from tests.test_base import TestBase
from tests.tools import distance_sorting_functions, tests_resource

from sedona.core.enums import FileDataSplitter, IndexType
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import KNNQuery
from sedona.core.SpatialRDD import RectangleRDD

inputLocation = os.path.join(tests_resource, "zcta510-small.csv")
queryWindowSet = os.path.join(tests_resource, "zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.001
queryPolygonSet = os.path.join(tests_resource, "primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = Envelope(-171.090042, 145.830505, -14.373765, 49.00127)
matchCount = 17599
matchWithOriginalDuplicatesCount = 17738


class TestRectangleKNN(TestBase):
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)
    loop_times = 1
    query_point = Point(-84.01, 34.01)
    top_k = 100
    query_polygon = Polygon(
        [
            (-84.01, 34.01),
            (-84.01, 34.11),
            (-83.91, 34.11),
            (-83.91, 34.01),
            (-84.01, 34.01),
        ]
    )
    query_line = LineString(
        [(-84.01, 34.01), (-84.01, 34.11), (-83.91, 34.11), (-83.91, 34.01)]
    )

    def test_spatial_knn_query(self):
        rectangle_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(
                rectangle_rdd, self.query_point, self.top_k, False
            )

            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        rectangle_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True)
        rectangle_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(
                rectangle_rdd, self.query_point, self.top_k, False
            )

            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_correctness(self):
        rectangle_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True)

        result_no_index = KNNQuery.SpatialKnnQuery(
            rectangle_rdd, self.query_point, self.top_k, False
        )
        rectangle_rdd.buildIndex(IndexType.RTREE, False)

        result_with_index = KNNQuery.SpatialKnnQuery(
            rectangle_rdd, self.query_point, self.top_k, True
        )

        sorted_result_no_index = sorted(
            result_no_index,
            key=lambda geo_data: distance_sorting_functions(geo_data, self.query_point),
        )

        sorted_result_with_index = sorted(
            result_with_index,
            key=lambda geo_data: distance_sorting_functions(geo_data, self.query_point),
        )

        difference = 0
        for x in range(self.top_k):
            difference += sorted_result_no_index[x].geom.distance(
                sorted_result_with_index[x].geom
            )

        assert difference == 0

    def test_spatial_knn_using_polygon(self):
        rectangle_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True)

        result_no_index = KNNQuery.SpatialKnnQuery(
            rectangle_rdd, self.query_polygon, self.top_k, False
        )

        assert len(result_no_index) > 0

    def test_spatial_knn_using_linestring(self):
        rectangle_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True)

        result_no_index = KNNQuery.SpatialKnnQuery(
            rectangle_rdd, self.query_line, self.top_k, False
        )

        assert len(result_no_index) > 0
