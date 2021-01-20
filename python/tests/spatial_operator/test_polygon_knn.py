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

from shapely.geometry import Point

from sedona.core.SpatialRDD import PolygonRDD
from sedona.core.enums import IndexType, FileDataSplitter
from sedona.core.spatialOperator import KNNQuery
from tests.test_base import TestBase
from tests.tools import tests_resource, distance_sorting_functions

input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"


class TestPolygonKnn(TestBase):

    loop_times = 5
    top_k = 100
    query_point = Point(-84.01, 34.01)

    def test_spatial_knn_query(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, False)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, True)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_correctness(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)
        result_no_index = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, False)
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        result_with_index = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, True)

        sorted_result_no_index = sorted(result_no_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        sorted_result_with_index = sorted(result_with_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        difference = 0
        for x in range(self.top_k):
            difference += sorted_result_no_index[x].geom.distance(sorted_result_with_index[x].geom)

        assert difference == 0
