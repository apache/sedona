# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

from shapely.geometry import Point

from tests import tests_resource
from tests.test_base import TestBase

from sedona.spark.core.enums import FileDataSplitter, IndexType
from sedona.spark.core.spatialOperator import KNNQuery
from sedona.spark.core.SpatialRDD import LineStringRDD

input_location = os.path.join(tests_resource, "primaryroads-linestring.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"


class TestLineStringKnn(TestBase):

    loop_times = 1
    query_point = Point(-84.01, 34.01)

    def test_spatial_knn_query(self):
        line_string_rdd = LineStringRDD(self.sc, input_location, splitter, True)
        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(
                line_string_rdd, self.query_point, 5, False
            )
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        line_string_rdd = LineStringRDD(self.sc, input_location, splitter, True)
        line_string_rdd.buildIndex(IndexType.RTREE, False)
        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(
                line_string_rdd, self.query_point, 5, False
            )
            assert result.__len__() > -1
            assert result[0].getUserData() is not None
