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

from sedona.core.SpatialRDD import PointRDD
from sedona.core.enums import IndexType, FileDataSplitter
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery
from tests.test_base import TestBase
from tests.tools import tests_resource

input_location = os.path.join(tests_resource, "arealm-small.csv")
queryWindowSet = os.path.join(tests_resource, "zcta510-small.csv")
offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.01
queryPolygonSet = "primaryroads-polygon.csv"
inputCount = 3000
inputBoundary = Envelope(
    minx=-173.120769,
    maxx=-84.965961,
    miny=30.244859,
    maxy=71.355134
)
rectangleMatchCount = 103
rectangleMatchWithOriginalDuplicatesCount = 103
polygonMatchCount = 472
polygonMatchWithOriginalDuplicatesCount = 562


class TestPointRange(TestBase):
    loop_times = 5
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)

    def test_spatial_range_query(self):
        spatial_rdd = PointRDD(self.sc, input_location, offset, splitter, False)
        for i in range(self.loop_times):
            result_size = RangeQuery.\
                SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False)\
                .count()
            assert result_size == 2830

    def test_spatial_range_query_using_index(self):
        spatial_rdd = PointRDD(self.sc, input_location, offset, splitter, False)

        spatial_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result_size = RangeQuery.\
                SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False)\
                .count()
            assert result_size == 2830
