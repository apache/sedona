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

from tests.properties.crs_transform import (
    input_location,
    offset,
    splitter,
    loop_times,
    query_envelope,
)
from tests.test_base import TestBase

from sedona.spark.core.spatialOperator import RangeQuery
from sedona.spark.core.SpatialRDD import PointRDD


class TestCrsTransformation(TestBase):

    def test_spatial_range_query(self):
        spatial_rdd = PointRDD(self.sc, input_location, offset, splitter, True)
        spatial_rdd.flipCoordinates()
        spatial_rdd.CRSTransform("epsg:4326", "epsg:3005")

        for i in range(loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd, query_envelope, False, False
            ).count()
            assert result_size == 3127

        assert (
            RangeQuery.SpatialRangeQuery(spatial_rdd, query_envelope, False, False)
            .take(10)[1]
            .getUserData()
            is not None
        )
