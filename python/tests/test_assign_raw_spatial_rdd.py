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

from sedona.core.SpatialRDD import PointRDD
from tests.properties.point_properties import input_location, offset, splitter, num_partitions
from tests.test_base import TestBase
from pyspark import StorageLevel


class TestSpatialRddAssignment(TestBase):

    def test_raw_spatial_rdd_assignment(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()

        empty_point_rdd = PointRDD()
        empty_point_rdd.rawSpatialRDD = spatial_rdd.rawSpatialRDD
        empty_point_rdd.analyze()
        assert empty_point_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicates()
        assert empty_point_rdd.boundaryEnvelope == spatial_rdd.boundaryEnvelope

        assert empty_point_rdd.rawSpatialRDD.map(lambda x: x.geom.area).collect()[0] == 0.0
        assert empty_point_rdd.rawSpatialRDD.take(9)[4].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
