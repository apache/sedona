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

from pyspark import StorageLevel

from sedona.core.SpatialRDD import PointRDD, CircleRDD
from tests.test_base import TestBase
from tests.properties.point_properties import input_location, offset, splitter, num_partitions


class TestCircleRDD(TestBase):

    def test_circle_rdd(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )

        circle_rdd = CircleRDD(spatial_rdd, 0.5)

        circle_rdd.analyze()

        assert circle_rdd.approximateTotalCount == 3000

        assert circle_rdd.rawSpatialRDD.take(1)[0].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
        assert circle_rdd.rawSpatialRDD.take(1)[0].geom.radius == 0.5
