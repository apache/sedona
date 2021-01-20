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

from sedona.core.enums import FileDataSplitter
from sedona.core.SpatialRDD import PointRDD
from sedona.core.SpatialRDD import PolygonRDD
from tests.test_base import TestBase
from tests.tools import tests_resource

point_path = os.path.join(tests_resource, "points.csv")
counties_path = os.path.join(tests_resource, "county_small.tsv")


class TestSpatialRDD(TestBase):

    def test_creating_point_rdd(self):
        point_rdd = PointRDD(
            self.spark._sc,
            point_path,
            4,
            FileDataSplitter.WKT,
            True
        )

        point_rdd.analyze()
        cnt = point_rdd.countWithoutDuplicates()
        assert cnt == 12796, f"Point RDD should have 12796 but found {cnt}"

    def test_creating_polygon_rdd(self):
        polygon_rdd = PolygonRDD(
            self.spark._sc,
            counties_path,
            0,
            1,
            FileDataSplitter.WKT,
            True
        )

        polygon_rdd.analyze()

        cnt = polygon_rdd.countWithoutDuplicates()

        assert cnt == 103, f"Polygon RDD should have 103 but found {cnt}"
