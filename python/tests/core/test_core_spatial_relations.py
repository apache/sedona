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

from sedona.core.SpatialRDD import PointRDD, PolygonRDD
from sedona.core.enums import FileDataSplitter, GridType
from sedona.core.spatialOperator import JoinQuery
from tests.test_base import TestBase
from tests.tools import tests_path

point_path = os.path.join(tests_path, "resources/points.csv")
counties_path = os.path.join(tests_path, "resources/counties_tsv.csv")


class TestJoinQuery(TestBase):

    def test_spatial_join_query(self):
        point_rdd = PointRDD(
            self.sc,
            point_path,
            4,
            FileDataSplitter.WKT,
            True
        )

        polygon_rdd = PolygonRDD(
            self.sc,
            counties_path,
            2,
            3,
            FileDataSplitter.WKT,
            True
        )

        point_rdd.analyze()
        point_rdd.spatialPartitioning(GridType.KDBTREE, num_partitions=10)
        polygon_rdd.spatialPartitioning(point_rdd.getPartitioner())
        result = JoinQuery.SpatialJoinQuery(
            point_rdd,
            polygon_rdd,
            True,
            False
        )

        assert result.count() == 26
