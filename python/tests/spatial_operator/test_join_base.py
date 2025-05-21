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

from tests.test_base import TestBase

from sedona.spark.core.enums import GridType
from sedona.spark.core.SpatialRDD import (
    LineStringRDD,
    PointRDD,
    PolygonRDD,
    RectangleRDD,
)
from sedona.spark.core.SpatialRDD.spatial_rdd import SpatialRDD


class TestJoinBase(TestBase):

    use_legacy_apis = False

    def create_point_rdd(self, location, splitter, num_partitions):
        return PointRDD(self.sc, location, 1, splitter, False, num_partitions)

    def create_linestring_rdd(self, location, splitter, num_partitions):
        return LineStringRDD(self.sc, location, splitter, True, num_partitions)

    def create_polygon_rdd(self, location, splitter, num_partitions):
        return PolygonRDD(self.sc, location, splitter, True, num_partitions)

    def create_rectangle_rdd(self, location, splitter, num_partitions):
        rectangle_rdd = RectangleRDD(self.sc, location, splitter, True, num_partitions)
        rectangle_rdd.analyze()
        return rectangle_rdd

    def partition_rdds(self, query_rdd: SpatialRDD, spatial_rdd: SpatialRDD, grid_type):
        spatial_rdd.spatialPartitioning(grid_type)
        return query_rdd.spatialPartitioning(spatial_rdd.getPartitioner())

    def expect_to_preserve_original_duplicates(self, grid_type):
        return grid_type == GridType.QUADTREE or grid_type == GridType.KDBTREE

    def count_join_results(self, results):
        count = 0
        for row_data in results:
            joined_data = row_data[1]
            count += joined_data.__len__()
        return count

    def sanity_check_join_results(self, results):
        for raw_data in results:
            assert raw_data[1].__len__()
            for geo_data in raw_data[1]:
                assert raw_data[0].geom.intersects(geo_data.geom)

    def sanity_check_flat_join_results(self, results):
        for row_data in results:
            assert row_data[0].geom.intersects(row_data[1].geom)
