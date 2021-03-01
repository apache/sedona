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

import pytest
import os

from sedona.core.enums import FileDataSplitter, GridType, IndexType
from sedona.core.enums.join_build_side import JoinBuildSide
from sedona.core.spatialOperator import JoinQuery
from sedona.core.spatialOperator.join_params import JoinParams
from tests.spatial_operator.test_join_base import TestJoinBase
from tests.tools import tests_resource

input_location = os.path.join(tests_resource, "arealm-small.csv")
input_location_query_window = os.path.join(tests_resource, "zcta510-small.csv")
offset = 1
splitter = FileDataSplitter.CSV
numPartitions = 11
distance = 0.01
query_polygon_set = os.path.join(tests_resource, "primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = -173.120769, -84.965961, 30.244859, 71.355134
rectangle_match_count = 103
rectangle_match_with_original_duplicates_count = 103
polygon_match_count = 472
polygon_match_with_original_duplicates_count = 562


def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


parameters = [
    dict(num_partitions=11, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, grid_type=GridType.KDBTREE),
]


class TestRectangleJoin(TestJoinBase):
    params = {
        "test_nested_loop_with_rectangles": parameters,
        "test_nested_loop_with_polygons": parameters,
        "test_index_int": parameters,
        "test_rtree_with_rectangles": parameters,
        "test_r_tree_with_polygons": parameters,
        "test_quad_tree_with_rectangles": parameters,
        "test_quad_tree_with_polygons": parameters,
        "test_dynamic_r_tree_with_rectangles": parameters,
        "test_dynamic_r_tree_with_polygons": parameters
    }

    def test_nested_loop_with_rectangles(self, num_partitions, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.nested_loop(query_rdd, num_partitions, grid_type, rectangle_match_count)

    def test_nested_loop_with_polygons(self, num_partitions, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        expected_count = polygon_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) else polygon_match_count
        self.nested_loop(query_rdd, num_partitions, grid_type, expected_count)

    def nested_loop(self, query_rdd, num_partitions, grid_type, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(
            query_rdd, spatial_rdd, grid_type)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        assert expected_count == self.count_join_results(result)

    def test_rtree_with_rectangles(self, num_partitions, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, grid_type, IndexType.RTREE, polygon_match_count

        )

    def test_r_tree_with_polygons(self, num_partitions, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        expected_count = polygon_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) else polygon_match_count
        self.index_int(
            query_rdd, num_partitions, grid_type, IndexType.RTREE, expected_count

        )

    def test_quad_tree_with_rectangles(self, num_partitions, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, grid_type, IndexType.QUADTREE, polygon_match_count

        )

    def test_quad_tree_with_polygons(self, num_partitions, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        expected_count = polygon_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) else polygon_match_count
        self.index_int(
            query_rdd, num_partitions, grid_type, IndexType.QUADTREE, expected_count

        )

    def index_int(self, query_rdd, num_partitions, grid_type, index_type, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)
        spatial_rdd.buildIndex(index_type, True)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        assert expected_count, self.count_join_results(result)

    def test_dynamic_r_tree_with_rectangles(self, grid_type, num_partitions):
        polygon_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        expected_count = rectangle_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) \
            else rectangle_match_count
        self.dynamic_rtree_int(polygon_rdd, num_partitions, grid_type, IndexType.RTREE, expected_count)

    def test_dynamic_r_tree_with_polygons(self, grid_type, num_partitions):
        polygon_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        expected_count = polygon_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) \
            else polygon_match_count
        self.dynamic_rtree_int(polygon_rdd, num_partitions, grid_type, IndexType.RTREE, expected_count)

    def dynamic_rtree_int(self, query_rdd, num_partitions, grid_type, index_type, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)
        join_params = JoinParams(True, True, index_type, JoinBuildSide.LEFT)
        results = JoinQuery.spatialJoin(query_rdd, spatial_rdd, join_params).collect()

        self.sanity_check_flat_join_results(results)

        assert expected_count == results.__len__()
