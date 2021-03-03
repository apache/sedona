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

input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
query_window_set = os.path.join(tests_resource, "zcta510-small.csv")
query_polygon_set = os.path.join(tests_resource, "primaryroads-polygon.csv")
splitter = FileDataSplitter.CSV
contains_match_count = 6941
contains_match_with_original_duplicates = 9334
intersects_match_count = 24323
intersects_match_count_with_original_duplicates = 32726


def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


parameters = [
    dict(num_partitions=11, grid_type=GridType.QUADTREE, intersects=False),
    dict(num_partitions=11, grid_type=GridType.QUADTREE, intersects=False),
    dict(num_partitions=11, grid_type=GridType.QUADTREE, intersects=True),
    dict(num_partitions=11, grid_type=GridType.QUADTREE, intersects=True),
    dict(num_partitions=11, grid_type=GridType.KDBTREE, intersects=True)
]
params_dyn = [{**param, **{"index_type": IndexType.QUADTREE}} for param in parameters]
params_dyn.extend([{**param, **{"index_type": IndexType.RTREE}} for param in parameters])


class TestRectangleJoin(TestJoinBase):
    params = {
        "test_nested_loop": parameters,
        "test_dynamic_index_int": params_dyn,
        "test_index_int": params_dyn
    }

    def test_nested_loop(self, num_partitions, grid_type, intersects):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_polygon_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, intersects).collect()

        self.sanity_check_join_results(result)
        assert self.get_expected_with_original_duplicates_count(intersects) == self.count_join_results(result)

    def test_dynamic_index_int(self, num_partitions, grid_type, index_type, intersects):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_polygon_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)

        join_params = JoinParams(True, intersects, index_type, JoinBuildSide.LEFT)
        result = JoinQuery.spatialJoin(query_rdd, spatial_rdd, join_params).collect()

        self.sanity_check_flat_join_results(result)

        expected_count = self.get_expected_with_original_duplicates_count(intersects) \
            if self.expect_to_preserve_original_duplicates(grid_type) else self.get_expected_count(intersects)
        assert expected_count == result.__len__()

    def test_index_int(self, num_partitions, grid_type, index_type, intersects):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_polygon_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)
        spatial_rdd.buildIndex(index_type, True)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, True, intersects).collect()

        self.sanity_check_join_results(result)
        assert self.get_expected_with_original_duplicates_count(intersects) == self.count_join_results(result)

    def get_expected_count(self, intersects):
        return intersects_match_count if intersects else contains_match_count

    def get_expected_with_original_duplicates_count(self, intersects):
        return intersects_match_count_with_original_duplicates if intersects else contains_match_with_original_duplicates
