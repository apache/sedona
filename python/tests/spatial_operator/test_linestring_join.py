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

input_location = os.path.join(tests_resource, "primaryroads-linestring.csv")
query_window_set = os.path.join(tests_resource, "zcta510-small.csv")
splitter = FileDataSplitter.CSV
query_polygon_set = os.path.join(tests_resource, "primaryroads-polygon.csv")
match_count = 535
match_with_original_duplicates_count = 875


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
params_dyn = [{**param, **{"index_type": IndexType.QUADTREE}} for param in parameters]
params_dyn.extend([{**param, **{"index_type": IndexType.RTREE}} for param in parameters])


class TestRectangleJoin(TestJoinBase):
    params = {
        "test_nested_loop": parameters,
        "test_dynamic_index_int": params_dyn,
        "test_index_int": params_dyn
    }

    def test_nested_loop(self, num_partitions, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_linestring_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        expected_count = match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) else match_count

        assert expected_count == self.count_join_results(result)

    def test_dynamic_index_int(self, num_partitions, grid_type, index_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_linestring_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)

        join_params = JoinParams(True, True, index_type, JoinBuildSide.LEFT)
        result = JoinQuery.spatialJoin(query_rdd, spatial_rdd, join_params).collect()

        self.sanity_check_flat_join_results(result)

        expected_count = match_with_original_duplicates_count \
            if self.expect_to_preserve_original_duplicates(grid_type) else match_count

        assert expected_count == result.__len__()

    def test_index_int(self, num_partitions, grid_type, index_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        spatial_rdd = self.create_linestring_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type)
        spatial_rdd.buildIndex(index_type, True)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        expected_count = match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(
            grid_type) else match_count
        assert expected_count == self.count_join_results(result)
