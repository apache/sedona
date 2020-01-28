import pytest
import os

from geo_pyspark.core.enums import FileDataSplitter, GridType, IndexType
from geo_pyspark.core.enums.join_build_side import JoinBuildSide
from geo_pyspark.core.spatialOperator import JoinQuery
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from tests.spatial_operator.test_join_base import TestJoinBase
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/arealm-small.csv")
input_location_query_window = os.path.join(tests_path, "resources/zcta510-small.csv")
offset=1
splitter=FileDataSplitter.CSV
numPartitions=11
distance=0.01
query_polygon_set = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
inputCount=3000
inputBoundary=-173.120769, -84.965961, 30.244859, 71.355134
rectangle_match_count=103
rectangle_match_with_original_duplicates_count=103
polygon_match_count = 472
polygon_match_with_original_duplicates_count = 562


def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


parameters = [
    dict(num_partitions=11, use_legacy_apis=True, grid_type=GridType.RTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.RTREE),
    dict(num_partitions=11, use_legacy_apis=True, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.KDBTREE),
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

    def test_nested_loop_with_rectangles(self, num_partitions, grid_type, use_legacy_apis):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.nested_loop(query_rdd, num_partitions, grid_type, use_legacy_apis, rectangle_match_count)

    def test_nested_loop_with_polygons(self, num_partitions, grid_type, use_legacy_apis):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        self.nested_loop(query_rdd, num_partitions, grid_type, use_legacy_apis, polygon_match_count)

    def nested_loop(self, query_rdd, num_partitions, grid_type, use_legacy_apis, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(
            query_rdd, spatial_rdd, grid_type, use_legacy_apis)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        assert expected_count == self.count_join_results(result)

    def test_rtree_with_rectangles(self, num_partitions, use_legacy_apis, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.RTREE, polygon_match_count

        )

    def test_r_tree_with_polygons(self, num_partitions, use_legacy_apis, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.RTREE, polygon_match_count

        )

    def test_quad_tree_with_rectangles(self, num_partitions, use_legacy_apis, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.QUADTREE, polygon_match_count

        )

    def test_quad_tree_with_polygons(self, num_partitions, use_legacy_apis, grid_type):
        query_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        self.index_int(
            query_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.QUADTREE, polygon_match_count

        )

    def index_int(self, query_rdd, num_partitions, use_legacy_apis, grid_type, index_type, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type, use_legacy_apis)
        spatial_rdd.buildIndex(index_type, True)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        assert expected_count, self.count_join_results(result)

    def test_dynamic_r_tree_with_rectangles(self, grid_type, num_partitions, use_legacy_apis):
        polygon_rdd = self.create_rectangle_rdd(input_location_query_window, splitter, num_partitions)
        expected_count = rectangle_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(grid_type) \
            else rectangle_match_count
        self.dynamic_rtree_int(polygon_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.RTREE, expected_count)

    def test_dynamic_r_tree_with_polygons(self, grid_type, num_partitions, use_legacy_apis):
        polygon_rdd = self.create_polygon_rdd(query_polygon_set, splitter, num_partitions)
        expected_count = polygon_match_with_original_duplicates_count if self.expect_to_preserve_original_duplicates(grid_type) \
            else polygon_match_count
        self.dynamic_rtree_int(polygon_rdd, num_partitions, use_legacy_apis, grid_type, IndexType.RTREE, expected_count)

    def dynamic_rtree_int(self, query_rdd, num_partitions, use_legacy_apis, grid_type, index_type, expected_count):
        spatial_rdd = self.create_point_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type, use_legacy_apis)
        join_params = JoinParams(True, index_type, JoinBuildSide.LEFT)
        results = JoinQuery.spatialJoin(query_rdd, spatial_rdd, join_params).collect()

        self.sanity_check_flat_join_results(results)

        assert expected_count == results.__len__()
