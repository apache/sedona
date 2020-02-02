import os

from shapely.geometry import Point
from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import PointRDD, CircleRDD
from geo_pyspark.core.enums import FileDataSplitter, IndexType, GridType
from geo_pyspark.core.geom.envelope import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery, JoinQuery
from tests.test_base import TestBase
from tests.tools import tests_path

point_rdd_input_location = os.path.join(tests_path, "resources/arealm-small.csv")

point_rdd_splitter = FileDataSplitter.CSV
point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

polygon_rdd_input_location = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 1
polygon_rdd_end_offset = 5

knn_query_point = Point(-84.01, 34.01)
range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
join_query_partitioning_type = GridType.QUADTREE
each_query_loop_times = 20

shape_file_input_location = os.path.join(tests_path, "resources/shapefiles/polygon")


class TestScalaExample(TestBase):

    def test_spatial_range_query(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
        assert object_rdd.rawSpatialRDD.is_cached
        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, False).count()

        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, False).count()

    def test_spatial_range_query_using_index(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY)
        object_rdd.buildIndex(point_rdd_index_type, False)
        object_rdd.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
        assert object_rdd.indexedRawRDD.is_cached

        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, True).count

    def test_spatial_knn_query(self):
        pass

    def test_spatial_knn_query_using_index(self):
        pass

    def test_spatial_join_query(self):
        pass

    def test_spatial_join_using_index(self):
        pass

    def test_distance_join_query(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY)
        query_window_rdd = CircleRDD(object_rdd, 0.1)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
        assert object_rdd.spatialPartitionedRDD.is_cached

        query_window_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, False, True).count()

    def test_distance_join_using_index(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY)

        query_window_rdd = CircleRDD(object_rdd, 0.1)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, True)

        object_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
        assert object_rdd.indexedRDD.is_cached
        assert query_window_rdd.spatialPartitionedRDD.is_cached

        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, True, True).count()

    def test_crs_transformation_spatial_range_query(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY,
                 "epsg:4326", "epsg:3005")

        object_rdd.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
        assert object_rdd.rawSpatialRDD.is_cached
        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, False).count()
            assert result_size > -1

    def test_crs_transformation_spatial_range_query_using_index(self):
        object_rdd = PointRDD(self.sc, point_rdd_input_location, point_rdd_offset,
                             point_rdd_splitter, True, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005")
        object_rdd.buildIndex(point_rdd_index_type, False)
        object_rdd.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, True).count()
            assert result_size > -1

    def test_indexed_rdd_assignment(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True)
        query_window_rdd = CircleRDD(object_rdd, 0.1)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        object_rdd.buildIndex(IndexType.QUADTREE, True)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, False)

        object_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
        # object_rdd.indexedRDD.count()

        import time

        start = time.time()
        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, True, True).count()
        diff = time.time() - start

        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True)
        query_window_rdd = CircleRDD(object_rdd, 0.1)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        object_rdd.buildIndex(IndexType.QUADTREE, True)

        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, False)

        start1 = time.time()
        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, True, True).count()
        diff1 = time.time() - start1
        assert diff < diff1
