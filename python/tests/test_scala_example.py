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

from shapely.geometry import Point
from pyspark import StorageLevel

from sedona.core.SpatialRDD import PointRDD, CircleRDD, PolygonRDD
from sedona.core.enums import FileDataSplitter, IndexType, GridType
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery, JoinQuery, KNNQuery
from tests.test_base import TestBase
from tests.tools import tests_resource

point_rdd_input_location = os.path.join(tests_resource, "arealm-small.csv")

point_rdd_splitter = FileDataSplitter.CSV
point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

polygon_rdd_input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9

knn_query_point = Point(-84.01, 34.01)
range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
join_query_partitioning_type = GridType.QUADTREE
each_query_loop_times = 20

shape_file_input_location = os.path.join(tests_resource, "shapefiles/polygon")


class TestScalaExample(TestBase):

    def test_spatial_range_query(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.rawJvmSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
        for _ in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(object_rdd, range_query_window, False, False).count()

        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.rawJvmSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
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
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.rawJvmSpatialRDD.persist(StorageLevel.MEMORY_ONLY)

        for _ in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, False)

    def test_spatial_knn_query_using_index(self):
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        object_rdd.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

        for _ in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, True)

    def test_spatial_join_query(self):
        query_window_rdd = PolygonRDD(
            self.sc, polygon_rdd_input_location, polygon_rdd_start_offset, polygon_rdd_end_offset,
            polygon_rdd_splitter, True
        )
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY)

        object_rdd.spatialPartitioning(join_query_partitioning_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.jvmSpatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.jvmSpatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

        for _ in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(object_rdd, query_window_rdd, False, True).count()

    def test_spatial_join_using_index(self):
        query_window_rdd = PolygonRDD(
            self.sc, polygon_rdd_input_location, polygon_rdd_start_offset,
            polygon_rdd_end_offset, polygon_rdd_splitter, True
        )
        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True, StorageLevel.MEMORY_ONLY
        )

        object_rdd.spatialPartitioning(join_query_partitioning_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(point_rdd_index_type, True)

        object_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.jvmSpatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

        for _ in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window_rdd, True, False
            ).count()

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
        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        object_rdd.buildIndex(IndexType.QUADTREE, True)

        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, False)

        object_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.jvmSpatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
        query_window_rdd.jvmSpatialPartitionedRDD.count()
        object_rdd.indexedRDD.count()

        import time

        start = time.time()
        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, True, True).count()
        diff = time.time() - start

        object_rdd = PointRDD(
            self.sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, True)
        query_window_rdd = CircleRDD(object_rdd, 0.1)

        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        object_rdd.buildIndex(IndexType.QUADTREE, True)

        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, False)

        start1 = time.time()
        for _ in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(object_rdd, query_window_rdd, True, True).count()
