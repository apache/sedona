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

from sedona.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD
from sedona.core.enums import GridType
from sedona.core.geom.circle import Circle
from sedona.core.spatialOperator import RangeQuery, KNNQuery, JoinQuery
from tests.properties.crs_transform import *
from tests.properties.polygon_properties import grid_type
from tests.test_base import TestBase
from tests.tools import distance_sorting_functions


class TestCrsTransformation(TestBase):

    def test_spatial_range_query(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            StorageLevel.MEMORY_ONLY,
            "epsg:4326",
            "epsg:3005"
        )

        for i in range(loop_times):
            result_size = RangeQuery.SpatialRangeQuery(spatial_rdd, query_envelope, False, False).count()
            assert result_size == 3127

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_envelope, False, False).take(10)[1].getUserData() is not None

    def test_spatial_range_query_using_index(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            StorageLevel.MEMORY_ONLY,
            "epsg:4326",
            "epsg:3005"
        )
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(loop_times):
            result_size = RangeQuery.SpatialRangeQuery(spatial_rdd, query_envelope, False, False).count()
            assert result_size == 3127

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_envelope, False, False).take(10)[1].getUserData() is not None

    def test_spatial_knn_query(self):
        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )

        for i in range(loop_times):
            result = KNNQuery.SpatialKnnQuery(point_rdd, query_point, top_k, False)
            assert result.__len__() > 0
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )
        point_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(loop_times):
            result = KNNQuery.SpatialKnnQuery(point_rdd, query_point, top_k, False)
            assert result.__len__() > 0
            assert result[0].getUserData() is not None

    def test_spatial_knn_correctness(self):
        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )
        result_no_index = KNNQuery.SpatialKnnQuery(point_rdd, query_point, top_k, False)
        point_rdd.buildIndex(IndexType.RTREE, False)
        result_with_index = KNNQuery.SpatialKnnQuery(point_rdd, query_point, top_k, True)

        sorted_result_no_index = sorted(result_no_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, query_point))

        sorted_result_with_index = sorted(result_with_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, query_point))

        difference = 0
        for x in range(top_k):
            difference += sorted_result_no_index[x].geom.distance(sorted_result_with_index[x].geom)

        assert difference == 0

    def test_spatial_join_query_with_polygon_rdd(self):
        query_rdd = PolygonRDD(
            self.sc, input_location_query_polygon, splitter, True,
            num_partitions, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )

        spatial_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, num_partitions,
            StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )
        spatial_rdd.spatialPartitioning(grid_type)
        query_rdd.spatialPartitioning(spatial_rdd.getPartitioner())

        result = JoinQuery.SpatialJoinQuery(spatial_rdd, query_rdd, False, True).collect()
        assert result[1][0].getUserData() is not None

        for data in result:
            if data[1].__len__() != 0:
                for right_data in data[1]:
                    assert right_data.getUserData() is not None

    def test_spatial_join_query_with_polygon_rdd_using_index(self):
        query_rdd = PolygonRDD(
            self.sc, input_location_query_polygon, splitter, True,
            num_partitions, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )

        spatial_rdd = PointRDD(
            self.sc,
            input_location, offset, splitter, True, num_partitions,
            StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005"
        )

        query_rdd.analyze()
        spatial_rdd.analyze()

        spatial_rdd.spatialPartitioning(grid_type)

        spatial_rdd.buildIndex(IndexType.RTREE, True)

        query_rdd.spatialPartitioning(spatial_rdd.getPartitioner())

        result = JoinQuery.SpatialJoinQuery(spatial_rdd, query_rdd, False, True).collect()

        assert result[1][0].getUserData() is not None

        for data in result:
            if data[1].__len__() != 0:
                for right_data in data[1]:
                    assert right_data.getUserData() is not None
