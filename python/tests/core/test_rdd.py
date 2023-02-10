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

import logging

from pyspark import StorageLevel
from shapely.geometry import Point

from sedona.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD
from sedona.core.enums import GridType, FileDataSplitter, IndexType
from sedona.core.enums.join_build_side import JoinBuildSide
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery, KNNQuery, JoinQuery
from sedona.core.spatialOperator.join_params import JoinParams
import os

from tests.properties.polygon_properties import polygon_rdd_input_location, polygon_rdd_start_offset, polygon_rdd_end_offset, \
    polygon_rdd_splitter, polygon_rdd_index_type
from tests.test_base import TestBase
from tests.tools import tests_resource

point_rdd_input_location = os.path.join(tests_resource, "arealm-small.csv")

point_rdd_splitter = FileDataSplitter.CSV

point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

knn_query_point = Point(-84.01, 34.01)

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)

join_query_partitionin_type = GridType.QUADTREE
each_query_loop_times = 1


class TestSpatialRDD(TestBase):

    def test_empty_constructor_test(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd_copy = PointRDD()
        object_rdd_copy.rawJvmSpatialRDD = object_rdd.rawJvmSpatialRDD
        object_rdd_copy.analyze()

    def test_spatial_range_query(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False)

        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, False
            ).count()
            logging.info(result_size)

    def test_range_query_using_index(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, True).count

    def test_knn_query(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        for i in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, False)

    def test_knn_query_with_index(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i  in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, True)

    def test_spaltial_join(self):
        query_window_rdd = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )

        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        for x in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window_rdd, False, True).count

    def test_spatial_join_using_index(self):
        query_window = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(point_rdd_index_type, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window, True, False).count()

    def test_spatial_join_using_index_on_polygons(self):
        query_window = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner())

        query_window.buildIndex(polygon_rdd_index_type, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd,
                query_window,
                True,
                False
            ).count()

    def test_spatial_join_query_using_index_on_polygons(self):
        query_window_rdd = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window_rdd, True, False
            )

    def test_spatial_join_query_and_build_index_on_points_on_the_fly(self):
        query_window = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner())

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd,
                query_window,
                True,
                False
            ).count()

    def test_spatial_join_query_and_build_index_on_polygons_on_the_fly(self):
        query_window_rdd = PolygonRDD(
            self.sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )

        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        for i in range(each_query_loop_times):
            join_params = JoinParams(True, False, polygon_rdd_index_type, JoinBuildSide.LEFT)
            resultSize = JoinQuery.spatialJoin(
                query_window_rdd,
                object_rdd,
                join_params
            ).count()

    def test_distance_join_query(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        query_window_rdd = CircleRDD(object_rdd, 0.1)
        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        for i in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(
                object_rdd,
                query_window_rdd,
                False,
                True).count()

    def test_distance_join_query_using_index(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        query_window_rdd = CircleRDD(object_rdd, 0.1)
        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

        object_rdd.buildIndex(IndexType.RTREE, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(
                object_rdd,
                query_window_rdd,
                True,
                True
            ).count

    def test_crs_transformed_spatial_range_query(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False,
            newLevel=StorageLevel.DISK_ONLY,
            sourceEpsgCRSCode="epsg:4326",
            targetEpsgCode="epsg:3005"
        )
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, False
            )

    def test_crs_transformed_spatial_range_query_using_index(self):
        object_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False,
            newLevel=StorageLevel.DISK_ONLY,
            sourceEpsgCRSCode="epsg:4326",
            targetEpsgCode="epsg:3005"
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd,
                range_query_window,
                False,
                True
            ).count
