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

from sedona.core.SpatialRDD import CircleRDD
from sedona.core.enums import GridType, IndexType
from sedona.core.formatMapper import WktReader
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.core.spatialOperator.join_query_raw import JoinQueryRaw
from sedona.core.spatialOperator.range_query_raw import RangeQueryRaw
from tests.test_base import TestBase

import os

from tests.tools import tests_resource
from shapely.wkt import loads

bank_csv_path = os.path.join(tests_resource, "small/points.csv")
areas_csv_path = os.path.join(tests_resource, "small/areas.csv")


class TestOmitPythonJvmSerdeToRDD(TestBase):
    expected_pois_within_areas_ids = [['4', '4'], ['1', '6'], ['2', '1'], ['3', '3'], ['3', '7']]

    def test_spatial_join_to_spatial_rdd(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.spatialJoin(poi_point_rdd, areas_polygon_rdd, JoinParams(considerBoundaryIntersection=True))
        sedona_rdd = jvm_sedona_rdd.to_rdd().collect()
        assert sedona_rdd.__len__() == 5

    def test_distance_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        circle_rdd = CircleRDD(poi_point_rdd, 2.0)

        circle_rdd.analyze()
        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.DistanceJoinQueryFlat(poi_point_rdd, circle_rdd, False, True)

        assert jvm_sedona_rdd.to_rdd().collect().__len__() == 10

    def test_spatial_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.SpatialJoinQueryFlat(
            poi_point_rdd, areas_polygon_rdd, False, True)

        assert jvm_sedona_rdd.to_rdd().collect().__len__() == 5

    def test_range_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)

        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        poi_point_rdd.buildIndex(IndexType.QUADTREE, False)

        result = RangeQueryRaw.SpatialRangeQuery(
            poi_point_rdd, loads("POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))"), True, True
        )

        rdd = result.to_rdd()

        assert rdd.collect().__len__() == 4
